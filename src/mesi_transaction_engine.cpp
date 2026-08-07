#include "mesi_transaction_engine.h"

#include <algorithm>
#include <condition_variable>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace cxlmemsim::mesi_v2 {

namespace {

enum class PendingPhase : std::uint8_t { Open, Completed, Committing, TimedOut, Disconnected, SendFailed };

std::uint64_t holder(std::uint16_t host_id) noexcept { return std::uint64_t{1} << host_id; }

struct ExpectedAck {
    std::uint16_t host_id{};
    std::uint64_t session_id{};
    std::uint64_t snoop_id{};
    MesiState prior_state{MesiState::I};
    protocol_v2::CoherenceFrame snoop{};
    bool dispatched{};
    bool acknowledged{};
    bool persistence_retry_requested{};
    bool persistence_attempted{};
    std::optional<std::array<std::byte, 64>> returned_data;
};

TransitionResult unchanged(TransitionStatus status, const DirectorySnapshot &snapshot) { return {status, snapshot}; }

} // namespace

struct TransactionDependencies {
    CoherenceMemoryBackend *memory{};
    CoherenceTransport *transport{};
    MesiTransactionEngine::Duration snoop_timeout{};
};

struct PendingTransaction {
    mutable std::mutex mutex;
    std::condition_variable changed;
    PendingPhase phase{PendingPhase::Open};
    MesiTransactionEngine::Operation operation{MesiTransactionEngine::Operation::Gets};
    TransactionRequest requester;
    std::uint64_t line_address{};
    std::uint64_t starting_epoch{};
    std::uint64_t target_epoch{};
    MesiTransactionEngine::TimePoint deadline;
    DirectorySnapshot starting_snapshot;
    std::shared_ptr<const TransactionDependencies> dependencies;
    std::optional<std::array<std::byte, 64>> prefetched_data;
    std::vector<ExpectedAck> expected;
    std::size_t acknowledged_count{};
    bool timeout_requested{};
    bool disconnect_requested{};
    bool send_failed_requested{};
};

MesiTransactionEngine::MesiTransactionEngine(MesiDirectory &directory, Duration snoop_timeout,
                                             std::uint64_t first_snoop_id)
    : directory_(directory), next_snoop_id_(first_snoop_id) {
    if (snoop_timeout <= Duration::zero())
        snoop_timeout = std::chrono::nanoseconds(1);
    dependencies_ = std::make_shared<TransactionDependencies>(TransactionDependencies{nullptr, nullptr, snoop_timeout});
}

MesiTransactionEngine::MesiTransactionEngine(MesiDirectory &directory, CoherenceMemoryBackend &memory,
                                             CoherenceTransport &transport, Duration snoop_timeout,
                                             std::uint64_t first_snoop_id)
    : MesiTransactionEngine(directory, snoop_timeout, first_snoop_id) {
    configure(memory, transport, snoop_timeout);
}

MesiTransactionEngine::~MesiTransactionEngine() = default;

void MesiTransactionEngine::configure(CoherenceMemoryBackend &memory, CoherenceTransport &transport,
                                      Duration snoop_timeout) {
    if (snoop_timeout <= Duration::zero())
        snoop_timeout = std::chrono::nanoseconds(1);
    auto dependencies =
        std::make_shared<TransactionDependencies>(TransactionDependencies{&memory, &transport, snoop_timeout});
    std::lock_guard lock(dependencies_mutex_);
    dependencies_ = std::move(dependencies);
}

std::shared_ptr<const TransactionDependencies> MesiTransactionEngine::dependencySnapshot() const {
    std::lock_guard lock(dependencies_mutex_);
    return dependencies_;
}

bool MesiTransactionEngine::bindSession(std::uint16_t host_id, std::uint64_t session_id) {
    if (host_id >= MesiDirectory::kMaximumHosts || session_id == 0)
        return false;
    std::lock_guard lock(sessions_mutex_);
    sessions_[host_id] = session_id;
    return true;
}

std::uint64_t MesiTransactionEngine::sessionFor(std::uint16_t host_id) const {
    std::lock_guard lock(sessions_mutex_);
    const auto found = sessions_.find(host_id);
    return found == sessions_.end() ? 0 : found->second;
}

protocol_v2::Status MesiTransactionEngine::validateRequest(const TransactionRequest &request) const {
    if (request.host_id >= MesiDirectory::kMaximumHosts)
        return protocol_v2::Status::InvalidState;
    if (request.session_id == 0)
        return protocol_v2::Status::Ok;
    return sessionFor(request.host_id) == request.session_id ? protocol_v2::Status::Ok
                                                             : protocol_v2::Status::StaleSession;
}

protocol_v2::Status MesiTransactionEngine::statusFor(TransitionStatus status) noexcept {
    switch (status) {
    case TransitionStatus::Committed:
    case TransitionStatus::NoChange:
        return protocol_v2::Status::Ok;
    case TransitionStatus::StaleMetadata:
        return protocol_v2::Status::StaleEpoch;
    case TransitionStatus::UnalignedAddress:
    case TransitionStatus::InvalidHost:
    case TransitionStatus::InvalidState:
        return protocol_v2::Status::InvalidState;
    }
    return protocol_v2::Status::InvalidState;
}

TransactionResult MesiTransactionEngine::gets(std::uint64_t line_address, TransactionRequest request) {
    return acquire(line_address, request, Operation::Gets);
}

TransactionResult MesiTransactionEngine::getm(std::uint64_t line_address, TransactionRequest request) {
    return acquire(line_address, request, Operation::Getm);
}

TransactionResult MesiTransactionEngine::upgrade(std::uint64_t line_address, TransactionRequest request) {
    return acquire(line_address, request, Operation::Upgrade);
}

TransitionResult MesiTransactionEngine::puts(std::uint64_t line_address, std::uint16_t requester) {
    return directory_.puts(line_address, requester);
}

TransitionResult MesiTransactionEngine::putm(std::uint64_t line_address, std::uint16_t requester) {
    return directory_.putm(line_address, requester);
}

TransactionResult MesiTransactionEngine::acquire(std::uint64_t line_address, TransactionRequest request,
                                                 Operation operation) {
    if (const auto request_status = validateRequest(request); request_status != protocol_v2::Status::Ok)
        return {request_status, unchanged(TransitionStatus::InvalidHost, {}), false, {}};

    const auto dependencies = dependencySnapshot();
    auto locked = directory_.lockLine(line_address);
    if (!locked)
        return {protocol_v2::Status::InvalidState, unchanged(TransitionStatus::UnalignedAddress, {}), false, {}};
    const auto current = locked->snapshot();
    if (locked->pendingTransaction())
        return {protocol_v2::Status::InvalidState, unchanged(TransitionStatus::InvalidState, current), false, {}};
    if ((operation == Operation::Getm && current.state == MesiState::S &&
         (current.sharers & holder(request.host_id)) != 0) ||
        (operation == Operation::Upgrade &&
         !((current.state == MesiState::E && current.owner == request.host_id) ||
           (current.state == MesiState::S && (current.sharers & holder(request.host_id)) != 0)))) {
        return {protocol_v2::Status::InvalidState, unchanged(TransitionStatus::InvalidState, current), false, {}};
    }

    bool needs_snoops = false;
    switch (operation) {
    case Operation::Gets:
        needs_snoops =
            (current.state == MesiState::E || current.state == MesiState::M) && current.owner != request.host_id;
        break;
    case Operation::Getm:
        needs_snoops =
            current.state == MesiState::S ||
            ((current.state == MesiState::E || current.state == MesiState::M) && current.owner != request.host_id);
        break;
    case Operation::Upgrade:
        needs_snoops = current.state == MesiState::S && (current.sharers & ~holder(request.host_id)) != 0;
        break;
    }
    return needs_snoops ? transact(*locked, request, operation, current, dependencies)
                        : direct(*locked, request, operation, current, dependencies);
}

TransactionResult MesiTransactionEngine::direct(MesiDirectory::LockedLine &line, TransactionRequest request,
                                                Operation operation, const DirectorySnapshot &current,
                                                const std::shared_ptr<const TransactionDependencies> &dependencies) {
    DirectorySnapshot next = current;
    bool valid = false;
    switch (operation) {
    case Operation::Gets:
        if (current.state == MesiState::I) {
            next = {MesiState::E, request.host_id, 0, current.epoch, true};
            valid = true;
        } else if (current.state == MesiState::S) {
            next = {MesiState::S, std::nullopt, current.sharers | holder(request.host_id), current.epoch, true};
            valid = true;
        }
        break;
    case Operation::Getm:
        if (current.state == MesiState::I) {
            next = {MesiState::M, request.host_id, 0, current.epoch, false};
            valid = true;
        }
        break;
    case Operation::Upgrade:
        if ((current.state == MesiState::E && current.owner == request.host_id) ||
            (current.state == MesiState::S && (current.sharers & holder(request.host_id)) != 0)) {
            next = {MesiState::M, request.host_id, 0, current.epoch, false};
            valid = true;
        }
        break;
    }

    if (!valid)
        return {protocol_v2::Status::InvalidState, unchanged(TransitionStatus::InvalidState, current), false, {}};

    auto data = std::array<std::byte, 64>{};
    if (operation != Operation::Upgrade) {
        if (dependencies->memory == nullptr && request.session_id != 0)
            return {protocol_v2::Status::IoError, unchanged(TransitionStatus::InvalidState, current), false, {}};
        if (dependencies->memory != nullptr) {
            try {
                data = dependencies->memory->readLine(line.lineAddress());
            } catch (...) {
                return {protocol_v2::Status::IoError, unchanged(TransitionStatus::InvalidState, current), false, {}};
            }
        }
    }

    std::unique_lock<std::mutex> session_lock;
    if (request.session_id != 0) {
        session_lock = std::unique_lock(sessions_mutex_);
        const auto found = sessions_.find(request.host_id);
        if (found == sessions_.end() || found->second != request.session_id) {
            return {protocol_v2::Status::HostFenced, unchanged(TransitionStatus::InvalidState, current), false, {}};
        }
    }

    TransitionResult transition{TransitionStatus::InvalidState, current};
    switch (operation) {
    case Operation::Gets:
        transition = line.commitGets(request.host_id, current, next);
        break;
    case Operation::Getm:
        transition = line.commitGetm(request.host_id, current, next);
        break;
    case Operation::Upgrade:
        transition = line.commitUpgrade(request.host_id, current, next);
        break;
    }
    const auto status = statusFor(transition.status);
    const bool granted = status == protocol_v2::Status::Ok;
    return {status, transition, granted, data};
}

std::optional<std::uint64_t> MesiTransactionEngine::allocateSnoopId() noexcept {
    auto current = next_snoop_id_.load(std::memory_order_relaxed);
    while (current != 0) {
        const auto next = current == std::numeric_limits<std::uint64_t>::max() ? 0 : current + 1;
        if (next_snoop_id_.compare_exchange_weak(current, next, std::memory_order_relaxed, std::memory_order_relaxed))
            return current;
    }
    return std::nullopt;
}

void MesiTransactionEngine::registerPending(const std::shared_ptr<PendingTransaction> &pending) {
    std::lock_guard lock(active_mutex_);
    active_by_line_[pending->line_address] = pending;
    for (const auto &expected : pending->expected)
        active_by_snoop_[expected.snoop_id] = pending;
}

void MesiTransactionEngine::unregisterPending(const std::shared_ptr<PendingTransaction> &pending) {
    std::lock_guard lock(active_mutex_);
    for (const auto &expected : pending->expected) {
        const auto found = active_by_snoop_.find(expected.snoop_id);
        if (found != active_by_snoop_.end() && found->second.lock() == pending)
            active_by_snoop_.erase(found);
    }
    const auto found = active_by_line_.find(pending->line_address);
    if (found != active_by_line_.end() && found->second.lock() == pending)
        active_by_line_.erase(found);
}

bool MesiTransactionEngine::pendingSessionsCurrent(const PendingTransaction &pending) const {
    std::lock_guard lock(sessions_mutex_);
    const auto matches = [&](std::uint16_t host_id, std::uint64_t session_id) {
        const auto found = sessions_.find(host_id);
        return found != sessions_.end() && found->second == session_id;
    };
    if (pending.requester.session_id != 0 && !matches(pending.requester.host_id, pending.requester.session_id))
        return false;
    return std::all_of(pending.expected.begin(), pending.expected.end(),
                       [&](const ExpectedAck &expected) { return matches(expected.host_id, expected.session_id); });
}

TransactionResult MesiTransactionEngine::transact(MesiDirectory::LockedLine &line, TransactionRequest request,
                                                  Operation operation, const DirectorySnapshot &current,
                                                  const std::shared_ptr<const TransactionDependencies> &dependencies) {
    auto *transport = dependencies->transport;
    if (transport == nullptr)
        return {protocol_v2::Status::HostFenced, unchanged(TransitionStatus::InvalidState, current), false, {}};
    if (current.state == MesiState::M && dependencies->memory == nullptr)
        return {protocol_v2::Status::IoError, unchanged(TransitionStatus::InvalidState, current), false, {}};

    std::optional<std::array<std::byte, 64>> prefetched_data;
    if (current.server_copy_current && operation != Operation::Upgrade) {
        if (dependencies->memory == nullptr)
            return {protocol_v2::Status::IoError, unchanged(TransitionStatus::InvalidState, current), false, {}};
        try {
            prefetched_data = dependencies->memory->readLine(line.lineAddress());
        } catch (...) {
            return {protocol_v2::Status::IoError, unchanged(TransitionStatus::InvalidState, current), false, {}};
        }
    }

    auto pending = std::make_shared<PendingTransaction>();
    pending->operation = operation;
    pending->requester = request;
    pending->line_address = line.lineAddress();
    pending->starting_epoch = current.epoch;
    pending->target_epoch = current.epoch + 1;
    pending->starting_snapshot = current;
    pending->dependencies = dependencies;
    pending->prefetched_data = prefetched_data;
    pending->deadline = Clock::now() + dependencies->snoop_timeout;

    bool snoop_ids_exhausted = false;
    auto add_snoop = [&](std::uint16_t host_id, MesiState prior_state, protocol_v2::Opcode opcode) {
        const auto session_id = sessionFor(host_id);
        if (session_id == 0)
            return false;
        const auto snoop_id = allocateSnoopId();
        if (!snoop_id) {
            snoop_ids_exhausted = true;
            return false;
        }
        auto snoop = protocol_v2::initializeFrame(opcode);
        protocol_v2::setSrcHost(snoop, protocol_v2::kServerHost);
        protocol_v2::setDstHost(snoop, host_id);
        protocol_v2::setSessionId(snoop, session_id);
        protocol_v2::setSnoopId(snoop, *snoop_id);
        protocol_v2::setAddress(snoop, pending->line_address);
        protocol_v2::setEpoch(snoop, pending->starting_epoch);
        pending->expected.push_back({host_id, session_id, protocol_v2::snoopId(snoop), prior_state, snoop});
        return true;
    };

    bool sessions_available = true;
    if (operation == Operation::Gets) {
        const auto opcode =
            current.state == MesiState::M ? protocol_v2::Opcode::SnpDataDowngrade : protocol_v2::Opcode::SnpDowngrade;
        sessions_available = current.owner && add_snoop(*current.owner, current.state, opcode);
    } else if (current.state == MesiState::S) {
        for (std::uint16_t host_id = 0; host_id < MesiDirectory::kMaximumHosts; ++host_id) {
            if ((current.sharers & holder(host_id)) != 0 &&
                !(operation == Operation::Upgrade && host_id == request.host_id)) {
                sessions_available =
                    add_snoop(host_id, MesiState::S, protocol_v2::Opcode::SnpInv) && sessions_available;
            }
        }
    } else {
        const auto opcode =
            current.state == MesiState::M ? protocol_v2::Opcode::SnpDataInv : protocol_v2::Opcode::SnpInv;
        sessions_available = current.owner && add_snoop(*current.owner, current.state, opcode);
    }

    if (!sessions_available || pending->expected.empty()) {
        const auto status = snoop_ids_exhausted ? protocol_v2::Status::IoError : protocol_v2::Status::HostFenced;
        return {status, unchanged(TransitionStatus::InvalidState, current), false, {}};
    }

    line.setPendingTransaction(pending);

    auto cleanup = [&] {
        line.setPendingTransaction(nullptr);
        unregisterPending(pending);
    };
    auto request_send_failure = [&] {
        pending->send_failed_requested = true;
        if (pending->phase == PendingPhase::Completed) {
            if (pending->disconnect_requested)
                pending->phase = PendingPhase::Disconnected;
            else if (pending->timeout_requested)
                pending->phase = PendingPhase::TimedOut;
            else
                pending->phase = PendingPhase::SendFailed;
        }
        pending->changed.notify_all();
    };
    auto request_timeout_if_expired = [&] {
        if (Clock::now() < pending->deadline)
            return;
        pending->timeout_requested = true;
        if (pending->phase == PendingPhase::Completed && !pending->disconnect_requested)
            pending->phase = PendingPhase::TimedOut;
        pending->changed.notify_all();
    };

    try {
        registerPending(pending);
        // Registration closes the disconnect scan gap. This exact validation
        // then admits fanout only if both the requester and every snapshotted
        // snoop target still belong to the same session generation.
        if (!pendingSessionsCurrent(*pending)) {
            cleanup();
            return {protocol_v2::Status::HostFenced, unchanged(TransitionStatus::InvalidState, current), false, {}};
        }
        for (auto &expected : pending->expected) {
            {
                std::lock_guard lock(pending->mutex);
                request_timeout_if_expired();
                if (pending->phase != PendingPhase::Open || pending->disconnect_requested ||
                    pending->timeout_requested || pending->send_failed_requested)
                    break;
                expected.dispatched = true;
            }
            bool sent = false;
            try {
                sent = transport->sendToHost(expected.host_id, expected.snoop);
            } catch (...) {
                std::lock_guard lock(pending->mutex);
                request_timeout_if_expired();
                if (pending->phase == PendingPhase::Open || pending->phase == PendingPhase::Completed)
                    request_send_failure();
                break;
            }
            {
                std::lock_guard lock(pending->mutex);
                request_timeout_if_expired();
                if (!sent && (pending->phase == PendingPhase::Open || pending->phase == PendingPhase::Completed))
                    request_send_failure();
                if (pending->phase != PendingPhase::Open || pending->disconnect_requested ||
                    pending->timeout_requested || pending->send_failed_requested)
                    break;
            }
        }

        {
            std::unique_lock lock(pending->mutex);
            while (pending->phase == PendingPhase::Open) {
                if (Clock::now() >= pending->deadline)
                    pending->timeout_requested = true;

                const bool close_requested =
                    pending->disconnect_requested || pending->timeout_requested || pending->send_failed_requested;
                auto persistence =
                    std::find_if(pending->expected.begin(), pending->expected.end(), [&](const ExpectedAck &expected) {
                        return !expected.acknowledged && expected.returned_data &&
                               expected.persistence_retry_requested &&
                               (!close_requested || !expected.persistence_attempted);
                    });
                if (persistence != pending->expected.end()) {
                    persistence->persistence_retry_requested = false;
                    persistence->persistence_attempted = true;
                    const auto data = *persistence->returned_data;
                    auto *memory = pending->dependencies->memory;
                    lock.unlock();
                    bool persisted = false;
                    if (memory != nullptr) {
                        try {
                            memory->writeLine(pending->line_address, data);
                            persisted = true;
                        } catch (...) {
                            // An undurable dirty effect remains unaccepted.
                        }
                    }
                    lock.lock();
                    if (Clock::now() >= pending->deadline)
                        pending->timeout_requested = true;
                    if (persisted) {
                        persistence->acknowledged = true;
                        ++pending->acknowledged_count;
                        persistence->persistence_retry_requested = false;
                    }

                    if (pending->disconnect_requested) {
                        pending->phase = PendingPhase::Disconnected;
                    } else if (pending->timeout_requested) {
                        pending->phase = PendingPhase::TimedOut;
                    } else if (pending->send_failed_requested) {
                        pending->phase = PendingPhase::SendFailed;
                    } else if (persisted && pending->acknowledged_count == pending->expected.size()) {
                        pending->phase = PendingPhase::Completed;
                    }
                    pending->changed.notify_all();
                    continue;
                }

                if (pending->disconnect_requested) {
                    pending->phase = PendingPhase::Disconnected;
                    break;
                }
                if (pending->timeout_requested) {
                    pending->phase = PendingPhase::TimedOut;
                    break;
                }
                if (pending->send_failed_requested) {
                    pending->phase = PendingPhase::SendFailed;
                    break;
                }

                pending->changed.wait_until(lock, pending->deadline, [&] {
                    return pending->phase != PendingPhase::Open || pending->disconnect_requested ||
                           pending->timeout_requested || pending->send_failed_requested ||
                           std::any_of(
                               pending->expected.begin(), pending->expected.end(),
                               [](const ExpectedAck &expected) { return expected.persistence_retry_requested; });
                });
            }
        }

        auto result = reconcile(line, pending);
        cleanup();
        return result;
    } catch (...) {
        cleanup();
        throw;
    }
}

TransactionResult MesiTransactionEngine::reconcile(MesiDirectory::LockedLine &line,
                                                   const std::shared_ptr<PendingTransaction> &pending) {
    PendingPhase phase;
    std::uint64_t acknowledged_hosts = 0;
    std::size_t acknowledged_count = 0;
    bool all_dispatched = true;
    std::optional<std::array<std::byte, 64>> returned_dirty;
    {
        std::lock_guard lock(pending->mutex);
        phase = pending->phase;
        acknowledged_count = pending->acknowledged_count;
        for (const auto &expected : pending->expected) {
            all_dispatched = all_dispatched && expected.dispatched;
            if (!expected.acknowledged)
                continue;
            acknowledged_hosts |= holder(expected.host_id);
            if (expected.returned_data)
                returned_dirty = expected.returned_data;
        }
    }

    const auto &current = pending->starting_snapshot;
    const bool all_acknowledged = acknowledged_count == pending->expected.size();
    const bool candidate_grant = phase == PendingPhase::Completed && all_dispatched && all_acknowledged;
    auto data = std::array<std::byte, 64>{};

    // Every fallible backend operation precedes ACK acceptance. Reconciliation
    // therefore consumes only already-prefetched or already-persisted bytes;
    // accepted endpoint effects cannot be rolled back by an I/O exception.
    if (candidate_grant && pending->operation != Operation::Upgrade) {
        if (returned_dirty)
            data = *returned_dirty;
        else if (pending->prefetched_data)
            data = *pending->prefetched_data;
        else
            throw std::logic_error("completed MESI transaction has no response data");
    }

    {
        std::lock_guard lock(pending->mutex);
        if (pending->phase == PendingPhase::Completed && all_dispatched &&
            pending->acknowledged_count == pending->expected.size()) {
            // This is the grant/commit linearization point. Disconnects that
            // win before it fence the requester; later disconnects apply to a
            // transaction whose grant has already committed logically.
            pending->phase = PendingPhase::Committing;
        }
        phase = pending->phase;
    }

    const bool grant = phase == PendingPhase::Committing && all_dispatched && all_acknowledged;
    DirectorySnapshot next = current;
    bool has_effect = false;

    if (grant) {
        switch (pending->operation) {
        case Operation::Gets:
            next = {MesiState::S, std::nullopt, holder(*current.owner) | holder(pending->requester.host_id),
                    current.epoch, true};
            break;
        case Operation::Getm:
            next = {MesiState::M, pending->requester.host_id, 0, current.epoch, false};
            break;
        case Operation::Upgrade:
            next = {MesiState::M, pending->requester.host_id, 0, current.epoch, false};
            break;
        }
        has_effect = true;
    } else if (acknowledged_hosts != 0) {
        switch (pending->operation) {
        case Operation::Gets:
            if (current.owner && (acknowledged_hosts & holder(*current.owner)) != 0) {
                next = {MesiState::S, std::nullopt, holder(*current.owner), current.epoch, true};
                has_effect = true;
            }
            break;
        case Operation::Getm:
            if (current.state == MesiState::S) {
                const auto remaining = current.sharers & ~acknowledged_hosts;
                next = remaining == 0 ? DirectorySnapshot{MesiState::I, std::nullopt, 0, current.epoch, true}
                                      : DirectorySnapshot{MesiState::S, std::nullopt, remaining, current.epoch, true};
                has_effect = remaining != current.sharers;
            } else if (current.owner && (acknowledged_hosts & holder(*current.owner)) != 0) {
                next = {MesiState::I, std::nullopt, 0, current.epoch, true};
                has_effect = true;
            }
            break;
        case Operation::Upgrade: {
            const auto remaining = current.sharers & ~acknowledged_hosts;
            next = {MesiState::S, std::nullopt, remaining, current.epoch, true};
            has_effect = remaining != current.sharers;
            break;
        }
        }
    }

    TransitionResult transition = unchanged(TransitionStatus::NoChange, current);
    if (has_effect) {
        switch (pending->operation) {
        case Operation::Gets:
            transition = line.commitGets(pending->requester.host_id, current, next);
            break;
        case Operation::Getm:
            transition = line.commitGetm(pending->requester.host_id, current, next);
            break;
        case Operation::Upgrade:
            transition = line.commitUpgrade(pending->requester.host_id, current, next);
            break;
        }
    }

    protocol_v2::Status status = protocol_v2::Status::InvalidState;
    switch (phase) {
    case PendingPhase::Committing:
        status = statusFor(transition.status);
        break;
    case PendingPhase::Completed:
        status = protocol_v2::Status::InvalidState;
        break;
    case PendingPhase::TimedOut:
        status = protocol_v2::Status::CoherenceTimeout;
        break;
    case PendingPhase::Disconnected:
        status = protocol_v2::Status::HostFenced;
        break;
    case PendingPhase::SendFailed:
        status = protocol_v2::Status::IoError;
        break;
    case PendingPhase::Open:
        status = protocol_v2::Status::InvalidState;
        break;
    }

    const bool granted = grant && transition.succeeded() && status == protocol_v2::Status::Ok;
    if (!granted)
        data = {};
    return {status, transition, granted, data};
}

AckDisposition MesiTransactionEngine::handleSnoopAck(const protocol_v2::CoherenceFrame &ack) {
    if (protocol_v2::opcode(ack) != protocol_v2::Opcode::SnoopAck || protocol_v2::snoopId(ack) == 0)
        return AckDisposition::Invalid;

    std::shared_ptr<PendingTransaction> pending;
    {
        std::lock_guard lock(active_mutex_);
        const auto found = active_by_snoop_.find(protocol_v2::snoopId(ack));
        if (found == active_by_snoop_.end() || !(pending = found->second.lock()))
            return AckDisposition::Stale;
    }

    std::unique_lock lock(pending->mutex);
    if (pending->phase != PendingPhase::Open)
        return AckDisposition::Stale;
    if (Clock::now() >= pending->deadline) {
        pending->timeout_requested = true;
        pending->changed.notify_all();
        return AckDisposition::Stale;
    }
    if (pending->disconnect_requested || pending->timeout_requested || pending->send_failed_requested)
        return AckDisposition::Stale;
    auto expected = std::find_if(pending->expected.begin(), pending->expected.end(),
                                 [&](const ExpectedAck &item) { return item.snoop_id == protocol_v2::snoopId(ack); });
    if (expected == pending->expected.end())
        return AckDisposition::Stale;
    if (!expected->dispatched)
        return AckDisposition::Stale;
    if (protocol_v2::srcHost(ack) != expected->host_id || protocol_v2::sessionId(ack) != expected->session_id ||
        protocol_v2::address(ack) != pending->line_address || protocol_v2::epoch(ack) != pending->starting_epoch ||
        pending->target_epoch != pending->starting_epoch + 1)
        return AckDisposition::Stale;
    if (expected->acknowledged)
        return AckDisposition::Duplicate;

    const auto success_state = protocol_v2::opcode(expected->snoop) == protocol_v2::Opcode::SnpDowngrade ||
                                       protocol_v2::opcode(expected->snoop) == protocol_v2::Opcode::SnpDataDowngrade
                                   ? protocol_v2::LineState::S
                                   : protocol_v2::LineState::I;
    const auto failure_state = static_cast<protocol_v2::LineState>(expected->prior_state);
    if (!protocol_v2::validateSnoopAck(ack, expected->snoop,
                                       {protocol_v2::AckStrength::MODEL, success_state, failure_state}) ||
        protocol_v2::status(ack) != protocol_v2::Status::Ok)
        return AckDisposition::Invalid;

    std::optional<std::array<std::byte, 64>> returned_data;
    if (protocol_v2::payloadLength(ack) == protocol_v2::kLineSize) {
        std::array<std::byte, 64> data{};
        std::transform(ack.data.begin(), ack.data.end(), data.begin(),
                       [](std::uint8_t byte) { return static_cast<std::byte>(byte); });
        returned_data = data;
    }

    if (returned_data) {
        if (expected->returned_data && *expected->returned_data != *returned_data)
            return AckDisposition::Invalid;
        expected->returned_data = returned_data;
        expected->persistence_retry_requested = true;
        pending->changed.notify_all();
        return AckDisposition::Deferred;
    }

    expected->acknowledged = true;
    ++pending->acknowledged_count;
    if (pending->disconnect_requested) {
        pending->phase = PendingPhase::Disconnected;
    } else if (pending->timeout_requested) {
        pending->phase = PendingPhase::TimedOut;
    } else if (pending->send_failed_requested) {
        pending->phase = PendingPhase::SendFailed;
    } else if (pending->acknowledged_count == pending->expected.size()) {
        pending->phase = PendingPhase::Completed;
    }
    pending->changed.notify_all();
    return AckDisposition::Accepted;
}

std::size_t MesiTransactionEngine::progress(TimePoint now) {
    std::vector<std::shared_ptr<PendingTransaction>> active;
    {
        std::lock_guard lock(active_mutex_);
        active.reserve(active_by_line_.size());
        for (auto iterator = active_by_line_.begin(); iterator != active_by_line_.end();) {
            if (auto pending = iterator->second.lock()) {
                active.push_back(std::move(pending));
                ++iterator;
            } else {
                iterator = active_by_line_.erase(iterator);
            }
        }
    }

    std::size_t expired = 0;
    for (const auto &pending : active) {
        std::lock_guard lock(pending->mutex);
        if (pending->phase == PendingPhase::Open && !pending->timeout_requested && pending->deadline <= now) {
            pending->timeout_requested = true;
            pending->changed.notify_all();
            ++expired;
        }
    }
    return expired;
}

std::size_t MesiTransactionEngine::notifyDisconnect(std::uint16_t host_id, std::uint64_t session_id) {
    {
        std::lock_guard lock(sessions_mutex_);
        const auto found = sessions_.find(host_id);
        if (found != sessions_.end() && found->second == session_id)
            sessions_.erase(found);
    }

    std::vector<std::shared_ptr<PendingTransaction>> active;
    {
        std::lock_guard lock(active_mutex_);
        active.reserve(active_by_line_.size());
        for (const auto &[address, weak] : active_by_line_) {
            (void)address;
            if (auto pending = weak.lock())
                active.push_back(std::move(pending));
        }
    }

    std::size_t interrupted = 0;
    for (const auto &pending : active) {
        std::lock_guard lock(pending->mutex);
        if (pending->phase != PendingPhase::Open && pending->phase != PendingPhase::Completed)
            continue;
        bool affected = pending->requester.host_id == host_id && pending->requester.session_id == session_id;
        if (pending->phase == PendingPhase::Open) {
            for (const auto &expected : pending->expected) {
                if (!expected.acknowledged && expected.host_id == host_id && expected.session_id == session_id)
                    affected = true;
            }
        }
        if (affected && !pending->disconnect_requested) {
            if (pending->phase == PendingPhase::Open) {
                pending->disconnect_requested = true;
            } else {
                pending->phase = PendingPhase::Disconnected;
            }
            pending->changed.notify_all();
            ++interrupted;
        }
    }
    return interrupted;
}

} // namespace cxlmemsim::mesi_v2
