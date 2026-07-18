#include "mesi_directory.h"

#include <stdexcept>
#include <unordered_map>

namespace cxlmemsim::mesi_v2 {

struct MesiDirectory::Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<DirectoryEntry>> entries;
};

bool isValidSnapshot(const DirectorySnapshot &snapshot) noexcept {
    const bool has_valid_owner = snapshot.owner.has_value() && *snapshot.owner < MesiDirectory::kMaximumHosts;
    switch (snapshot.state) {
    case MesiState::I:
        return !snapshot.owner.has_value() && snapshot.sharers == 0 && snapshot.server_copy_current;
    case MesiState::S:
        return !snapshot.owner.has_value() && snapshot.sharers != 0 && snapshot.server_copy_current;
    case MesiState::E:
        return has_valid_owner && snapshot.sharers == 0 && snapshot.server_copy_current;
    case MesiState::M:
        return has_valid_owner && snapshot.sharers == 0 && !snapshot.server_copy_current;
    }
    return false;
}

MesiDirectory::MesiDirectory(std::size_t shard_count) : shard_count_(shard_count) {
    if (shard_count == 0)
        throw std::invalid_argument("MESI directory requires at least one shard");

    shards_.reserve(shard_count_);
    for (std::size_t index = 0; index < shard_count_; ++index)
        shards_.push_back(std::make_unique<Shard>());
}

MesiDirectory::~MesiDirectory() = default;

MesiDirectory::LockedLine::LockedLine(MesiDirectory &directory, std::shared_ptr<DirectoryEntry> entry,
                                      std::unique_lock<std::mutex> transaction_lock) noexcept
    : directory_(&directory), entry_(std::move(entry)), transaction_lock_(std::move(transaction_lock)) {}

void MesiDirectory::LockedLine::requireLock() const {
    if (directory_ == nullptr || entry_ == nullptr || !transaction_lock_.owns_lock())
        throw std::logic_error("MESI locked-line guard does not own a transaction lock");
}

std::uint64_t MesiDirectory::LockedLine::lineAddress() const {
    requireLock();
    return entry_->lineAddress();
}

DirectorySnapshot MesiDirectory::LockedLine::snapshot() const {
    requireLock();
    const auto current = MesiDirectory::snapshotOf(*entry_);
    MesiDirectory::validateSnapshot(current);
    return current;
}

EntryDiagnostics MesiDirectory::LockedLine::diagnostics() const {
    requireLock();
    return MesiDirectory::diagnosticsOf(*entry_);
}

std::shared_ptr<PendingTransaction> MesiDirectory::LockedLine::pendingTransaction() const {
    requireLock();
    return MesiDirectory::pendingTransactionOf(*entry_);
}

void MesiDirectory::LockedLine::setPendingTransaction(std::shared_ptr<PendingTransaction> pending_transaction) {
    requireLock();
    MesiDirectory::setPendingTransactionOf(*entry_, std::move(pending_transaction));
}

TransitionResult MesiDirectory::LockedLine::commit(DirectoryOperation operation, const DirectorySnapshot &expected,
                                                   const DirectorySnapshot &next) {
    requireLock();
    return directory_->commitLocked(*entry_, operation, expected, next);
}

bool MesiDirectory::aligned(std::uint64_t line_address) noexcept { return (line_address & (kLineSize - 1)) == 0; }

bool MesiDirectory::validHost(std::uint16_t host_id) noexcept { return host_id < kMaximumHosts; }

bool MesiDirectory::validOperation(DirectoryOperation operation) noexcept {
    switch (operation) {
    case DirectoryOperation::Gets:
    case DirectoryOperation::Getm:
    case DirectoryOperation::Upgrade:
    case DirectoryOperation::Puts:
    case DirectoryOperation::Putm:
        return true;
    }
    return false;
}

std::uint64_t MesiDirectory::holderBit(std::uint16_t host_id) noexcept { return std::uint64_t{1} << host_id; }

std::optional<std::size_t> MesiDirectory::shardIndexFor(std::uint64_t line_address) const noexcept {
    if (!aligned(line_address))
        return std::nullopt;
    return (line_address / kLineSize) % shard_count_;
}

std::shared_ptr<DirectoryEntry> MesiDirectory::getOrCreate(std::uint64_t line_address) {
    const auto shard_index = shardIndexFor(line_address);
    if (!shard_index)
        return nullptr;

    auto &shard = *shards_[*shard_index];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto existing = shard.entries.find(line_address);
    if (existing != shard.entries.end())
        return existing->second;

    auto entry = std::make_shared<DirectoryEntry>(line_address);
    shard.entries.emplace(line_address, entry);
    return entry;
}

std::optional<MesiDirectory::LockedLine> MesiDirectory::lockLine(std::uint64_t line_address) {
    auto entry = getOrCreate(line_address);
    if (entry == nullptr)
        return std::nullopt;

    // getOrCreate has released the shard lock before this per-entry lock is
    // acquired, so no path nests the two lock classes.
    std::unique_lock<std::mutex> transaction_lock(entry->transaction_mutex);
    LockedLine locked(*this, std::move(entry), std::move(transaction_lock));
    return std::optional<LockedLine>(std::move(locked));
}

std::optional<DirectorySnapshot> MesiDirectory::inspect(std::uint64_t line_address) const {
    const auto shard_index = shardIndexFor(line_address);
    if (!shard_index)
        return std::nullopt;

    std::shared_ptr<DirectoryEntry> entry;
    {
        auto &shard = *shards_[*shard_index];
        std::lock_guard<std::mutex> lock(shard.mutex);
        const auto found = shard.entries.find(line_address);
        if (found == shard.entries.end())
            return DirectorySnapshot{};
        entry = found->second;
    }

    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto snapshot = snapshotOf(*entry);
    validateSnapshot(snapshot);
    return snapshot;
}

std::size_t MesiDirectory::allocatedLineCount() const {
    std::size_t count = 0;
    for (const auto &shard_ptr : shards_) {
        std::lock_guard<std::mutex> lock(shard_ptr->mutex);
        count += shard_ptr->entries.size();
    }
    return count;
}

DirectorySnapshot MesiDirectory::snapshotOf(const DirectoryEntry &entry) noexcept {
    return {entry.state, entry.owner, entry.sharers.to_ullong(), entry.epoch, entry.server_copy_current};
}

EntryDiagnostics MesiDirectory::diagnosticsOf(const DirectoryEntry &entry) noexcept { return entry.diagnostics; }

std::shared_ptr<PendingTransaction> MesiDirectory::pendingTransactionOf(const DirectoryEntry &entry) noexcept {
    return entry.pending_transaction;
}

void MesiDirectory::setPendingTransactionOf(DirectoryEntry &entry,
                                            std::shared_ptr<PendingTransaction> pending_transaction) noexcept {
    entry.pending_transaction = std::move(pending_transaction);
}

bool MesiDirectory::snapshotsEqual(const DirectorySnapshot &left, const DirectorySnapshot &right) noexcept {
    return left.state == right.state && left.owner == right.owner && left.sharers == right.sharers &&
           left.epoch == right.epoch && left.server_copy_current == right.server_copy_current;
}

bool MesiDirectory::metadataEqual(const DirectorySnapshot &left, const DirectorySnapshot &right) noexcept {
    return left.state == right.state && left.owner == right.owner && left.sharers == right.sharers &&
           left.server_copy_current == right.server_copy_current;
}

TransitionResult MesiDirectory::rejected(TransitionStatus status, const DirectorySnapshot &snapshot) noexcept {
    return {status, snapshot};
}

void MesiDirectory::validateSnapshot(const DirectorySnapshot &snapshot) {
#if !defined(NDEBUG) || defined(CXLMEMSIM_MESI_DIRECTORY_TESTING)
    if (!isValidSnapshot(snapshot))
        throw std::logic_error("strict MESI directory invariant violation");
#else
    (void)snapshot;
#endif
}

std::atomic<std::uint64_t> &MesiDirectory::counterFor(DirectoryOperation operation) noexcept {
    switch (operation) {
    case DirectoryOperation::Gets:
        return gets_transitions_;
    case DirectoryOperation::Getm:
        return getm_transitions_;
    case DirectoryOperation::Upgrade:
        return upgrade_transitions_;
    case DirectoryOperation::Puts:
        return puts_transitions_;
    case DirectoryOperation::Putm:
        return putm_transitions_;
    }
    return gets_transitions_;
}

std::uint64_t &MesiDirectory::diagnosticFor(DirectoryEntry &entry, DirectoryOperation operation) noexcept {
    switch (operation) {
    case DirectoryOperation::Gets:
        return entry.diagnostics.gets;
    case DirectoryOperation::Getm:
        return entry.diagnostics.getm;
    case DirectoryOperation::Upgrade:
        return entry.diagnostics.upgrade;
    case DirectoryOperation::Puts:
        return entry.diagnostics.puts;
    case DirectoryOperation::Putm:
        return entry.diagnostics.putm;
    }
    return entry.diagnostics.gets;
}

TransitionResult MesiDirectory::commitLocked(DirectoryEntry &entry, DirectoryOperation operation,
                                             const DirectorySnapshot &expected, DirectorySnapshot next) {
    const auto current = snapshotOf(entry);
    validateSnapshot(current);
    if (!validOperation(operation) || !isValidSnapshot(current) || !isValidSnapshot(expected) || !isValidSnapshot(next))
        return rejected(TransitionStatus::InvalidState, current);
    if (!snapshotsEqual(current, expected) || next.epoch != expected.epoch)
        return rejected(TransitionStatus::StaleMetadata, current);
    if (metadataEqual(current, next))
        return rejected(TransitionStatus::NoChange, current);

    next.epoch = current.epoch + 1;
    entry.state = next.state;
    entry.owner = next.owner;
    entry.sharers = std::bitset<64>(next.sharers);
    entry.epoch = next.epoch;
    entry.server_copy_current = next.server_copy_current;
    counterFor(operation).fetch_add(1, std::memory_order_relaxed);
    ++diagnosticFor(entry, operation);
    validateSnapshot(snapshotOf(entry));
    return {TransitionStatus::Committed, next};
}

TransitionResult MesiDirectory::gets(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();

    switch (current.state) {
    case MesiState::I:
        return locked->commit(DirectoryOperation::Gets, current, {MesiState::E, requester, 0, current.epoch, true});
    case MesiState::S: {
        const auto requester_bit = holderBit(requester);
        return locked->commit(DirectoryOperation::Gets, current,
                              {MesiState::S, std::nullopt, current.sharers | requester_bit, current.epoch, true});
    }
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return locked->commit(
            DirectoryOperation::Gets, current,
            {MesiState::S, std::nullopt, holderBit(*current.owner) | holderBit(requester), current.epoch, true});
    case MesiState::M:
        return rejected(TransitionStatus::InvalidState, current);
    }
    return rejected(TransitionStatus::InvalidState, current);
}

TransitionResult MesiDirectory::getm(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();

    switch (current.state) {
    case MesiState::I:
    case MesiState::S:
        return locked->commit(DirectoryOperation::Getm, current, {MesiState::M, requester, 0, current.epoch, false});
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return locked->commit(DirectoryOperation::Getm, current, {MesiState::M, requester, 0, current.epoch, false});
    case MesiState::M:
        return rejected(TransitionStatus::InvalidState, current);
    }
    return rejected(TransitionStatus::InvalidState, current);
}

TransitionResult MesiDirectory::upgrade(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();
    if (current.state != MesiState::E || current.owner != requester)
        return rejected(TransitionStatus::InvalidState, current);
    return locked->commit(DirectoryOperation::Upgrade, current, {MesiState::M, requester, 0, current.epoch, false});
}

TransitionResult MesiDirectory::puts(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();

    if (current.state == MesiState::E && current.owner == requester)
        return locked->commit(DirectoryOperation::Puts, current, {MesiState::I, std::nullopt, 0, current.epoch, true});

    if (current.state == MesiState::S) {
        const auto requester_bit = holderBit(requester);
        if ((current.sharers & requester_bit) == 0)
            return rejected(TransitionStatus::InvalidState, current);
        const auto remaining = current.sharers & ~requester_bit;
        const auto next_state = remaining == 0 ? MesiState::I : MesiState::S;
        return locked->commit(DirectoryOperation::Puts, current,
                              {next_state, std::nullopt, remaining, current.epoch, true});
    }

    return rejected(TransitionStatus::InvalidState, current);
}

TransitionResult MesiDirectory::putm(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();
    if (current.state != MesiState::M || current.owner != requester)
        return rejected(TransitionStatus::InvalidState, current);
    return locked->commit(DirectoryOperation::Putm, current, {MesiState::I, std::nullopt, 0, current.epoch, true});
}

TransitionCounters MesiDirectory::transitionCounters() const noexcept {
    return {
        gets_transitions_.load(std::memory_order_relaxed),    getm_transitions_.load(std::memory_order_relaxed),
        upgrade_transitions_.load(std::memory_order_relaxed), puts_transitions_.load(std::memory_order_relaxed),
        putm_transitions_.load(std::memory_order_relaxed),
    };
}

} // namespace cxlmemsim::mesi_v2
