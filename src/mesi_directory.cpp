#include "mesi_directory.h"

#include <atomic>
#include <stdexcept>
#include <unordered_map>

namespace cxlmemsim::mesi_v2 {

struct MesiDirectory::Shard {
    mutable std::mutex mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<DirectoryEntry>> entries;
};

struct MesiDirectory::State {
    TransitionResult commit(DirectoryEntry &entry, DirectoryOperation operation, std::uint16_t requester,
                            const DirectorySnapshot &expected, DirectorySnapshot next);
    TransitionCounters transitionCounters() const noexcept;

private:
    std::atomic<std::uint64_t> &counterFor(DirectoryOperation operation) noexcept;

    std::atomic<std::uint64_t> gets_transitions{};
    std::atomic<std::uint64_t> getm_transitions{};
    std::atomic<std::uint64_t> upgrade_transitions{};
    std::atomic<std::uint64_t> puts_transitions{};
    std::atomic<std::uint64_t> putm_transitions{};
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

MesiDirectory::MesiDirectory(std::size_t shard_count) : shard_count_(shard_count), state_(std::make_shared<State>()) {
    if (shard_count == 0)
        throw std::invalid_argument("MESI directory requires at least one shard");

    shards_.reserve(shard_count_);
    for (std::size_t index = 0; index < shard_count_; ++index)
        shards_.push_back(std::make_unique<Shard>());
}

MesiDirectory::~MesiDirectory() = default;

MesiDirectory::LockedLine::LockedLine(std::shared_ptr<State> state, std::shared_ptr<DirectoryEntry> entry,
                                      std::unique_lock<std::mutex> transaction_lock) noexcept
    : state_(std::move(state)), entry_(std::move(entry)), transaction_lock_(std::move(transaction_lock)) {}

MesiDirectory::LockedLine &MesiDirectory::LockedLine::operator=(LockedLine &&other) noexcept {
    if (this == &other)
        return *this;

    // Release the destination mutex before replacing the shared owner of the
    // mutex-bearing entry. This also works when the destination is moved-from.
    transaction_lock_ = std::unique_lock<std::mutex>{};
    state_ = std::move(other.state_);
    entry_ = std::move(other.entry_);
    transaction_lock_ = std::move(other.transaction_lock_);
    return *this;
}

void MesiDirectory::LockedLine::requireLock() const {
    if (state_ == nullptr || entry_ == nullptr || !transaction_lock_.owns_lock())
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

TransitionResult MesiDirectory::LockedLine::commitGets(std::uint16_t requester, const DirectorySnapshot &expected,
                                                       const DirectorySnapshot &next) {
    requireLock();
    return state_->commit(*entry_, DirectoryOperation::Gets, requester, expected, next);
}

TransitionResult MesiDirectory::LockedLine::commitGetm(std::uint16_t requester, const DirectorySnapshot &expected,
                                                       const DirectorySnapshot &next) {
    requireLock();
    return state_->commit(*entry_, DirectoryOperation::Getm, requester, expected, next);
}

TransitionResult MesiDirectory::LockedLine::commitUpgrade(std::uint16_t requester, const DirectorySnapshot &expected,
                                                          const DirectorySnapshot &next) {
    requireLock();
    return state_->commit(*entry_, DirectoryOperation::Upgrade, requester, expected, next);
}

TransitionResult MesiDirectory::LockedLine::commitPuts(std::uint16_t requester, const DirectorySnapshot &expected,
                                                       const DirectorySnapshot &next) {
    requireLock();
    return state_->commit(*entry_, DirectoryOperation::Puts, requester, expected, next);
}

TransitionResult MesiDirectory::LockedLine::commitPutm(std::uint16_t requester, const DirectorySnapshot &expected,
                                                       const DirectorySnapshot &next) {
    requireLock();
    return state_->commit(*entry_, DirectoryOperation::Putm, requester, expected, next);
}

bool MesiDirectory::aligned(std::uint64_t line_address) noexcept { return (line_address & (kLineSize - 1)) == 0; }

bool MesiDirectory::validHost(std::uint16_t host_id) noexcept { return host_id < kMaximumHosts; }

std::uint64_t MesiDirectory::holderBit(std::uint16_t host_id) noexcept { return std::uint64_t{1} << host_id; }

bool MesiDirectory::hasSharer(const DirectorySnapshot &snapshot, std::uint16_t host_id) noexcept {
    return (snapshot.sharers & holderBit(host_id)) != 0;
}

bool MesiDirectory::validTransition(DirectoryOperation operation, std::uint16_t requester,
                                    const DirectorySnapshot &current, const DirectorySnapshot &next) noexcept {
    switch (operation) {
    case DirectoryOperation::Gets:
        return validGetsTransition(requester, current, next);
    case DirectoryOperation::Getm:
        return validGetmTransition(requester, current, next);
    case DirectoryOperation::Upgrade:
        return validUpgradeTransition(requester, current, next);
    case DirectoryOperation::Puts:
        return validPutsTransition(requester, current, next);
    case DirectoryOperation::Putm:
        return validPutmTransition(requester, current, next);
    }
    return false;
}

bool MesiDirectory::validGetsTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                        const DirectorySnapshot &next) noexcept {
    const auto requester_bit = holderBit(requester);
    switch (current.state) {
    case MesiState::I:
        return next.state == MesiState::E && next.owner == requester;
    case MesiState::S:
        return next.state == MesiState::S && next.sharers == (current.sharers | requester_bit);
    case MesiState::E:
    case MesiState::M: {
        if (current.owner == requester)
            return false;
        if (metadataEqual(current, next))
            return true;
        const auto old_owner_bit = holderBit(*current.owner);
        return next.state == MesiState::S &&
               (next.sharers == old_owner_bit || next.sharers == (old_owner_bit | requester_bit));
    }
    }
    return false;
}

bool MesiDirectory::validGetmTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                        const DirectorySnapshot &next) noexcept {
    switch (current.state) {
    case MesiState::I:
        return next.state == MesiState::M && next.owner == requester;
    case MesiState::S:
        if (hasSharer(current, requester))
            return false;
        if (next.state == MesiState::M)
            return next.owner == requester;
        if (next.state == MesiState::I)
            return true;
        return next.state == MesiState::S && (next.sharers & ~current.sharers) == 0;
    case MesiState::E:
    case MesiState::M:
        if (current.owner == requester)
            return false;
        return metadataEqual(current, next) || next.state == MesiState::I ||
               (next.state == MesiState::M && next.owner == requester);
    }
    return false;
}

bool MesiDirectory::validUpgradeTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                           const DirectorySnapshot &next) noexcept {
    if (current.state == MesiState::E && current.owner == requester)
        return metadataEqual(current, next) || (next.state == MesiState::M && next.owner == requester);
    if (current.state != MesiState::S || !hasSharer(current, requester))
        return false;
    if (next.state == MesiState::M)
        return next.owner == requester;
    return next.state == MesiState::S && (next.sharers & ~current.sharers) == 0 && hasSharer(next, requester);
}

bool MesiDirectory::validPutsTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                        const DirectorySnapshot &next) noexcept {
    if (current.state == MesiState::E)
        return current.owner == requester && next.state == MesiState::I;
    if (current.state != MesiState::S || !hasSharer(current, requester))
        return false;
    const auto remaining = current.sharers & ~holderBit(requester);
    return next.sharers == remaining && next.state == (remaining == 0 ? MesiState::I : MesiState::S);
}

bool MesiDirectory::validPutmTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                        const DirectorySnapshot &next) noexcept {
    return current.state == MesiState::M && current.owner == requester && next.state == MesiState::I;
}

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
    LockedLine locked(state_, std::move(entry), std::move(transaction_lock));
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

std::atomic<std::uint64_t> &MesiDirectory::State::counterFor(DirectoryOperation operation) noexcept {
    switch (operation) {
    case DirectoryOperation::Gets:
        return gets_transitions;
    case DirectoryOperation::Getm:
        return getm_transitions;
    case DirectoryOperation::Upgrade:
        return upgrade_transitions;
    case DirectoryOperation::Puts:
        return puts_transitions;
    case DirectoryOperation::Putm:
        return putm_transitions;
    }
    return gets_transitions;
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

TransitionResult MesiDirectory::State::commit(DirectoryEntry &entry, DirectoryOperation operation,
                                              std::uint16_t requester, const DirectorySnapshot &expected,
                                              DirectorySnapshot next) {
    const auto current = MesiDirectory::snapshotOf(entry);
    MesiDirectory::validateSnapshot(current);
    if (!MesiDirectory::validHost(requester))
        return MesiDirectory::rejected(TransitionStatus::InvalidHost, current);
    if (!isValidSnapshot(current) || !isValidSnapshot(expected) || !isValidSnapshot(next))
        return MesiDirectory::rejected(TransitionStatus::InvalidState, current);
    if (!MesiDirectory::snapshotsEqual(current, expected) || next.epoch != expected.epoch)
        return MesiDirectory::rejected(TransitionStatus::StaleMetadata, current);
    if (!MesiDirectory::validTransition(operation, requester, current, next))
        return MesiDirectory::rejected(TransitionStatus::InvalidState, current);
    if (MesiDirectory::metadataEqual(current, next))
        return MesiDirectory::rejected(TransitionStatus::NoChange, current);

    next.epoch = current.epoch + 1;
    entry.state = next.state;
    entry.owner = next.owner;
    entry.sharers = std::bitset<64>(next.sharers);
    entry.epoch = next.epoch;
    entry.server_copy_current = next.server_copy_current;
    counterFor(operation).fetch_add(1, std::memory_order_relaxed);
    ++MesiDirectory::diagnosticFor(entry, operation);
    MesiDirectory::validateSnapshot(MesiDirectory::snapshotOf(entry));
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
        return locked->commitGets(requester, current, {MesiState::E, requester, 0, current.epoch, true});
    case MesiState::S: {
        const auto requester_bit = holderBit(requester);
        return locked->commitGets(requester, current,
                                  {MesiState::S, std::nullopt, current.sharers | requester_bit, current.epoch, true});
    }
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return locked->commitGets(
            requester, current,
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
        return locked->commitGetm(requester, current, {MesiState::M, requester, 0, current.epoch, false});
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return locked->commitGetm(requester, current, {MesiState::M, requester, 0, current.epoch, false});
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
    return locked->commitUpgrade(requester, current, {MesiState::M, requester, 0, current.epoch, false});
}

TransitionResult MesiDirectory::puts(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto locked = lockLine(line_address);
    const auto current = locked->snapshot();

    if (current.state == MesiState::E && current.owner == requester)
        return locked->commitPuts(requester, current, {MesiState::I, std::nullopt, 0, current.epoch, true});

    if (current.state == MesiState::S) {
        const auto requester_bit = holderBit(requester);
        if ((current.sharers & requester_bit) == 0)
            return rejected(TransitionStatus::InvalidState, current);
        const auto remaining = current.sharers & ~requester_bit;
        const auto next_state = remaining == 0 ? MesiState::I : MesiState::S;
        return locked->commitPuts(requester, current, {next_state, std::nullopt, remaining, current.epoch, true});
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
    return locked->commitPutm(requester, current, {MesiState::I, std::nullopt, 0, current.epoch, true});
}

TransitionCounters MesiDirectory::transitionCounters() const noexcept { return state_->transitionCounters(); }

TransitionCounters MesiDirectory::State::transitionCounters() const noexcept {
    return {
        gets_transitions.load(std::memory_order_relaxed),    getm_transitions.load(std::memory_order_relaxed),
        upgrade_transitions.load(std::memory_order_relaxed), puts_transitions.load(std::memory_order_relaxed),
        putm_transitions.load(std::memory_order_relaxed),
    };
}

} // namespace cxlmemsim::mesi_v2
