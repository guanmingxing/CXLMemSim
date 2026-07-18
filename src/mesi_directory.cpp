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

bool MesiDirectory::aligned(std::uint64_t line_address) noexcept { return (line_address & (kLineSize - 1)) == 0; }

bool MesiDirectory::validHost(std::uint16_t host_id) noexcept { return host_id < kMaximumHosts; }

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
    return {entry.state, entry.owner, entry.sharers, entry.epoch, entry.server_copy_current};
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

TransitionResult MesiDirectory::commit(DirectoryEntry &entry, DirectorySnapshot next,
                                       std::atomic<std::uint64_t> &counter) {
    validateSnapshot(snapshotOf(entry));
    next.epoch = entry.epoch + 1;
    validateSnapshot(next);

    entry.state = next.state;
    entry.owner = next.owner;
    entry.sharers = next.sharers;
    entry.epoch = next.epoch;
    entry.server_copy_current = next.server_copy_current;
    counter.fetch_add(1, std::memory_order_relaxed);
    return {TransitionStatus::Committed, next};
}

TransitionResult MesiDirectory::gets(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto entry = getOrCreate(line_address);
    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto current = snapshotOf(*entry);
    validateSnapshot(current);

    switch (current.state) {
    case MesiState::I:
        return commit(*entry, {MesiState::E, requester, 0, current.epoch, true}, gets_transitions_);
    case MesiState::S: {
        const auto requester_bit = holderBit(requester);
        if ((current.sharers & requester_bit) != 0)
            return rejected(TransitionStatus::NoChange, current);
        return commit(*entry, {MesiState::S, std::nullopt, current.sharers | requester_bit, current.epoch, true},
                      gets_transitions_);
    }
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return commit(
            *entry, {MesiState::S, std::nullopt, holderBit(*current.owner) | holderBit(requester), current.epoch, true},
            gets_transitions_);
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

    auto entry = getOrCreate(line_address);
    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto current = snapshotOf(*entry);
    validateSnapshot(current);

    switch (current.state) {
    case MesiState::I:
    case MesiState::S:
        return commit(*entry, {MesiState::M, requester, 0, current.epoch, false}, getm_transitions_);
    case MesiState::E:
        if (current.owner == requester)
            return rejected(TransitionStatus::InvalidState, current);
        return commit(*entry, {MesiState::M, requester, 0, current.epoch, false}, getm_transitions_);
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

    auto entry = getOrCreate(line_address);
    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto current = snapshotOf(*entry);
    validateSnapshot(current);
    if (current.state != MesiState::E || current.owner != requester)
        return rejected(TransitionStatus::InvalidState, current);
    return commit(*entry, {MesiState::M, requester, 0, current.epoch, false}, upgrade_transitions_);
}

TransitionResult MesiDirectory::puts(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto entry = getOrCreate(line_address);
    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto current = snapshotOf(*entry);
    validateSnapshot(current);

    if (current.state == MesiState::E && current.owner == requester)
        return commit(*entry, {MesiState::I, std::nullopt, 0, current.epoch, true}, puts_transitions_);

    if (current.state == MesiState::S) {
        const auto requester_bit = holderBit(requester);
        if ((current.sharers & requester_bit) == 0)
            return rejected(TransitionStatus::InvalidState, current);
        const auto remaining = current.sharers & ~requester_bit;
        const auto next_state = remaining == 0 ? MesiState::I : MesiState::S;
        return commit(*entry, {next_state, std::nullopt, remaining, current.epoch, true}, puts_transitions_);
    }

    return rejected(TransitionStatus::InvalidState, current);
}

TransitionResult MesiDirectory::putm(std::uint64_t line_address, std::uint16_t requester) {
    if (!aligned(line_address))
        return rejected(TransitionStatus::UnalignedAddress, {});
    if (!validHost(requester))
        return rejected(TransitionStatus::InvalidHost, {});

    auto entry = getOrCreate(line_address);
    std::lock_guard<std::mutex> lock(entry->transaction_mutex);
    const auto current = snapshotOf(*entry);
    validateSnapshot(current);
    if (current.state != MesiState::M || current.owner != requester)
        return rejected(TransitionStatus::InvalidState, current);
    return commit(*entry, {MesiState::I, std::nullopt, 0, current.epoch, true}, putm_transitions_);
}

TransitionCounters MesiDirectory::transitionCounters() const noexcept {
    return {
        gets_transitions_.load(std::memory_order_relaxed),    getm_transitions_.load(std::memory_order_relaxed),
        upgrade_transitions_.load(std::memory_order_relaxed), puts_transitions_.load(std::memory_order_relaxed),
        putm_transitions_.load(std::memory_order_relaxed),
    };
}

} // namespace cxlmemsim::mesi_v2
