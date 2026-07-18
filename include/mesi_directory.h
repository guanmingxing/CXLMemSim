#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace cxlmemsim::mesi_v2 {

enum class MesiState : std::uint8_t { I, S, E, M };

struct DirectorySnapshot {
    MesiState state{MesiState::I};
    std::optional<std::uint16_t> owner;
    std::uint64_t sharers{};
    std::uint64_t epoch{};
    bool server_copy_current{true};
};

bool isValidSnapshot(const DirectorySnapshot &snapshot) noexcept;

enum class TransitionStatus : std::uint8_t {
    Committed,
    NoChange,
    UnalignedAddress,
    InvalidHost,
    InvalidState,
};

struct TransitionResult {
    TransitionStatus status{TransitionStatus::InvalidState};
    DirectorySnapshot snapshot;

    bool succeeded() const noexcept {
        return status == TransitionStatus::Committed || status == TransitionStatus::NoChange;
    }
    bool committed() const noexcept { return status == TransitionStatus::Committed; }
};

struct TransitionCounters {
    std::uint64_t gets{};
    std::uint64_t getm{};
    std::uint64_t upgrade{};
    std::uint64_t puts{};
    std::uint64_t putm{};
};

struct DirectoryEntry {
    explicit DirectoryEntry(std::uint64_t address) noexcept : line_address(address) {}
    DirectoryEntry(const DirectoryEntry &) = delete;
    DirectoryEntry &operator=(const DirectoryEntry &) = delete;

    const std::uint64_t line_address;
    MesiState state{MesiState::I};
    std::optional<std::uint16_t> owner;
    std::uint64_t sharers{};
    std::uint64_t epoch{};
    bool server_copy_current{true};
    mutable std::mutex transaction_mutex;
};

class MesiDirectory {
public:
    static constexpr std::size_t kLineSize = 64;
    static constexpr std::size_t kDefaultShardCount = 256;
    static constexpr std::uint16_t kMaximumHosts = 64;

    explicit MesiDirectory(std::size_t shard_count = kDefaultShardCount);
    ~MesiDirectory();

    MesiDirectory(const MesiDirectory &) = delete;
    MesiDirectory &operator=(const MesiDirectory &) = delete;

    // Unaligned addresses are rejected: getOrCreate returns nullptr, inspect and
    // shardIndexFor return nullopt, and transitions return UnalignedAddress.
    std::shared_ptr<DirectoryEntry> getOrCreate(std::uint64_t line_address);
    std::optional<DirectorySnapshot> inspect(std::uint64_t line_address) const;
    std::size_t allocatedLineCount() const;

    // The mapping is deliberately fixed and observable: line number modulo the
    // configured shard count. This does not allocate a directory entry.
    std::optional<std::size_t> shardIndexFor(std::uint64_t line_address) const noexcept;

    TransitionResult gets(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult getm(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult upgrade(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult puts(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult putm(std::uint64_t line_address, std::uint16_t requester);

    TransitionCounters transitionCounters() const noexcept;

private:
    struct Shard;

    static bool aligned(std::uint64_t line_address) noexcept;
    static bool validHost(std::uint16_t host_id) noexcept;
    static std::uint64_t holderBit(std::uint16_t host_id) noexcept;
    static DirectorySnapshot snapshotOf(const DirectoryEntry &entry) noexcept;
    static TransitionResult rejected(TransitionStatus status, const DirectorySnapshot &snapshot) noexcept;
    static void validateSnapshot(const DirectorySnapshot &snapshot);
    static TransitionResult commit(DirectoryEntry &entry, DirectorySnapshot next, std::atomic<std::uint64_t> &counter);

    const std::size_t shard_count_;
    std::vector<std::unique_ptr<Shard>> shards_;
    std::atomic<std::uint64_t> gets_transitions_{};
    std::atomic<std::uint64_t> getm_transitions_{};
    std::atomic<std::uint64_t> upgrade_transitions_{};
    std::atomic<std::uint64_t> puts_transitions_{};
    std::atomic<std::uint64_t> putm_transitions_{};
};

} // namespace cxlmemsim::mesi_v2
