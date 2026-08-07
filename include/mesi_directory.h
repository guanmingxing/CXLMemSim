#pragma once

#include <bitset>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace cxlmemsim::mesi_v2 {

struct PendingTransaction;

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
    StaleMetadata,
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

struct EntryDiagnostics {
    std::uint64_t gets{};
    std::uint64_t getm{};
    std::uint64_t upgrade{};
    std::uint64_t puts{};
    std::uint64_t putm{};
};

class DirectoryEntry {
public:
    explicit DirectoryEntry(std::uint64_t address) noexcept : line_address(address) {}
    DirectoryEntry(const DirectoryEntry &) = delete;
    DirectoryEntry &operator=(const DirectoryEntry &) = delete;

    std::uint64_t lineAddress() const noexcept { return line_address; }

private:
    friend class MesiDirectory;

    const std::uint64_t line_address;
    MesiState state{MesiState::I};
    std::optional<std::uint16_t> owner;
    std::bitset<64> sharers;
    std::uint64_t epoch{};
    bool server_copy_current{true};
    std::shared_ptr<PendingTransaction> pending_transaction;
    EntryDiagnostics diagnostics;
    mutable std::mutex transaction_mutex;
};

class MesiDirectory {
private:
    enum class DirectoryOperation : std::uint8_t { Gets, Getm, Upgrade, Puts, Putm };
    struct State;

public:
    static constexpr std::size_t kLineSize = 64;
    static constexpr std::size_t kDefaultShardCount = 256;
    static constexpr std::uint16_t kMaximumHosts = 64;

    explicit MesiDirectory(std::size_t shard_count = kDefaultShardCount);
    ~MesiDirectory();

    MesiDirectory(const MesiDirectory &) = delete;
    MesiDirectory &operator=(const MesiDirectory &) = delete;

    class LockedLine {
    public:
        LockedLine(const LockedLine &) = delete;
        LockedLine &operator=(const LockedLine &) = delete;
        LockedLine(LockedLine &&) noexcept = default;
        LockedLine &operator=(LockedLine &&other) noexcept;
        ~LockedLine() = default;

        std::uint64_t lineAddress() const;
        DirectorySnapshot snapshot() const;
        EntryDiagnostics diagnostics() const;
        std::shared_ptr<PendingTransaction> pendingTransaction() const;
        void setPendingTransaction(std::shared_ptr<PendingTransaction> pending_transaction);

        // These requester-aware finalizers are the Task 4 post-snoop commit
        // surface. expected and next must both carry the current epoch. A real
        // metadata change commits next with epoch + 1 and increments exactly
        // one matching global and per-entry counter.
        TransitionResult commitGets(std::uint16_t requester, const DirectorySnapshot &expected,
                                    const DirectorySnapshot &next);
        TransitionResult commitGetm(std::uint16_t requester, const DirectorySnapshot &expected,
                                    const DirectorySnapshot &next);
        TransitionResult commitUpgrade(std::uint16_t requester, const DirectorySnapshot &expected,
                                       const DirectorySnapshot &next);
        TransitionResult commitPuts(std::uint16_t requester, const DirectorySnapshot &expected,
                                    const DirectorySnapshot &next);
        TransitionResult commitPutm(std::uint16_t requester, const DirectorySnapshot &expected,
                                    const DirectorySnapshot &next);

    private:
        friend class MesiDirectory;

        LockedLine(std::shared_ptr<State> state, std::shared_ptr<DirectoryEntry> entry,
                   std::unique_lock<std::mutex> transaction_lock) noexcept;
        void requireLock() const;

        // The state owns the commit machinery and global counters. Keeping it
        // alive makes an escaped guard independent of MesiDirectory lifetime.
        std::shared_ptr<State> state_;
        std::shared_ptr<DirectoryEntry> entry_;
        std::unique_lock<std::mutex> transaction_lock_;
    };

    // Unaligned addresses are rejected: getOrCreate returns nullptr, inspect and
    // lockLine/shardIndexFor return nullopt, and transitions return
    // UnalignedAddress.
    std::shared_ptr<DirectoryEntry> getOrCreate(std::uint64_t line_address);
    std::optional<DirectorySnapshot> inspect(std::uint64_t line_address) const;
    std::size_t allocatedLineCount() const;

    // Acquires a stable entry reference under its shard lock, releases the
    // shard lock, and only then takes the entry transaction mutex. The returned
    // move-only guard is the only production surface for metadata commits and
    // pending-transaction access.
    std::optional<LockedLine> lockLine(std::uint64_t line_address);

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
    static bool hasSharer(const DirectorySnapshot &snapshot, std::uint16_t host_id) noexcept;
    static bool validTransition(DirectoryOperation operation, std::uint16_t requester, const DirectorySnapshot &current,
                                const DirectorySnapshot &next) noexcept;
    static bool validGetsTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                    const DirectorySnapshot &next) noexcept;
    static bool validGetmTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                    const DirectorySnapshot &next) noexcept;
    static bool validUpgradeTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                       const DirectorySnapshot &next) noexcept;
    static bool validPutsTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                    const DirectorySnapshot &next) noexcept;
    static bool validPutmTransition(std::uint16_t requester, const DirectorySnapshot &current,
                                    const DirectorySnapshot &next) noexcept;
    static DirectorySnapshot snapshotOf(const DirectoryEntry &entry) noexcept;
    static EntryDiagnostics diagnosticsOf(const DirectoryEntry &entry) noexcept;
    static std::shared_ptr<PendingTransaction> pendingTransactionOf(const DirectoryEntry &entry) noexcept;
    static void setPendingTransactionOf(DirectoryEntry &entry,
                                        std::shared_ptr<PendingTransaction> pending_transaction) noexcept;
    static bool snapshotsEqual(const DirectorySnapshot &left, const DirectorySnapshot &right) noexcept;
    static bool metadataEqual(const DirectorySnapshot &left, const DirectorySnapshot &right) noexcept;
    static TransitionResult rejected(TransitionStatus status, const DirectorySnapshot &snapshot) noexcept;
    static void validateSnapshot(const DirectorySnapshot &snapshot);
    static std::uint64_t &diagnosticFor(DirectoryEntry &entry, DirectoryOperation operation) noexcept;

    const std::size_t shard_count_;
    std::shared_ptr<State> state_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

} // namespace cxlmemsim::mesi_v2
