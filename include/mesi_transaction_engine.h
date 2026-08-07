#pragma once

#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "coherence_transport.h"
#include "mesi_directory.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace cxlmemsim::mesi_v2 {

struct TransactionDependencies;

struct TransactionRequest {
    std::uint16_t host_id{};
    std::uint64_t session_id{};
    std::uint64_t request_id{};
};

enum class AckDisposition : std::uint8_t { Accepted, Deferred, Duplicate, Stale, Invalid };

struct TransactionResult {
    protocol_v2::Status status{protocol_v2::Status::InvalidState};
    TransitionResult transition;
    bool granted{};
    std::array<std::byte, 64> data{};
};

class MesiTransactionEngine {
public:
    enum class Operation : std::uint8_t { Gets, Getm, Upgrade };
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    // first_snoop_id seeds the never-reused monotonic allocator. Zero means
    // the identifier space is already exhausted and transactions fail closed.
    explicit MesiTransactionEngine(MesiDirectory &directory, Duration snoop_timeout = std::chrono::milliseconds(1000),
                                   std::uint64_t first_snoop_id = 1);
    MesiTransactionEngine(MesiDirectory &directory, CoherenceMemoryBackend &memory, CoherenceTransport &transport,
                          Duration snoop_timeout = std::chrono::milliseconds(1000), std::uint64_t first_snoop_id = 1);
    ~MesiTransactionEngine();

    MesiTransactionEngine(const MesiTransactionEngine &) = delete;
    MesiTransactionEngine &operator=(const MesiTransactionEngine &) = delete;

    // The configured backend and transport are caller-owned and must outlive
    // this engine and every transaction that may have captured them. A
    // transaction captures the complete configuration atomically, so a
    // concurrent configure() cannot mix dependency generations.
    void configure(CoherenceMemoryBackend &memory, CoherenceTransport &transport, Duration snoop_timeout);

    // Inserts a previously unbound session or confirms the identical generation. A different generation fails without
    // changing the binding; replacement is allowed only after notifyDisconnect() removes the matching old generation.
    bool bindSession(std::uint16_t host_id, std::uint64_t session_id);
    std::uint64_t sessionFor(std::uint16_t host_id) const;

    TransactionResult gets(std::uint64_t line_address, TransactionRequest request);
    TransactionResult getm(std::uint64_t line_address, TransactionRequest request);
    TransactionResult upgrade(std::uint64_t line_address, TransactionRequest request);
    TransitionResult puts(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult putm(std::uint64_t line_address, std::uint16_t requester);

    AckDisposition handleSnoopAck(const protocol_v2::CoherenceFrame &ack);
    std::size_t progress(TimePoint now = Clock::now());
    std::size_t notifyDisconnect(std::uint16_t host_id, std::uint64_t session_id);

private:
    TransactionResult acquire(std::uint64_t line_address, TransactionRequest request, Operation operation);
    TransactionResult direct(MesiDirectory::LockedLine &line, TransactionRequest request, Operation operation,
                             const DirectorySnapshot &current,
                             const std::shared_ptr<const TransactionDependencies> &dependencies);
    TransactionResult transact(MesiDirectory::LockedLine &line, TransactionRequest request, Operation operation,
                               const DirectorySnapshot &current,
                               const std::shared_ptr<const TransactionDependencies> &dependencies);
    TransactionResult reconcile(MesiDirectory::LockedLine &line, const std::shared_ptr<PendingTransaction> &pending);
    protocol_v2::Status validateRequest(const TransactionRequest &request,
                                        const std::shared_ptr<const TransactionDependencies> &dependencies) const;
    std::optional<std::uint64_t> allocateSnoopId() noexcept;
    void registerPending(const std::shared_ptr<PendingTransaction> &pending);
    void unregisterPending(const std::shared_ptr<PendingTransaction> &pending);
    bool pendingSessionsCurrent(const PendingTransaction &pending) const;
    std::shared_ptr<const TransactionDependencies> dependencySnapshot() const;
    static protocol_v2::Status statusFor(TransitionStatus status) noexcept;

    MesiDirectory &directory_;
    mutable std::mutex dependencies_mutex_;
    std::shared_ptr<const TransactionDependencies> dependencies_;
    std::atomic<std::uint64_t> next_snoop_id_{1};

    mutable std::mutex sessions_mutex_;
    std::unordered_map<std::uint16_t, std::uint64_t> sessions_;

    mutable std::mutex active_mutex_;
    std::unordered_map<std::uint64_t, std::weak_ptr<PendingTransaction>> active_by_snoop_;
    std::unordered_map<std::uint64_t, std::weak_ptr<PendingTransaction>> active_by_line_;
};

} // namespace cxlmemsim::mesi_v2
