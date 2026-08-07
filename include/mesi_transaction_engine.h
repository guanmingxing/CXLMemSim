#pragma once

#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "coherence_transport.h"
#include "endpoint_session_registry.h"
#include "mesi_directory.h"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <unordered_map>
#include <vector>

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
    std::uint64_t old_value{};
};

enum class HostFailurePolicy : std::uint8_t { RequireFenceAck, AssertProcessStopped, ForceDataLoss };
enum class AdministrativeStatus : std::uint8_t {
    Ok,
    FenceAckRequired,
    DirtyDataPresent,
    DataLoss,
    StaleSession,
    InvalidHost,
};

struct EvictionResult {
    AdministrativeStatus status{AdministrativeStatus::InvalidHost};
    std::size_t clean_removed{};
    std::size_t dirty_lost{};
};

enum class AuditEventKind : std::uint8_t {
    Timeout,
    PartialAck,
    ForcedCleanRemoval,
    ForcedDirtyLoss,
    StaleAck,
    InvalidOwnership,
};
enum class AuditSeverity : std::uint8_t { Info, Warning, High };

struct CoherenceAuditCounters {
    std::uint64_t timeout{};
    std::uint64_t partial_ack{};
    std::uint64_t forced_clean_removal{};
    std::uint64_t forced_dirty_loss{};
    std::uint64_t stale_ack{};
    std::uint64_t invalid_ownership{};
};

struct CoherenceAuditRecord {
    AuditEventKind kind{AuditEventKind::Timeout};
    AuditSeverity severity{AuditSeverity::Info};
    std::uint16_t host_id{};
    std::uint64_t session_id{};
    std::uint64_t line_address{};
    std::uint64_t epoch{};
};

class MesiTransactionEngine {
public:
    using HostFailurePolicy = mesi_v2::HostFailurePolicy;
    enum class Operation : std::uint8_t { Gets, Getm, Upgrade, AtomicFaa, AtomicCas };
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
    // PUTS/PUTM validate the exact live session while holding the line transaction lock. PUTM writes all 64 bytes
    // before its typed metadata commit; backend callbacks are synchronous and may reenter unrelated engine/registry
    // APIs, but must not recursively acquire the same line. A failed write or stale session/epoch leaves ownership
    // unchanged.
    TransactionResult puts(std::uint64_t line_address, TransactionRequest request, std::uint64_t installed_epoch);
    TransactionResult putm(std::uint64_t line_address, TransactionRequest request, std::uint64_t installed_epoch,
                           std::span<const std::byte, 64> data);
    TransitionResult puts(std::uint64_t line_address, std::uint16_t requester);
    TransitionResult putm(std::uint64_t line_address, std::uint16_t requester);

    // Atomics operate on naturally aligned 64-bit scalars contained in one line. They acquire exclusive coherence under
    // the existing line transaction lock, persist the complete updated line, advance the directory epoch once, and only
    // then return the old scalar and M grant. Coherence or backend failure executes no atomic update.
    TransactionResult fetchAdd(std::uint64_t address, TransactionRequest request, std::uint64_t value);
    TransactionResult compareExchange(std::uint64_t address, TransactionRequest request, std::uint64_t expected,
                                      std::uint64_t desired);

    // FENCE and UNREGISTER wait on the target session's operation watermark without holding registry, engine-session,
    // or line locks. UNREGISTER snapshots reverse-index candidates, then revalidates and commits at most one directory
    // line at a time.
    protocol_v2::Status fence(EndpointSessionRegistry &registry, SessionId session_id, BindingId binding_id,
                              std::uint64_t request_id);
    protocol_v2::Status unregisterSession(EndpointSessionRegistry &registry, std::uint16_t host_id,
                                          SessionId session_id, BindingId binding_id,
                                          const protocol_v2::CoherenceFrame &unregister_request);

    // RequireFenceAck authorizes clean removal only when matching_host_fence_ack is true. AssertProcessStopped is the
    // caller's explicit assertion that the endpoint can no longer access clean bytes. Neither may discard M.
    // ForceDataLoss is the sole dirty-discard path and returns DataLoss even when cleanup succeeds; it is always
    // counted and recorded at High severity.
    EvictionResult evictHost(EndpointSessionRegistry &registry, std::uint16_t host_id, SessionId session_id,
                             BindingId binding_id, HostFailurePolicy policy, bool matching_host_fence_ack = false);

    CoherenceAuditCounters auditCounters() const noexcept;
    std::vector<CoherenceAuditRecord> auditRecords() const;

    AckDisposition handleSnoopAck(const protocol_v2::CoherenceFrame &ack);
    std::size_t progress(TimePoint now = Clock::now());
    std::size_t notifyDisconnect(std::uint16_t host_id, std::uint64_t session_id);

private:
    struct AtomicArguments {
        std::size_t offset{};
        std::uint64_t operand{};
        std::uint64_t expected{};
    };
    TransactionResult acquire(std::uint64_t line_address, TransactionRequest request, Operation operation,
                              std::optional<AtomicArguments> atomic = std::nullopt);
    TransactionResult direct(MesiDirectory::LockedLine &line, TransactionRequest request, Operation operation,
                             const DirectorySnapshot &current,
                             const std::shared_ptr<const TransactionDependencies> &dependencies,
                             std::optional<AtomicArguments> atomic);
    TransactionResult transact(MesiDirectory::LockedLine &line, TransactionRequest request, Operation operation,
                               const DirectorySnapshot &current,
                               const std::shared_ptr<const TransactionDependencies> &dependencies,
                               std::optional<AtomicArguments> atomic);
    TransactionResult reconcile(MesiDirectory::LockedLine &line, const std::shared_ptr<PendingTransaction> &pending);
    protocol_v2::Status validateRequest(const TransactionRequest &request,
                                        const std::shared_ptr<const TransactionDependencies> &dependencies) const;
    std::optional<std::uint64_t> allocateSnoopId() noexcept;
    void registerPending(const std::shared_ptr<PendingTransaction> &pending);
    void unregisterPending(const std::shared_ptr<PendingTransaction> &pending);
    bool pendingSessionsCurrent(const PendingTransaction &pending) const;
    std::size_t interruptHost(std::uint16_t host_id, std::uint64_t session_id, bool remove_binding);
    std::shared_ptr<const TransactionDependencies> dependencySnapshot() const;
    static protocol_v2::Status statusFor(TransitionStatus status) noexcept;
    void recordAudit(AuditEventKind kind, AuditSeverity severity, std::uint16_t host_id, std::uint64_t session_id,
                     std::uint64_t line_address, std::uint64_t epoch);

    MesiDirectory &directory_;
    mutable std::mutex dependencies_mutex_;
    std::shared_ptr<const TransactionDependencies> dependencies_;
    std::atomic<std::uint64_t> next_snoop_id_{1};

    mutable std::mutex sessions_mutex_;
    std::unordered_map<std::uint16_t, std::uint64_t> sessions_;

    mutable std::mutex active_mutex_;
    std::unordered_map<std::uint64_t, std::weak_ptr<PendingTransaction>> active_by_snoop_;
    std::unordered_map<std::uint64_t, std::weak_ptr<PendingTransaction>> active_by_line_;

    std::atomic<std::uint64_t> timeout_events_{};
    std::atomic<std::uint64_t> partial_ack_events_{};
    std::atomic<std::uint64_t> forced_clean_removals_{};
    std::atomic<std::uint64_t> forced_dirty_losses_{};
    std::atomic<std::uint64_t> stale_acks_{};
    std::atomic<std::uint64_t> invalid_ownership_events_{};
    mutable std::mutex audit_mutex_;
    std::vector<CoherenceAuditRecord> audit_records_;
};

} // namespace cxlmemsim::mesi_v2
