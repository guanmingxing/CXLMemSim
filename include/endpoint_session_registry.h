#pragma once

#include "coherence_protocol_v2.h"

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace cxlmemsim {

using SessionId = std::uint64_t;
using ResponseSender = std::function<bool(const protocol_v2::CoherenceFrame &)>;

class EndpointSessionRegistry;

class BindingId {
public:
    constexpr BindingId() noexcept = default;
    constexpr explicit operator bool() const noexcept { return value_ != 0; }
    friend constexpr bool operator==(BindingId, BindingId) noexcept = default;

private:
    explicit constexpr BindingId(std::uint64_t value) noexcept : value_(value) {}

    std::uint64_t value_{};
    friend class EndpointSessionRegistry;
};

class SessionGenerationToken {
public:
    constexpr SessionGenerationToken() noexcept = default;
    constexpr explicit operator bool() const noexcept { return session_id_ != 0; }

private:
    constexpr SessionGenerationToken(SessionId session_id, std::uint64_t generation) noexcept
        : session_id_(session_id), generation_(generation) {}
    SessionId session_id_{};
    std::uint64_t generation_{};
    friend class EndpointSessionRegistry;
};

class OperationAuthority {
public:
    constexpr OperationAuthority() noexcept = default;
    OperationAuthority(const OperationAuthority &) = delete;
    OperationAuthority &operator=(const OperationAuthority &) = delete;
    constexpr OperationAuthority(OperationAuthority &&other) noexcept
        : session_id_(std::exchange(other.session_id_, 0)), generation_(std::exchange(other.generation_, 0)),
          request_id_(std::exchange(other.request_id_, 0)), nonce_(std::exchange(other.nonce_, 0)) {}
    constexpr OperationAuthority &operator=(OperationAuthority &&other) noexcept {
        if (this != &other) {
            session_id_ = std::exchange(other.session_id_, 0);
            generation_ = std::exchange(other.generation_, 0);
            request_id_ = std::exchange(other.request_id_, 0);
            nonce_ = std::exchange(other.nonce_, 0);
        }
        return *this;
    }
    constexpr explicit operator bool() const noexcept { return nonce_ != 0; }

private:
    constexpr OperationAuthority(SessionId session_id, std::uint64_t generation, std::uint64_t request_id,
                                 std::uint64_t nonce) noexcept
        : session_id_(session_id), generation_(generation), request_id_(request_id), nonce_(nonce) {}
    SessionId session_id_{};
    std::uint64_t generation_{};
    std::uint64_t request_id_{};
    std::uint64_t nonce_{};
    friend class EndpointSessionRegistry;
};

class CleanupAuthority {
public:
    constexpr CleanupAuthority() noexcept = default;
    CleanupAuthority(const CleanupAuthority &) = delete;
    CleanupAuthority &operator=(const CleanupAuthority &) = delete;
    constexpr CleanupAuthority(CleanupAuthority &&other) noexcept
        : session_id_(std::exchange(other.session_id_, 0)), generation_(std::exchange(other.generation_, 0)),
          nonce_(std::exchange(other.nonce_, 0)) {}
    constexpr CleanupAuthority &operator=(CleanupAuthority &&other) noexcept {
        if (this != &other) {
            session_id_ = std::exchange(other.session_id_, 0);
            generation_ = std::exchange(other.generation_, 0);
            nonce_ = std::exchange(other.nonce_, 0);
        }
        return *this;
    }
    constexpr explicit operator bool() const noexcept { return nonce_ != 0; }

private:
    constexpr CleanupAuthority(SessionId session_id, std::uint64_t generation, std::uint64_t nonce) noexcept
        : session_id_(session_id), generation_(generation), nonce_(nonce) {}
    SessionId session_id_{};
    std::uint64_t generation_{};
    std::uint64_t nonce_{};
    friend class EndpointSessionRegistry;
};

class UnregisterAuthority {
public:
    constexpr UnregisterAuthority() noexcept = default;
    UnregisterAuthority(const UnregisterAuthority &) = delete;
    UnregisterAuthority &operator=(const UnregisterAuthority &) = delete;
    constexpr UnregisterAuthority(UnregisterAuthority &&other) noexcept
        : session_id_(std::exchange(other.session_id_, 0)), generation_(std::exchange(other.generation_, 0)),
          request_id_(std::exchange(other.request_id_, 0)), nonce_(std::exchange(other.nonce_, 0)) {}
    constexpr UnregisterAuthority &operator=(UnregisterAuthority &&other) noexcept {
        if (this != &other) {
            session_id_ = std::exchange(other.session_id_, 0);
            generation_ = std::exchange(other.generation_, 0);
            request_id_ = std::exchange(other.request_id_, 0);
            nonce_ = std::exchange(other.nonce_, 0);
        }
        return *this;
    }
    constexpr explicit operator bool() const noexcept { return nonce_ != 0; }

private:
    constexpr UnregisterAuthority(SessionId session_id, std::uint64_t generation, std::uint64_t request_id,
                                  std::uint64_t nonce) noexcept
        : session_id_(session_id), generation_(generation), request_id_(request_id), nonce_(nonce) {}
    SessionId session_id_{};
    std::uint64_t generation_{};
    std::uint64_t request_id_{};
    std::uint64_t nonce_{};
    friend class EndpointSessionRegistry;
};

#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
namespace endpoint_session_registry_test {
enum class FailurePoint {
    SessionIndexInsertion,
    HostIndexInsertion,
    DrainDeliveryContextBookkeeping,
    DrainResponseBookkeeping
};
void failNext(FailurePoint point) noexcept;
} // namespace endpoint_session_registry_test
#endif

enum class SessionState { Active, OfflineRetained, Fenced, Closed };
enum class PinResponseResult {
    Pinned,
    Duplicate,
    DeliveryFailed,
    Conflict,
    Backpressure,
    StaleRequest,
    SessionUnavailable,
    InvalidResponse
};
enum class RequestAdmissionResult {
    Accepted,
    Duplicate,
    Conflict,
    Backpressure,
    StaleRequest,
    InvalidRequest,
    SessionUnavailable
};

struct OperationAdmission {
    RequestAdmissionResult result{RequestAdmissionResult::SessionUnavailable};
    OperationAuthority authority;
};

struct RegistrationRequest {
    std::uint16_t host_id;
    SessionId requested_session_id;
    std::uint64_t capabilities;
    std::uint32_t cache_capacity;
    std::uint16_t cache_ways;
    std::string transport_name;
    ResponseSender sender;
    bool defer_response_replay{};
};

struct RegistrationResult {
    protocol_v2::Status status{protocol_v2::Status::InvalidState};
    SessionId session_id{};
    BindingId binding_id{};
    std::uint64_t negotiated_capabilities{};
    std::uint32_t cache_capacity{};
    std::uint16_t cache_ways{};
    std::uint16_t line_size{static_cast<std::uint16_t>(protocol_v2::kLineSize)};
    protocol_v2::AckStrength ack_strength{protocol_v2::AckStrength::MODEL};
};

struct SessionSnapshot {
    SessionId session_id;
    BindingId binding_id;
    std::uint16_t host_id;
    SessionState state;
    std::uint64_t capabilities;
    std::uint32_t cache_capacity;
    std::uint16_t cache_ways;
    std::string transport_name;
    bool has_sender;
    std::uint64_t response_watermark;
    std::uint64_t replay_floor;
    std::size_t pinned_response_count;
    std::size_t pinned_response_limit;
    bool response_backpressured;
    bool closed_final_response_pinned;
    std::uint64_t operation_completion_watermark;
    bool unregister_in_progress;
};

struct HolderSnapshot {
    std::vector<std::uint64_t> clean;
    std::vector<std::uint64_t> modified;
};

enum class HolderPermission : std::uint8_t { None, Clean, Modified };

class EndpointSessionRegistry {
public:
    explicit EndpointSessionRegistry(std::uint16_t max_hosts = protocol_v2::kMaximumHosts,
                                     std::size_t max_pinned_responses_per_session = 1024);

    RegistrationResult registerEndpoint(const RegistrationRequest &request);
    bool publishPendingResponses(SessionId session_id, BindingId binding_id);
    bool disconnectAbruptly(std::uint16_t host_id, SessionId session_id, BindingId binding_id);
    protocol_v2::Status gracefulClose(std::uint16_t host_id, SessionId session_id, BindingId binding_id,
                                      const protocol_v2::CoherenceFrame &unregister_request);

    // Replay-safe control/lifecycle admission. Holder-changing v2 commands are rejected here and must use
    // admitOperation() so their exact directory/index effects cannot escape request-scoped authority.
    RequestAdmissionResult admitRequest(SessionId session_id, BindingId binding_id,
                                        const protocol_v2::CoherenceFrame &request);
    // Atomically accepts a handler request and returns its sole move-only authority. The authority binds the admitting
    // generation and request ID, remains valid across transport retirement only until terminal completion, and cannot
    // authorize a resumed binding or another request. No method below holds the registry mutex across callbacks.
    OperationAdmission admitOperation(SessionId session_id, BindingId binding_id,
                                      const protocol_v2::CoherenceFrame &request);
    PinResponseResult pinResponse(SessionId session_id, const protocol_v2::CoherenceFrame &request,
                                  const protocol_v2::CoherenceFrame &response);
    PinResponseResult pinResponse(const OperationAuthority &authority, const protocol_v2::CoherenceFrame &request,
                                  const protocol_v2::CoherenceFrame &response);
    PinResponseResult pinAndCompleteOperation(OperationAuthority &authority, const protocol_v2::CoherenceFrame &request,
                                              const protocol_v2::CoherenceFrame &response);
    std::optional<protocol_v2::CoherenceFrame> pinnedResponse(SessionId session_id, BindingId binding_id,
                                                              const protocol_v2::CoherenceFrame &request) const;
    bool acknowledgeResponses(SessionId session_id, BindingId binding_id, std::uint64_t highest_contiguous_consumed);
    std::uint64_t responseWatermark(SessionId session_id) const;
    std::uint64_t replayFloor(SessionId session_id) const;
    std::vector<std::uint64_t> pinnedResponseIds(SessionId session_id) const;

    // Operation completion is independent from response-consumption acknowledgement. admitRequest() creates an
    // incomplete operation; completeOperation() may be called out of order and advances only the highest contiguous
    // completion watermark. waitForOperationsBefore() snapshots the per-session wait state under the registry mutex and
    // releases that mutex before blocking. These methods never invoke response callbacks and different sessions never
    // share a wait mutex.
    bool completeOperation(SessionId session_id, BindingId binding_id, std::uint64_t request_id);
    bool completeOperation(OperationAuthority &authority);
    bool waitForOperationsBefore(SessionId session_id, BindingId binding_id, std::uint64_t request_id) const;
    // Waits through an already captured admission watermark. A zero watermark skips blocking only after exact identity
    // validation. The wait is generation exact, releases all registry locks, and wakes false when that binding retires.
    bool waitForOperationsThrough(SessionId session_id, BindingId binding_id, std::uint64_t request_id) const;
    bool waitForOperationsThrough(SessionGenerationToken generation, std::uint64_t request_id) const;
    std::uint64_t operationCompletionWatermark(SessionId session_id) const;
    // Lifecycle handlers use these read-only checks before any external mutation. They compare under the registry lock
    // and neither retain that lock nor invoke callbacks.
    bool admittedRequestHasOpcode(SessionId session_id, BindingId binding_id, std::uint64_t request_id,
                                  protocol_v2::Opcode opcode) const;
    bool admittedRequestMatches(SessionId session_id, BindingId binding_id,
                                const protocol_v2::CoherenceFrame &request) const;

    // Waits until the reverse holder index contains no modified candidates. The registry mutex is not held while
    // waiting. Callers must first stop ordinary admission (for example with fenceSession()) so a new dirty holder
    // cannot race success.
    bool waitForModifiedDrain(SessionId session_id, BindingId binding_id) const;

    // Administrative fencing is idempotent for the same live binding, or for an exact retained session with no binding.
    // It blocks ordinary commands while retaining the bounded drain opcodes needed to finish coherence. Once the caller
    // has revalidated and removed every reverse-index holding, completeEviction() retires callbacks and releases the
    // host identity for a new generation. Neither method invokes a sender callback or acquires a directory lock.
    protocol_v2::Status fenceSession(std::uint16_t host_id, SessionId session_id, BindingId binding_id);
    // Captures the immutable current binding generation for administrative work. Empty BindingId is accepted only for
    // the current retained-offline or fenced generation. Disconnect retires an unpinned token and wakes its waits.
    std::optional<SessionGenerationToken> captureGeneration(std::uint16_t host_id, SessionId session_id,
                                                            BindingId binding_id) const;
    // sealFencedSession closes the bounded PUTM/FENCE/heartbeat drain and returns the final admitted watermark. Neither
    // method waits or invokes callbacks. controlFrameAdmissible is the transport-neutral route for contextual SNOOP_ACK
    // frames and validates only the exact still-live fenced binding; the engine performs opcode-specific validation.
    std::optional<std::uint64_t> sealFencedSession(std::uint16_t host_id, SessionId session_id, BindingId binding_id);
    std::optional<std::uint64_t> sealFencedSession(SessionGenerationToken generation);
    // After the sealed operation barrier completes, exactly one administrative cleanup may acquire a move-only
    // capability. A subsequent transport disconnect retires only BindingId callbacks, not that capability.
    // abortFencedCleanup releases a failed attempt without reopening ordinary admission. These methods neither wait nor
    // invoke callbacks.
    std::optional<CleanupAuthority> freezeFencedGenerationForCleanup(SessionGenerationToken generation);
    void abortFencedCleanup(CleanupAuthority &authority) noexcept;
    bool controlFrameAdmissible(SessionId session_id, BindingId binding_id,
                                const protocol_v2::CoherenceFrame &frame) const;
    bool completeEviction(std::uint16_t host_id, SessionId session_id, BindingId binding_id);
    bool completeEviction(std::uint16_t host_id, CleanupAuthority &authority);
    bool drainOpcodeAdmissible(protocol_v2::Opcode opcode) const noexcept;

    // Reverse-index mutation is exact to the immutable live binding. After sealing, callers may publish only effects
    // from requests admitted before the returned seal watermark; the administrative waiter establishes that barrier
    // before snapshot. No callback from a retired binding can mutate a resumed binding's index.
    bool addCleanHolder(SessionId session_id, BindingId binding_id, std::uint64_t line_address);
    bool removeCleanHolder(SessionId session_id, BindingId binding_id, std::uint64_t line_address);
    bool addModifiedHolder(SessionId session_id, BindingId binding_id, std::uint64_t line_address);
    bool removeModifiedHolder(SessionId session_id, BindingId binding_id, std::uint64_t line_address);
    std::vector<std::uint64_t> cleanHolders(SessionId session_id, BindingId binding_id) const;
    std::vector<std::uint64_t> modifiedHolders(SessionId session_id, BindingId binding_id) const;
    HolderSnapshot holderSnapshot(SessionId session_id, BindingId binding_id) const;
    bool addCleanHolder(const OperationAuthority &authority, std::uint64_t line_address);
    bool removeCleanHolder(const OperationAuthority &authority, std::uint64_t line_address);
    bool addModifiedHolder(const OperationAuthority &authority, std::uint64_t line_address);
    bool removeModifiedHolder(const OperationAuthority &authority, std::uint64_t line_address);
    // Reserves any cache-capacity increase before the directory can issue snoops or mutate metadata. A committed
    // directory transition then reconciles the complete per-line reverse index while the directory line lock is held.
    // The reserved set node is moved into the clean/modified set without allocation. Failed/no-change operations must
    // abort the reservation before consuming their authority; completeOperation() also cleans it up fail closed.
    bool reserveHolderTransition(const OperationAuthority &authority, std::uint64_t line_address,
                                 HolderPermission desired_permission);
    bool reconcileCommittedLine(const OperationAuthority &authority, std::uint64_t line_address,
                                std::uint64_t clean_hosts, std::uint64_t modified_hosts) noexcept;
    void abortHolderTransition(const OperationAuthority &authority) noexcept;
    bool removeCleanHolder(const CleanupAuthority &authority, std::uint64_t line_address);
    bool removeModifiedHolder(const CleanupAuthority &authority, std::uint64_t line_address);
    HolderSnapshot holderSnapshot(const CleanupAuthority &authority) const;

    // UNREGISTER is already the terminal admitted ordinary command. Freezing after its earlier-operation wait prevents
    // binding retirement during the preflight/commit interval. abortUnregister releases that pin and reopens admission
    // after a failed close without changing directory or holder metadata; the failed response remains independently
    // pinnable for ordered replay.
    std::optional<UnregisterAuthority> freezeUnregister(SessionId session_id, BindingId binding_id,
                                                        const protocol_v2::CoherenceFrame &unregister_request);
    bool preflightGracefulClose(const UnregisterAuthority &authority, std::uint16_t host_id,
                                const protocol_v2::CoherenceFrame &unregister_request) const;
    protocol_v2::Status completeGracefulClose(UnregisterAuthority &authority, std::uint16_t host_id,
                                              const protocol_v2::CoherenceFrame &unregister_request);
    void abortUnregister(UnregisterAuthority &authority) noexcept;

    std::optional<SessionSnapshot> inspect(SessionId session_id) const;

private:
    using StoredResponseSender = std::shared_ptr<const ResponseSender>;

    struct PinnedResponse {
        protocol_v2::CoherenceFrame request;
        protocol_v2::CoherenceFrame response;
    };

    struct Session {
        Session(SessionId session_id, BindingId binding_id, std::uint16_t host, std::uint64_t negotiated_capabilities,
                std::uint32_t capacity, std::uint16_t ways, std::string transport,
                StoredResponseSender response_sender);

        SessionId id{};
        BindingId binding_id{};
        std::uint16_t host_id{};
        SessionState state{SessionState::Active};
        std::uint64_t capabilities{};
        std::uint32_t cache_capacity{};
        std::uint16_t cache_ways{};
        std::string transport_name;
        StoredResponseSender sender;
        std::uint64_t response_watermark{};
        std::uint64_t published_response_watermark{};
        std::optional<std::uint64_t> pending_response_ack;
        bool closed_final_response_pinned{};
        std::optional<protocol_v2::CoherenceFrame> close_request;
        std::uint64_t binding_generation{};
        std::unordered_map<std::uint64_t, std::size_t> in_flight_deliveries;
        std::unordered_map<std::uint64_t, std::size_t> in_flight_response_deliveries;
        std::map<std::uint64_t, protocol_v2::CoherenceFrame> admitted_requests;
        std::map<std::uint64_t, PinnedResponse> pinned_responses;
        struct OperationRecord {
            std::uint64_t binding_generation{};
            std::uint64_t nonce{};
            protocol_v2::Opcode opcode{protocol_v2::Opcode::Heartbeat};
            std::uint64_t line_address{};
            bool claimed{};
            bool terminal{};
            bool response_reclaimed{};
            bool holder_transition_reserved{};
            bool reserved_new_slot{};
            HolderPermission desired_permission{HolderPermission::None};
        };
        std::map<std::uint64_t, OperationRecord> operation_records;
        std::optional<std::uint64_t> unregister_request_id;
        std::uint64_t unregister_owner{};
        bool drain_sealed{};
        std::optional<std::uint64_t> sealed_cutoff;
        std::uint64_t cleanup_owner{};
        std::uint64_t next_request_id{1};
        std::uint64_t publication_cursor{1};
        bool publishing{};
        std::uint64_t publisher_generation{};
        std::set<std::uint64_t> clean_holders;
        std::set<std::uint64_t> modified_holders;
        std::set<std::uint64_t> holder_reservations;
        struct OperationState {
            mutable std::mutex mutex;
            std::condition_variable changed;
            std::uint64_t binding_generation{};
            std::uint64_t completion_watermark{};
            std::set<std::uint64_t> completed_out_of_order;
        };
        struct HolderDrainState {
            mutable std::mutex mutex;
            std::condition_variable changed;
            std::uint64_t binding_generation{};
            std::size_t modified_count{};
        };
        std::shared_ptr<OperationState> operations{std::make_shared<OperationState>()};
        std::shared_ptr<HolderDrainState> holder_drain{std::make_shared<HolderDrainState>()};
    };

    bool validHolderSession(const Session &session, BindingId binding_id) const noexcept;
    static bool validGeneration(const Session &session, SessionGenerationToken generation) noexcept;
    static bool validOperationAuthority(const Session &session, const OperationAuthority &authority) noexcept;
    enum class HolderEffect : std::uint8_t { AddClean, RemoveClean, AddModified, RemoveModified };
    static bool validOperationHolderEffect(const Session &session, const OperationAuthority &authority,
                                           std::uint64_t line_address, HolderEffect effect) noexcept;
    static bool validCleanupAuthority(const Session &session, const CleanupAuthority &authority) noexcept;
    static bool validUnregisterAuthority(const Session &session, const UnregisterAuthority &authority) noexcept;
    static bool requiresOperationAuthority(protocol_v2::Opcode opcode) noexcept;
    bool validOrdinaryRequest(const Session &session, const protocol_v2::CoherenceFrame &request) const noexcept;
    OperationAdmission admitRequestLocked(Session &session, const protocol_v2::CoherenceFrame &request,
                                          bool claim_operation);
    bool beginDrainLocked(Session &session, std::uint64_t &generation, StoredResponseSender &sender,
                          StoredResponseSender &&staged_sender);
    PinResponseResult pinResponseImpl(const OperationAuthority *authority, OperationAuthority *completion_authority,
                                      SessionId session_id, const protocol_v2::CoherenceFrame &request,
                                      const protocol_v2::CoherenceFrame &response);
    bool drainResponses(const std::shared_ptr<Session> &session, std::uint64_t generation,
                        const StoredResponseSender &sender);
    StoredResponseSender finishDeliveryAttemptLocked(Session &session, std::uint64_t response_id, bool delivered);
    StoredResponseSender reclaimResponsesLocked(Session &session, std::uint64_t consumed);
    void retireFailedBinding(const std::shared_ptr<Session> &session, std::uint64_t generation);
    void waitForRetiredGeneration(std::unique_lock<std::mutex> &lock, const Session &session, std::uint64_t generation);
    static bool aligned(std::uint64_t line_address) noexcept;
    RegistrationResult resultFor(const Session &session, protocol_v2::Status status) const;
    bool validRegistration(const RegistrationRequest &request) const noexcept;
    static StoredResponseSender copySender(const ResponseSender &sender);
    static void completeOperationStateLocked(Session::OperationState &operations, std::uint64_t request_id);
    static void abortHolderTransitionLocked(Session &session, Session::OperationRecord &operation) noexcept;
    static void publishBindingGeneration(Session &session);

    const std::uint16_t max_hosts_;
    const std::size_t max_pinned_responses_per_session_;
    mutable std::mutex mutex_;
    std::condition_variable delivery_finished_;
    SessionId next_session_id_{1};
    std::uint64_t next_binding_id_{1};
    std::uint64_t next_authority_id_{1};
    std::unordered_map<SessionId, std::shared_ptr<Session>> sessions_;
    std::unordered_map<std::uint16_t, SessionId> host_sessions_;
};

} // namespace cxlmemsim
