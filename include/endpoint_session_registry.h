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
#include <vector>

namespace cxlmemsim {

using SessionId = std::uint64_t;
using ResponseSender = std::function<bool(const protocol_v2::CoherenceFrame &)>;

enum class SessionState { Active, OfflineRetained, Fenced, Closed };
enum class PinResponseResult {
    Pinned,
    Duplicate,
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

struct RegistrationRequest {
    std::uint16_t host_id;
    SessionId requested_session_id;
    std::uint64_t capabilities;
    std::uint32_t cache_capacity;
    std::uint16_t cache_ways;
    std::string transport_name;
    ResponseSender sender;
};

struct RegistrationResult {
    protocol_v2::Status status{protocol_v2::Status::InvalidState};
    SessionId session_id{};
    std::uint64_t negotiated_capabilities{};
    std::uint32_t cache_capacity{};
    std::uint16_t cache_ways{};
    std::uint16_t line_size{static_cast<std::uint16_t>(protocol_v2::kLineSize)};
    protocol_v2::AckStrength ack_strength{protocol_v2::AckStrength::MODEL};
};

struct SessionSnapshot {
    SessionId session_id;
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
};

class EndpointSessionRegistry {
public:
    explicit EndpointSessionRegistry(std::uint16_t max_hosts = protocol_v2::kMaximumHosts,
                                     std::size_t max_pinned_responses_per_session = 1024);

    RegistrationResult registerEndpoint(const RegistrationRequest &request);
    bool disconnectAbruptly(std::uint16_t host_id, SessionId session_id);
    protocol_v2::Status gracefulClose(std::uint16_t host_id, SessionId session_id,
                                      const protocol_v2::CoherenceFrame &unregister_request);

    RequestAdmissionResult admitRequest(SessionId session_id, const protocol_v2::CoherenceFrame &request);
    PinResponseResult pinResponse(SessionId session_id, const protocol_v2::CoherenceFrame &request,
                                  const protocol_v2::CoherenceFrame &response);
    bool acknowledgeResponses(SessionId session_id, std::uint64_t highest_contiguous_consumed);
    std::uint64_t responseWatermark(SessionId session_id) const;
    std::uint64_t replayFloor(SessionId session_id) const;
    std::vector<std::uint64_t> pinnedResponseIds(SessionId session_id) const;

    bool addCleanHolder(SessionId session_id, std::uint64_t line_address);
    bool removeCleanHolder(SessionId session_id, std::uint64_t line_address);
    bool addModifiedHolder(SessionId session_id, std::uint64_t line_address);
    bool removeModifiedHolder(SessionId session_id, std::uint64_t line_address);
    std::vector<std::uint64_t> cleanHolders(SessionId session_id) const;
    std::vector<std::uint64_t> modifiedHolders(SessionId session_id) const;

    std::optional<SessionSnapshot> inspect(SessionId session_id) const;

private:
    struct PinnedResponse {
        protocol_v2::CoherenceFrame request;
        protocol_v2::CoherenceFrame response;
    };

    struct Session {
        Session(SessionId session_id, std::uint16_t host, std::uint64_t negotiated_capabilities, std::uint32_t capacity,
                std::uint16_t ways, std::string transport, ResponseSender response_sender);

        SessionId id{};
        std::uint16_t host_id{};
        SessionState state{SessionState::Active};
        std::uint64_t capabilities{};
        std::uint32_t cache_capacity{};
        std::uint16_t cache_ways{};
        std::string transport_name;
        ResponseSender sender;
        std::uint64_t response_watermark{};
        std::uint64_t published_response_watermark{};
        bool closed_final_response_pinned{};
        std::optional<protocol_v2::CoherenceFrame> close_request;
        std::uint64_t binding_generation{};
        std::unordered_map<std::uint64_t, std::size_t> in_flight_deliveries;
        std::map<std::uint64_t, protocol_v2::CoherenceFrame> admitted_requests;
        std::map<std::uint64_t, PinnedResponse> pinned_responses;
        std::optional<std::uint64_t> unregister_request_id;
        std::uint64_t next_request_id{1};
        std::uint64_t publication_cursor{1};
        bool publishing{};
        std::uint64_t publisher_generation{};
        std::set<std::uint64_t> clean_holders;
        std::set<std::uint64_t> modified_holders;
    };

    bool validHolderSession(const Session &session) const noexcept;
    bool validOrdinaryRequest(const Session &session, const protocol_v2::CoherenceFrame &request) const noexcept;
    bool beginDrainLocked(Session &session, std::uint64_t &generation, ResponseSender &sender,
                          ResponseSender staged_sender = {});
    bool drainResponses(const std::shared_ptr<Session> &session, std::uint64_t generation,
                        const ResponseSender &sender);
    void retireFailedBinding(const std::shared_ptr<Session> &session, std::uint64_t generation);
    void waitForRetiredGeneration(std::unique_lock<std::mutex> &lock, const Session &session, std::uint64_t generation);
    static bool aligned(std::uint64_t line_address) noexcept;
    RegistrationResult resultFor(const Session &session, protocol_v2::Status status) const;
    bool validRegistration(const RegistrationRequest &request) const noexcept;

    const std::uint16_t max_hosts_;
    const std::size_t max_pinned_responses_per_session_;
    mutable std::mutex mutex_;
    std::condition_variable delivery_finished_;
    SessionId next_session_id_{1};
    std::unordered_map<SessionId, std::shared_ptr<Session>> sessions_;
    std::unordered_map<std::uint16_t, SessionId> host_sessions_;
};

} // namespace cxlmemsim
