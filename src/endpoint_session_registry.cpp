#include "endpoint_session_registry.h"

#include <algorithm>
#include <limits>
#include <new>
#include <utility>

namespace cxlmemsim {
namespace {
constexpr std::uint64_t kModelSnoop = static_cast<std::uint64_t>(protocol_v2::Capability::MODEL_SNOOP);

#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
thread_local std::optional<endpoint_session_registry_test::FailurePoint> pending_failure;

void failIfRequested(endpoint_session_registry_test::FailurePoint point) {
    if (pending_failure == point) {
        pending_failure.reset();
        throw std::bad_alloc();
    }
}
#endif

struct DeliveryContext {
    const EndpointSessionRegistry *registry;
    SessionId session_id;
    std::uint64_t generation;
};
thread_local std::vector<DeliveryContext> delivery_stack;

bool sameFrame(const protocol_v2::CoherenceFrame &a, const protocol_v2::CoherenceFrame &b) noexcept {
    return protocol_v2::encodeFrame(a) == protocol_v2::encodeFrame(b);
}
} // namespace

#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
namespace endpoint_session_registry_test {
void failNext(FailurePoint point) noexcept { pending_failure = point; }
} // namespace endpoint_session_registry_test
#endif

EndpointSessionRegistry::Session::Session(SessionId session_id, BindingId binding, std::uint16_t host,
                                          std::uint64_t negotiated_capabilities, std::uint32_t capacity,
                                          std::uint16_t ways, std::string transport,
                                          StoredResponseSender response_sender)
    : id(session_id), binding_id(binding), host_id(host), capabilities(negotiated_capabilities),
      cache_capacity(capacity), cache_ways(ways), transport_name(std::move(transport)),
      sender(std::move(response_sender)) {}

EndpointSessionRegistry::EndpointSessionRegistry(std::uint16_t max_hosts, std::size_t limit)
    : max_hosts_(std::min(max_hosts, protocol_v2::kMaximumHosts)),
      max_pinned_responses_per_session_(limit == 0 ? 1 : limit) {}

RegistrationResult EndpointSessionRegistry::registerEndpoint(const RegistrationRequest &request) {
    if (request.host_id >= max_hosts_)
        return {.status = protocol_v2::Status::InvalidState};
    if ((request.capabilities & ~protocol_v2::kKnownCapabilities) != 0 || (request.capabilities & kModelSnoop) == 0)
        return {.status = protocol_v2::Status::NoCapability};
    if (!validRegistration(request))
        return {.status = protocol_v2::Status::InvalidState};

    if (request.requested_session_id == 0) {
        for (;;) {
            std::shared_ptr<Session> expected_old;
            SessionId id;
            BindingId binding_id;
            {
                std::lock_guard lock(mutex_);
                const auto host = host_sessions_.find(request.host_id);
                if (host != host_sessions_.end()) {
                    expected_old = sessions_.at(host->second);
                    if (expected_old->state != SessionState::Closed || !expected_old->closed_final_response_pinned ||
                        !expected_old->pinned_responses.empty())
                        return {.status = protocol_v2::Status::DuplicateHost};
                }
                if (next_session_id_ == 0 || next_session_id_ == std::numeric_limits<SessionId>::max() ||
                    next_binding_id_ == 0 || next_binding_id_ == std::numeric_limits<std::uint64_t>::max())
                    return {.status = protocol_v2::Status::InvalidState};
                id = next_session_id_;
                binding_id = BindingId{next_binding_id_};
            }

            auto staged_sender = copySender(request.sender);
            std::string staged_transport = request.transport_name;
            auto session = std::make_shared<Session>(
                id, binding_id, request.host_id, request.capabilities & protocol_v2::kSupportedCapabilities,
                request.cache_capacity, request.cache_ways, std::move(staged_transport), std::move(staged_sender));
            std::shared_ptr<Session> retired_session;
            RegistrationResult result;
            bool retry = false;
            {
                std::lock_guard lock(mutex_);
                const auto host = host_sessions_.find(request.host_id);
                const bool host_changed =
                    (!expected_old && host != host_sessions_.end()) ||
                    (expected_old &&
                     (host == host_sessions_.end() || sessions_.at(host->second) != expected_old ||
                      expected_old->state != SessionState::Closed || !expected_old->closed_final_response_pinned ||
                      !expected_old->pinned_responses.empty()));
                if (host_changed || next_session_id_ != id || next_binding_id_ != binding_id.value_) {
                    retry = true;
                } else {
#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
                    failIfRequested(endpoint_session_registry_test::FailurePoint::SessionIndexInsertion);
#endif
                    const auto [session_position, session_inserted] = sessions_.emplace(id, session);
                    if (!session_inserted) {
                        retry = true;
                    } else {
                        try {
                            if (host == host_sessions_.end()) {
#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
                                failIfRequested(endpoint_session_registry_test::FailurePoint::HostIndexInsertion);
#endif
                                const auto [unused, host_inserted] = host_sessions_.emplace(request.host_id, id);
                                (void)unused;
                                if (!host_inserted) {
                                    sessions_.erase(session_position);
                                    retry = true;
                                }
                            } else {
                                host->second = id;
                            }
                        } catch (...) {
                            sessions_.erase(session_position);
                            throw;
                        }
                        if (!retry) {
                            if (expected_old) {
                                retired_session = expected_old;
                                sessions_.erase(expected_old->id);
                            }
                            ++next_session_id_;
                            ++next_binding_id_;
                            result = resultFor(*session, protocol_v2::Status::Ok);
                        }
                    }
                }
            }
            if (!retry)
                return result;
        }
    }

    for (;;) {
        std::shared_ptr<Session> session;
        SessionState observed_state;
        std::uint64_t observed_generation;
        std::uint64_t staged_binding_value;
        std::uint64_t staged_publication_cursor;
        bool stage_drain;
        {
            std::lock_guard lock(mutex_);
            const auto found = sessions_.find(request.requested_session_id);
            if (found == sessions_.end())
                return {.status = protocol_v2::Status::StaleSession};
            session = found->second;
            if ((session->state == SessionState::Active ||
                 (session->state == SessionState::Closed && session->sender)) &&
                session->host_id == request.host_id)
                return resultFor(*session, protocol_v2::Status::DuplicateHost);
            const bool closed_replay_pending =
                session->state == SessionState::Closed && !session->sender &&
                (!session->closed_final_response_pinned || !session->pinned_responses.empty());
            if ((session->state != SessionState::OfflineRetained && !closed_replay_pending) ||
                session->host_id != request.host_id || session->capabilities != request.capabilities ||
                session->cache_capacity != request.cache_capacity || session->cache_ways != request.cache_ways)
                return {.status = protocol_v2::Status::StaleSession};
            if (session->binding_generation == std::numeric_limits<std::uint64_t>::max() || next_binding_id_ == 0 ||
                next_binding_id_ == std::numeric_limits<std::uint64_t>::max())
                return {.status = protocol_v2::Status::InvalidState};
            observed_state = session->state;
            observed_generation = session->binding_generation;
            staged_binding_value = next_binding_id_;
            staged_publication_cursor = session->response_watermark + 1;
            stage_drain =
                request.sender && !session->publishing && session->pinned_responses.contains(staged_publication_cursor);
        }

        auto staged_sender = copySender(request.sender);
        std::string staged_transport = request.transport_name;
        StoredResponseSender staged_drain_sender;
        if (stage_drain)
            staged_drain_sender = copySender(*staged_sender);

        StoredResponseSender sender;
        StoredResponseSender retired_sender;
        RegistrationResult result;
        std::uint64_t generation{};
        bool retry = false;
        {
            std::lock_guard lock(mutex_);
            const auto found = sessions_.find(request.requested_session_id);
            const bool state_changed =
                found == sessions_.end() || found->second != session || session->state != observed_state ||
                session->binding_generation != observed_generation || next_binding_id_ != staged_binding_value ||
                staged_publication_cursor != session->response_watermark + 1 ||
                stage_drain != (request.sender && !session->publishing &&
                                session->pinned_responses.contains(staged_publication_cursor));
            if (state_changed) {
                retry = true;
            } else {
                ++session->binding_generation;
                publishBindingGeneration(*session);
                generation = session->binding_generation;
                session->binding_id = BindingId{staged_binding_value};
                ++next_binding_id_;
                if (session->state == SessionState::OfflineRetained)
                    session->state = SessionState::Active;
                session->transport_name = std::move(staged_transport);
                retired_sender = std::move(session->sender);
                session->sender = std::move(staged_sender);
                session->publication_cursor = staged_publication_cursor;
                result = resultFor(*session, protocol_v2::Status::Ok);
                if (stage_drain)
                    (void)beginDrainLocked(*session, generation, sender, std::move(staged_drain_sender));
            }
        }
        if (retry)
            continue;
        try {
            if (sender && !drainResponses(session, generation, sender)) {
                result.status = protocol_v2::Status::IoError;
                result.binding_id = {};
            }
        } catch (...) {
            retireFailedBinding(session, generation);
            throw;
        }
        return result;
    }
}

std::size_t localDeliveryCount(const EndpointSessionRegistry *registry, SessionId session_id,
                               std::uint64_t generation) {
    return static_cast<std::size_t>(std::count_if(delivery_stack.begin(), delivery_stack.end(), [&](const auto &entry) {
        return entry.registry == registry && entry.session_id == session_id && entry.generation == generation;
    }));
}

void EndpointSessionRegistry::waitForRetiredGeneration(std::unique_lock<std::mutex> &lock, const Session &session,
                                                       std::uint64_t generation) {
    const auto local = localDeliveryCount(this, session.id, generation);
    delivery_finished_.wait(lock, [&] {
        const auto found = session.in_flight_deliveries.find(generation);
        return found == session.in_flight_deliveries.end() || found->second <= local;
    });
}

bool EndpointSessionRegistry::disconnectAbruptly(std::uint16_t host_id, SessionId session_id, BindingId binding_id) {
    StoredResponseSender retired_sender;
    std::shared_ptr<Session> session;
    std::unique_lock lock(mutex_);
    const auto found = sessions_.find(session_id);
    const bool closed_replay_pending =
        found != sessions_.end() && found->second->state == SessionState::Closed &&
        (!found->second->closed_final_response_pinned || !found->second->pinned_responses.empty());
    if (!binding_id || found == sessions_.end() || found->second->host_id != host_id ||
        found->second->binding_id != binding_id ||
        (found->second->state != SessionState::Active && !closed_replay_pending))
        return false;
    session = found->second;
    if (session->binding_generation == std::numeric_limits<std::uint64_t>::max())
        return false;
    const auto retired = session->binding_generation;
    if (session->state == SessionState::Active)
        session->state = SessionState::OfflineRetained;
    retired_sender = std::move(session->sender);
    session->binding_id = {};
    session->transport_name.clear();
    session->publishing = false;
    ++session->binding_generation;
    publishBindingGeneration(*session);
    waitForRetiredGeneration(lock, *session, retired);
    return true;
}

protocol_v2::Status EndpointSessionRegistry::gracefulClose(std::uint16_t host_id, SessionId session_id,
                                                           BindingId binding_id,
                                                           const protocol_v2::CoherenceFrame &unregister_request) {
    std::shared_ptr<Session> session;
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (!binding_id || found == sessions_.end() || found->second->host_id != host_id ||
        found->second->binding_id != binding_id)
        return protocol_v2::Status::StaleSession;
    session = found->second;
    const auto id = protocol_v2::requestId(unregister_request);
    const auto admitted = session->admitted_requests.find(id);
    if (session->state != SessionState::Active || !session->clean_holders.empty() ||
        !session->modified_holders.empty() ||
        protocol_v2::opcode(unregister_request) != protocol_v2::Opcode::Unregister ||
        admitted == session->admitted_requests.end() || !sameFrame(admitted->second, unregister_request) ||
        session->unregister_request_id != id ||
        session->admitted_requests.upper_bound(id) != session->admitted_requests.end() ||
        session->binding_generation == std::numeric_limits<std::uint64_t>::max())
        return protocol_v2::Status::InvalidState;
    for (auto lower = session->admitted_requests.begin(); lower != admitted; ++lower) {
        if (!session->pinned_responses.contains(lower->first))
            return protocol_v2::Status::InvalidState;
    }
    session->state = SessionState::Closed;
    session->close_request = unregister_request;
    session->closed_final_response_pinned = session->pinned_responses.contains(id);
    return protocol_v2::Status::Ok;
}

bool EndpointSessionRegistry::validOrdinaryRequest(const Session &session,
                                                   const protocol_v2::CoherenceFrame &request) const noexcept {
    const auto op = protocol_v2::opcode(request);
    return protocol_v2::validateFrame(request) && protocol_v2::requestId(request) != 0 &&
           protocol_v2::requestId(request) != std::numeric_limits<std::uint64_t>::max() &&
           protocol_v2::sessionId(request) == session.id && protocol_v2::srcHost(request) == session.host_id &&
           protocol_v2::dstHost(request) == protocol_v2::kServerHost && op != protocol_v2::Opcode::Register &&
           op != protocol_v2::Opcode::Response && op != protocol_v2::Opcode::SnoopAck &&
           op != protocol_v2::Opcode::SnpInv && op != protocol_v2::Opcode::SnpDowngrade &&
           op != protocol_v2::Opcode::SnpDataInv && op != protocol_v2::Opcode::SnpDataDowngrade &&
           op != protocol_v2::Opcode::HostFence;
}

RequestAdmissionResult EndpointSessionRegistry::admitRequest(SessionId session_id, BindingId binding_id,
                                                             const protocol_v2::CoherenceFrame &request) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id)
        return RequestAdmissionResult::SessionUnavailable;
    auto &session = *found->second;
    const bool active = session.state == SessionState::Active;
    const bool fenced_drain =
        session.state == SessionState::Fenced && drainOpcodeAdmissible(protocol_v2::opcode(request));
    if ((!active && !fenced_drain) || !validOrdinaryRequest(session, request))
        return RequestAdmissionResult::InvalidRequest;
    const auto id = protocol_v2::requestId(request);
    if (const auto existing = session.admitted_requests.find(id); existing != session.admitted_requests.end())
        return sameFrame(existing->second, request) ? RequestAdmissionResult::Duplicate
                                                    : RequestAdmissionResult::Conflict;
    if (session.unregister_request_id)
        return RequestAdmissionResult::InvalidRequest;
    if (id < session.next_request_id || id <= session.response_watermark)
        return RequestAdmissionResult::StaleRequest;
    if (id != session.next_request_id)
        return RequestAdmissionResult::InvalidRequest;
    if (session.admitted_requests.size() >= max_pinned_responses_per_session_)
        return RequestAdmissionResult::Backpressure;
    session.admitted_requests.emplace(id, request);
    if (protocol_v2::opcode(request) == protocol_v2::Opcode::Unregister)
        session.unregister_request_id = id;
    if (session.next_request_id == std::numeric_limits<std::uint64_t>::max())
        return RequestAdmissionResult::InvalidRequest;
    ++session.next_request_id;
    return RequestAdmissionResult::Accepted;
}

bool EndpointSessionRegistry::beginDrainLocked(Session &session, std::uint64_t &generation,
                                               StoredResponseSender &sender, StoredResponseSender &&staged_sender) {
    const bool deliverable_state = session.state == SessionState::Active || session.state == SessionState::Fenced ||
                                   session.state == SessionState::Closed;
    if (!deliverable_state || !session.sender || !staged_sender || session.publishing ||
        !session.pinned_responses.contains(session.publication_cursor))
        return false;
    sender = std::move(staged_sender);
    generation = session.binding_generation;
    session.publishing = true;
    session.publisher_generation = generation;
    return true;
}

void EndpointSessionRegistry::retireFailedBinding(const std::shared_ptr<Session> &session, std::uint64_t generation) {
    StoredResponseSender retired_sender;
    {
        std::lock_guard lock(mutex_);
        if (session->binding_generation != generation)
            return;
        if (session->binding_generation != std::numeric_limits<std::uint64_t>::max())
            ++session->binding_generation;
        publishBindingGeneration(*session);
        if (session->state != SessionState::Closed)
            session->state = SessionState::OfflineRetained;
        retired_sender = std::move(session->sender);
        session->binding_id = {};
        session->transport_name.clear();
        session->publishing = false;
    }
}

EndpointSessionRegistry::StoredResponseSender EndpointSessionRegistry::reclaimResponsesLocked(Session &session,
                                                                                              std::uint64_t consumed) {
    session.response_watermark = consumed;
    session.pinned_responses.erase(session.pinned_responses.begin(), session.pinned_responses.upper_bound(consumed));
    session.admitted_requests.erase(session.admitted_requests.begin(), session.admitted_requests.upper_bound(consumed));
    if (session.state == SessionState::Closed && session.closed_final_response_pinned &&
        session.pinned_responses.empty()) {
        auto retired_sender = std::move(session.sender);
        session.binding_id = {};
        session.transport_name.clear();
        return retired_sender;
    }
    return {};
}

EndpointSessionRegistry::StoredResponseSender
EndpointSessionRegistry::finishDeliveryAttemptLocked(Session &session, std::uint64_t response_id, bool delivered) {
    const auto found = session.in_flight_response_deliveries.find(response_id);
    if (found != session.in_flight_response_deliveries.end() && --found->second == 0)
        session.in_flight_response_deliveries.erase(found);

    if (delivered) {
        session.published_response_watermark = std::max(session.published_response_watermark, response_id);
        if (session.pending_response_ack && *session.pending_response_ack <= session.published_response_watermark) {
            auto retired_sender = reclaimResponsesLocked(session, *session.pending_response_ack);
            session.pending_response_ack.reset();
            return retired_sender;
        }
    } else if (session.pending_response_ack && *session.pending_response_ack > session.published_response_watermark &&
               !session.in_flight_response_deliveries.contains(*session.pending_response_ack)) {
        session.pending_response_ack.reset();
    }
    return {};
}

bool EndpointSessionRegistry::drainResponses(const std::shared_ptr<Session> &session, std::uint64_t generation,
                                             const StoredResponseSender &sender) {
    for (;;) {
        protocol_v2::CoherenceFrame frame;
        {
            std::lock_guard lock(mutex_);
            if (session->binding_generation != generation ||
                (session->state != SessionState::Active && session->state != SessionState::Fenced &&
                 session->state != SessionState::Closed) ||
                !session->publishing || session->publisher_generation != generation) {
                return true;
            }
            const auto found = session->pinned_responses.find(session->publication_cursor);
            if (found == session->pinned_responses.end()) {
                session->publishing = false;
                return true;
            }
            frame = found->second.response;
            auto generation_delivery = session->in_flight_deliveries.end();
            bool delivery_context_pushed = false;
            try {
#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
                failIfRequested(endpoint_session_registry_test::FailurePoint::DrainDeliveryContextBookkeeping);
#endif
                delivery_stack.push_back({this, session->id, generation});
                delivery_context_pushed = true;
                generation_delivery = session->in_flight_deliveries.try_emplace(generation, 0).first;
                ++generation_delivery->second;
#ifdef CXLMEMSIM_ENDPOINT_SESSION_REGISTRY_TESTING
                failIfRequested(endpoint_session_registry_test::FailurePoint::DrainResponseBookkeeping);
#endif
                ++session->in_flight_response_deliveries.try_emplace(protocol_v2::requestId(frame), 0).first->second;
            } catch (...) {
                if (generation_delivery != session->in_flight_deliveries.end() && --generation_delivery->second == 0)
                    session->in_flight_deliveries.erase(generation_delivery);
                if (delivery_context_pushed)
                    delivery_stack.pop_back();
                session->publishing = false;
                throw;
            }
        }
        bool delivered;
        try {
            delivered = (*sender)(frame);
        } catch (...) {
            delivery_stack.pop_back();
            StoredResponseSender retired_sender;
            {
                std::lock_guard lock(mutex_);
                auto found = session->in_flight_deliveries.find(generation);
                if (found != session->in_flight_deliveries.end() && --found->second == 0)
                    session->in_flight_deliveries.erase(found);
                retired_sender = finishDeliveryAttemptLocked(*session, protocol_v2::requestId(frame), false);
                delivery_finished_.notify_all();
            }
            retireFailedBinding(session, generation);
            throw;
        }
        delivery_stack.pop_back();
        StoredResponseSender retired_sender;
        {
            std::lock_guard lock(mutex_);
            auto found = session->in_flight_deliveries.find(generation);
            if (found != session->in_flight_deliveries.end() && --found->second == 0)
                session->in_flight_deliveries.erase(found);
            retired_sender = finishDeliveryAttemptLocked(*session, protocol_v2::requestId(frame), delivered);
            delivery_finished_.notify_all();
            if (session->binding_generation != generation)
                return true;
            if (!delivered) {
                session->publishing = false;
            } else if (session->publication_cursor != std::numeric_limits<std::uint64_t>::max()) {
                ++session->publication_cursor;
            }
        }
        if (!delivered) {
            retireFailedBinding(session, generation);
            return false;
        }
    }
}

PinResponseResult EndpointSessionRegistry::pinResponse(SessionId session_id, const protocol_v2::CoherenceFrame &request,
                                                       const protocol_v2::CoherenceFrame &response_frame) {
    std::shared_ptr<Session> session;
    std::uint64_t generation{};
    std::uint64_t observed_generation{};
    StoredResponseSender sender_to_copy;
    PinResponseResult result = PinResponseResult::Pinned;
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(session_id);
        if (found == sessions_.end())
            return PinResponseResult::SessionUnavailable;
        session = found->second;
        const auto id = protocol_v2::requestId(request);
        if (!validOrdinaryRequest(*session, request) || !protocol_v2::validateResponse(response_frame, request))
            return PinResponseResult::InvalidResponse;
        const auto admitted = session->admitted_requests.find(id);
        if (admitted == session->admitted_requests.end())
            return id <= session->response_watermark ? PinResponseResult::StaleRequest
                                                     : PinResponseResult::InvalidResponse;
        if (!sameFrame(admitted->second, request))
            return PinResponseResult::Conflict;
        if (protocol_v2::opcode(request) == protocol_v2::Opcode::Unregister && session->state != SessionState::Closed)
            return PinResponseResult::InvalidResponse;
        if (const auto existing = session->pinned_responses.find(id); existing != session->pinned_responses.end()) {
            if (!sameFrame(existing->second.request, request) || !sameFrame(existing->second.response, response_frame))
                return PinResponseResult::Conflict;
            result = PinResponseResult::Duplicate;
        } else {
            if (session->state == SessionState::Closed &&
                (!session->close_request || !sameFrame(*session->close_request, request) ||
                 session->closed_final_response_pinned))
                return PinResponseResult::InvalidResponse;
            if (session->state != SessionState::Active && session->state != SessionState::OfflineRetained &&
                session->state != SessionState::Fenced && session->state != SessionState::Closed)
                return PinResponseResult::SessionUnavailable;
            session->pinned_responses.emplace(id, PinnedResponse{request, response_frame});
            if (session->state == SessionState::Closed)
                session->closed_final_response_pinned = true;
        }
        const bool deliverable_state = session->state == SessionState::Active ||
                                       session->state == SessionState::Fenced || session->state == SessionState::Closed;
        if (deliverable_state && session->sender && !session->publishing &&
            session->pinned_responses.contains(session->publication_cursor)) {
            sender_to_copy = session->sender;
            observed_generation = session->binding_generation;
        }
    }
    completeOperationState(session->operations, protocol_v2::requestId(request));
    StoredResponseSender staged_sender;
    if (sender_to_copy)
        staged_sender = copySender(*sender_to_copy);
    StoredResponseSender sender;
    if (staged_sender) {
        std::lock_guard lock(mutex_);
        if (session->binding_generation == observed_generation && session->sender == sender_to_copy)
            (void)beginDrainLocked(*session, generation, sender, std::move(staged_sender));
    }
    if (sender)
        (void)drainResponses(session, generation, sender);
    return result;
}

bool EndpointSessionRegistry::acknowledgeResponses(SessionId session_id, BindingId binding_id, std::uint64_t consumed) {
    StoredResponseSender retired_sender;
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id)
        return false;
    auto &session = *found->second;
    if (consumed <= session.response_watermark)
        return true;
    if (consumed == std::numeric_limits<std::uint64_t>::max())
        return false;
    const bool speculative = consumed > session.published_response_watermark;
    if (speculative && session.pending_response_ack && consumed <= *session.pending_response_ack)
        return true;
    if (speculative && !session.in_flight_response_deliveries.contains(consumed))
        return false;
    std::uint64_t expected = session.response_watermark + 1;
    while (expected <= consumed) {
        if (!session.pinned_responses.contains(expected))
            return false;
        if (expected == std::numeric_limits<std::uint64_t>::max())
            return false;
        ++expected;
    }
    if (speculative) {
        session.pending_response_ack = consumed;
        return true;
    }
    retired_sender = reclaimResponsesLocked(session, consumed);
    return true;
}

std::uint64_t EndpointSessionRegistry::responseWatermark(SessionId id) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    return found == sessions_.end() ? 0 : found->second->response_watermark;
}
std::uint64_t EndpointSessionRegistry::replayFloor(SessionId id) const {
    const auto watermark = responseWatermark(id);
    return watermark == std::numeric_limits<std::uint64_t>::max() ? watermark : watermark + 1;
}
std::vector<std::uint64_t> EndpointSessionRegistry::pinnedResponseIds(SessionId id) const {
    std::lock_guard lock(mutex_);
    std::vector<std::uint64_t> ids;
    const auto found = sessions_.find(id);
    if (found != sessions_.end())
        for (const auto &[request_id, unused] : found->second->pinned_responses) {
            (void)unused;
            ids.push_back(request_id);
        }
    return ids;
}

void EndpointSessionRegistry::completeOperationState(const std::shared_ptr<Session::OperationState> &operations,
                                                     std::uint64_t request_id) {
    std::lock_guard lock(operations->mutex);
    if (request_id <= operations->completion_watermark)
        return;
    operations->completed_out_of_order.insert(request_id);
    while (operations->completion_watermark != std::numeric_limits<std::uint64_t>::max()) {
        const auto next = operations->completion_watermark + 1;
        if (operations->completed_out_of_order.erase(next) == 0)
            break;
        operations->completion_watermark = next;
    }
    operations->changed.notify_all();
}

void EndpointSessionRegistry::publishBindingGeneration(Session &session) {
    {
        std::lock_guard lock(session.operations->mutex);
        session.operations->binding_generation = session.binding_generation;
        session.operations->changed.notify_all();
    }
    {
        std::lock_guard lock(session.holder_drain->mutex);
        session.holder_drain->binding_generation = session.binding_generation;
        session.holder_drain->changed.notify_all();
    }
}

bool EndpointSessionRegistry::completeOperation(SessionId id, BindingId binding_id, std::uint64_t request_id) {
    std::shared_ptr<Session::OperationState> operations;
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(id);
        if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id ||
            !found->second->admitted_requests.contains(request_id))
            return false;
        operations = found->second->operations;
    }
    completeOperationState(operations, request_id);
    return true;
}

bool EndpointSessionRegistry::waitForOperationsBefore(SessionId id, BindingId binding_id,
                                                      std::uint64_t request_id) const {
    if (request_id == 0)
        return false;
    std::shared_ptr<Session::OperationState> operations;
    std::uint64_t generation{};
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(id);
        if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id ||
            !found->second->admitted_requests.contains(request_id))
            return false;
        operations = found->second->operations;
        generation = found->second->binding_generation;
    }
    std::unique_lock lock(operations->mutex);
    operations->changed.wait(lock, [&] {
        return operations->completion_watermark >= request_id - 1 || operations->binding_generation != generation;
    });
    return operations->binding_generation == generation;
}

std::uint64_t EndpointSessionRegistry::operationCompletionWatermark(SessionId id) const {
    std::shared_ptr<Session::OperationState> operations;
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(id);
        if (found == sessions_.end())
            return 0;
        operations = found->second->operations;
    }
    std::lock_guard lock(operations->mutex);
    return operations->completion_watermark;
}

bool EndpointSessionRegistry::admittedRequestHasOpcode(SessionId id, BindingId binding_id, std::uint64_t request_id,
                                                       protocol_v2::Opcode expected_opcode) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id)
        return false;
    const auto request = found->second->admitted_requests.find(request_id);
    return request != found->second->admitted_requests.end() && protocol_v2::opcode(request->second) == expected_opcode;
}

bool EndpointSessionRegistry::admittedRequestMatches(SessionId id, BindingId binding_id,
                                                     const protocol_v2::CoherenceFrame &expected_request) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id)
        return false;
    const auto request = found->second->admitted_requests.find(protocol_v2::requestId(expected_request));
    return request != found->second->admitted_requests.end() && sameFrame(request->second, expected_request);
}

bool EndpointSessionRegistry::waitForModifiedDrain(SessionId id, BindingId binding_id) const {
    std::shared_ptr<Session::HolderDrainState> holder_drain;
    std::uint64_t generation{};
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(id);
        if (!binding_id || found == sessions_.end() || found->second->binding_id != binding_id)
            return false;
        holder_drain = found->second->holder_drain;
        generation = found->second->binding_generation;
    }
    std::unique_lock lock(holder_drain->mutex);
    holder_drain->changed.wait(
        lock, [&] { return holder_drain->modified_count == 0 || holder_drain->binding_generation != generation; });
    return holder_drain->binding_generation == generation;
}

protocol_v2::Status EndpointSessionRegistry::fenceSession(std::uint16_t host_id, SessionId id, BindingId binding_id) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end() || found->second->host_id != host_id)
        return protocol_v2::Status::StaleSession;
    const auto &session = *found->second;
    const bool live_binding = binding_id && session.binding_id == binding_id &&
                              (session.state == SessionState::Active || session.state == SessionState::Fenced);
    const bool retained_offline =
        !binding_id && !session.binding_id &&
        (session.state == SessionState::OfflineRetained || session.state == SessionState::Fenced);
    if (!live_binding && !retained_offline)
        return protocol_v2::Status::InvalidState;
    found->second->state = SessionState::Fenced;
    return protocol_v2::Status::Ok;
}

bool EndpointSessionRegistry::completeEviction(std::uint16_t host_id, SessionId id, BindingId binding_id) {
    StoredResponseSender retired_sender;
    std::shared_ptr<Session> session;
    std::unique_lock lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end() || found->second->host_id != host_id)
        return false;
    session = found->second;
    const bool matching_binding =
        (binding_id && session->binding_id == binding_id) || (!binding_id && !session->binding_id);
    if (!matching_binding || session->state != SessionState::Fenced || !session->clean_holders.empty() ||
        !session->modified_holders.empty() || session->binding_generation == std::numeric_limits<std::uint64_t>::max())
        return false;

    const auto retired = session->binding_generation;
    ++session->binding_generation;
    publishBindingGeneration(*session);
    retired_sender = std::move(session->sender);
    session->binding_id = {};
    session->transport_name.clear();
    session->publishing = false;
    waitForRetiredGeneration(lock, *session, retired);
    session->state = SessionState::Closed;

    const auto host = host_sessions_.find(host_id);
    if (host == host_sessions_.end() || host->second != id)
        return false;
    host_sessions_.erase(host);
    sessions_.erase(found);
    return true;
}

bool EndpointSessionRegistry::drainOpcodeAdmissible(protocol_v2::Opcode opcode) const noexcept {
    return opcode == protocol_v2::Opcode::Putm || opcode == protocol_v2::Opcode::Fence ||
           opcode == protocol_v2::Opcode::Heartbeat || opcode == protocol_v2::Opcode::SnoopAck ||
           opcode == protocol_v2::Opcode::HostFence;
}

bool EndpointSessionRegistry::addCleanHolder(SessionId id, std::uint64_t line) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end() || !validHolderSession(*found->second) || !aligned(line) ||
        found->second->modified_holders.contains(line))
        return false;
    auto &session = *found->second;
    if (session.clean_holders.contains(line))
        return true;
    if (session.clean_holders.size() + session.modified_holders.size() >=
        session.cache_capacity / protocol_v2::kLineSize)
        return false;
    session.clean_holders.insert(line);
    return true;
}
bool EndpointSessionRegistry::removeCleanHolder(SessionId id, std::uint64_t line) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    return found != sessions_.end() && validHolderSession(*found->second) &&
           found->second->clean_holders.erase(line) != 0;
}
bool EndpointSessionRegistry::addModifiedHolder(SessionId id, std::uint64_t line) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end() || !validHolderSession(*found->second) || !aligned(line) ||
        found->second->clean_holders.contains(line))
        return false;
    auto &session = *found->second;
    if (session.modified_holders.contains(line))
        return true;
    if (session.clean_holders.size() + session.modified_holders.size() >=
        session.cache_capacity / protocol_v2::kLineSize)
        return false;
    session.modified_holders.insert(line);
    {
        std::lock_guard drain_lock(session.holder_drain->mutex);
        session.holder_drain->modified_count = session.modified_holders.size();
        session.holder_drain->changed.notify_all();
    }
    return true;
}
bool EndpointSessionRegistry::removeModifiedHolder(SessionId id, std::uint64_t line) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end() || !validHolderSession(*found->second) ||
        found->second->modified_holders.erase(line) == 0)
        return false;
    {
        std::lock_guard drain_lock(found->second->holder_drain->mutex);
        found->second->holder_drain->modified_count = found->second->modified_holders.size();
        found->second->holder_drain->changed.notify_all();
    }
    return true;
}
std::vector<std::uint64_t> EndpointSessionRegistry::cleanHolders(SessionId id) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    return found == sessions_.end()
               ? std::vector<std::uint64_t>{}
               : std::vector<std::uint64_t>(found->second->clean_holders.begin(), found->second->clean_holders.end());
}
std::vector<std::uint64_t> EndpointSessionRegistry::modifiedHolders(SessionId id) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    return found == sessions_.end() ? std::vector<std::uint64_t>{}
                                    : std::vector<std::uint64_t>(found->second->modified_holders.begin(),
                                                                 found->second->modified_holders.end());
}

HolderSnapshot EndpointSessionRegistry::holderSnapshot(SessionId id) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end())
        return {};
    return {{found->second->clean_holders.begin(), found->second->clean_holders.end()},
            {found->second->modified_holders.begin(), found->second->modified_holders.end()}};
}

std::optional<SessionSnapshot> EndpointSessionRegistry::inspect(SessionId id) const {
    std::shared_ptr<Session::OperationState> operations;
    SessionSnapshot snapshot;
    {
        std::lock_guard lock(mutex_);
        const auto found = sessions_.find(id);
        if (found == sessions_.end())
            return std::nullopt;
        const auto &s = *found->second;
        operations = s.operations;
        snapshot = SessionSnapshot{s.id,
                                   s.binding_id,
                                   s.host_id,
                                   s.state,
                                   s.capabilities,
                                   s.cache_capacity,
                                   s.cache_ways,
                                   s.transport_name,
                                   static_cast<bool>(s.sender),
                                   s.response_watermark,
                                   s.response_watermark == std::numeric_limits<std::uint64_t>::max()
                                       ? s.response_watermark
                                       : s.response_watermark + 1,
                                   s.pinned_responses.size(),
                                   max_pinned_responses_per_session_,
                                   s.admitted_requests.size() >= max_pinned_responses_per_session_,
                                   s.closed_final_response_pinned,
                                   0};
    }
    std::lock_guard operation_lock(operations->mutex);
    snapshot.operation_completion_watermark = operations->completion_watermark;
    return snapshot;
}

bool EndpointSessionRegistry::validHolderSession(const Session &s) const noexcept {
    return s.state == SessionState::Active || s.state == SessionState::OfflineRetained ||
           s.state == SessionState::Fenced;
}
bool EndpointSessionRegistry::aligned(std::uint64_t line) noexcept { return line % protocol_v2::kLineSize == 0; }
RegistrationResult EndpointSessionRegistry::resultFor(const Session &s, protocol_v2::Status status) const {
    return {status,
            s.id,
            status == protocol_v2::Status::Ok ? s.binding_id : BindingId{},
            s.capabilities,
            s.cache_capacity,
            s.cache_ways,
            static_cast<std::uint16_t>(protocol_v2::kLineSize),
            protocol_v2::AckStrength::MODEL};
}
bool EndpointSessionRegistry::validRegistration(const RegistrationRequest &r) const noexcept {
    return r.cache_capacity >= protocol_v2::kLineSize && r.cache_ways != 0 &&
           r.cache_capacity % protocol_v2::kLineSize == 0 &&
           (r.cache_capacity / protocol_v2::kLineSize) % r.cache_ways == 0;
}
EndpointSessionRegistry::StoredResponseSender EndpointSessionRegistry::copySender(const ResponseSender &sender) {
    return sender ? std::make_shared<const ResponseSender>(sender) : nullptr;
}
} // namespace cxlmemsim
