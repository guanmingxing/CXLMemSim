#include "endpoint_session_registry.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace cxlmemsim {
namespace {
constexpr std::uint64_t kModelSnoop = static_cast<std::uint64_t>(protocol_v2::Capability::MODEL_SNOOP);

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

EndpointSessionRegistry::Session::Session(SessionId session_id, std::uint16_t host,
                                          std::uint64_t negotiated_capabilities, std::uint32_t capacity,
                                          std::uint16_t ways, std::string transport, ResponseSender response_sender)
    : id(session_id), host_id(host), capabilities(negotiated_capabilities), cache_capacity(capacity), cache_ways(ways),
      transport_name(std::move(transport)), sender(std::move(response_sender)) {}

EndpointSessionRegistry::EndpointSessionRegistry(std::uint16_t max_hosts, std::size_t limit)
    : max_hosts_(std::min(max_hosts, protocol_v2::kMaximumHosts)),
      max_pinned_responses_per_session_(limit == 0 ? 1 : limit) {}

RegistrationResult EndpointSessionRegistry::registerEndpoint(const RegistrationRequest &request) {
    std::shared_ptr<Session> session;
    RegistrationResult result;
    std::uint64_t generation{};
    ResponseSender sender;
    {
        std::lock_guard lock(mutex_);
        if (request.host_id >= max_hosts_)
            return {.status = protocol_v2::Status::InvalidState};
        if ((request.capabilities & ~protocol_v2::kKnownCapabilities) != 0 || (request.capabilities & kModelSnoop) == 0)
            return {.status = protocol_v2::Status::NoCapability};
        if (!validRegistration(request))
            return {.status = protocol_v2::Status::InvalidState};

        if (request.requested_session_id == 0) {
            const auto host = host_sessions_.find(request.host_id);
            if (host != host_sessions_.end()) {
                const auto old = sessions_.at(host->second);
                if (old->state != SessionState::Closed || !old->closed_final_response_pinned ||
                    !old->pinned_responses.empty())
                    return {.status = protocol_v2::Status::DuplicateHost};
                sessions_.erase(old->id);
                host_sessions_.erase(host);
            }
            if (next_session_id_ == 0 || next_session_id_ == std::numeric_limits<SessionId>::max())
                return {.status = protocol_v2::Status::InvalidState};
            const auto id = next_session_id_;
            ++next_session_id_;
            session = std::make_shared<Session>(
                id, request.host_id, request.capabilities & protocol_v2::kSupportedCapabilities, request.cache_capacity,
                request.cache_ways, request.transport_name, request.sender);
            sessions_.emplace(id, session);
            host_sessions_.emplace(request.host_id, id);
            return resultFor(*session, protocol_v2::Status::Ok);
        }

        const auto found = sessions_.find(request.requested_session_id);
        if (found == sessions_.end())
            return {.status = protocol_v2::Status::StaleSession};
        session = found->second;
        if (session->state == SessionState::Active && session->host_id == request.host_id)
            return resultFor(*session, protocol_v2::Status::DuplicateHost);
        if (session->state != SessionState::OfflineRetained || session->host_id != request.host_id ||
            session->capabilities != request.capabilities || session->cache_capacity != request.cache_capacity ||
            session->cache_ways != request.cache_ways)
            return {.status = protocol_v2::Status::StaleSession};
        if (session->binding_generation == std::numeric_limits<std::uint64_t>::max())
            return {.status = protocol_v2::Status::InvalidState};
        ++session->binding_generation;
        generation = session->binding_generation;
        session->state = SessionState::Active;
        session->transport_name = request.transport_name;
        session->sender = request.sender;
        session->publication_cursor = session->response_watermark + 1;
        result = resultFor(*session, protocol_v2::Status::Ok);
        (void)beginDrainLocked(*session, generation, sender);
    }
    if (sender && !drainResponses(session, generation, sender)) {
        result.status = protocol_v2::Status::IoError;
    }
    return result;
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

bool EndpointSessionRegistry::disconnectAbruptly(std::uint16_t host_id, SessionId session_id) {
    std::unique_lock lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (found == sessions_.end() || found->second->host_id != host_id || found->second->state != SessionState::Active)
        return false;
    auto &session = *found->second;
    if (session.binding_generation == std::numeric_limits<std::uint64_t>::max())
        return false;
    const auto retired = session.binding_generation;
    session.state = SessionState::OfflineRetained;
    session.sender = {};
    session.transport_name.clear();
    session.publishing = false;
    ++session.binding_generation;
    waitForRetiredGeneration(lock, session, retired);
    return true;
}

protocol_v2::Status EndpointSessionRegistry::gracefulClose(std::uint16_t host_id, SessionId session_id,
                                                           const protocol_v2::CoherenceFrame &unregister_request) {
    std::unique_lock lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (found == sessions_.end() || found->second->host_id != host_id)
        return protocol_v2::Status::StaleSession;
    auto &session = *found->second;
    const auto id = protocol_v2::requestId(unregister_request);
    const auto admitted = session.admitted_requests.find(id);
    if (session.state != SessionState::Active || !session.clean_holders.empty() || !session.modified_holders.empty() ||
        protocol_v2::opcode(unregister_request) != protocol_v2::Opcode::Unregister ||
        admitted == session.admitted_requests.end() || !sameFrame(admitted->second, unregister_request) ||
        session.binding_generation == std::numeric_limits<std::uint64_t>::max())
        return protocol_v2::Status::InvalidState;
    const auto retired = session.binding_generation;
    ++session.binding_generation;
    session.state = SessionState::Closed;
    session.close_request = unregister_request;
    session.publishing = false;
    waitForRetiredGeneration(lock, session, retired);
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

RequestAdmissionResult EndpointSessionRegistry::admitRequest(SessionId session_id,
                                                             const protocol_v2::CoherenceFrame &request) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (found == sessions_.end())
        return RequestAdmissionResult::SessionUnavailable;
    auto &session = *found->second;
    if (session.state != SessionState::Active || !validOrdinaryRequest(session, request))
        return RequestAdmissionResult::InvalidRequest;
    const auto id = protocol_v2::requestId(request);
    if (const auto existing = session.admitted_requests.find(id); existing != session.admitted_requests.end())
        return sameFrame(existing->second, request) ? RequestAdmissionResult::Duplicate
                                                    : RequestAdmissionResult::Conflict;
    if (id < session.next_request_id || id <= session.response_watermark)
        return RequestAdmissionResult::StaleRequest;
    if (id != session.next_request_id)
        return RequestAdmissionResult::InvalidRequest;
    if (session.admitted_requests.size() >= max_pinned_responses_per_session_)
        return RequestAdmissionResult::Backpressure;
    session.admitted_requests.emplace(id, request);
    if (session.next_request_id == std::numeric_limits<std::uint64_t>::max())
        return RequestAdmissionResult::InvalidRequest;
    ++session.next_request_id;
    return RequestAdmissionResult::Accepted;
}

bool EndpointSessionRegistry::beginDrainLocked(Session &session, std::uint64_t &generation, ResponseSender &sender) {
    const bool deliverable_state = session.state == SessionState::Active || session.state == SessionState::Closed;
    if (!deliverable_state || !session.sender || session.publishing ||
        !session.pinned_responses.contains(session.publication_cursor))
        return false;
    generation = session.binding_generation;
    sender = session.sender;
    session.publishing = true;
    session.publisher_generation = generation;
    return true;
}

void EndpointSessionRegistry::retireFailedBinding(const std::shared_ptr<Session> &session, std::uint64_t generation) {
    std::lock_guard lock(mutex_);
    if (session->binding_generation != generation)
        return;
    if (session->binding_generation != std::numeric_limits<std::uint64_t>::max())
        ++session->binding_generation;
    session->state = SessionState::OfflineRetained;
    session->sender = {};
    session->transport_name.clear();
    session->publishing = false;
}

bool EndpointSessionRegistry::drainResponses(const std::shared_ptr<Session> &session, std::uint64_t generation,
                                             const ResponseSender &sender) {
    for (;;) {
        protocol_v2::CoherenceFrame frame;
        {
            std::lock_guard lock(mutex_);
            if (session->binding_generation != generation ||
                (session->state != SessionState::Active && session->state != SessionState::Closed) ||
                !session->publishing || session->publisher_generation != generation) {
                return true;
            }
            const auto found = session->pinned_responses.find(session->publication_cursor);
            if (found == session->pinned_responses.end()) {
                session->publishing = false;
                return true;
            }
            frame = found->second.response;
            ++session->in_flight_deliveries[generation];
        }
        delivery_stack.push_back({this, session->id, generation});
        bool delivered;
        try {
            delivered = sender(frame);
        } catch (...) {
            delivery_stack.pop_back();
            {
                std::lock_guard lock(mutex_);
                auto found = session->in_flight_deliveries.find(generation);
                if (found != session->in_flight_deliveries.end() && --found->second == 0)
                    session->in_flight_deliveries.erase(found);
                delivery_finished_.notify_all();
            }
            retireFailedBinding(session, generation);
            throw;
        }
        delivery_stack.pop_back();
        {
            std::lock_guard lock(mutex_);
            auto found = session->in_flight_deliveries.find(generation);
            if (found != session->in_flight_deliveries.end() && --found->second == 0)
                session->in_flight_deliveries.erase(found);
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
    ResponseSender sender;
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
        if (const auto existing = session->pinned_responses.find(id); existing != session->pinned_responses.end())
            return sameFrame(existing->second.request, request) && sameFrame(existing->second.response, response_frame)
                       ? PinResponseResult::Duplicate
                       : PinResponseResult::Conflict;
        if (session->state == SessionState::Closed &&
            (!session->close_request || !sameFrame(*session->close_request, request) ||
             session->closed_final_response_pinned))
            return PinResponseResult::InvalidResponse;
        if (session->state != SessionState::Active && session->state != SessionState::OfflineRetained &&
            session->state != SessionState::Closed)
            return PinResponseResult::SessionUnavailable;
        session->pinned_responses.emplace(id, PinnedResponse{request, response_frame});
        if (session->state == SessionState::Closed)
            session->closed_final_response_pinned = true;
        (void)beginDrainLocked(*session, generation, sender);
    }
    if (sender)
        (void)drainResponses(session, generation, sender);
    return result;
}

bool EndpointSessionRegistry::acknowledgeResponses(SessionId session_id, std::uint64_t consumed) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(session_id);
    if (found == sessions_.end())
        return false;
    auto &session = *found->second;
    if (consumed <= session.response_watermark)
        return true;
    if (consumed == std::numeric_limits<std::uint64_t>::max())
        return false;
    std::uint64_t expected = session.response_watermark + 1;
    while (expected <= consumed) {
        if (!session.pinned_responses.contains(expected))
            return false;
        if (expected == std::numeric_limits<std::uint64_t>::max())
            return false;
        ++expected;
    }
    session.response_watermark = consumed;
    session.pinned_responses.erase(session.pinned_responses.begin(), session.pinned_responses.upper_bound(consumed));
    session.admitted_requests.erase(session.admitted_requests.begin(), session.admitted_requests.upper_bound(consumed));
    if (session.state == SessionState::Closed && session.closed_final_response_pinned &&
        session.pinned_responses.empty()) {
        session.sender = {};
        session.transport_name.clear();
    }
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
    return true;
}
bool EndpointSessionRegistry::removeModifiedHolder(SessionId id, std::uint64_t line) {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    return found != sessions_.end() && validHolderSession(*found->second) &&
           found->second->modified_holders.erase(line) != 0;
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

std::optional<SessionSnapshot> EndpointSessionRegistry::inspect(SessionId id) const {
    std::lock_guard lock(mutex_);
    const auto found = sessions_.find(id);
    if (found == sessions_.end())
        return std::nullopt;
    const auto &s = *found->second;
    return SessionSnapshot{s.id,
                           s.host_id,
                           s.state,
                           s.capabilities,
                           s.cache_capacity,
                           s.cache_ways,
                           s.transport_name,
                           static_cast<bool>(s.sender),
                           s.response_watermark,
                           s.response_watermark == std::numeric_limits<std::uint64_t>::max() ? s.response_watermark
                                                                                             : s.response_watermark + 1,
                           s.pinned_responses.size(),
                           max_pinned_responses_per_session_,
                           s.admitted_requests.size() >= max_pinned_responses_per_session_,
                           s.closed_final_response_pinned};
}

bool EndpointSessionRegistry::validHolderSession(const Session &s) const noexcept {
    return s.state == SessionState::Active || s.state == SessionState::OfflineRetained ||
           s.state == SessionState::Fenced;
}
bool EndpointSessionRegistry::aligned(std::uint64_t line) noexcept { return line % protocol_v2::kLineSize == 0; }
RegistrationResult EndpointSessionRegistry::resultFor(const Session &s, protocol_v2::Status status) const {
    return {status,
            s.id,
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
} // namespace cxlmemsim
