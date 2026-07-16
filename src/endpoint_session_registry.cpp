#include "endpoint_session_registry.h"

#include <limits>
#include <utility>

namespace cxlmemsim {

namespace {
constexpr std::uint64_t kModelSnoop = static_cast<std::uint64_t>(protocol_v2::Capability::MODEL_SNOOP);
}

EndpointSessionRegistry::EndpointSessionRegistry(std::uint16_t max_hosts, std::size_t max_pinned_responses_per_session)
    : max_hosts_(max_hosts <= protocol_v2::kMaximumHosts ? max_hosts : protocol_v2::kMaximumHosts),
      max_pinned_responses_per_session_(max_pinned_responses_per_session == 0 ? 1 : max_pinned_responses_per_session) {}

RegistrationResult EndpointSessionRegistry::registerEndpoint(const RegistrationRequest &request) {
    std::vector<protocol_v2::CoherenceFrame> replay;
    ResponseSender sender;
    RegistrationResult result;
    {
        std::lock_guard lock(mutex_);
        if (request.host_id >= max_hosts_) {
            return {.status = protocol_v2::Status::InvalidState};
        }
        if ((request.capabilities & kModelSnoop) == 0) {
            return {.status = protocol_v2::Status::NoCapability};
        }
        if (request.cache_capacity == 0 || request.cache_ways == 0) {
            return {.status = protocol_v2::Status::InvalidState};
        }

        if (request.requested_session_id == 0) {
            const auto host = host_sessions_.find(request.host_id);
            if (host != host_sessions_.end() && sessions_.at(host->second).state != SessionState::Closed) {
                return {.status = protocol_v2::Status::DuplicateHost};
            }
            if (next_session_id_ == 0 || next_session_id_ == std::numeric_limits<SessionId>::max()) {
                return {.status = protocol_v2::Status::InvalidState};
            }
            const auto id = next_session_id_++;
            Session session{id,
                            request.host_id,
                            SessionState::Active,
                            request.capabilities,
                            request.cache_capacity,
                            request.cache_ways,
                            request.transport_name,
                            request.sender};
            auto [position, inserted] = sessions_.emplace(id, std::move(session));
            (void)inserted;
            host_sessions_[request.host_id] = id;
            return resultFor(position->second, protocol_v2::Status::Ok);
        }

        const auto position = sessions_.find(request.requested_session_id);
        if (position == sessions_.end()) {
            return {.status = protocol_v2::Status::StaleSession};
        }
        auto &session = position->second;
        if (session.state == SessionState::Active && session.host_id == request.host_id) {
            return resultFor(session, protocol_v2::Status::DuplicateHost);
        }
        if (session.state != SessionState::OfflineRetained || session.host_id != request.host_id ||
            session.capabilities != request.capabilities || session.cache_capacity != request.cache_capacity ||
            session.cache_ways != request.cache_ways) {
            return {.status = protocol_v2::Status::StaleSession};
        }
        session.state = SessionState::Active;
        session.transport_name = request.transport_name;
        session.sender = request.sender;
        sender = session.sender;
        for (const auto &[request_id, frame] : session.pinned_responses) {
            (void)request_id;
            replay.push_back(frame);
        }
        result = resultFor(session, protocol_v2::Status::Ok);
    }

    if (sender) {
        for (const auto &frame : replay) {
            (void)sender(frame);
        }
    }
    return result;
}

bool EndpointSessionRegistry::disconnectAbruptly(std::uint16_t host_id, SessionId session_id) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end() || position->second.host_id != host_id ||
        position->second.state != SessionState::Active) {
        return false;
    }
    position->second.state = SessionState::OfflineRetained;
    position->second.sender = {};
    position->second.transport_name.clear();
    return true;
}

protocol_v2::Status EndpointSessionRegistry::gracefulClose(std::uint16_t host_id, SessionId session_id) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end() || position->second.host_id != host_id) {
        return protocol_v2::Status::StaleSession;
    }
    auto &session = position->second;
    if (session.state != SessionState::Active || !session.modified_holders.empty()) {
        return protocol_v2::Status::InvalidState;
    }
    session.state = SessionState::Closed;
    session.clean_holders.clear();
    session.modified_holders.clear();
    return protocol_v2::Status::Ok;
}

PinResponseResult EndpointSessionRegistry::pinResponse(SessionId session_id, std::uint64_t request_id,
                                                       const protocol_v2::CoherenceFrame &response) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end()) {
        return PinResponseResult::SessionUnavailable;
    }
    auto &session = position->second;
    if (request_id == 0 || protocol_v2::requestId(response) != request_id) {
        return PinResponseResult::InvalidResponse;
    }
    if (request_id <= session.response_watermark) {
        return PinResponseResult::StaleRequest;
    }
    if (session.pinned_responses.contains(request_id)) {
        return PinResponseResult::Duplicate;
    }
    if (session.pinned_responses.size() >= max_pinned_responses_per_session_) {
        return PinResponseResult::Backpressure;
    }
    session.pinned_responses.emplace(request_id, response);
    return PinResponseResult::Pinned;
}

bool EndpointSessionRegistry::acknowledgeResponses(SessionId session_id, std::uint64_t highest_contiguous_consumed) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end()) {
        return false;
    }
    auto &session = position->second;
    if (highest_contiguous_consumed <= session.response_watermark) {
        return true;
    }
    session.response_watermark = highest_contiguous_consumed;
    session.pinned_responses.erase(session.pinned_responses.begin(),
                                   session.pinned_responses.upper_bound(highest_contiguous_consumed));
    return true;
}

std::uint64_t EndpointSessionRegistry::responseWatermark(SessionId session_id) const {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    return position == sessions_.end() ? 0 : position->second.response_watermark;
}

std::uint64_t EndpointSessionRegistry::replayFloor(SessionId session_id) const {
    const auto watermark = responseWatermark(session_id);
    return watermark == std::numeric_limits<std::uint64_t>::max() ? watermark : watermark + 1;
}

std::vector<std::uint64_t> EndpointSessionRegistry::pinnedResponseIds(SessionId session_id) const {
    std::lock_guard lock(mutex_);
    std::vector<std::uint64_t> ids;
    const auto position = sessions_.find(session_id);
    if (position != sessions_.end()) {
        for (const auto &[id, frame] : position->second.pinned_responses) {
            (void)frame;
            ids.push_back(id);
        }
    }
    return ids;
}

bool EndpointSessionRegistry::addCleanHolder(SessionId session_id, std::uint64_t line_address) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end() || !validHolderSession(position->second) || !aligned(line_address) ||
        position->second.modified_holders.contains(line_address)) {
        return false;
    }
    position->second.clean_holders.insert(line_address);
    return true;
}

bool EndpointSessionRegistry::removeCleanHolder(SessionId session_id, std::uint64_t line_address) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    return position != sessions_.end() && position->second.clean_holders.erase(line_address) != 0;
}

bool EndpointSessionRegistry::addModifiedHolder(SessionId session_id, std::uint64_t line_address) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end() || !validHolderSession(position->second) || !aligned(line_address) ||
        position->second.clean_holders.contains(line_address)) {
        return false;
    }
    position->second.modified_holders.insert(line_address);
    return true;
}

bool EndpointSessionRegistry::removeModifiedHolder(SessionId session_id, std::uint64_t line_address) {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    return position != sessions_.end() && position->second.modified_holders.erase(line_address) != 0;
}

std::vector<std::uint64_t> EndpointSessionRegistry::cleanHolders(SessionId session_id) const {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    return position == sessions_.end() ? std::vector<std::uint64_t>{}
                                       : std::vector<std::uint64_t>(position->second.clean_holders.begin(),
                                                                    position->second.clean_holders.end());
}

std::vector<std::uint64_t> EndpointSessionRegistry::modifiedHolders(SessionId session_id) const {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    return position == sessions_.end() ? std::vector<std::uint64_t>{}
                                       : std::vector<std::uint64_t>(position->second.modified_holders.begin(),
                                                                    position->second.modified_holders.end());
}

std::optional<SessionSnapshot> EndpointSessionRegistry::inspect(SessionId session_id) const {
    std::lock_guard lock(mutex_);
    const auto position = sessions_.find(session_id);
    if (position == sessions_.end()) {
        return std::nullopt;
    }
    const auto &session = position->second;
    return SessionSnapshot{session.id,
                           session.host_id,
                           session.state,
                           session.capabilities,
                           session.cache_capacity,
                           session.cache_ways,
                           session.transport_name,
                           static_cast<bool>(session.sender),
                           session.response_watermark,
                           session.response_watermark == std::numeric_limits<std::uint64_t>::max()
                               ? session.response_watermark
                               : session.response_watermark + 1,
                           session.pinned_responses.size(),
                           max_pinned_responses_per_session_,
                           session.pinned_responses.size() >= max_pinned_responses_per_session_};
}

bool EndpointSessionRegistry::validHolderSession(const Session &session) const noexcept {
    return session.state == SessionState::Active || session.state == SessionState::OfflineRetained ||
           session.state == SessionState::Fenced;
}

bool EndpointSessionRegistry::aligned(std::uint64_t line_address) noexcept {
    return line_address % protocol_v2::kLineSize == 0;
}

RegistrationResult EndpointSessionRegistry::resultFor(const Session &session, protocol_v2::Status status) const {
    return {status,
            session.id,
            session.capabilities & protocol_v2::kSupportedCapabilities,
            session.cache_capacity,
            session.cache_ways,
            static_cast<std::uint16_t>(protocol_v2::kLineSize),
            protocol_v2::AckStrength::MODEL};
}

} // namespace cxlmemsim
