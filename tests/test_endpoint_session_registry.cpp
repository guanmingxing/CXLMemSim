#include "endpoint_session_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace cxlmemsim;
using namespace cxlmemsim::protocol_v2;

namespace {

int failures = 0;

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __func__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            ++failures;                                                                                                \
        }                                                                                                              \
    } while (false)

constexpr std::uint64_t kModelSnoop = static_cast<std::uint64_t>(Capability::MODEL_SNOOP);
constexpr std::uint64_t kNativeFlush = static_cast<std::uint64_t>(Capability::NATIVE_FLUSH);

RegistrationRequest request(std::uint16_t host, std::string transport = "tcp", ResponseSender sender = {}) {
    return {host, 0, kModelSnoop, 256 * 1024, 4, std::move(transport), std::move(sender)};
}

CoherenceFrame protocolRequest(Opcode opcode, std::uint64_t request_id, SessionId session_id, std::uint16_t host) {
    auto frame = initializeFrame(opcode);
    setRequestId(frame, request_id);
    setSessionId(frame, session_id);
    setSrcHost(frame, host);
    setDstHost(frame, kServerHost);
    return frame;
}

CoherenceFrame response(const CoherenceFrame &request) {
    auto frame = initializeFrame(Opcode::Response);
    setRequestId(frame, requestId(request));
    setSessionId(frame, sessionId(request));
    setSrcHost(frame, kServerHost);
    setDstHost(frame, srcHost(request));
    return frame;
}

PinResponseResult pinHeartbeat(EndpointSessionRegistry &registry, SessionId session_id, std::uint64_t request_id,
                               std::uint16_t host) {
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, request_id, session_id, host);
    (void)registry.admitRequest(session_id, heartbeat);
    return registry.pinResponse(session_id, heartbeat, response(heartbeat));
}

Status closeSession(EndpointSessionRegistry &registry, std::uint16_t host, SessionId session_id,
                    std::uint64_t request_id) {
    const auto unregister_request = protocolRequest(Opcode::Unregister, request_id, session_id, host);
    if (registry.admitRequest(session_id, unregister_request) != RequestAdmissionResult::Accepted)
        return Status::InvalidState;
    return registry.gracefulClose(host, session_id, unregister_request);
}

void testFreshRegistrationAndValidation() {
    EndpointSessionRegistry registry;
    auto sender = [](const CoherenceFrame &) { return true; };
    auto first = registry.registerEndpoint(request(3, "tcp-A", sender));
    CHECK(first.status == Status::Ok);
    CHECK(first.session_id != 0);
    CHECK(first.negotiated_capabilities == kModelSnoop);
    CHECK(first.cache_capacity == 256 * 1024);
    CHECK(first.cache_ways == 4);
    CHECK(first.line_size == kLineSize);

    const auto snapshot = registry.inspect(first.session_id);
    CHECK(snapshot.has_value());
    CHECK(snapshot->host_id == 3);
    CHECK(snapshot->state == SessionState::Active);
    CHECK(snapshot->capabilities == kModelSnoop);
    CHECK(snapshot->cache_capacity == 256 * 1024);
    CHECK(snapshot->cache_ways == 4);
    CHECK(snapshot->transport_name == "tcp-A");
    CHECK(snapshot->has_sender);

    const auto second = registry.registerEndpoint(request(4));
    CHECK(second.status == Status::Ok);
    CHECK(second.session_id > first.session_id);
    CHECK(registry.registerEndpoint(request(3)).status == Status::DuplicateHost);
    CHECK(registry.registerEndpoint(request(64)).status == Status::InvalidState);
    auto no_snoop = request(5);
    no_snoop.capabilities = 0;
    CHECK(registry.registerEndpoint(no_snoop).status == Status::NoCapability);
    auto known_unsupported = request(5);
    known_unsupported.capabilities |= kNativeFlush;
    const auto negotiated = registry.registerEndpoint(known_unsupported);
    CHECK(negotiated.status == Status::Ok);
    CHECK(negotiated.negotiated_capabilities == kModelSnoop);
    CHECK(registry.inspect(negotiated.session_id)->capabilities == kModelSnoop);
    CHECK(registry.disconnectAbruptly(5, negotiated.session_id));
    auto negotiated_resume = request(5);
    negotiated_resume.requested_session_id = negotiated.session_id;
    CHECK(registry.registerEndpoint(negotiated_resume).status == Status::Ok);
    CHECK(registry.disconnectAbruptly(5, negotiated.session_id));
    auto expanded_resume = negotiated_resume;
    expanded_resume.capabilities = kModelSnoop | kNativeFlush;
    CHECK(registry.registerEndpoint(expanded_resume).status == Status::StaleSession);
    auto unknown = request(6);
    unknown.capabilities |= 1ULL << 10;
    CHECK(registry.registerEndpoint(unknown).status == Status::NoCapability);

    for (const auto [capacity, ways] :
         std::vector<std::pair<std::uint32_t, std::uint16_t>>{{0, 4}, {63, 1}, {65, 1}, {64, 0}, {128, 3}}) {
        auto bad_geometry = request(12);
        bad_geometry.cache_capacity = capacity;
        bad_geometry.cache_ways = ways;
        CHECK(registry.registerEndpoint(bad_geometry).status == Status::InvalidState);
    }
    auto one_line = request(12);
    one_line.cache_capacity = 64;
    one_line.cache_ways = 1;
    CHECK(registry.registerEndpoint(one_line).status == Status::Ok);

    EndpointSessionRegistry smaller(2);
    CHECK(smaller.registerEndpoint(request(2)).status == Status::InvalidState);
}

void testDisconnectResumeAndReplay() {
    EndpointSessionRegistry registry;
    const auto original = registry.registerEndpoint(request(7));
    CHECK(registry.addCleanHolder(original.session_id, 0x1000));
    CHECK(registry.addModifiedHolder(original.session_id, 0x2000));
    CHECK(pinHeartbeat(registry, original.session_id, 1, 7) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, original.session_id, 2, 7) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(7, original.session_id));
    CHECK(registry.inspect(original.session_id)->state == SessionState::OfflineRetained);
    CHECK(registry.cleanHolders(original.session_id) == std::vector<std::uint64_t>{0x1000});
    CHECK(registry.modifiedHolders(original.session_id) == std::vector<std::uint64_t>{0x2000});
    CHECK(registry.pinnedResponseIds(original.session_id) == (std::vector<std::uint64_t>{1, 2}));

    auto stale = request(7);
    stale.requested_session_id = original.session_id + 1;
    CHECK(registry.registerEndpoint(stale).status == Status::StaleSession);
    auto mismatch = request(7);
    mismatch.requested_session_id = original.session_id;
    mismatch.cache_ways = 8;
    CHECK(registry.registerEndpoint(mismatch).status == Status::StaleSession);
    auto capability_mismatch = request(7);
    capability_mismatch.requested_session_id = original.session_id;
    capability_mismatch.capabilities |= kNativeFlush;
    CHECK(registry.registerEndpoint(capability_mismatch).status == Status::StaleSession);
    auto capacity_mismatch = request(7);
    capacity_mismatch.requested_session_id = original.session_id;
    capacity_mismatch.cache_capacity *= 2;
    CHECK(registry.registerEndpoint(capacity_mismatch).status == Status::StaleSession);
    auto wrong_host = request(8);
    wrong_host.requested_session_id = original.session_id;
    CHECK(registry.registerEndpoint(wrong_host).status == Status::StaleSession);

    std::vector<std::uint64_t> replayed;
    auto resume = request(7, "rdma-B", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        CHECK(registry.inspect(original.session_id)->state == SessionState::Active);
        if (requestId(frame) == 1) {
            CHECK(registry.registerEndpoint(request(10)).status == Status::Ok);
        }
        return true;
    });
    resume.requested_session_id = original.session_id;
    const auto resumed = registry.registerEndpoint(resume);
    CHECK(resumed.status == Status::Ok);
    CHECK(resumed.session_id == original.session_id);
    CHECK(replayed == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.inspect(original.session_id)->transport_name == "rdma-B");
    CHECK(registry.pinnedResponseIds(original.session_id) == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.registerEndpoint(resume).status == Status::DuplicateHost);

    CHECK(registry.disconnectAbruptly(7, original.session_id));
    auto exact_negotiated_resume = request(7);
    exact_negotiated_resume.requested_session_id = original.session_id;
    CHECK(registry.registerEndpoint(exact_negotiated_resume).status == Status::Ok);
}

void testReplayDoesNotBlockRegistry() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(1));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 1) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(1, registered.session_id));

    std::promise<void> callback_entered;
    std::promise<void> allow_callback_exit;
    auto exit_future = allow_callback_exit.get_future().share();
    auto resume = request(1, "shm", [&](const CoherenceFrame &) {
        callback_entered.set_value();
        exit_future.wait();
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    callback_entered.get_future().wait();
    auto concurrent = std::async(std::launch::async, [&] { return registry.registerEndpoint(request(2)); });
    CHECK(concurrent.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(concurrent.get().status == Status::Ok);
    allow_callback_exit.set_value();
    CHECK(replay.get().status == Status::Ok);
}

void testDisconnectWaitsForCurrentBindingReplay() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(13));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 13) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(13, registered.session_id));

    std::promise<void> replay_entered;
    std::promise<void> release_replay;
    const auto release = release_replay.get_future().share();
    auto resume = request(13, "tcp-new", [&](const CoherenceFrame &) {
        replay_entered.set_value();
        release.wait();
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    replay_entered.get_future().wait();

    auto disconnect =
        std::async(std::launch::async, [&] { return registry.disconnectAbruptly(13, registered.session_id); });
    CHECK(disconnect.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout);
    release_replay.set_value();
    CHECK(replay.get().status == Status::Ok);
    CHECK(disconnect.get());
    CHECK(registry.inspect(registered.session_id)->state == SessionState::OfflineRetained);
}

void testCallbackCanDisconnectItsOwnBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(3));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 3) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 3) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(3, registered.session_id));

    std::vector<std::uint64_t> replayed;
    auto resume = request(3, "tcp-reentrant", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        CHECK(registry.disconnectAbruptly(3, registered.session_id));
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    CHECK(replay.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(replay.get().status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->state == SessionState::OfflineRetained);
}

void testThrowingReplayReleasesItsBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(4));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 4) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(4, registered.session_id));

    auto resume = request(4, "tcp-throwing",
                          [](const CoherenceFrame &) -> bool { throw std::runtime_error("replay delivery failed"); });
    resume.requested_session_id = registered.session_id;
    bool propagated = false;
    try {
        (void)registry.registerEndpoint(resume);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);

    const auto snapshot = registry.inspect(registered.session_id);
    CHECK(snapshot->state == SessionState::OfflineRetained);
    CHECK(!snapshot->has_sender);
    CHECK(snapshot->transport_name.empty());
}

void testNestedThrowingReplayRestoresOuterDeliveryContext() {
    EndpointSessionRegistry registry;
    const auto outer = registry.registerEndpoint(request(5));
    const auto inner = registry.registerEndpoint(request(6));
    CHECK(pinHeartbeat(registry, outer.session_id, 1, 5) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, inner.session_id, 1, 6) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(5, outer.session_id));
    CHECK(registry.disconnectAbruptly(6, inner.session_id));

    auto inner_resume = request(6, "tcp-inner", [](const CoherenceFrame &) -> bool {
        throw std::runtime_error("nested replay delivery failed");
    });
    inner_resume.requested_session_id = inner.session_id;
    auto outer_resume = request(5, "tcp-outer", [&](const CoherenceFrame &) {
        try {
            (void)registry.registerEndpoint(inner_resume);
        } catch (const std::runtime_error &) {
        }
        CHECK(registry.disconnectAbruptly(5, outer.session_id));
        return true;
    });
    outer_resume.requested_session_id = outer.session_id;

    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(outer_resume); });
    CHECK(replay.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(replay.get().status == Status::Ok);
    CHECK(registry.inspect(outer.session_id)->state == SessionState::OfflineRetained);
}

void testHeartbeatWatermark() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(2));
    const auto one = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 2);
    const auto two = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 2);
    const auto three = protocolRequest(Opcode::Heartbeat, 3, registered.session_id, 2);
    const auto four = protocolRequest(Opcode::Heartbeat, 4, registered.session_id, 2);
    CHECK(registry.admitRequest(registered.session_id, one) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, two) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, three) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, four) == RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(registered.session_id, one, response(one)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, two, response(two)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, four, response(four)) == PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, 2));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.replayFloor(registered.session_id) == 3);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(!registry.acknowledgeResponses(registered.session_id, 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.pinResponse(registered.session_id, three, response(three)) == PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 4));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(registry.replayFloor(registered.session_id) == 5);
    CHECK(!registry.acknowledgeResponses(registered.session_id, 5));
    CHECK(!registry.acknowledgeResponses(registered.session_id, std::numeric_limits<std::uint64_t>::max()));
    CHECK(registry.responseWatermark(registered.session_id) == 4);
    CHECK(registry.replayFloor(registered.session_id) == 5);
}

void testPinResponseCorrelationAndConflict() {
    EndpointSessionRegistry registry;
    const auto first = registry.registerEndpoint(request(14));
    const auto second = registry.registerEndpoint(request(15));
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, first.session_id, 14);
    CHECK(registry.admitRequest(first.session_id, heartbeat) == RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(first.session_id, heartbeat, response(heartbeat)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(first.session_id, heartbeat, response(heartbeat)) == PinResponseResult::Duplicate);

    const auto same_id_different_request = protocolRequest(Opcode::Fence, 1, first.session_id, 14);
    CHECK(validateFrame(same_id_different_request));
    CHECK(registry.pinResponse(first.session_id, same_id_different_request, response(same_id_different_request)) ==
          PinResponseResult::Conflict);

    auto conflict = response(heartbeat);
    setStatus(conflict, Status::IoError);
    CHECK(registry.pinResponse(first.session_id, heartbeat, conflict) == PinResponseResult::Conflict);

    auto wrong_session_request = protocolRequest(Opcode::Heartbeat, 2, second.session_id, 14);
    CHECK(registry.pinResponse(first.session_id, wrong_session_request, response(wrong_session_request)) ==
          PinResponseResult::InvalidResponse);
    auto wrong_host_request = protocolRequest(Opcode::Heartbeat, 2, first.session_id, 15);
    CHECK(registry.pinResponse(first.session_id, wrong_host_request, response(wrong_host_request)) ==
          PinResponseResult::InvalidResponse);
    auto invalid_request = protocolRequest(Opcode::Heartbeat, 2, first.session_id, 14);
    setMagic(invalid_request, 0);
    CHECK(registry.pinResponse(first.session_id, invalid_request, response(invalid_request)) ==
          PinResponseResult::InvalidResponse);
    auto mismatched_response = response(protocolRequest(Opcode::Heartbeat, 3, first.session_id, 14));
    CHECK(registry.pinResponse(first.session_id, protocolRequest(Opcode::Heartbeat, 2, first.session_id, 14),
                               mismatched_response) == PinResponseResult::InvalidResponse);
    auto impossible =
        protocolRequest(Opcode::Heartbeat, std::numeric_limits<std::uint64_t>::max(), first.session_id, 14);
    CHECK(registry.pinResponse(first.session_id, impossible, response(impossible)) ==
          PinResponseResult::InvalidResponse);
}

void testHolderIndexesAndGracefulClose() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(6, "tcp", [](const CoherenceFrame &) { return true; }));
    CHECK(!registry.addCleanHolder(registered.session_id, 0x1001));
    CHECK(registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(!registry.addModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.removeCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addModifiedHolder(registered.session_id, 0x1000));
    CHECK(!registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addCleanHolder(registered.session_id, 0x2000));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 6) == PinResponseResult::Pinned);
    CHECK(closeSession(registry, 6, registered.session_id, 2) == Status::InvalidState);
    CHECK(registry.removeModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.gracefulClose(7, registered.session_id,
                                 protocolRequest(Opcode::Unregister, 2, registered.session_id, 6)) ==
          Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id,
                                 protocolRequest(Opcode::Unregister, 2, registered.session_id, 6)) ==
          Status::InvalidState);
    CHECK(registry.cleanHolders(registered.session_id) == std::vector<std::uint64_t>{0x2000});
    const auto clean_snapshot = registry.cleanHolders(registered.session_id);
    for (const auto line : clean_snapshot)
        CHECK(registry.removeCleanHolder(registered.session_id, line));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 2, registered.session_id, 6);
    CHECK(registry.gracefulClose(6, registered.session_id, unregister_request) == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(!registry.inspect(registered.session_id)->closed_final_response_pinned);
    CHECK(registry.registerEndpoint(request(6)).status == Status::DuplicateHost);
    CHECK(registry.cleanHolders(registered.session_id).empty());
    CHECK(registry.modifiedHolders(registered.session_id).empty());
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(registry.inspect(registered.session_id)->closed_final_response_pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 3, 6) == PinResponseResult::InvalidResponse);
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{2});
    CHECK(registry.acknowledgeResponses(registered.session_id, 2));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(!registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.inspect(registered.session_id)->transport_name.empty());
    CHECK(!registry.addCleanHolder(registered.session_id, 0x3000));
    CHECK(!registry.removeCleanHolder(registered.session_id, 0x2000));
    auto closed_resume = request(6);
    closed_resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(closed_resume).status == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id, unregister_request) == Status::InvalidState);
    CHECK(!registry.disconnectAbruptly(6, registered.session_id));

    const auto replacement = registry.registerEndpoint(request(6));
    CHECK(replacement.status == Status::Ok);
    CHECK(replacement.session_id != registered.session_id);
    CHECK(!registry.inspect(registered.session_id).has_value());
}

void testPinnedResponseBoundAndRecovery() {
    EndpointSessionRegistry registry(64, 2);
    const auto registered = registry.registerEndpoint(request(11));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 11) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 11) == PinResponseResult::Pinned);
    const auto full = registry.inspect(registered.session_id);
    CHECK(full->pinned_response_count == 2);
    CHECK(full->pinned_response_limit == 2);
    CHECK(full->response_backpressured);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 11) == PinResponseResult::Duplicate);
    const auto third = protocolRequest(Opcode::Heartbeat, 3, registered.session_id, 11);
    CHECK(registry.admitRequest(registered.session_id, third) == RequestAdmissionResult::Backpressure);
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(!registry.inspect(registered.session_id)->response_backpressured);
    CHECK(pinHeartbeat(registry, registered.session_id, 3, 11) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 11) == PinResponseResult::StaleRequest);
    const auto fourth = protocolRequest(Opcode::Heartbeat, 4, registered.session_id, 11);
    CHECK(registry.pinResponse(registered.session_id, fourth,
                               response(protocolRequest(Opcode::Heartbeat, 5, registered.session_id, 11))) ==
          PinResponseResult::InvalidResponse);
    const auto unavailable = protocolRequest(Opcode::Heartbeat, 4, registered.session_id + 1, 11);
    CHECK(registry.pinResponse(registered.session_id + 1, unavailable, response(unavailable)) ==
          PinResponseResult::SessionUnavailable);
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{2, 3}));

    CHECK(registry.disconnectAbruptly(11, registered.session_id));
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{2, 3}));

    EndpointSessionRegistry zero_limit(64, 0);
    const auto zero_registered = zero_limit.registerEndpoint(request(12));
    CHECK(zero_limit.inspect(zero_registered.session_id)->pinned_response_limit == 1);
    CHECK(pinHeartbeat(zero_limit, zero_registered.session_id, 1, 12) == PinResponseResult::Pinned);
    const auto zero_second = protocolRequest(Opcode::Heartbeat, 2, zero_registered.session_id, 12);
    CHECK(zero_limit.admitRequest(zero_registered.session_id, zero_second) == RequestAdmissionResult::Backpressure);
}

void testDefaultHostBoundaryAndClamp() {
    EndpointSessionRegistry registry;
    for (std::uint16_t host = 0; host < 64; ++host) {
        CHECK(registry.registerEndpoint(request(host)).status == Status::Ok);
    }
    CHECK(registry.registerEndpoint(request(64)).status == Status::InvalidState);

    EndpointSessionRegistry clamped(100);
    CHECK(clamped.registerEndpoint(request(63)).status == Status::Ok);
    CHECK(clamped.registerEndpoint(request(64)).status == Status::InvalidState);
}

void testOrderedAdmissionAndPublication() {
    std::vector<std::uint64_t> delivered;
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(20, "tcp", [&](const CoherenceFrame &frame) {
        delivered.push_back(requestId(frame));
        return true;
    }));
    std::vector<CoherenceFrame> requests;
    for (std::uint64_t id = 1; id <= 4; ++id) {
        requests.push_back(protocolRequest(Opcode::Heartbeat, id, registered.session_id, 20));
        CHECK(registry.admitRequest(registered.session_id, requests.back()) == RequestAdmissionResult::Accepted);
    }
    CHECK(registry.admitRequest(registered.session_id, requests[1]) == RequestAdmissionResult::Duplicate);
    auto conflicting = requests[1];
    setOpcode(conflicting, Opcode::Fence);
    CHECK(registry.admitRequest(registered.session_id, conflicting) == RequestAdmissionResult::Conflict);
    CHECK(registry.admitRequest(registered.session_id, protocolRequest(Opcode::Heartbeat, 6, registered.session_id,
                                                                       20)) == RequestAdmissionResult::InvalidRequest);

    CHECK(registry.pinResponse(registered.session_id, requests[3], response(requests[3])) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, requests[1], response(requests[1])) == PinResponseResult::Pinned);
    CHECK(delivered.empty());
    CHECK(registry.pinResponse(registered.session_id, requests[0], response(requests[0])) == PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.pinResponse(registered.session_id, requests[2], response(requests[2])) == PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2, 3, 4}));
    CHECK(!registry.acknowledgeResponses(registered.session_id, 5));
}

void testReplayBindingCannotMigrateToReplacement() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(21));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 21) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 21) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(21, registered.session_id));

    std::promise<void> old_entered;
    std::promise<void> release_old;
    const auto release = release_old.get_future().share();
    std::vector<std::uint64_t> old_deliveries;
    std::vector<std::uint64_t> replacement_deliveries;
    auto old_resume = request(21, "old", [&](const CoherenceFrame &frame) {
        old_deliveries.push_back(requestId(frame));
        old_entered.set_value();
        release.wait();
        return false;
    });
    old_resume.requested_session_id = registered.session_id;
    auto old_replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(old_resume); });
    old_entered.get_future().wait();
    auto disconnect =
        std::async(std::launch::async, [&] { return registry.disconnectAbruptly(21, registered.session_id); });
    while (registry.inspect(registered.session_id)->state != SessionState::OfflineRetained)
        std::this_thread::yield();
    auto replacement = request(21, "replacement", [&](const CoherenceFrame &frame) {
        replacement_deliveries.push_back(requestId(frame));
        return true;
    });
    replacement.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(replacement).status == Status::Ok);
    release_old.set_value();
    CHECK(old_replay.get().status == Status::Ok);
    CHECK(disconnect.get());
    CHECK(old_deliveries == std::vector<std::uint64_t>{1});
    CHECK(replacement_deliveries == (std::vector<std::uint64_t>{1, 2}));
}

void testFalseDeliveryRetiresOnlyFailedBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(22));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 22) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(22, registered.session_id));
    auto failing = request(22, "failing", [](const CoherenceFrame &) { return false; });
    failing.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(failing).status == Status::IoError);
    const auto failed = registry.inspect(registered.session_id);
    CHECK(failed->state == SessionState::OfflineRetained);
    CHECK(!failed->has_sender);
    CHECK(failed->transport_name.empty());
    std::vector<std::uint64_t> replayed;
    auto replacement = request(22, "good", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        return true;
    });
    replacement.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(replacement).status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
}

void testNestedRetirementSkipsOuterDeliveryFrames() {
    for (const bool close_outer : {false, true}) {
        EndpointSessionRegistry registry;
        const auto outer = registry.registerEndpoint(request(23));
        const auto inner = registry.registerEndpoint(request(24));
        CoherenceFrame outer_request;
        if (close_outer) {
            outer_request = protocolRequest(Opcode::Unregister, 1, outer.session_id, 23);
            CHECK(registry.admitRequest(outer.session_id, outer_request) == RequestAdmissionResult::Accepted);
            CHECK(registry.pinResponse(outer.session_id, outer_request, response(outer_request)) ==
                  PinResponseResult::Pinned);
        } else {
            CHECK(pinHeartbeat(registry, outer.session_id, 1, 23) == PinResponseResult::Pinned);
        }
        CHECK(pinHeartbeat(registry, inner.session_id, 1, 24) == PinResponseResult::Pinned);
        CHECK(registry.disconnectAbruptly(23, outer.session_id));
        CHECK(registry.disconnectAbruptly(24, inner.session_id));
        auto inner_resume = request(24, "inner", [&](const CoherenceFrame &) {
            if (close_outer)
                CHECK(registry.gracefulClose(23, outer.session_id, outer_request) == Status::Ok);
            else
                CHECK(registry.disconnectAbruptly(23, outer.session_id));
            return true;
        });
        inner_resume.requested_session_id = inner.session_id;
        auto outer_resume = request(23, "outer", [&](const CoherenceFrame &) {
            CHECK(registry.registerEndpoint(inner_resume).status == Status::Ok);
            return true;
        });
        outer_resume.requested_session_id = outer.session_id;
        auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(outer_resume); });
        CHECK(replay.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
        CHECK(replay.get().status == Status::Ok);
    }
}

void testConcurrentPinJoinsBlockedReplayDrain() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(25));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 25) == PinResponseResult::Pinned);
    const auto second = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 25);
    CHECK(registry.admitRequest(registered.session_id, second) == RequestAdmissionResult::Accepted);
    CHECK(registry.disconnectAbruptly(25, registered.session_id));
    std::promise<void> entered;
    std::promise<void> release_callback;
    const auto release = release_callback.get_future().share();
    std::vector<std::uint64_t> delivered;
    auto resume = request(25, "replay", [&](const CoherenceFrame &frame) {
        delivered.push_back(requestId(frame));
        if (requestId(frame) == 1) {
            entered.set_value();
            release.wait();
        }
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    entered.get_future().wait();
    CHECK(registry.pinResponse(registered.session_id, second, response(second)) == PinResponseResult::Pinned);
    release_callback.set_value();
    CHECK(replay.get().status == Status::Ok);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
}

void testRequestSpecificCloseAndHolderBound() {
    EndpointSessionRegistry registry;
    auto one_line = request(26);
    one_line.cache_capacity = 64;
    one_line.cache_ways = 1;
    const auto registered = registry.registerEndpoint(one_line);
    CHECK(registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(!registry.addModifiedHolder(registered.session_id, 0x2000));
    CHECK(registry.removeCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addModifiedHolder(registered.session_id, 0x2000));
    CHECK(registry.disconnectAbruptly(26, registered.session_id));
    CHECK(!registry.addCleanHolder(registered.session_id, 0x3000));
    CHECK(registry.removeModifiedHolder(registered.session_id, 0x2000));
    auto resume = one_line;
    resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    const auto close_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 26);
    CHECK(registry.admitRequest(registered.session_id, close_request) == RequestAdmissionResult::Accepted);
    const auto wrong = protocolRequest(Opcode::Unregister, 2, registered.session_id, 26);
    CHECK(registry.gracefulClose(26, registered.session_id, wrong) == Status::InvalidState);
    CHECK(registry.gracefulClose(26, registered.session_id, close_request) == Status::Ok);
    CHECK(registry.pinResponse(registered.session_id, wrong, response(wrong)) == PinResponseResult::InvalidResponse);
    CHECK(registry.pinResponse(registered.session_id, close_request, response(close_request)) ==
          PinResponseResult::Pinned);
}

} // namespace

int main() {
    testFreshRegistrationAndValidation();
    testDisconnectResumeAndReplay();
    testReplayDoesNotBlockRegistry();
    testDisconnectWaitsForCurrentBindingReplay();
    testCallbackCanDisconnectItsOwnBinding();
    testThrowingReplayReleasesItsBinding();
    testNestedThrowingReplayRestoresOuterDeliveryContext();
    testHeartbeatWatermark();
    testPinResponseCorrelationAndConflict();
    testHolderIndexesAndGracefulClose();
    testPinnedResponseBoundAndRecovery();
    testDefaultHostBoundaryAndClamp();
    testOrderedAdmissionAndPublication();
    testReplayBindingCannotMigrateToReplacement();
    testFalseDeliveryRetiresOnlyFailedBinding();
    testNestedRetirementSkipsOuterDeliveryFrames();
    testConcurrentPinJoinsBlockedReplayDrain();
    testRequestSpecificCloseAndHolderBound();
    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "endpoint session registry tests passed\n";
    return EXIT_SUCCESS;
}
