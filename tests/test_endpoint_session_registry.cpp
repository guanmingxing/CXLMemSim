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
    return registry.pinResponse(session_id, heartbeat, response(heartbeat));
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
    CHECK(pinHeartbeat(registry, original.session_id, 9, 7) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, original.session_id, 3, 7) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(7, original.session_id));
    CHECK(registry.inspect(original.session_id)->state == SessionState::OfflineRetained);
    CHECK(registry.cleanHolders(original.session_id) == std::vector<std::uint64_t>{0x1000});
    CHECK(registry.modifiedHolders(original.session_id) == std::vector<std::uint64_t>{0x2000});
    CHECK(registry.pinnedResponseIds(original.session_id) == (std::vector<std::uint64_t>{3, 9}));

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
        if (requestId(frame) == 3) {
            CHECK(registry.registerEndpoint(request(10)).status == Status::Ok);
        }
        return requestId(frame) != 9;
    });
    resume.requested_session_id = original.session_id;
    const auto resumed = registry.registerEndpoint(resume);
    CHECK(resumed.status == Status::Ok);
    CHECK(resumed.session_id == original.session_id);
    CHECK(replayed == (std::vector<std::uint64_t>{3, 9}));
    CHECK(registry.inspect(original.session_id)->transport_name == "rdma-B");
    CHECK(registry.pinnedResponseIds(original.session_id) == (std::vector<std::uint64_t>{3, 9}));
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

    auto disconnect =
        std::async(std::launch::async, [&] { return registry.disconnectAbruptly(4, registered.session_id); });
    CHECK(disconnect.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(disconnect.get());
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
    for (const auto id : {1ULL, 2ULL, 4ULL}) {
        CHECK(pinHeartbeat(registry, registered.session_id, id, 2) == PinResponseResult::Pinned);
    }
    CHECK(registry.acknowledgeResponses(registered.session_id, 2));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.replayFloor(registered.session_id) == 3);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(!registry.acknowledgeResponses(registered.session_id, 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(pinHeartbeat(registry, registered.session_id, 3, 2) == PinResponseResult::Pinned);
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
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(registry.removeModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.gracefulClose(7, registered.session_id) == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(registry.cleanHolders(registered.session_id) == std::vector<std::uint64_t>{0x2000});
    const auto clean_snapshot = registry.cleanHolders(registered.session_id);
    for (const auto line : clean_snapshot)
        CHECK(registry.removeCleanHolder(registered.session_id, line));
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(!registry.inspect(registered.session_id)->closed_final_response_pinned);
    CHECK(registry.registerEndpoint(request(6)).status == Status::DuplicateHost);
    CHECK(registry.cleanHolders(registered.session_id).empty());
    CHECK(registry.modifiedHolders(registered.session_id).empty());
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->has_sender);
    const auto unregister_request = protocolRequest(Opcode::Unregister, 2, registered.session_id, 6);
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
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
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
    CHECK(pinHeartbeat(registry, registered.session_id, 3, 11) == PinResponseResult::Backpressure);
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
    CHECK(pinHeartbeat(zero_limit, zero_registered.session_id, 2, 12) == PinResponseResult::Backpressure);
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
    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "endpoint session registry tests passed\n";
    return EXIT_SUCCESS;
}
