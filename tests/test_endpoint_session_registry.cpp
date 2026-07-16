#include "endpoint_session_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
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

RegistrationRequest request(std::uint16_t host, std::string transport = "tcp", ResponseSender sender = {}) {
    return {host, 0, kModelSnoop, 256 * 1024, 4, std::move(transport), std::move(sender)};
}

CoherenceFrame response(std::uint64_t request_id) {
    auto frame = initializeFrame(Opcode::Response);
    setRequestId(frame, request_id);
    return frame;
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

    EndpointSessionRegistry smaller(2);
    CHECK(smaller.registerEndpoint(request(2)).status == Status::InvalidState);
}

void testDisconnectResumeAndReplay() {
    EndpointSessionRegistry registry;
    const auto original = registry.registerEndpoint(request(7));
    CHECK(registry.addCleanHolder(original.session_id, 0x1000));
    CHECK(registry.addModifiedHolder(original.session_id, 0x2000));
    CHECK(registry.pinResponse(original.session_id, 9, response(9)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(original.session_id, 3, response(3)) == PinResponseResult::Pinned);
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
    capability_mismatch.capabilities |= 1ULL << 10;
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
}

void testReplayDoesNotBlockRegistry() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(1));
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)) == PinResponseResult::Pinned);
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

void testHeartbeatWatermark() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(2));
    for (const auto id : {1ULL, 2ULL, 4ULL}) {
        CHECK(registry.pinResponse(registered.session_id, id, response(id)) == PinResponseResult::Pinned);
    }
    CHECK(registry.acknowledgeResponses(registered.session_id, 2));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.replayFloor(registered.session_id) == 3);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, 4));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(registry.replayFloor(registered.session_id) == 5);
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
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)) == PinResponseResult::Pinned);
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(registry.removeModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.gracefulClose(7, registered.session_id) == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(registry.cleanHolders(registered.session_id).empty());
    CHECK(registry.modifiedHolders(registered.session_id).empty());
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.pinResponse(registered.session_id, 2, response(2)) == PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{2});
    CHECK(registry.acknowledgeResponses(registered.session_id, 2));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    auto closed_resume = request(6);
    closed_resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(closed_resume).status == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(!registry.disconnectAbruptly(6, registered.session_id));
}

void testPinnedResponseBoundAndRecovery() {
    EndpointSessionRegistry registry(64, 2);
    const auto registered = registry.registerEndpoint(request(11));
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, 2, response(2)) == PinResponseResult::Pinned);
    const auto full = registry.inspect(registered.session_id);
    CHECK(full->pinned_response_count == 2);
    CHECK(full->pinned_response_limit == 2);
    CHECK(full->response_backpressured);
    CHECK(registry.pinResponse(registered.session_id, 2, response(2)) == PinResponseResult::Duplicate);
    CHECK(registry.pinResponse(registered.session_id, 3, response(3)) == PinResponseResult::Backpressure);
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(registered.session_id, 1));
    CHECK(!registry.inspect(registered.session_id)->response_backpressured);
    CHECK(registry.pinResponse(registered.session_id, 3, response(3)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)) == PinResponseResult::StaleRequest);
    CHECK(registry.pinResponse(registered.session_id, 4, response(5)) == PinResponseResult::InvalidResponse);
    CHECK(registry.pinResponse(registered.session_id + 1, 4, response(4)) == PinResponseResult::SessionUnavailable);
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{2, 3}));

    CHECK(registry.disconnectAbruptly(11, registered.session_id));
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{2, 3}));

    EndpointSessionRegistry zero_limit(64, 0);
    const auto zero_registered = zero_limit.registerEndpoint(request(12));
    CHECK(zero_limit.inspect(zero_registered.session_id)->pinned_response_limit == 1);
    CHECK(zero_limit.pinResponse(zero_registered.session_id, 1, response(1)) == PinResponseResult::Pinned);
    CHECK(zero_limit.pinResponse(zero_registered.session_id, 2, response(2)) == PinResponseResult::Backpressure);
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
    testHeartbeatWatermark();
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
