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
    CHECK(registry.pinResponse(original.session_id, 9, response(9)));
    CHECK(registry.pinResponse(original.session_id, 3, response(3)));
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
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)));
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
        CHECK(registry.pinResponse(registered.session_id, id, response(id)));
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
    const auto registered = registry.registerEndpoint(request(6));
    CHECK(!registry.addCleanHolder(registered.session_id, 0x1001));
    CHECK(registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(!registry.addModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.removeCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addModifiedHolder(registered.session_id, 0x1000));
    CHECK(!registry.addCleanHolder(registered.session_id, 0x1000));
    CHECK(registry.addCleanHolder(registered.session_id, 0x2000));
    CHECK(registry.pinResponse(registered.session_id, 1, response(1)));
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(registry.removeModifiedHolder(registered.session_id, 0x1000));
    CHECK(registry.gracefulClose(7, registered.session_id) == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(registry.cleanHolders(registered.session_id).empty());
    CHECK(registry.modifiedHolders(registered.session_id).empty());
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(registry.gracefulClose(6, registered.session_id) == Status::InvalidState);
    CHECK(!registry.disconnectAbruptly(6, registered.session_id));
}

} // namespace

int main() {
    testFreshRegistrationAndValidation();
    testDisconnectResumeAndReplay();
    testReplayDoesNotBlockRegistry();
    testHeartbeatWatermark();
    testHolderIndexesAndGracefulClose();
    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "endpoint session registry tests passed\n";
    return EXIT_SUCCESS;
}
