#include "endpoint_session_registry.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
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
constexpr auto kCompletionTimeout = std::chrono::seconds(2);

template <typename Future> void waitReadyOrExit(Future &future, const char *context) {
    if (future.wait_for(kCompletionTimeout) != std::future_status::ready) {
        std::cerr << context << " timed out\n";
        std::_Exit(EXIT_FAILURE);
    }
}

template <typename Future> auto getReadyOrExit(Future &future, const char *context) {
    waitReadyOrExit(future, context);
    return future.get();
}

template <typename Predicate> void waitUntilOrExit(Predicate predicate, const char *context) {
    const auto deadline = std::chrono::steady_clock::now() + kCompletionTimeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            std::cerr << context << " timed out\n";
            std::_Exit(EXIT_FAILURE);
        }
        std::this_thread::yield();
    }
}

struct ThrowingCopySender {
    std::shared_ptr<bool> throw_on_copy;
    std::shared_ptr<std::size_t> deliveries;

    ThrowingCopySender(std::shared_ptr<bool> should_throw, std::shared_ptr<std::size_t> delivery_count)
        : throw_on_copy(std::move(should_throw)), deliveries(std::move(delivery_count)) {}
    ThrowingCopySender(const ThrowingCopySender &other)
        : throw_on_copy(other.throw_on_copy), deliveries(other.deliveries) {
        if (*throw_on_copy)
            throw std::runtime_error("sender copy failed");
    }
    ThrowingCopySender(ThrowingCopySender &&) noexcept = default;

    bool operator()(const CoherenceFrame &) const {
        ++*deliveries;
        return true;
    }
};

struct ThrowOnNthCopySender {
    std::shared_ptr<std::size_t> copies;
    std::shared_ptr<std::size_t> deliveries;
    std::size_t throw_on_copy;

    ThrowOnNthCopySender(std::shared_ptr<std::size_t> copy_count, std::shared_ptr<std::size_t> delivery_count,
                         std::size_t throw_on)
        : copies(std::move(copy_count)), deliveries(std::move(delivery_count)), throw_on_copy(throw_on) {}
    ThrowOnNthCopySender(const ThrowOnNthCopySender &other)
        : copies(other.copies), deliveries(other.deliveries), throw_on_copy(other.throw_on_copy) {
        if (++*copies == throw_on_copy)
            throw std::runtime_error("sender copy failed");
    }
    ThrowOnNthCopySender(ThrowOnNthCopySender &&) noexcept = default;

    bool operator()(const CoherenceFrame &) const {
        ++*deliveries;
        return true;
    }
};

struct InspectingCopySender {
    EndpointSessionRegistry *registry;
    SessionId inspected_session_id;
    std::shared_ptr<std::atomic<bool>> inspect_on_copy;
    std::shared_ptr<std::atomic<bool>> inspected;

    InspectingCopySender(EndpointSessionRegistry *registry_to_inspect, SessionId session_id,
                         std::shared_ptr<std::atomic<bool>> should_inspect,
                         std::shared_ptr<std::atomic<bool>> did_inspect)
        : registry(registry_to_inspect), inspected_session_id(session_id), inspect_on_copy(std::move(should_inspect)),
          inspected(std::move(did_inspect)) {}
    InspectingCopySender(const InspectingCopySender &other)
        : registry(other.registry), inspected_session_id(other.inspected_session_id),
          inspect_on_copy(other.inspect_on_copy), inspected(other.inspected) {
        if (inspect_on_copy->load()) {
            (void)registry->inspect(inspected_session_id);
            inspected->store(true);
        }
    }
    InspectingCopySender(InspectingCopySender &&) noexcept = default;

    bool operator()(const CoherenceFrame &) const { return true; }
};

struct SenderDestructionRaceState {
    explicit SenderDestructionRaceState(EndpointSessionRegistry *registry_to_inspect)
        : registry(registry_to_inspect), allow_copy(allow_copy_promise.get_future().share()),
          allow_delivery(allow_delivery_promise.get_future().share()) {}

    EndpointSessionRegistry *registry;
    SessionId session_id{};
    std::atomic<bool> inspect_next_copy_on_destruction{false};
    std::promise<void> staged_copy_waiting;
    std::promise<void> allow_copy_promise;
    std::shared_future<void> allow_copy;
    std::promise<void> delivery_entered;
    std::promise<void> allow_delivery_promise;
    std::shared_future<void> allow_delivery;
    std::promise<void> destruction_entered;
};

struct InspectingDestructionSender {
    std::shared_ptr<SenderDestructionRaceState> state;
    bool inspect_on_destruction{};

    explicit InspectingDestructionSender(std::shared_ptr<SenderDestructionRaceState> shared_state)
        : state(std::move(shared_state)) {}
    InspectingDestructionSender(const InspectingDestructionSender &other) : state(other.state) {
        inspect_on_destruction = state->inspect_next_copy_on_destruction.exchange(false);
        if (inspect_on_destruction) {
            state->staged_copy_waiting.set_value();
            waitReadyOrExit(state->allow_copy, "staged sender copy release");
        }
    }
    InspectingDestructionSender(InspectingDestructionSender &&other) noexcept
        : state(std::move(other.state)), inspect_on_destruction(std::exchange(other.inspect_on_destruction, false)) {}
    ~InspectingDestructionSender() {
        if (inspect_on_destruction) {
            state->destruction_entered.set_value();
            (void)state->registry->inspect(state->session_id);
        }
    }

    bool operator()(const CoherenceFrame &) const {
        state->delivery_entered.set_value();
        waitReadyOrExit(state->allow_delivery, "winning delivery release");
        return true;
    }
};

RegistrationRequest request(std::uint16_t host, std::string transport = "tcp", ResponseSender sender = {}) {
    return {host, 0, kModelSnoop, 256 * 1024, 4, std::move(transport), std::move(sender)};
}

BindingId activeBinding(EndpointSessionRegistry &registry, SessionId session_id) {
    const auto snapshot = registry.inspect(session_id);
    return snapshot ? snapshot->binding_id : BindingId{};
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
    (void)registry.admitRequest(session_id, activeBinding(registry, session_id), heartbeat);
    return registry.pinResponse(session_id, heartbeat, response(heartbeat));
}

Status closeSession(EndpointSessionRegistry &registry, std::uint16_t host, SessionId session_id,
                    std::uint64_t request_id) {
    const auto unregister_request = protocolRequest(Opcode::Unregister, request_id, session_id, host);
    if (registry.admitRequest(session_id, activeBinding(registry, session_id), unregister_request) !=
        RequestAdmissionResult::Accepted)
        return Status::InvalidState;
    return registry.gracefulClose(host, session_id, activeBinding(registry, session_id), unregister_request);
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
    CHECK(registry.disconnectAbruptly(5, negotiated.session_id, activeBinding(registry, negotiated.session_id)));
    auto negotiated_resume = request(5);
    negotiated_resume.requested_session_id = negotiated.session_id;
    CHECK(registry.registerEndpoint(negotiated_resume).status == Status::Ok);
    CHECK(registry.disconnectAbruptly(5, negotiated.session_id, activeBinding(registry, negotiated.session_id)));
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
    CHECK(registry.addCleanHolder(original.session_id, original.binding_id, 0x1000));
    CHECK(registry.addModifiedHolder(original.session_id, original.binding_id, 0x2000));
    CHECK(pinHeartbeat(registry, original.session_id, 1, 7) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, original.session_id, 2, 7) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(7, original.session_id, activeBinding(registry, original.session_id)));
    CHECK(registry.inspect(original.session_id)->state == SessionState::OfflineRetained);
    const auto offline_generation = registry.captureGeneration(7, original.session_id, BindingId{});
    CHECK(offline_generation.has_value());
    CHECK(registry.holderSnapshot(*offline_generation).clean == std::vector<std::uint64_t>{0x1000});
    CHECK(registry.holderSnapshot(*offline_generation).modified == std::vector<std::uint64_t>{0x2000});
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

    CHECK(registry.disconnectAbruptly(7, original.session_id, activeBinding(registry, original.session_id)));
    auto exact_negotiated_resume = request(7);
    exact_negotiated_resume.requested_session_id = original.session_id;
    CHECK(registry.registerEndpoint(exact_negotiated_resume).status == Status::Ok);
}

void testRetiredBindingCannotOperateAfterResume() {
    EndpointSessionRegistry registry;
    const auto original = registry.registerEndpoint(request(48));
    const auto retired_binding = original.binding_id;
    const auto first = protocolRequest(Opcode::Heartbeat, 1, original.session_id, 48);
    CHECK(registry.admitRequest(original.session_id, retired_binding, first) == RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(original.session_id, first, response(first)) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(48, original.session_id, retired_binding));

    auto resume = request(48, "resumed", [](const CoherenceFrame &) { return true; });
    resume.requested_session_id = original.session_id;
    const auto resumed = registry.registerEndpoint(resume);
    CHECK(resumed.status == Status::Ok);
    CHECK(resumed.binding_id != retired_binding);

    const auto second = protocolRequest(Opcode::Heartbeat, 2, original.session_id, 48);
    CHECK(registry.admitRequest(original.session_id, retired_binding, second) ==
          RequestAdmissionResult::SessionUnavailable);
    CHECK(!registry.acknowledgeResponses(original.session_id, retired_binding, 1));
    CHECK(!registry.disconnectAbruptly(48, original.session_id, retired_binding));
    CHECK(registry.gracefulClose(48, original.session_id, retired_binding, second) == Status::StaleSession);

    CHECK(registry.acknowledgeResponses(original.session_id, resumed.binding_id, 1));
    CHECK(registry.admitRequest(original.session_id, resumed.binding_id, second) == RequestAdmissionResult::Accepted);
}

void testFreshRegistrationSenderCopyFailureDoesNotConsumeSessionId() {
    EndpointSessionRegistry registry;
    auto throw_on_copy = std::make_shared<bool>(false);
    auto deliveries = std::make_shared<std::size_t>(0);
    ResponseSender sender{ThrowingCopySender{throw_on_copy, deliveries}};
    auto failing = request(33, "copy-fails", std::move(sender));
    *throw_on_copy = true;

    bool propagated = false;
    try {
        (void)registry.registerEndpoint(failing);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);

    const auto retry = registry.registerEndpoint(request(33));
    CHECK(retry.status == Status::Ok);
    CHECK(retry.session_id == 1);
    CHECK(*deliveries == 0);
}

void expectRegistrationInsertionFailureDoesNotConsumeSessionId(
    endpoint_session_registry_test::FailurePoint failure_point) {
    EndpointSessionRegistry registry;
    endpoint_session_registry_test::failNext(failure_point);
    bool propagated = false;
    try {
        (void)registry.registerEndpoint(request(40));
    } catch (const std::bad_alloc &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(!registry.inspect(1).has_value());

    const auto retry = registry.registerEndpoint(request(40));
    CHECK(retry.status == Status::Ok);
    CHECK(retry.session_id == 1);
    CHECK(registry.registerEndpoint(request(40)).status == Status::DuplicateHost);
}

void testFreshRegistrationRollsBackIfSessionIndexInsertionThrows() {
    expectRegistrationInsertionFailureDoesNotConsumeSessionId(
        endpoint_session_registry_test::FailurePoint::SessionIndexInsertion);
}

void testFreshRegistrationRollsBackIfHostIndexInsertionThrows() {
    expectRegistrationInsertionFailureDoesNotConsumeSessionId(
        endpoint_session_registry_test::FailurePoint::HostIndexInsertion);
}

void testSenderCopyCanInspectRegistryWithoutDeadlock() {
    auto registry = std::make_shared<EndpointSessionRegistry>();
    const auto existing = registry->registerEndpoint(request(41));
    auto inspect_on_copy = std::make_shared<std::atomic<bool>>(false);
    auto inspected = std::make_shared<std::atomic<bool>>(false);
    ResponseSender sender{InspectingCopySender{registry.get(), existing.session_id, inspect_on_copy, inspected}};
    auto registration = std::make_shared<RegistrationRequest>(request(42, "reentrant-copy", std::move(sender)));
    inspect_on_copy->store(true);

    auto completed = std::make_shared<std::promise<Status>>();
    auto completion = completed->get_future();
    std::thread worker([registry, registration, completed] {
        try {
            completed->set_value(registry->registerEndpoint(*registration).status);
        } catch (...) {
            completed->set_exception(std::current_exception());
        }
    });
    waitReadyOrExit(completion, "sender copy re-entry");
    worker.join();
    CHECK(getReadyOrExit(completion, "sender copy result") == Status::Ok);
    CHECK(inspected->load());
}

void testLosingDrainSenderIsDestroyedAfterRegistryUnlock() {
    EndpointSessionRegistry registry;
    auto state = std::make_shared<SenderDestructionRaceState>(&registry);
    ResponseSender sender{InspectingDestructionSender{state}};
    const auto registered = registry.registerEndpoint(request(47, "destruction-reentrant", std::move(sender)));
    state->session_id = registered.session_id;
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 47);
    const auto heartbeat_response = response(heartbeat);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), heartbeat) ==
          RequestAdmissionResult::Accepted);

    state->inspect_next_copy_on_destruction.store(true);
    auto staged_copy_waiting = state->staged_copy_waiting.get_future();
    auto destruction_entered = state->destruction_entered.get_future();
    std::promise<PinResponseResult> losing_result;
    auto losing_completion = losing_result.get_future();
    std::thread losing_publisher(
        [&] { losing_result.set_value(registry.pinResponse(registered.session_id, heartbeat, heartbeat_response)); });
    waitReadyOrExit(staged_copy_waiting, "losing staged sender copy");

    auto delivery_entered = state->delivery_entered.get_future();
    std::promise<PinResponseResult> winning_result;
    auto winning_completion = winning_result.get_future();
    std::thread winning_publisher(
        [&] { winning_result.set_value(registry.pinResponse(registered.session_id, heartbeat, heartbeat_response)); });
    waitReadyOrExit(delivery_entered, "winning sender delivery");
    state->allow_copy_promise.set_value();
    waitReadyOrExit(destruction_entered, "losing sender destruction");
    state->allow_delivery_promise.set_value();

    waitReadyOrExit(losing_completion, "losing publisher completion");
    waitReadyOrExit(winning_completion, "winning publisher completion");
    losing_publisher.join();
    winning_publisher.join();
    CHECK(getReadyOrExit(losing_completion, "losing publisher result") == PinResponseResult::Pinned);
    CHECK(getReadyOrExit(winning_completion, "winning publisher result") == PinResponseResult::Duplicate);
}

void testDisconnectRetainsSessionWhileClosedSessionIsReplaced() {
    for (int iteration = 0; iteration < 64; ++iteration) {
        EndpointSessionRegistry registry;
        std::promise<void> old_delivery_entered;
        std::promise<void> release_old_delivery;
        const auto release = release_old_delivery.get_future().share();
        const auto registered = registry.registerEndpoint(request(43, "old", [&](const CoherenceFrame &) {
            old_delivery_entered.set_value();
            waitReadyOrExit(release, "old delivery release");
            return true;
        }));
        const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 43);
        const auto unregister_request = protocolRequest(Opcode::Unregister, 2, registered.session_id, 43);
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), heartbeat) ==
              RequestAdmissionResult::Accepted);
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                    unregister_request) == RequestAdmissionResult::Accepted);

        auto old_publish = std::async(std::launch::async, [&] {
            return registry.pinResponse(registered.session_id, heartbeat, response(heartbeat));
        });
        auto old_delivery = old_delivery_entered.get_future();
        waitReadyOrExit(old_delivery, "old delivery entry");
        auto disconnect = std::async(std::launch::async, [&] {
            return registry.disconnectAbruptly(43, registered.session_id,
                                               activeBinding(registry, registered.session_id));
        });

        waitUntilOrExit([&] { return registry.inspect(registered.session_id)->state == SessionState::OfflineRetained; },
                        "offline retained transition");

        auto resume = request(43, "resumed", [](const CoherenceFrame &) { return true; });
        resume.requested_session_id = registered.session_id;
        CHECK(registry.registerEndpoint(resume).status == Status::Ok);
        CHECK(registry.gracefulClose(43, registered.session_id, activeBinding(registry, registered.session_id),
                                     unregister_request) == Status::Ok);
        CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
              PinResponseResult::Pinned);
        CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 2));

        const auto replacement = registry.registerEndpoint(request(43));
        CHECK(replacement.status == Status::Ok);
        CHECK(replacement.session_id == 2);
        release_old_delivery.set_value();
        CHECK(getReadyOrExit(old_publish, "old publication completion") == PinResponseResult::Pinned);
        CHECK(getReadyOrExit(disconnect, "old disconnect completion"));
    }
}

void testReplacementSenderCopyFailurePreservesClosedSession() {
    EndpointSessionRegistry registry;
    const auto registered =
        registry.registerEndpoint(request(36, "original", [](const CoherenceFrame &) { return true; }));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 36);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                unregister_request) == RequestAdmissionResult::Accepted);
    CHECK(registry.gracefulClose(36, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::Ok);
    CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));

    auto throw_on_copy = std::make_shared<bool>(false);
    auto deliveries = std::make_shared<std::size_t>(0);
    ResponseSender sender{ThrowingCopySender{throw_on_copy, deliveries}};
    auto failing = request(36, "copy-fails", std::move(sender));
    *throw_on_copy = true;

    bool propagated = false;
    try {
        (void)registry.registerEndpoint(failing);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);
    const auto after_failure = registry.inspect(registered.session_id);
    CHECK(after_failure.has_value());
    if (after_failure)
        CHECK(after_failure->state == SessionState::Closed);

    CHECK(registry.registerEndpoint(request(36)).status == Status::Ok);
    CHECK(*deliveries == 0);
}

void testResumeSenderCopyFailurePreservesOfflineBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(34));
    CHECK(registry.disconnectAbruptly(34, registered.session_id, activeBinding(registry, registered.session_id)));

    auto throw_on_copy = std::make_shared<bool>(false);
    auto deliveries = std::make_shared<std::size_t>(0);
    ResponseSender sender{ThrowingCopySender{throw_on_copy, deliveries}};
    auto failing = request(34, "copy-fails", std::move(sender));
    failing.requested_session_id = registered.session_id;
    *throw_on_copy = true;

    bool propagated = false;
    try {
        (void)registry.registerEndpoint(failing);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);
    const auto after_failure = registry.inspect(registered.session_id);
    CHECK(after_failure->state == SessionState::OfflineRetained);
    CHECK(!after_failure->has_sender);
    CHECK(after_failure->transport_name.empty());

    auto retry = request(34, "retry", [](const CoherenceFrame &) { return true; });
    retry.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(retry).status == Status::Ok);
    CHECK(*deliveries == 0);
}

void testResumeDrainCopyFailurePreservesOfflineBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(37));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 37) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(37, registered.session_id, activeBinding(registry, registered.session_id)));

    auto copies = std::make_shared<std::size_t>(0);
    auto deliveries = std::make_shared<std::size_t>(0);
    ResponseSender sender{ThrowOnNthCopySender{copies, deliveries, 2}};
    auto failing = request(37, "second-copy-fails", std::move(sender));
    failing.requested_session_id = registered.session_id;

    bool propagated = false;
    try {
        (void)registry.registerEndpoint(failing);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(*copies == 2);
    const auto after_failure = registry.inspect(registered.session_id);
    CHECK(after_failure->state == SessionState::OfflineRetained);
    CHECK(!after_failure->has_sender);
    CHECK(after_failure->transport_name.empty());
    CHECK(*deliveries == 0);

    auto retry = request(37, "retry", [&](const CoherenceFrame &) {
        ++*deliveries;
        return true;
    });
    retry.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(retry).status == Status::Ok);
    CHECK(*deliveries == 1);
}

void testDuplicatePinRestartsDrainAfterSenderCopyFailure() {
    EndpointSessionRegistry registry;
    auto throw_on_copy = std::make_shared<bool>(false);
    auto deliveries = std::make_shared<std::size_t>(0);
    ResponseSender sender{ThrowingCopySender{throw_on_copy, deliveries}};
    const auto registered = registry.registerEndpoint(request(35, "throwing-copy", std::move(sender)));
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 35);
    const auto heartbeat_response = response(heartbeat);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), heartbeat) ==
          RequestAdmissionResult::Accepted);
    *throw_on_copy = true;

    bool propagated = false;
    try {
        (void)registry.pinResponse(registered.session_id, heartbeat, heartbeat_response);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Active);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
    CHECK(*deliveries == 0);

    *throw_on_copy = false;
    CHECK(registry.pinResponse(registered.session_id, heartbeat, heartbeat_response) == PinResponseResult::Duplicate);
    CHECK(*deliveries == 1);
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
}

void expectDrainBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable(
    endpoint_session_registry_test::FailurePoint failure_point) {
    EndpointSessionRegistry registry;
    std::size_t deliveries = 0;
    const auto registered = registry.registerEndpoint(request(49, "tcp", [&](const CoherenceFrame &) {
        ++deliveries;
        return true;
    }));
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 49);
    const auto heartbeat_response = response(heartbeat);
    CHECK(registry.admitRequest(registered.session_id, registered.binding_id, heartbeat) ==
          RequestAdmissionResult::Accepted);

    endpoint_session_registry_test::failNext(failure_point);
    bool propagated = false;
    try {
        (void)registry.pinResponse(registered.session_id, heartbeat, heartbeat_response);
    } catch (const std::bad_alloc &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(deliveries == 0);
    CHECK(registry.pinResponse(registered.session_id, heartbeat, heartbeat_response) == PinResponseResult::Duplicate);
    CHECK(deliveries == 1);

    std::promise<bool> disconnected;
    auto disconnect_completion = disconnected.get_future();
    std::thread disconnect(
        [&] { disconnected.set_value(registry.disconnectAbruptly(49, registered.session_id, registered.binding_id)); });
    waitReadyOrExit(disconnect_completion, "disconnect after drain bookkeeping failure");
    disconnect.join();
    CHECK(getReadyOrExit(disconnect_completion, "disconnect result after drain bookkeeping failure"));
}

void testDeliveryContextBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable() {
    expectDrainBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable(
        endpoint_session_registry_test::FailurePoint::DrainDeliveryContextBookkeeping);
}

void testResponseBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable() {
    expectDrainBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable(
        endpoint_session_registry_test::FailurePoint::DrainResponseBookkeeping);
}

void expectResumeBookkeepingFailureRetiresBindingAndAllowsReplay(
    endpoint_session_registry_test::FailurePoint failure_point) {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(50));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 50) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(50, registered.session_id, registered.binding_id));

    std::size_t deliveries = 0;
    auto failing_resume = request(50, "failing-resume", [&](const CoherenceFrame &) {
        ++deliveries;
        return true;
    });
    failing_resume.requested_session_id = registered.session_id;
    endpoint_session_registry_test::failNext(failure_point);
    bool propagated = false;
    try {
        (void)registry.registerEndpoint(failing_resume);
    } catch (const std::bad_alloc &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(deliveries == 0);
    const auto failed = registry.inspect(registered.session_id);
    CHECK(failed->state == SessionState::OfflineRetained);
    CHECK(!failed->binding_id);
    CHECK(!failed->has_sender);
    CHECK(failed->transport_name.empty());
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});

    auto retry = request(50, "retry", [&](const CoherenceFrame &) {
        ++deliveries;
        return true;
    });
    retry.requested_session_id = registered.session_id;
    const auto resumed = registry.registerEndpoint(retry);
    CHECK(resumed.status == Status::Ok);
    CHECK(resumed.binding_id);
    CHECK(deliveries == 1);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
}

void testResumeDeliveryContextBookkeepingFailureRetiresBindingAndAllowsReplay() {
    expectResumeBookkeepingFailureRetiresBindingAndAllowsReplay(
        endpoint_session_registry_test::FailurePoint::DrainDeliveryContextBookkeeping);
}

void testResumeResponseBookkeepingFailureRetiresBindingAndAllowsReplay() {
    expectResumeBookkeepingFailureRetiresBindingAndAllowsReplay(
        endpoint_session_registry_test::FailurePoint::DrainResponseBookkeeping);
}

void testReplayDoesNotBlockRegistry() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(1));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 1) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(1, registered.session_id, activeBinding(registry, registered.session_id)));

    std::promise<void> callback_entered;
    std::promise<void> allow_callback_exit;
    auto exit_future = allow_callback_exit.get_future().share();
    auto resume = request(1, "shm", [&](const CoherenceFrame &) {
        callback_entered.set_value();
        waitReadyOrExit(exit_future, "replay callback release");
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    auto entered = callback_entered.get_future();
    waitReadyOrExit(entered, "replay callback entry");
    auto concurrent = std::async(std::launch::async, [&] { return registry.registerEndpoint(request(2)); });
    CHECK(getReadyOrExit(concurrent, "concurrent registration").status == Status::Ok);
    allow_callback_exit.set_value();
    CHECK(getReadyOrExit(replay, "replay completion").status == Status::Ok);
}

void testDisconnectWaitsForCurrentBindingReplay() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(13));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 13) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(13, registered.session_id, activeBinding(registry, registered.session_id)));

    std::promise<void> replay_entered;
    std::promise<void> release_replay;
    const auto release = release_replay.get_future().share();
    auto resume = request(13, "tcp-new", [&](const CoherenceFrame &) {
        replay_entered.set_value();
        waitReadyOrExit(release, "current binding replay release");
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    auto entered = replay_entered.get_future();
    waitReadyOrExit(entered, "current binding replay entry");

    auto disconnect = std::async(std::launch::async, [&] {
        return registry.disconnectAbruptly(13, registered.session_id, activeBinding(registry, registered.session_id));
    });
    waitUntilOrExit([&] { return registry.inspect(registered.session_id)->state == SessionState::OfflineRetained; },
                    "current binding disconnect retirement");
    CHECK(disconnect.wait_for(std::chrono::seconds(0)) == std::future_status::timeout);
    release_replay.set_value();
    CHECK(getReadyOrExit(replay, "current binding replay completion").status == Status::Ok);
    CHECK(getReadyOrExit(disconnect, "current binding disconnect completion"));
    CHECK(registry.inspect(registered.session_id)->state == SessionState::OfflineRetained);
}

void testCallbackCanDisconnectItsOwnBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(3));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 3) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 3) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(3, registered.session_id, activeBinding(registry, registered.session_id)));

    std::vector<std::uint64_t> replayed;
    auto resume = request(3, "tcp-reentrant", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        CHECK(registry.disconnectAbruptly(3, registered.session_id, activeBinding(registry, registered.session_id)));
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    const auto disconnected_binding = getReadyOrExit(replay, "self-disconnecting replay completion");
    CHECK(disconnected_binding.status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->state == SessionState::OfflineRetained);
    CHECK(!registry.acknowledgeResponses(registered.session_id, disconnected_binding.binding_id, 1));
    auto second_resume = request(3, "tcp-second", [](const CoherenceFrame &) { return true; });
    second_resume.requested_session_id = registered.session_id;
    const auto second_binding = registry.registerEndpoint(second_resume);
    CHECK(second_binding.status == Status::Ok);
    CHECK(registry.acknowledgeResponses(registered.session_id, second_binding.binding_id, 1));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{2});
}

void testThrowingReplayReleasesItsBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(4));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 4) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(4, registered.session_id, activeBinding(registry, registered.session_id)));

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
    CHECK(registry.disconnectAbruptly(5, outer.session_id, activeBinding(registry, outer.session_id)));
    CHECK(registry.disconnectAbruptly(6, inner.session_id, activeBinding(registry, inner.session_id)));

    auto inner_resume = request(6, "tcp-inner", [](const CoherenceFrame &) -> bool {
        throw std::runtime_error("nested replay delivery failed");
    });
    inner_resume.requested_session_id = inner.session_id;
    auto outer_resume = request(5, "tcp-outer", [&](const CoherenceFrame &) {
        try {
            (void)registry.registerEndpoint(inner_resume);
        } catch (const std::runtime_error &) {
        }
        CHECK(registry.disconnectAbruptly(5, outer.session_id, activeBinding(registry, outer.session_id)));
        return true;
    });
    outer_resume.requested_session_id = outer.session_id;

    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(outer_resume); });
    CHECK(getReadyOrExit(replay, "nested throwing replay completion").status == Status::Ok);
    CHECK(registry.inspect(outer.session_id)->state == SessionState::OfflineRetained);
}

void testHeartbeatWatermark() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(2, "tcp", [](const CoherenceFrame &) { return true; }));
    const auto one = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 2);
    const auto two = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 2);
    const auto three = protocolRequest(Opcode::Heartbeat, 3, registered.session_id, 2);
    const auto four = protocolRequest(Opcode::Heartbeat, 4, registered.session_id, 2);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), one) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), two) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), three) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), four) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(registered.session_id, one, response(one)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, two, response(two)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, four, response(four)) == PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 2));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.replayFloor(registered.session_id) == 3);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
    CHECK(registry.responseWatermark(registered.session_id) == 2);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(!registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.pinResponse(registered.session_id, three, response(three)) == PinResponseResult::Pinned);
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 3));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{4});
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 4));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(registry.replayFloor(registered.session_id) == 5);
    CHECK(!registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 5));
    CHECK(!registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id),
                                         std::numeric_limits<std::uint64_t>::max()));
    CHECK(registry.responseWatermark(registered.session_id) == 4);
    CHECK(registry.replayFloor(registered.session_id) == 5);
}

void testAcknowledgementRequiresSuccessfulPublication() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(27));
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, registered.session_id, 27);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), heartbeat) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(registered.session_id, heartbeat, response(heartbeat)) == PinResponseResult::Pinned);
    CHECK(!registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
    CHECK(registry.responseWatermark(registered.session_id) == 0);
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});

    CHECK(registry.disconnectAbruptly(27, registered.session_id, activeBinding(registry, registered.session_id)));
    std::vector<std::uint64_t> replayed;
    auto resume = request(27, "tcp-resumed", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        return true;
    });
    resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
}

void testReentrantAcknowledgementDuringSuccessfulPublication() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool acknowledged = false;
    const auto registered = registry.registerEndpoint(request(40, "tcp", [&](const CoherenceFrame &frame) {
        acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), requestId(frame));
        return true;
    }));
    session_id = registered.session_id;

    CHECK(pinHeartbeat(registry, session_id, 1, 40) == PinResponseResult::Pinned);
    CHECK(acknowledged);
    CHECK(registry.responseWatermark(session_id) == 1);
    CHECK(registry.pinnedResponseIds(session_id).empty());
}

void testReentrantAcknowledgementAndClosePublishesTerminalResponse() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    CoherenceFrame unregister_request;
    bool first_acknowledged = false;
    Status close_status = Status::InvalidState;
    std::vector<std::uint64_t> delivered;
    const auto registered = registry.registerEndpoint(request(51, "tcp", [&](const CoherenceFrame &frame) {
        delivered.push_back(requestId(frame));
        if (requestId(frame) == 1) {
            first_acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1);
            close_status =
                registry.gracefulClose(51, session_id, activeBinding(registry, session_id), unregister_request);
        }
        return true;
    }));
    session_id = registered.session_id;
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, session_id, 51);
    unregister_request = protocolRequest(Opcode::Unregister, 2, session_id, 51);
    CHECK(registry.admitRequest(session_id, registered.binding_id, heartbeat) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(session_id, registered.binding_id, unregister_request) ==
          RequestAdmissionResult::Accepted);

    CHECK(registry.pinResponse(session_id, heartbeat, response(heartbeat)) == PinResponseResult::Pinned);
    CHECK(first_acknowledged);
    CHECK(close_status == Status::Ok);
    CHECK(registry.responseWatermark(session_id) == 1);
    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 2));
    CHECK(registry.responseWatermark(session_id) == 2);
    CHECK(registry.pinnedResponseIds(session_id).empty());
}

void testFinalResponsePinnedDuringPreviousCallbackUsesSinglePublisher() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    CoherenceFrame unregister_request;
    std::atomic<std::size_t> first_response_deliveries{0};
    bool first_acknowledged = false;
    Status close_status = Status::InvalidState;
    std::mutex delivered_mutex;
    std::vector<std::uint64_t> delivered;
    std::promise<void> first_closed;
    auto first_closed_completion = first_closed.get_future();
    std::promise<void> release_first;
    const auto release = release_first.get_future().share();
    const auto registered = registry.registerEndpoint(request(52, "tcp", [&](const CoherenceFrame &frame) {
        const auto id = requestId(frame);
        {
            std::lock_guard lock(delivered_mutex);
            delivered.push_back(id);
        }
        if (id == 1 && first_response_deliveries.fetch_add(1) == 0) {
            first_acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1);
            close_status =
                registry.gracefulClose(52, session_id, activeBinding(registry, session_id), unregister_request);
            first_closed.set_value();
            waitReadyOrExit(release, "release first response after concurrent final pin");
        }
        return true;
    }));
    session_id = registered.session_id;
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, session_id, 52);
    unregister_request = protocolRequest(Opcode::Unregister, 2, session_id, 52);
    CHECK(registry.admitRequest(session_id, registered.binding_id, heartbeat) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(session_id, registered.binding_id, unregister_request) ==
          RequestAdmissionResult::Accepted);

    auto first_publish = std::async(std::launch::async,
                                    [&] { return registry.pinResponse(session_id, heartbeat, response(heartbeat)); });
    waitReadyOrExit(first_closed_completion, "reentrant close before concurrent final pin");
    CHECK(first_acknowledged);
    CHECK(close_status == Status::Ok);
    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    release_first.set_value();
    CHECK(getReadyOrExit(first_publish, "first publication after concurrent final pin") == PinResponseResult::Pinned);

    std::vector<std::uint64_t> observed;
    {
        std::lock_guard lock(delivered_mutex);
        observed = delivered;
    }
    CHECK(observed == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 2));
    CHECK(registry.responseWatermark(session_id) == 2);
    CHECK(registry.pinnedResponseIds(session_id).empty());
}

void testReentrantAcknowledgementOfFinalUnregisterAllowsReplacement() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool acknowledged = false;
    const auto registered = registry.registerEndpoint(request(41, "tcp", [&](const CoherenceFrame &frame) {
        acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), requestId(frame));
        return true;
    }));
    session_id = registered.session_id;
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, session_id, 41);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), unregister_request) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.gracefulClose(41, session_id, activeBinding(registry, session_id), unregister_request) ==
          Status::Ok);

    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(acknowledged);
    CHECK(registry.responseWatermark(session_id) == 1);
    CHECK(registry.pinnedResponseIds(session_id).empty());
    CHECK(registry.registerEndpoint(request(41)).status == Status::Ok);
}

void testReentrantAcknowledgementWithFalseDeliveryRequiresReplay() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool acknowledged = false;
    const auto registered = registry.registerEndpoint(request(42, "failing", [&](const CoherenceFrame &frame) {
        acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), requestId(frame));
        return false;
    }));
    session_id = registered.session_id;

    CHECK(pinHeartbeat(registry, session_id, 1, 42) == PinResponseResult::Pinned);
    CHECK(acknowledged);
    CHECK(registry.responseWatermark(session_id) == 0);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{1});

    std::vector<std::uint64_t> replayed;
    auto resume = request(42, "resumed", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        return true;
    });
    resume.requested_session_id = session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
    CHECK(registry.responseWatermark(session_id) == 0);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1));
}

void testReentrantAcknowledgementWithThrowingDeliveryRequiresReplay() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool acknowledged = false;
    const auto registered = registry.registerEndpoint(request(43, "throwing", [&](const CoherenceFrame &frame) {
        acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), requestId(frame));
        throw std::runtime_error("delivery failed after ACK");
        return true;
    }));
    session_id = registered.session_id;

    bool propagated = false;
    try {
        (void)pinHeartbeat(registry, session_id, 1, 43);
    } catch (const std::runtime_error &) {
        propagated = true;
    }
    CHECK(propagated);
    CHECK(acknowledged);
    CHECK(registry.responseWatermark(session_id) == 0);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{1});

    std::vector<std::uint64_t> replayed;
    auto resume = request(43, "resumed", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        return true;
    });
    resume.requested_session_id = session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{1});
    CHECK(registry.responseWatermark(session_id) == 0);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1));
}

void testFuturePinnedResponseCannotBeAcknowledgedWhileEarlierResponseIsInFlight() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool future_acknowledged = true;
    const auto registered = registry.registerEndpoint(request(44, "tcp", [&](const CoherenceFrame &frame) {
        if (requestId(frame) == 1)
            future_acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 2);
        return true;
    }));
    session_id = registered.session_id;
    const auto first = protocolRequest(Opcode::Heartbeat, 1, session_id, 44);
    const auto second = protocolRequest(Opcode::Heartbeat, 2, session_id, 44);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), first) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), second) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.pinResponse(session_id, second, response(second)) == PinResponseResult::Pinned);

    CHECK(registry.pinResponse(session_id, first, response(first)) == PinResponseResult::Pinned);
    CHECK(!future_acknowledged);
    CHECK(registry.responseWatermark(session_id) == 0);
    CHECK(registry.pinnedResponseIds(session_id) == (std::vector<std::uint64_t>{1, 2}));
}

void testOverlappingDeliveryFailurePreservesAcknowledgementForSuccessfulAttempt() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool acknowledged = false;
    bool disconnected = false;
    Status nested_resume_status = Status::InvalidState;
    std::size_t replacement_deliveries = 0;
    const auto registered = registry.registerEndpoint(request(45, "old", [&](const CoherenceFrame &frame) {
        acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), requestId(frame));
        disconnected = registry.disconnectAbruptly(45, session_id, activeBinding(registry, session_id));
        auto replacement = request(45, "replacement", [&](const CoherenceFrame &) {
            ++replacement_deliveries;
            return false;
        });
        replacement.requested_session_id = session_id;
        nested_resume_status = registry.registerEndpoint(replacement).status;
        return true;
    }));
    session_id = registered.session_id;

    CHECK(pinHeartbeat(registry, session_id, 1, 45) == PinResponseResult::Pinned);
    CHECK(acknowledged);
    CHECK(disconnected);
    CHECK(nested_resume_status == Status::IoError);
    CHECK(replacement_deliveries == 1);
    CHECK(registry.responseWatermark(session_id) == 1);
    CHECK(registry.pinnedResponseIds(session_id).empty());
}

void testPublishedLowerAcknowledgementSurvivesHigherPendingFailure() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool higher_acknowledged = false;
    bool lower_acknowledged = false;
    std::vector<std::uint64_t> delivered;
    const auto registered = registry.registerEndpoint(request(46, "tcp", [&](const CoherenceFrame &frame) {
        const auto response_id = requestId(frame);
        delivered.push_back(response_id);
        if (response_id == 1)
            return true;
        higher_acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 2);
        lower_acknowledged = registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1);
        return false;
    }));
    session_id = registered.session_id;
    const auto first = protocolRequest(Opcode::Heartbeat, 1, session_id, 46);
    const auto second = protocolRequest(Opcode::Heartbeat, 2, session_id, 46);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), first) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), second) ==
          RequestAdmissionResult::Accepted);

    CHECK(registry.pinResponse(session_id, first, response(first)) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(session_id, second, response(second)) == PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
    CHECK(higher_acknowledged);
    CHECK(lower_acknowledged);
    CHECK(registry.responseWatermark(session_id) == 1);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{2});

    std::vector<std::uint64_t> replayed;
    auto resume = request(46, "resumed", [&](const CoherenceFrame &frame) {
        replayed.push_back(requestId(frame));
        return true;
    });
    resume.requested_session_id = session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    CHECK(replayed == std::vector<std::uint64_t>{2});
}

void testPinResponseCorrelationAndConflict() {
    EndpointSessionRegistry registry;
    const auto first = registry.registerEndpoint(request(14));
    const auto second = registry.registerEndpoint(request(15));
    const auto heartbeat = protocolRequest(Opcode::Heartbeat, 1, first.session_id, 14);
    CHECK(registry.admitRequest(first.session_id, activeBinding(registry, first.session_id), heartbeat) ==
          RequestAdmissionResult::Accepted);
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
    CHECK(!registry.addCleanHolder(registered.session_id, registered.binding_id, 0x1001));
    CHECK(registry.addCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(!registry.addModifiedHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.removeCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.addModifiedHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(!registry.addCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.addCleanHolder(registered.session_id, registered.binding_id, 0x2000));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 6) == PinResponseResult::Pinned);
    CHECK(closeSession(registry, 6, registered.session_id, 2) == Status::InvalidState);
    CHECK(registry.removeModifiedHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.gracefulClose(7, registered.session_id, activeBinding(registry, registered.session_id),
                                 protocolRequest(Opcode::Unregister, 2, registered.session_id, 6)) ==
          Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id, activeBinding(registry, registered.session_id),
                                 protocolRequest(Opcode::Unregister, 2, registered.session_id, 6)) ==
          Status::InvalidState);
    CHECK(registry.cleanHolders(registered.session_id, registered.binding_id) == std::vector<std::uint64_t>{0x2000});
    const auto clean_snapshot = registry.cleanHolders(registered.session_id, registered.binding_id);
    for (const auto line : clean_snapshot)
        CHECK(registry.removeCleanHolder(registered.session_id, registered.binding_id, line));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 2, registered.session_id, 6);
    CHECK(registry.gracefulClose(6, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(!registry.inspect(registered.session_id)->closed_final_response_pinned);
    CHECK(registry.registerEndpoint(request(6)).status == Status::DuplicateHost);
    CHECK(registry.cleanHolders(registered.session_id, registered.binding_id).empty());
    CHECK(registry.modifiedHolders(registered.session_id, registered.binding_id).empty());
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(registry.inspect(registered.session_id)->closed_final_response_pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 3, 6) == PinResponseResult::InvalidResponse);
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
    CHECK(registry.pinnedResponseIds(registered.session_id) == std::vector<std::uint64_t>{2});
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 2));
    CHECK(registry.pinnedResponseIds(registered.session_id).empty());
    CHECK(!registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.inspect(registered.session_id)->transport_name.empty());
    CHECK(!registry.addCleanHolder(registered.session_id, registered.binding_id, 0x3000));
    CHECK(!registry.removeCleanHolder(registered.session_id, registered.binding_id, 0x2000));
    auto closed_resume = request(6);
    closed_resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(closed_resume).status == Status::StaleSession);
    CHECK(registry.gracefulClose(6, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::StaleSession);
    CHECK(!registry.disconnectAbruptly(6, registered.session_id, activeBinding(registry, registered.session_id)));

    const auto replacement = registry.registerEndpoint(request(6));
    CHECK(replacement.status == Status::Ok);
    CHECK(replacement.session_id != registered.session_id);
    CHECK(!registry.inspect(registered.session_id).has_value());
}

void testPinnedResponseBoundAndRecovery() {
    EndpointSessionRegistry registry(64, 2);
    const auto registered = registry.registerEndpoint(request(11, "tcp", [](const CoherenceFrame &) { return true; }));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 11) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 11) == PinResponseResult::Pinned);
    const auto full = registry.inspect(registered.session_id);
    CHECK(full->pinned_response_count == 2);
    CHECK(full->pinned_response_limit == 2);
    CHECK(full->response_backpressured);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 11) == PinResponseResult::Duplicate);
    const auto third = protocolRequest(Opcode::Heartbeat, 3, registered.session_id, 11);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), third) ==
          RequestAdmissionResult::Backpressure);
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
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

    CHECK(registry.disconnectAbruptly(11, registered.session_id, activeBinding(registry, registered.session_id)));
    CHECK(registry.pinnedResponseIds(registered.session_id) == (std::vector<std::uint64_t>{2, 3}));

    EndpointSessionRegistry zero_limit(64, 0);
    const auto zero_registered = zero_limit.registerEndpoint(request(12));
    CHECK(zero_limit.inspect(zero_registered.session_id)->pinned_response_limit == 1);
    CHECK(pinHeartbeat(zero_limit, zero_registered.session_id, 1, 12) == PinResponseResult::Pinned);
    const auto zero_second = protocolRequest(Opcode::Heartbeat, 2, zero_registered.session_id, 12);
    CHECK(zero_limit.admitRequest(zero_registered.session_id, zero_registered.binding_id, zero_second) ==
          RequestAdmissionResult::Backpressure);
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
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                    requests.back()) == RequestAdmissionResult::Accepted);
    }
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), requests[1]) ==
          RequestAdmissionResult::Duplicate);
    auto conflicting = requests[1];
    setOpcode(conflicting, Opcode::Fence);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), conflicting) ==
          RequestAdmissionResult::Conflict);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                protocolRequest(Opcode::Heartbeat, 6, registered.session_id, 20)) ==
          RequestAdmissionResult::InvalidRequest);

    CHECK(registry.pinResponse(registered.session_id, requests[3], response(requests[3])) == PinResponseResult::Pinned);
    CHECK(registry.pinResponse(registered.session_id, requests[1], response(requests[1])) == PinResponseResult::Pinned);
    CHECK(delivered.empty());
    CHECK(registry.pinResponse(registered.session_id, requests[0], response(requests[0])) == PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.pinResponse(registered.session_id, requests[2], response(requests[2])) == PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2, 3, 4}));
    CHECK(!registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 5));
}

void testReplayBindingCannotMigrateToReplacement() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(21));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 21) == PinResponseResult::Pinned);
    CHECK(pinHeartbeat(registry, registered.session_id, 2, 21) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(21, registered.session_id, activeBinding(registry, registered.session_id)));

    std::promise<void> old_entered;
    std::promise<void> release_old;
    const auto release = release_old.get_future().share();
    std::vector<std::uint64_t> old_deliveries;
    std::vector<std::uint64_t> replacement_deliveries;
    auto old_resume = request(21, "old", [&](const CoherenceFrame &frame) {
        old_deliveries.push_back(requestId(frame));
        old_entered.set_value();
        waitReadyOrExit(release, "old replay release");
        return false;
    });
    old_resume.requested_session_id = registered.session_id;
    auto old_replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(old_resume); });
    auto entered = old_entered.get_future();
    waitReadyOrExit(entered, "old replay entry");
    auto disconnect = std::async(std::launch::async, [&] {
        return registry.disconnectAbruptly(21, registered.session_id, activeBinding(registry, registered.session_id));
    });
    waitUntilOrExit([&] { return registry.inspect(registered.session_id)->state == SessionState::OfflineRetained; },
                    "replacement replay offline transition");
    auto replacement = request(21, "replacement", [&](const CoherenceFrame &frame) {
        replacement_deliveries.push_back(requestId(frame));
        return true;
    });
    replacement.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(replacement).status == Status::Ok);
    release_old.set_value();
    CHECK(getReadyOrExit(old_replay, "old replay completion").status == Status::Ok);
    CHECK(getReadyOrExit(disconnect, "old binding disconnect completion"));
    CHECK(old_deliveries == std::vector<std::uint64_t>{1});
    CHECK(replacement_deliveries == (std::vector<std::uint64_t>{1, 2}));
}

void testFalseDeliveryRetiresOnlyFailedBinding() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(22));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 22) == PinResponseResult::Pinned);
    CHECK(registry.disconnectAbruptly(22, registered.session_id, activeBinding(registry, registered.session_id)));
    auto failing = request(22, "failing", [](const CoherenceFrame &) { return false; });
    failing.requested_session_id = registered.session_id;
    const auto failed_registration = registry.registerEndpoint(failing);
    CHECK(failed_registration.status == Status::IoError);
    CHECK(!failed_registration.binding_id);
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
            CHECK(pinHeartbeat(registry, outer.session_id, 1, 23) == PinResponseResult::Pinned);
            outer_request = protocolRequest(Opcode::Unregister, 2, outer.session_id, 23);
            CHECK(registry.admitRequest(outer.session_id, activeBinding(registry, outer.session_id), outer_request) ==
                  RequestAdmissionResult::Accepted);
        } else {
            CHECK(pinHeartbeat(registry, outer.session_id, 1, 23) == PinResponseResult::Pinned);
        }
        CHECK(pinHeartbeat(registry, inner.session_id, 1, 24) == PinResponseResult::Pinned);
        CHECK(registry.disconnectAbruptly(23, outer.session_id, activeBinding(registry, outer.session_id)));
        CHECK(registry.disconnectAbruptly(24, inner.session_id, activeBinding(registry, inner.session_id)));
        auto inner_resume = request(24, "inner", [&](const CoherenceFrame &) {
            if (close_outer)
                CHECK(registry.gracefulClose(23, outer.session_id, activeBinding(registry, outer.session_id),
                                             outer_request) == Status::Ok);
            else
                CHECK(registry.disconnectAbruptly(23, outer.session_id, activeBinding(registry, outer.session_id)));
            return true;
        });
        inner_resume.requested_session_id = inner.session_id;
        auto outer_resume = request(23, "outer", [&](const CoherenceFrame &) {
            CHECK(registry.registerEndpoint(inner_resume).status == Status::Ok);
            return true;
        });
        outer_resume.requested_session_id = outer.session_id;
        auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(outer_resume); });
        CHECK(getReadyOrExit(replay, "nested retirement replay completion").status == Status::Ok);
    }
}

void testConcurrentPinJoinsBlockedReplayDrain() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(25));
    CHECK(pinHeartbeat(registry, registered.session_id, 1, 25) == PinResponseResult::Pinned);
    const auto second = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 25);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), second) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.disconnectAbruptly(25, registered.session_id, activeBinding(registry, registered.session_id)));
    std::promise<void> entered;
    std::promise<void> release_callback;
    const auto release = release_callback.get_future().share();
    std::vector<std::uint64_t> delivered;
    auto resume = request(25, "replay", [&](const CoherenceFrame &frame) {
        delivered.push_back(requestId(frame));
        if (requestId(frame) == 1) {
            entered.set_value();
            waitReadyOrExit(release, "concurrent pin replay release");
        }
        return true;
    });
    resume.requested_session_id = registered.session_id;
    auto replay = std::async(std::launch::async, [&] { return registry.registerEndpoint(resume); });
    auto replay_entered = entered.get_future();
    waitReadyOrExit(replay_entered, "concurrent pin replay entry");
    CHECK(registry.pinResponse(registered.session_id, second, response(second)) == PinResponseResult::Pinned);
    release_callback.set_value();
    CHECK(getReadyOrExit(replay, "concurrent pin replay completion").status == Status::Ok);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
}

void testRequestSpecificCloseAndHolderBound() {
    EndpointSessionRegistry registry;
    auto one_line = request(26);
    one_line.cache_capacity = 64;
    one_line.cache_ways = 1;
    const auto registered = registry.registerEndpoint(one_line);
    CHECK(registry.addCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.addCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(!registry.addModifiedHolder(registered.session_id, registered.binding_id, 0x2000));
    CHECK(registry.removeCleanHolder(registered.session_id, registered.binding_id, 0x1000));
    CHECK(registry.addModifiedHolder(registered.session_id, registered.binding_id, 0x2000));
    CHECK(registry.disconnectAbruptly(26, registered.session_id, activeBinding(registry, registered.session_id)));
    CHECK(!registry.addCleanHolder(registered.session_id, registered.binding_id, 0x3000));
    const auto offline = registry.captureGeneration(26, registered.session_id, BindingId{});
    CHECK(offline.has_value());
    CHECK(registry.removeModifiedHolder(*offline, 0x2000));
    auto resume = one_line;
    resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    const auto close_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 26);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), close_request) ==
          RequestAdmissionResult::Accepted);
    const auto wrong = protocolRequest(Opcode::Unregister, 2, registered.session_id, 26);
    CHECK(registry.gracefulClose(26, registered.session_id, activeBinding(registry, registered.session_id), wrong) ==
          Status::InvalidState);
    CHECK(registry.gracefulClose(26, registered.session_id, activeBinding(registry, registered.session_id),
                                 close_request) == Status::Ok);
    CHECK(registry.pinResponse(registered.session_id, wrong, response(wrong)) == PinResponseResult::InvalidResponse);
    CHECK(registry.pinResponse(registered.session_id, close_request, response(close_request)) ==
          PinResponseResult::Pinned);
}

void testUnregisterResponseRequiresClosedIntent() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    std::vector<std::uint64_t> delivered;
    bool delivered_while_closed = false;
    const auto registered = registry.registerEndpoint(request(32, "tcp", [&](const CoherenceFrame &frame) {
        delivered_while_closed = registry.inspect(session_id)->state == SessionState::Closed;
        delivered.push_back(requestId(frame));
        return true;
    }));
    session_id = registered.session_id;
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, session_id, 32);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), unregister_request) ==
          RequestAdmissionResult::Accepted);

    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::InvalidResponse);
    CHECK(delivered.empty());
    CHECK(registry.pinnedResponseIds(session_id).empty());
    CHECK(registry.inspect(session_id)->state == SessionState::Active);

    CHECK(registry.gracefulClose(32, session_id, activeBinding(registry, session_id), unregister_request) ==
          Status::Ok);
    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(delivered == std::vector<std::uint64_t>{1});
    CHECK(delivered_while_closed);
    CHECK(registry.inspect(session_id)->state == SessionState::Closed);
}

void testClosedFinalResponsePublishesAndRemainsPinnedUntilAck() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    bool delivered_while_closed = false;
    const auto registered = registry.registerEndpoint(request(28, "tcp", [&](const CoherenceFrame &frame) {
        delivered_while_closed = registry.inspect(session_id)->state == SessionState::Closed;
        CHECK(requestId(frame) == 1);
        return true;
    }));
    session_id = registered.session_id;
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, session_id, 28);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), unregister_request) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.gracefulClose(28, session_id, activeBinding(registry, session_id), unregister_request) ==
          Status::Ok);
    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(delivered_while_closed);
    CHECK(registry.inspect(session_id)->state == SessionState::Closed);
    CHECK(registry.pinnedResponseIds(session_id) == std::vector<std::uint64_t>{1});
    CHECK(registry.registerEndpoint(request(28)).status == Status::DuplicateHost);
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 1));
    CHECK(registry.pinnedResponseIds(session_id).empty());
    CHECK(registry.registerEndpoint(request(28)).status == Status::Ok);
}

void testClosedFinalSendFailureResumesWithoutReopeningAdmission() {
    for (const bool throws : {false, true}) {
        EndpointSessionRegistry registry;
        const auto registered = registry.registerEndpoint(request(29, "failing", [=](const CoherenceFrame &) {
            if (throws)
                throw std::runtime_error("closed final delivery failed");
            return false;
        }));
        const auto unregister_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 29);
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                    unregister_request) == RequestAdmissionResult::Accepted);
        CHECK(registry.gracefulClose(29, registered.session_id, activeBinding(registry, registered.session_id),
                                     unregister_request) == Status::Ok);
        bool propagated = false;
        try {
            CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
                  PinResponseResult::Pinned);
        } catch (const std::runtime_error &) {
            propagated = true;
        }
        CHECK(propagated == throws);
        const auto failed = registry.inspect(registered.session_id);
        CHECK(failed->state == SessionState::Closed);
        CHECK(!failed->has_sender);
        CHECK(failed->transport_name.empty());
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                    protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 29)) ==
              RequestAdmissionResult::SessionUnavailable);
        CHECK(registry.registerEndpoint(request(29)).status == Status::DuplicateHost);

        std::vector<std::uint64_t> replayed;
        auto resume = request(29, "resumed", [&](const CoherenceFrame &frame) {
            CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
            replayed.push_back(requestId(frame));
            return true;
        });
        resume.requested_session_id = registered.session_id;
        CHECK(registry.registerEndpoint(resume).status == Status::Ok);
        CHECK(replayed == std::vector<std::uint64_t>{1});
        CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
        CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                    protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 29)) ==
              RequestAdmissionResult::InvalidRequest);
        CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
        CHECK(registry.registerEndpoint(request(29)).status == Status::Ok);
    }
}

void testClosedAbruptDisconnectBeforeFinalPinResumesClosed() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(30));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 30);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                unregister_request) == RequestAdmissionResult::Accepted);
    CHECK(registry.gracefulClose(30, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::Ok);
    CHECK(registry.disconnectAbruptly(30, registered.session_id, activeBinding(registry, registered.session_id)));
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(!registry.inspect(registered.session_id)->has_sender);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 30)) ==
          RequestAdmissionResult::SessionUnavailable);

    std::vector<std::uint64_t> delivered;
    auto resume = request(30, "resumed", [&](const CoherenceFrame &frame) {
        CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
        delivered.push_back(requestId(frame));
        return true;
    });
    resume.requested_session_id = registered.session_id;
    CHECK(registry.registerEndpoint(resume).status == Status::Ok);
    CHECK(registry.inspect(registered.session_id)->state == SessionState::Closed);
    CHECK(registry.pinResponse(registered.session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(delivered == std::vector<std::uint64_t>{1});
    CHECK(registry.acknowledgeResponses(registered.session_id, activeBinding(registry, registered.session_id), 1));
}

void testUnregisterAdmissionIsTerminal() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(38));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 38);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                unregister_request) == RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                unregister_request) == RequestAdmissionResult::Duplicate);

    auto conflicting_duplicate = unregister_request;
    setOpcode(conflicting_duplicate, Opcode::Heartbeat);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                conflicting_duplicate) == RequestAdmissionResult::Conflict);

    const auto next_heartbeat = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 38);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                next_heartbeat) == RequestAdmissionResult::InvalidRequest);
    const auto same_next_id = protocolRequest(Opcode::Fence, 2, registered.session_id, 38);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), same_next_id) ==
          RequestAdmissionResult::InvalidRequest);
    const auto later_unregister = protocolRequest(Opcode::Unregister, 3, registered.session_id, 38);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                later_unregister) == RequestAdmissionResult::InvalidRequest);

    CHECK(registry.gracefulClose(38, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::Ok);
}

void testUnregisterNeverClosesOverLaterAdmittedWork() {
    EndpointSessionRegistry registry;
    const auto registered = registry.registerEndpoint(request(39));
    const auto unregister_request = protocolRequest(Opcode::Unregister, 1, registered.session_id, 39);
    CHECK(registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id),
                                unregister_request) == RequestAdmissionResult::Accepted);
    const auto later_request = protocolRequest(Opcode::Heartbeat, 2, registered.session_id, 39);
    const auto later_admission =
        registry.admitRequest(registered.session_id, activeBinding(registry, registered.session_id), later_request);

    CHECK(later_admission != RequestAdmissionResult::Accepted ||
          registry.gracefulClose(39, registered.session_id, activeBinding(registry, registered.session_id),
                                 unregister_request) == Status::InvalidState);
}

void testCloseRejectsUnfinishedEarlierRequestThenPublishesInOrder() {
    EndpointSessionRegistry registry;
    SessionId session_id{};
    std::vector<std::uint64_t> delivered;
    const auto registered = registry.registerEndpoint(request(31, "tcp", [&](const CoherenceFrame &frame) {
        if (requestId(frame) == 2)
            CHECK(registry.inspect(session_id)->state == SessionState::Closed);
        delivered.push_back(requestId(frame));
        return true;
    }));
    session_id = registered.session_id;
    const auto earlier = protocolRequest(Opcode::Heartbeat, 1, session_id, 31);
    const auto unregister_request = protocolRequest(Opcode::Unregister, 2, session_id, 31);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), earlier) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.admitRequest(session_id, activeBinding(registry, session_id), unregister_request) ==
          RequestAdmissionResult::Accepted);
    CHECK(registry.gracefulClose(31, session_id, activeBinding(registry, session_id), unregister_request) ==
          Status::InvalidState);
    CHECK(registry.inspect(session_id)->state == SessionState::Active);
    CHECK(registry.pinResponse(session_id, earlier, response(earlier)) == PinResponseResult::Pinned);
    CHECK(delivered == std::vector<std::uint64_t>{1});
    CHECK(registry.gracefulClose(31, session_id, activeBinding(registry, session_id), unregister_request) ==
          Status::Ok);
    CHECK(registry.pinResponse(session_id, unregister_request, response(unregister_request)) ==
          PinResponseResult::Pinned);
    CHECK(delivered == (std::vector<std::uint64_t>{1, 2}));
    CHECK(registry.acknowledgeResponses(session_id, activeBinding(registry, session_id), 2));
}

} // namespace

int main() {
    testFreshRegistrationAndValidation();
    testDisconnectResumeAndReplay();
    testRetiredBindingCannotOperateAfterResume();
    testFreshRegistrationSenderCopyFailureDoesNotConsumeSessionId();
    testFreshRegistrationRollsBackIfSessionIndexInsertionThrows();
    testFreshRegistrationRollsBackIfHostIndexInsertionThrows();
    testSenderCopyCanInspectRegistryWithoutDeadlock();
    testLosingDrainSenderIsDestroyedAfterRegistryUnlock();
    testDisconnectRetainsSessionWhileClosedSessionIsReplaced();
    testReplacementSenderCopyFailurePreservesClosedSession();
    testResumeSenderCopyFailurePreservesOfflineBinding();
    testResumeDrainCopyFailurePreservesOfflineBinding();
    testDuplicatePinRestartsDrainAfterSenderCopyFailure();
    testDeliveryContextBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable();
    testResponseBookkeepingFailureLeavesDeliveryRetryableAndDisconnectable();
    testResumeDeliveryContextBookkeepingFailureRetiresBindingAndAllowsReplay();
    testResumeResponseBookkeepingFailureRetiresBindingAndAllowsReplay();
    testReplayDoesNotBlockRegistry();
    testDisconnectWaitsForCurrentBindingReplay();
    testCallbackCanDisconnectItsOwnBinding();
    testThrowingReplayReleasesItsBinding();
    testNestedThrowingReplayRestoresOuterDeliveryContext();
    testHeartbeatWatermark();
    testAcknowledgementRequiresSuccessfulPublication();
    testReentrantAcknowledgementDuringSuccessfulPublication();
    testReentrantAcknowledgementAndClosePublishesTerminalResponse();
    testFinalResponsePinnedDuringPreviousCallbackUsesSinglePublisher();
    testReentrantAcknowledgementOfFinalUnregisterAllowsReplacement();
    testReentrantAcknowledgementWithFalseDeliveryRequiresReplay();
    testReentrantAcknowledgementWithThrowingDeliveryRequiresReplay();
    testFuturePinnedResponseCannotBeAcknowledgedWhileEarlierResponseIsInFlight();
    testOverlappingDeliveryFailurePreservesAcknowledgementForSuccessfulAttempt();
    testPublishedLowerAcknowledgementSurvivesHigherPendingFailure();
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
    testUnregisterResponseRequiresClosedIntent();
    testClosedFinalResponsePublishesAndRemainsPinnedUntilAck();
    testClosedFinalSendFailureResumesWithoutReopeningAdmission();
    testClosedAbruptDisconnectBeforeFinalPinResumesClosed();
    testUnregisterAdmissionIsTerminal();
    testUnregisterNeverClosesOverLaterAdmittedWork();
    testCloseRejectsUnfinishedEarlierRequestThenPublishesInOrder();
    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "endpoint session registry tests passed\n";
    return EXIT_SUCCESS;
}
