#include "coherence_shm_transport_v2.h"

#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "coherence_server_v2.h"
#include "endpoint_session_registry.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <span>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace cxlmemsim;
using namespace cxlmemsim::mesi_v2;
using namespace cxlmemsim::protocol_v2;

namespace {

int failures{};
constexpr auto kIoTimeout = std::chrono::seconds(1);

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __func__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            ++failures;                                                                                                \
        }                                                                                                              \
    } while (false)

class Memory final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t address) override {
        std::lock_guard lock(mutex_);
        return lines_[address];
    }

    void writeLine(std::uint64_t address, std::span<const std::byte, kLineSize> data) override {
        std::lock_guard lock(mutex_);
        std::copy(data.begin(), data.end(), lines_[address].begin());
    }

private:
    std::mutex mutex_;
    std::map<std::uint64_t, std::array<std::byte, kLineSize>> lines_;
};

CoherenceFrame registration(std::uint16_t host) {
    auto frame = initializeFrame(Opcode::Register);
    setSrcHost(frame, host);
    setDstHost(frame, kServerHost);
    setCapabilities(frame, static_cast<std::uint64_t>(Capability::MODEL_SNOOP));
    setSize(frame, kLineSize);
    setValue(frame, 4096);
    setExpected(frame, 4);
    return frame;
}

CoherenceFrame command(Opcode op, std::uint16_t host, std::uint64_t session, std::uint64_t request_id,
                       std::uint64_t address, LineState state = LineState::I, std::uint64_t installed_epoch = 0) {
    auto frame = initializeFrame(op);
    setSrcHost(frame, host);
    setDstHost(frame, kServerHost);
    setSessionId(frame, session);
    setRequestId(frame, request_id);
    setAddress(frame, address);
    setLineState(frame, state);
    setEpoch(frame, installed_epoch);
    return frame;
}

CoherenceFrame ackFor(const CoherenceFrame &snoop) {
    auto ack = initializeFrame(Opcode::SnoopAck);
    setSrcHost(ack, dstHost(snoop));
    setDstHost(ack, kServerHost);
    setSessionId(ack, sessionId(snoop));
    setSnoopId(ack, snoopId(snoop));
    setAddress(ack, address(snoop));
    setEpoch(ack, epoch(snoop));
    setStatus(ack, Status::Ok);
    setAckStrength(ack, AckStrength::MODEL);
    const auto op = opcode(snoop);
    const bool downgrade = op == Opcode::SnpDowngrade || op == Opcode::SnpDataDowngrade;
    setLineState(ack, downgrade ? LineState::S : LineState::I);
    if (op == Opcode::SnpDataInv || op == Opcode::SnpDataDowngrade)
        setPayloadLength(ack, kLineSize);
    return ack;
}

CoherenceFrame receive(CoherenceShmChannelV2 &channel) {
    CoherenceFrame frame{};
    CHECK(channel.receive(frame, kIoTimeout) == CoherenceShmReceiveResult::Frame);
    return frame;
}

void runClientScenario(CoherenceShmChannelV2 &client_a, CoherenceShmChannelV2 &client_b) {
    const auto register_a = registration(1);
    const auto register_b = registration(2);
    CHECK(client_a.send(register_a, kIoTimeout));
    CHECK(client_b.send(register_b, kIoTimeout));
    const auto registered_a = receive(client_a);
    const auto registered_b = receive(client_b);
    CHECK(validateResponse(registered_a, register_a));
    CHECK(validateResponse(registered_b, register_b));
    CHECK(status(registered_a) == Status::Ok);
    CHECK(status(registered_b) == Status::Ok);

    const auto getm = command(Opcode::Getm, 1, sessionId(registered_a), 1, 0x1000);
    CHECK(client_a.send(getm, kIoTimeout));
    const auto getm_response = receive(client_a);
    CHECK(validateResponse(getm_response, getm));
    CHECK(status(getm_response) == Status::Ok);
    CHECK(lineState(getm_response) == LineState::M);

    CHECK(client_a.send(getm, kIoTimeout));
    CHECK(encodeFrame(receive(client_a)) == encodeFrame(getm_response));

    const auto gets = command(Opcode::Gets, 2, sessionId(registered_b), 1, 0x1000);
    CHECK(client_b.send(gets, kIoTimeout));
    const auto snoop = receive(client_a);
    CHECK(opcode(snoop) == Opcode::SnpDataDowngrade);
    CHECK(client_a.send(ackFor(snoop), kIoTimeout));
    const auto gets_response = receive(client_b);
    CHECK(validateResponse(gets_response, gets));
    CHECK(status(gets_response) == Status::Ok);
    CHECK(lineState(gets_response) == LineState::S);
}

void testRegistrationReplayAndDuplexSnoop() {
    const auto name = "/cxlmemsim_coherence_v2_test_" + std::to_string(::getpid());
    auto listener = CoherenceShmTransportV2::createServer(name, 4);
    auto connector_a = CoherenceShmTransportV2::openClient(name);
    auto connector_b = CoherenceShmTransportV2::openClient(name);
    CHECK(listener != nullptr);
    CHECK(connector_a != nullptr);
    CHECK(connector_b != nullptr);
    if (!listener || !connector_a || !connector_b)
        return;

    auto client_a = connector_a->connect(kIoTimeout);
    auto client_b = connector_b->connect(kIoTimeout);
    auto server_a = listener->accept(kIoTimeout);
    auto server_b = listener->accept(kIoTimeout);
    CHECK(client_a.has_value());
    CHECK(client_b.has_value());
    CHECK(server_a.has_value());
    CHECK(server_b.has_value());
    if (!client_a || !client_b || !server_a || !server_b)
        return;

    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(100));
    auto serving_a = std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_a); });
    auto serving_b = std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_b); });

    runClientScenario(*client_a, *client_b);

    client_a->close();
    client_b->close();
    CHECK(serving_a.wait_for(kIoTimeout) == std::future_status::ready);
    CHECK(serving_b.wait_for(kIoTimeout) == std::future_status::ready);
    CHECK(serving_a.get());
    CHECK(serving_b.get());
}

void testSameConnectionAckProgressesWhileOrdinaryDispatchWaits() {
    const auto name = "/cxlmemsim_coherence_v2_ack_test_" + std::to_string(::getpid());
    auto listener = CoherenceShmTransportV2::createServer(name, 2);
    auto connector_a = CoherenceShmTransportV2::openClient(name);
    auto connector_b = CoherenceShmTransportV2::openClient(name);
    auto client_a = connector_a->connect(kIoTimeout);
    auto client_b = connector_b->connect(kIoTimeout);
    auto server_a = listener->accept(kIoTimeout);
    auto server_b = listener->accept(kIoTimeout);

    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(200));
    auto serving_a = std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_a); });
    auto serving_b = std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_b); });

    CHECK(client_a->send(registration(7), kIoTimeout));
    CHECK(client_b->send(registration(8), kIoTimeout));
    const auto registered_a = receive(*client_a);
    const auto registered_b = receive(*client_b);
    CHECK(client_a->send(command(Opcode::Getm, 7, sessionId(registered_a), 1, 0x2000), kIoTimeout));
    CHECK(status(receive(*client_a)) == Status::Ok);
    CHECK(client_b->send(command(Opcode::Getm, 8, sessionId(registered_b), 1, 0x3000), kIoTimeout));
    CHECK(status(receive(*client_b)) == Status::Ok);

    CHECK(client_a->send(command(Opcode::Gets, 7, sessionId(registered_a), 2, 0x3000), kIoTimeout));
    const auto snoop_b = receive(*client_b);
    CHECK(client_b->send(command(Opcode::Gets, 8, sessionId(registered_b), 2, 0x2000), kIoTimeout));
    const auto snoop_a = receive(*client_a);
    CHECK(client_a->send(ackFor(snoop_a), kIoTimeout));
    CHECK(client_b->send(ackFor(snoop_b), kIoTimeout));
    CHECK(status(receive(*client_a)) == Status::Ok);
    CHECK(status(receive(*client_b)) == Status::Ok);

    client_a->close();
    client_b->close();
    CHECK(serving_a.wait_for(kIoTimeout) == std::future_status::ready);
    CHECK(serving_b.wait_for(kIoTimeout) == std::future_status::ready);
    CHECK(serving_a.get());
    CHECK(serving_b.get());
}

void verifyConcurrentProducers(CoherenceShmChannelV2 &producer, CoherenceShmChannelV2 &consumer) {
    constexpr std::size_t kProducerCount = 8;
    constexpr std::size_t kFramesPerProducer = 32;
    constexpr std::size_t kFrameCount = kProducerCount * kFramesPerProducer;
    std::barrier start_round(static_cast<std::ptrdiff_t>(kProducerCount));
    auto receiving = std::async(std::launch::async, [&] {
        std::set<std::uint64_t> ids;
        for (std::size_t index = 0; index < kFrameCount; ++index) {
            CoherenceFrame frame{};
            if (consumer.receive(frame, std::chrono::milliseconds(200)) != CoherenceShmReceiveResult::Frame)
                break;
            ids.insert(requestId(frame));
        }
        return ids;
    });
    std::vector<std::future<bool>> producers;
    for (std::size_t producer_id = 0; producer_id < kProducerCount; ++producer_id) {
        producers.push_back(std::async(std::launch::async, [&, producer_id] {
            for (std::size_t sequence = 0; sequence < kFramesPerProducer; ++sequence) {
                start_round.arrive_and_wait();
                auto frame = initializeFrame(Opcode::Heartbeat);
                setRequestId(frame, producer_id * kFramesPerProducer + sequence + 1);
                if (!producer.send(frame, kIoTimeout))
                    return false;
            }
            return true;
        }));
    }
    for (auto &result : producers)
        CHECK(result.get());
    CHECK(receiving.get().size() == kFrameCount);
}

void testConcurrentRingProducersAreSerializedInBothDirections() {
    const auto name = "/cxlmemsim_coherence_v2_producer_test_" + std::to_string(::getpid());
    auto listener = CoherenceShmTransportV2::createServer(name, 1);
    auto connector = CoherenceShmTransportV2::openClient(name);
    auto client = connector->connect(kIoTimeout);
    auto server = listener->accept(kIoTimeout);
    verifyConcurrentProducers(*client, *server);
    verifyConcurrentProducers(*server, *client);
    client->close();
    server->close();
}

void testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend() {
    const auto name = "/cxlmemsim_coherence_v2_send_failure_test_" + std::to_string(::getpid());
    auto listener = CoherenceShmTransportV2::createServer(name, 1);
    auto connector = CoherenceShmTransportV2::openClient(name);
    auto client = connector->connect(kIoTimeout);
    auto server_channel = listener->accept(kIoTimeout);

    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    auto serving =
        std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_channel, "shm"); });

    const auto register_request = registration(11);
    CHECK(client->send(register_request, kIoTimeout));
    const auto register_response = receive(*client);
    const auto session = sessionId(register_response);

    auto filler = initializeFrame(Opcode::Response);
    for (std::size_t index = 0; index < 16; ++index) {
        setRequestId(filler, 100 + index);
        CHECK(server_channel->send(filler, std::chrono::milliseconds(0)));
    }
    const auto request = command(Opcode::Gets, 11, session, 1, 0x4000);
    CHECK(client->send(request, kIoTimeout));

    const auto completed = serving.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    CHECK(completed);
    if (!completed)
        client->close();
    CHECK(!serving.get());
    const auto snapshot = registry.inspect(session);
    CHECK(snapshot.has_value());
    if (snapshot) {
        CHECK(snapshot->state == SessionState::OfflineRetained);
        CHECK(!snapshot->has_sender);
        CHECK(snapshot->pinned_response_count == 1);
    }
    client->close();
    server_channel->close();
}

void testDeliveryFailureCannotBeStarvedByInboundFlood() {
    const auto name = "/cxlmemsim_coherence_v2_delivery_flood_test_" + std::to_string(::getpid());
    auto listener = CoherenceShmTransportV2::createServer(name, 1);
    auto connector = CoherenceShmTransportV2::openClient(name);
    auto client = connector->connect(kIoTimeout);
    auto server_channel = listener->accept(kIoTimeout);

    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    auto serving =
        std::async(std::launch::async, [&] { return serveCoherenceV2ShmChannel(server, *server_channel, "shm"); });

    const auto register_request = registration(12);
    CHECK(client->send(register_request, kIoTimeout));
    const auto register_response = receive(*client);
    const auto session = sessionId(register_response);

    auto filler = initializeFrame(Opcode::Response);
    for (std::size_t index = 0; index < 16; ++index) {
        setRequestId(filler, 200 + index);
        CHECK(server_channel->send(filler, std::chrono::milliseconds(0)));
    }
    CHECK(client->send(command(Opcode::Gets, 12, session, 1, 0x5000), kIoTimeout));

    auto stale_ack = initializeFrame(Opcode::SnoopAck);
    setSrcHost(stale_ack, 12);
    setDstHost(stale_ack, kServerHost);
    setSessionId(stale_ack, session);
    setSnoopId(stale_ack, 1);
    setAddress(stale_ack, 0x6000);
    setEpoch(stale_ack, 1);
    setStatus(stale_ack, Status::Ok);
    setAckStrength(stale_ack, AckStrength::MODEL);
    setLineState(stale_ack, LineState::I);

    std::atomic<bool> stop_flood{};
    std::atomic<std::size_t> flooded_frames{};
    auto flooding = std::async(std::launch::async, [&] {
        while (!stop_flood.load(std::memory_order_acquire)) {
            if (!client->send(stale_ack, std::chrono::milliseconds(100)))
                break;
            flooded_frames.fetch_add(1, std::memory_order_relaxed);
        }
    });

    bool delivery_failed = false;
    const auto failure_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < failure_deadline) {
        const auto snapshot = registry.inspect(session);
        if (snapshot && snapshot->state == SessionState::OfflineRetained && !snapshot->has_sender) {
            delivery_failed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    CHECK(delivery_failed);
    CHECK(flooded_frames.load(std::memory_order_relaxed) != 0);

    const bool completed = serving.wait_for(std::chrono::milliseconds(300)) == std::future_status::ready;
    CHECK(completed);

    stop_flood.store(true, std::memory_order_release);
    flooding.get();
    if (!completed)
        CHECK(serving.wait_for(kIoTimeout) == std::future_status::ready);
    CHECK(!serving.get());
    client->close();
    server_channel->close();
}

void runExternalServerScenario(const std::string &name) {
    auto connector_a = CoherenceShmTransportV2::openClient(name);
    auto connector_b = CoherenceShmTransportV2::openClient(name);
    CHECK(connector_a != nullptr);
    CHECK(connector_b != nullptr);
    if (!connector_a || !connector_b)
        return;
    auto client_a = connector_a->connect(kIoTimeout);
    auto client_b = connector_b->connect(kIoTimeout);
    CHECK(client_a.has_value());
    CHECK(client_b.has_value());
    if (!client_a || !client_b)
        return;
    runClientScenario(*client_a, *client_b);
    client_a->close();
    client_b->close();
}

} // namespace

int main(int argc, char **argv) {
    if (argc == 3 && std::string(argv[1]) == "--connect")
        runExternalServerScenario(argv[2]);
    else {
        testRegistrationReplayAndDuplexSnoop();
        testSameConnectionAckProgressesWhileOrdinaryDispatchWaits();
        testConcurrentRingProducersAreSerializedInBothDirections();
        testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend();
        testDeliveryFailureCannotBeStarvedByInboundFlood();
    }
    if (failures != 0) {
        std::cerr << failures << " coherence SHM transport v2 test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "coherence SHM transport v2 tests passed\n";
    return EXIT_SUCCESS;
}
