#include "coherence_rdma_transport_v2.h"

#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "coherence_server_v2.h"
#include "endpoint_session_registry.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <future>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>

#ifdef HAS_RDMA
class RDMAConnection {};
#endif

using namespace cxlmemsim;
using namespace cxlmemsim::mesi_v2;
using namespace cxlmemsim::protocol_v2;

namespace {

int failures{};

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __func__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            ++failures;                                                                                                \
        }                                                                                                              \
    } while (false)

constexpr auto kTimeout = std::chrono::seconds(1);
constexpr std::uint64_t kLine = 0x1000;

class Memory final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t) override {
        std::lock_guard lock(mutex_);
        return line_;
    }

    void writeLine(std::uint64_t, std::span<const std::byte, kLineSize> line) override {
        std::lock_guard lock(mutex_);
        std::copy(line.begin(), line.end(), line_.begin());
    }

private:
    std::mutex mutex_;
    std::array<std::byte, kLineSize> line_{};
};

class InMemoryRdmaBackend {
public:
    CoherenceRdmaCallbacks callbacks() {
        return {
            .receive = [this](MutableCoherenceRdmaFrame bytes) { return receive(bytes); },
            .send = [this](ConstCoherenceRdmaFrame bytes) { return send(bytes); },
            .close = [this] { close(); },
        };
    }

    bool clientSend(const CoherenceFrame &frame) {
        std::lock_guard lock(mutex_);
        if (closed_)
            return false;
        client_to_server_.push_back(encodeFrame(frame));
        changed_.notify_all();
        return true;
    }

    std::optional<EncodedFrame>
    clientReceive(std::chrono::milliseconds timeout = std::chrono::duration_cast<std::chrono::milliseconds>(kTimeout)) {
        std::unique_lock lock(mutex_);
        if (!changed_.wait_for(lock, timeout, [&] { return closed_ || !server_to_client_.empty(); }))
            return std::nullopt;
        if (server_to_client_.empty())
            return std::nullopt;
        auto frame = server_to_client_.front();
        server_to_client_.pop_front();
        return frame;
    }

    bool waitUntilReceiveBlocked() {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, kTimeout, [&] { return receive_active_; });
    }

    bool sawConcurrentDisjointBuffers() const {
        std::lock_guard lock(mutex_);
        return concurrent_send_seen_ && buffers_disjoint_;
    }

    bool allCallbacksUsedFixedFrames() const {
        std::lock_guard lock(mutex_);
        return all_frame_sizes_correct_;
    }

    void failNextServerSend() {
        std::lock_guard lock(mutex_);
        fail_next_server_send_ = true;
    }

    void throwNextServerSend() {
        std::lock_guard lock(mutex_);
        throw_next_server_send_ = true;
    }

    std::size_t serverSendAttempts() const {
        std::lock_guard lock(mutex_);
        return server_send_attempts_;
    }

    void clientClose() { close(); }

private:
    CoherenceRdmaReceiveResult receive(MutableCoherenceRdmaFrame bytes) {
        std::unique_lock lock(mutex_);
        all_frame_sizes_correct_ &= bytes.size() == kFrameSize;
        receive_active_ = true;
        receive_begin_ = reinterpret_cast<std::uintptr_t>(bytes.data());
        changed_.notify_all();
        changed_.wait(lock, [&] { return closed_ || !client_to_server_.empty(); });
        if (client_to_server_.empty()) {
            receive_active_ = false;
            receive_begin_ = 0;
            changed_.notify_all();
            return CoherenceRdmaReceiveResult::Closed;
        }
        std::copy(client_to_server_.front().begin(), client_to_server_.front().end(), bytes.begin());
        client_to_server_.pop_front();
        receive_active_ = false;
        receive_begin_ = 0;
        changed_.notify_all();
        return CoherenceRdmaReceiveResult::Frame;
    }

    bool send(ConstCoherenceRdmaFrame bytes) {
        std::lock_guard lock(mutex_);
        ++server_send_attempts_;
        all_frame_sizes_correct_ &= bytes.size() == kFrameSize;
        if (closed_)
            return false;
        if (fail_next_server_send_) {
            fail_next_server_send_ = false;
            return false;
        }
        if (throw_next_server_send_) {
            throw_next_server_send_ = false;
            throw std::runtime_error("injected RDMA send failure");
        }
        if (receive_active_) {
            concurrent_send_seen_ = true;
            const auto send_begin = reinterpret_cast<std::uintptr_t>(bytes.data());
            const auto send_end = send_begin + bytes.size();
            const auto receive_end = receive_begin_ + kFrameSize;
            buffers_disjoint_ &= send_end <= receive_begin_ || receive_end <= send_begin;
        }
        EncodedFrame frame{};
        std::copy(bytes.begin(), bytes.end(), frame.begin());
        server_to_client_.push_back(frame);
        changed_.notify_all();
        return true;
    }

    void close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        changed_.notify_all();
    }

    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::deque<EncodedFrame> client_to_server_;
    std::deque<EncodedFrame> server_to_client_;
    bool closed_{};
    bool receive_active_{};
    bool concurrent_send_seen_{};
    bool buffers_disjoint_{true};
    bool all_frame_sizes_correct_{true};
    bool fail_next_server_send_{};
    bool throw_next_server_send_{};
    std::size_t server_send_attempts_{};
    std::uintptr_t receive_begin_{};
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

CoherenceFrame command(Opcode opcode, std::uint16_t host, std::uint64_t session, std::uint64_t request_id,
                       std::uint64_t address = kLine) {
    auto frame = initializeFrame(opcode);
    setSrcHost(frame, host);
    setDstHost(frame, kServerHost);
    setSessionId(frame, session);
    setRequestId(frame, request_id);
    setAddress(frame, address);
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
    setLineState(ack, LineState::S);
    if (opcode(snoop) == Opcode::SnpDataDowngrade) {
        setPayloadLength(ack, kLineSize);
    }
    return ack;
}

CoherenceFrame decode(const std::optional<EncodedFrame> &wire) {
    CoherenceFrame frame{};
    CHECK(wire.has_value());
    if (wire)
        CHECK(decodeFrame(*wire, frame));
    return frame;
}

void testCallbackTransportPreservesExactlyOneFixedFrame() {
    EncodedFrame expected{};
    for (std::size_t index = 0; index < expected.size(); ++index)
        expected[index] = static_cast<std::uint8_t>(index);
    EncodedFrame sent{};
    bool closed{};
    CallbackCoherenceRdmaByteTransport transport({
        .receive =
            [&expected](MutableCoherenceRdmaFrame bytes) {
                std::copy(expected.begin(), expected.end(), bytes.begin());
                return CoherenceRdmaReceiveResult::Frame;
            },
        .send =
            [&sent](ConstCoherenceRdmaFrame bytes) {
                std::copy(bytes.begin(), bytes.end(), sent.begin());
                return true;
            },
        .close = [&closed] { closed = true; },
    });

    EncodedFrame received{};
    CHECK(transport.receive(received) == CoherenceRdmaReceiveResult::Frame);
    CHECK(received == expected);
    CHECK(transport.send(received));
    CHECK(sent == expected);
    transport.close();
    CHECK(closed);
}

void testRegistrationSnoopAckReplayAndDuplexBufferIsolation() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(100));

    InMemoryRdmaBackend host_backend;
    InMemoryRdmaBackend device_backend;
    CallbackCoherenceRdmaByteTransport host_transport(host_backend.callbacks());
    CallbackCoherenceRdmaByteTransport device_transport(device_backend.callbacks());
    auto host_server =
        std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 1, host_transport, "rdma-fake"); });
    auto device_server =
        std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 2, device_transport, "rdma-fake"); });

    const auto host_register = registration(3);
    CHECK(host_backend.clientSend(host_register));
    const auto host_register_response = decode(host_backend.clientReceive());
    CHECK(validateResponse(host_register_response, host_register));
    CHECK(status(host_register_response) == Status::Ok);

    const auto host_gets = command(Opcode::Gets, 3, sessionId(host_register_response), 1);
    CHECK(host_backend.clientSend(host_gets));
    const auto host_gets_wire = host_backend.clientReceive();
    const auto host_gets_response = decode(host_gets_wire);
    CHECK(validateResponse(host_gets_response, host_gets));
    CHECK(lineState(host_gets_response) == LineState::E);

    CHECK(host_backend.clientSend(host_gets));
    const auto replay_wire = host_backend.clientReceive();
    CHECK(replay_wire == host_gets_wire);
    CHECK(!host_backend.clientReceive(std::chrono::milliseconds(10)).has_value());
    CHECK(host_backend.waitUntilReceiveBlocked());

    const auto device_register = registration(4);
    CHECK(device_backend.clientSend(device_register));
    const auto device_register_response = decode(device_backend.clientReceive());
    CHECK(validateResponse(device_register_response, device_register));

    const auto device_gets = command(Opcode::Gets, 4, sessionId(device_register_response), 1);
    CHECK(device_backend.clientSend(device_gets));
    const auto snoop = decode(host_backend.clientReceive());
    CHECK(opcode(snoop) == Opcode::SnpDowngrade);
    CHECK(dstHost(snoop) == 3);
    CHECK(host_backend.clientSend(ackFor(snoop)));

    const auto device_gets_response = decode(device_backend.clientReceive());
    CHECK(validateResponse(device_gets_response, device_gets));
    CHECK(status(device_gets_response) == Status::Ok);
    CHECK(lineState(device_gets_response) == LineState::S);
    CHECK(host_backend.sawConcurrentDisjointBuffers());
    CHECK(host_backend.allCallbacksUsedFixedFrames());
    CHECK(device_backend.allCallbacksUsedFixedFrames());

    host_backend.clientClose();
    device_backend.clientClose();
    CHECK(host_server.wait_for(kTimeout) == std::future_status::ready);
    CHECK(device_server.wait_for(kTimeout) == std::future_status::ready);
    CHECK(host_server.get());
    CHECK(device_server.get());
}

void testSameConnectionAckProgressesWhileOrdinaryDispatchWaits() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(200));
    InMemoryRdmaBackend a_backend;
    InMemoryRdmaBackend b_backend;
    CallbackCoherenceRdmaByteTransport a_transport(a_backend.callbacks());
    CallbackCoherenceRdmaByteTransport b_transport(b_backend.callbacks());
    auto serving_a = std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 21, a_transport); });
    auto serving_b = std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 22, b_transport); });

    CHECK(a_backend.clientSend(registration(9)));
    CHECK(b_backend.clientSend(registration(10)));
    const auto registered_a = decode(a_backend.clientReceive());
    const auto registered_b = decode(b_backend.clientReceive());
    CHECK(a_backend.clientSend(command(Opcode::Getm, 9, sessionId(registered_a), 1, 0x2000)));
    CHECK(status(decode(a_backend.clientReceive())) == Status::Ok);
    CHECK(b_backend.clientSend(command(Opcode::Getm, 10, sessionId(registered_b), 1, 0x3000)));
    CHECK(status(decode(b_backend.clientReceive())) == Status::Ok);

    CHECK(a_backend.clientSend(command(Opcode::Gets, 9, sessionId(registered_a), 2, 0x3000)));
    const auto snoop_b = decode(b_backend.clientReceive());
    CHECK(b_backend.clientSend(command(Opcode::Gets, 10, sessionId(registered_b), 2, 0x2000)));
    const auto snoop_a = decode(a_backend.clientReceive());
    CHECK(a_backend.clientSend(ackFor(snoop_a)));
    CHECK(b_backend.clientSend(ackFor(snoop_b)));
    const auto response_a = decode(a_backend.clientReceive());
    const auto response_b = decode(b_backend.clientReceive());
    if (status(response_a) != Status::Ok || status(response_b) != Status::Ok) {
        std::cerr << "crossed responses: a=" << static_cast<unsigned>(status(response_a))
                  << " b=" << static_cast<unsigned>(status(response_b)) << '\n';
    }
    CHECK(status(response_a) == Status::Ok);
    CHECK(status(response_b) == Status::Ok);

    a_backend.clientClose();
    b_backend.clientClose();
    CHECK(serving_a.wait_for(kTimeout) == std::future_status::ready);
    CHECK(serving_b.wait_for(kTimeout) == std::future_status::ready);
    CHECK(serving_a.get());
    CHECK(serving_b.get());
}

void testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    InMemoryRdmaBackend backend;
    CallbackCoherenceRdmaByteTransport transport(backend.callbacks());
    auto serving = std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 31, transport); });

    const auto register_request = registration(13);
    CHECK(backend.clientSend(register_request));
    const auto register_response = decode(backend.clientReceive());
    const auto session = sessionId(register_response);
    const auto sends_before_failure = backend.serverSendAttempts();

    backend.failNextServerSend();
    CHECK(backend.clientSend(command(Opcode::Gets, 13, session, 1, 0x4000)));
    const auto completed = serving.wait_for(kTimeout) == std::future_status::ready;
    CHECK(completed);
    if (!completed)
        backend.clientClose();
    CHECK(!serving.get());
    CHECK(backend.serverSendAttempts() == sends_before_failure + 1);
    CHECK(!backend.clientReceive(std::chrono::milliseconds(10)).has_value());
    const auto snapshot = registry.inspect(session);
    CHECK(snapshot.has_value());
    if (snapshot) {
        CHECK(snapshot->state == SessionState::OfflineRetained);
        CHECK(!snapshot->has_sender);
        CHECK(snapshot->pinned_response_count == 1);
    }
}

void testThrowingRdmaCallbackFailsConnectionWithoutEscaping() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    InMemoryRdmaBackend backend;
    CallbackCoherenceRdmaByteTransport transport(backend.callbacks());
    auto serving = std::async(std::launch::async, [&] { return serveCoherenceV2Rdma(server, 32, transport); });

    backend.throwNextServerSend();
    CHECK(backend.clientSend(registration(14)));
    CHECK(serving.wait_for(kTimeout) == std::future_status::ready);
    bool escaped = false;
    bool served = true;
    try {
        served = serving.get();
    } catch (const std::runtime_error &) {
        escaped = true;
    }
    CHECK(!escaped);
    CHECK(!served);
}

#ifdef HAS_RDMA
void testRdmaConnectionAdapterUsesExplicitSafeFrameOps() {
    RDMAConnection connection;
    bool sent{};
    auto transport = makeCoherenceRdmaConnectionTransportV2(
        connection, {
                        .receive_frame =
                            [](RDMAConnection &, MutableCoherenceRdmaFrame bytes) {
                                std::fill(bytes.begin(), bytes.end(), 0x5a);
                                return CoherenceRdmaReceiveResult::Frame;
                            },
                        .send_frame =
                            [&sent](RDMAConnection &, ConstCoherenceRdmaFrame bytes) {
                                sent = std::all_of(bytes.begin(), bytes.end(),
                                                   [](std::uint8_t byte) { return byte == 0x5a; });
                                return true;
                            },
                        .close = [](RDMAConnection &) {},
                    });
    EncodedFrame frame{};
    CHECK(transport->receive(frame) == CoherenceRdmaReceiveResult::Frame);
    CHECK(transport->send(frame));
    CHECK(sent);
}
#endif

} // namespace

int main() {
    static_assert(kFrameSize == 192);
    static_assert(sizeof(EncodedFrame) == 192);
    testCallbackTransportPreservesExactlyOneFixedFrame();
    testRegistrationSnoopAckReplayAndDuplexBufferIsolation();
    testSameConnectionAckProgressesWhileOrdinaryDispatchWaits();
    testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend();
    testThrowingRdmaCallbackFailsConnectionWithoutEscaping();
#ifdef HAS_RDMA
    testRdmaConnectionAdapterUsesExplicitSafeFrameOps();
#endif
    if (failures != 0) {
        std::cerr << failures << " coherence RDMA transport v2 test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "coherence RDMA transport v2 tests passed\n";
    return EXIT_SUCCESS;
}
