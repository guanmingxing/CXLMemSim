#include "coherence_server_v2.h"
#include "coherence_tcp_transport_v2.h"

#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "endpoint_session_registry.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <mutex>
#include <poll.h>
#include <span>
#include <sys/socket.h>
#include <unistd.h>

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

class Memory final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t) override {
        std::lock_guard lock(mutex_);
        return line_;
    }

    void writeLine(std::uint64_t, std::span<const std::byte, kLineSize> data) override {
        std::lock_guard lock(mutex_);
        std::copy(data.begin(), data.end(), line_.begin());
    }

private:
    std::mutex mutex_;
    std::array<std::byte, kLineSize> line_{};
};

bool writeAll(int fd, std::span<const std::uint8_t> bytes) {
    while (!bytes.empty()) {
        const auto count = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        if (count <= 0)
            return false;
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
    return true;
}

bool readAll(int fd, std::span<std::uint8_t> bytes) {
    while (!bytes.empty()) {
        const auto count = ::recv(fd, bytes.data(), bytes.size(), 0);
        if (count <= 0)
            return false;
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
    return true;
}

CoherenceFrame registration(std::uint16_t host, std::uint16_t wire_version = kProtocolVersion) {
    auto frame = initializeFrame(Opcode::Register);
    setVersion(frame, wire_version);
    setSrcHost(frame, host);
    setDstHost(frame, kServerHost);
    setCapabilities(frame, static_cast<std::uint64_t>(Capability::MODEL_SNOOP));
    setSize(frame, kLineSize);
    setValue(frame, 4096);
    setExpected(frame, 4);
    return frame;
}

CoherenceFrame command(Opcode opcode, std::uint16_t host, std::uint64_t session, std::uint64_t request_id,
                       std::uint64_t address) {
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
    const auto op = opcode(snoop);
    const bool downgrade = op == Opcode::SnpDowngrade || op == Opcode::SnpDataDowngrade;
    setLineState(ack, downgrade ? LineState::S : LineState::I);
    if (op == Opcode::SnpDataInv || op == Opcode::SnpDataDowngrade)
        setPayloadLength(ack, kLineSize);
    return ack;
}

CoherenceFrame receiveFrame(int fd) {
    EncodedFrame wire{};
    CHECK(readAll(fd, wire));
    CoherenceFrame frame{};
    CHECK(decodeFrame(wire, frame));
    return frame;
}

void testFramedStreamRegistrationSingleDeliveryAndReplay() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    int sockets[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);

    auto serving = std::async(std::launch::async, [&] { return serveCoherenceV2Stream(server, 1, sockets[0], "tcp"); });

    const auto register_request = registration(3);
    const auto register_wire = encodeFrame(register_request);
    CHECK(writeAll(sockets[1], std::span(register_wire).first(13)));
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
    CHECK(writeAll(sockets[1], std::span(register_wire).subspan(13)));
    const auto register_response = receiveFrame(sockets[1]);
    CHECK(validateResponse(register_response, register_request));
    CHECK(status(register_response) == Status::Ok);

    const auto request = command(Opcode::Gets, 3, sessionId(register_response), 1, 0x1000);
    CHECK(writeAll(sockets[1], encodeFrame(request)));
    const auto first_response = receiveFrame(sockets[1]);
    CHECK(validateResponse(first_response, request));
    CHECK(status(first_response) == Status::Ok);

    pollfd probe{sockets[1], POLLIN, 0};
    CHECK(::poll(&probe, 1, 10) == 0);

    CHECK(writeAll(sockets[1], encodeFrame(request)));
    const auto replay = receiveFrame(sockets[1]);
    CHECK(encodeFrame(replay) == encodeFrame(first_response));

    ::shutdown(sockets[1], SHUT_RDWR);
    ::close(sockets[1]);
    CHECK(serving.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(serving.get());
    ::close(sockets[0]);
}

void testSameConnectionAckProgressesWhileOrdinaryDispatchWaits() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(200));
    int a[2]{};
    int b[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, a) == 0);
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, b) == 0);
    auto serving_a = std::async(std::launch::async, [&] { return serveCoherenceV2Stream(server, 11, a[0], "tcp"); });
    auto serving_b = std::async(std::launch::async, [&] { return serveCoherenceV2Stream(server, 12, b[0], "tcp"); });

    const auto register_a = registration(5);
    const auto register_b = registration(6);
    CHECK(writeAll(a[1], encodeFrame(register_a)));
    CHECK(writeAll(b[1], encodeFrame(register_b)));
    const auto registered_a = receiveFrame(a[1]);
    const auto registered_b = receiveFrame(b[1]);

    const auto getm_a = command(Opcode::Getm, 5, sessionId(registered_a), 1, 0x2000);
    const auto getm_b = command(Opcode::Getm, 6, sessionId(registered_b), 1, 0x3000);
    CHECK(writeAll(a[1], encodeFrame(getm_a)));
    CHECK(status(receiveFrame(a[1])) == Status::Ok);
    CHECK(writeAll(b[1], encodeFrame(getm_b)));
    CHECK(status(receiveFrame(b[1])) == Status::Ok);

    const auto gets_a = command(Opcode::Gets, 5, sessionId(registered_a), 2, 0x3000);
    CHECK(writeAll(a[1], encodeFrame(gets_a)));
    const auto snoop_b = receiveFrame(b[1]);
    const auto gets_b = command(Opcode::Gets, 6, sessionId(registered_b), 2, 0x2000);
    CHECK(writeAll(b[1], encodeFrame(gets_b)));
    const auto snoop_a = receiveFrame(a[1]);
    CHECK(writeAll(a[1], encodeFrame(ackFor(snoop_a))));
    CHECK(writeAll(b[1], encodeFrame(ackFor(snoop_b))));
    CHECK(status(receiveFrame(a[1])) == Status::Ok);
    CHECK(status(receiveFrame(b[1])) == Status::Ok);

    ::shutdown(a[1], SHUT_RDWR);
    ::shutdown(b[1], SHUT_RDWR);
    ::close(a[1]);
    ::close(b[1]);
    CHECK(serving_a.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(serving_b.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(serving_a.get());
    CHECK(serving_b.get());
    ::close(a[0]);
    ::close(b[0]);
}

void testV1RegistrationClosesWithoutResponse() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    int sockets[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    auto serving = std::async(std::launch::async, [&] { return serveCoherenceV2Stream(server, 2, sockets[0], "tcp"); });

    CHECK(writeAll(sockets[1], encodeFrame(registration(4, 1))));
    std::array<std::uint8_t, 1> byte{};
    CHECK(::recv(sockets[1], byte.data(), byte.size(), 0) == 0);
    CHECK(serving.wait_for(std::chrono::seconds(1)) == std::future_status::ready);
    CHECK(serving.get());
    ::close(sockets[1]);
    ::close(sockets[0]);
}

void testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend() {
    Memory memory;
    MesiDirectory directory;
    EndpointSessionRegistry registry;
    MesiTransactionEngine engine(directory);
    CoherenceServerV2 server(engine, registry, memory, std::chrono::milliseconds(50));
    int sockets[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    auto serving = std::async(std::launch::async, [&] { return serveCoherenceV2Stream(server, 3, sockets[0], "tcp"); });

    const auto register_request = registration(7);
    CHECK(writeAll(sockets[1], encodeFrame(register_request)));
    const auto register_response = receiveFrame(sockets[1]);
    const auto session = sessionId(register_response);

    CHECK(::shutdown(sockets[1], SHUT_RD) == 0);
    const auto request = command(Opcode::Gets, 7, session, 1, 0x4000);
    CHECK(writeAll(sockets[1], encodeFrame(request)));

    const auto completed = serving.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    CHECK(completed);
    if (!completed)
        (void)::shutdown(sockets[1], SHUT_WR);
    CHECK(!serving.get());
    const auto snapshot = registry.inspect(session);
    CHECK(snapshot.has_value());
    if (snapshot) {
        CHECK(snapshot->state == SessionState::OfflineRetained);
        CHECK(!snapshot->has_sender);
        CHECK(snapshot->pinned_response_count == 1);
    }
    ::close(sockets[1]);
    ::close(sockets[0]);
}

} // namespace

int main() {
    testFramedStreamRegistrationSingleDeliveryAndReplay();
    testSameConnectionAckProgressesWhileOrdinaryDispatchWaits();
    testV1RegistrationClosesWithoutResponse();
    testPinnedResponseSendFailureClosesConnectionWithoutFallbackSend();
    if (failures != 0) {
        std::cerr << failures << " coherence TCP transport v2 test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "coherence TCP transport v2 tests passed\n";
    return EXIT_SUCCESS;
}
