#include "coherence_server_runtime_v2.h"

#include "coherence_protocol_v2.h"
#include "shared_memory_manager.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <unistd.h>

using namespace cxlmemsim;
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

bool writeAll(int fd, std::span<const std::uint8_t> bytes) {
    while (!bytes.empty()) {
        const auto count = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        if (count <= 0)
            return false;
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
    return true;
}

void testSharedMemoryBackendUsesServerBackingStore() {
    const auto name = "/cxlmemsim_v2_runtime_" + std::to_string(::getpid());
    SharedMemoryManager manager(1, name);
    CHECK(manager.initialize());
    SharedMemoryCoherenceBackend backend(manager);

    std::array<std::byte, kLineSize> written{};
    for (std::size_t index = 0; index < written.size(); ++index)
        written[index] = static_cast<std::byte>(index ^ 0x5aU);
    backend.writeLine(0x2000, written);
    CHECK(backend.readLine(0x2000) == written);

    std::array<std::uint8_t, kLineSize> direct{};
    CHECK(manager.read_cacheline(0x2000, direct.data(), direct.size()));
    CHECK(std::equal(direct.begin(), direct.end(), reinterpret_cast<const std::uint8_t *>(written.data())));
    manager.cleanup();
}

void testProtocolDetectionIsExplicitAndNonConsuming() {
    int sockets[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    const auto registration = encodeFrame(initializeFrame(Opcode::Register));
    CHECK(writeAll(sockets[1], registration));
    CHECK(detectServerClientProtocol(sockets[0], false) == ServerClientProtocol::LegacyV1);
    CHECK(detectServerClientProtocol(sockets[0], true) == ServerClientProtocol::CoherenceV2);

    std::array<std::uint8_t, 4> prefix{};
    CHECK(::recv(sockets[0], prefix.data(), prefix.size(), MSG_WAITALL) == static_cast<ssize_t>(prefix.size()));
    CHECK(prefix[0] == registration[0]);
    CHECK(prefix[1] == registration[1]);
    ::close(sockets[0]);
    ::close(sockets[1]);

    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    const std::array<std::uint8_t, 4> legacy_prefix{0, 0x34, 0x12, 0};
    CHECK(writeAll(sockets[1], legacy_prefix));
    CHECK(detectServerClientProtocol(sockets[0], true) == ServerClientProtocol::LegacyV1);
    ::close(sockets[0]);
    ::close(sockets[1]);
}

void testProtocolDetectionReportsCleanEof() {
    int sockets[2]{};
    CHECK(::socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) == 0);
    ::close(sockets[1]);
    CHECK(detectServerClientProtocol(sockets[0], true) == ServerClientProtocol::Closed);
    ::close(sockets[0]);
}

} // namespace

int main() {
    testSharedMemoryBackendUsesServerBackingStore();
    testProtocolDetectionIsExplicitAndNonConsuming();
    testProtocolDetectionReportsCleanEof();
    if (failures != 0) {
        std::cerr << failures << " coherence server runtime v2 test(s) failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "coherence server runtime v2 tests passed\n";
    return EXIT_SUCCESS;
}
