#include "coherence_server_runtime_v2.h"

#include "coherence_protocol_v2.h"
#include "shared_memory_manager.h"

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <sys/socket.h>

namespace cxlmemsim {

SharedMemoryCoherenceBackend::SharedMemoryCoherenceBackend(SharedMemoryManager &manager) noexcept
    : manager_(&manager) {}

std::array<std::byte, 64> SharedMemoryCoherenceBackend::readLine(std::uint64_t address) {
    std::array<std::byte, 64> line{};
    if (!manager_->read_cacheline(address, reinterpret_cast<std::uint8_t *>(line.data()), line.size()))
        throw std::runtime_error("coherence v2 shared-memory read failed");
    return line;
}

void SharedMemoryCoherenceBackend::writeLine(std::uint64_t address, std::span<const std::byte, 64> data) {
    if (!manager_->write_cacheline(address, reinterpret_cast<const std::uint8_t *>(data.data()), data.size()))
        throw std::runtime_error("coherence v2 shared-memory write failed");
}

ServerClientProtocol detectServerClientProtocol(int fd, bool coherence_v2_enabled) noexcept {
    if (fd < 0)
        return ServerClientProtocol::Error;
    if (!coherence_v2_enabled)
        return ServerClientProtocol::LegacyV1;

    std::array<std::uint8_t, sizeof(std::uint32_t)> prefix{};
    ssize_t count;
    do {
        count = ::recv(fd, prefix.data(), prefix.size(), MSG_PEEK | MSG_WAITALL);
    } while (count < 0 && errno == EINTR);
    if (count == 0)
        return ServerClientProtocol::Closed;
    if (count != static_cast<ssize_t>(prefix.size()))
        return ServerClientProtocol::Error;

    const auto wire_magic = static_cast<std::uint32_t>(prefix[0]) | (static_cast<std::uint32_t>(prefix[1]) << 8U) |
                            (static_cast<std::uint32_t>(prefix[2]) << 16U) |
                            (static_cast<std::uint32_t>(prefix[3]) << 24U);
    return wire_magic == protocol_v2::kMagic ? ServerClientProtocol::CoherenceV2 : ServerClientProtocol::LegacyV1;
}

} // namespace cxlmemsim
