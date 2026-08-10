#pragma once

#include "coherence_memory_backend.h"

#include <cstdint>

class SharedMemoryManager;

namespace cxlmemsim {

// Makes the protocol-v2 directory and the legacy server operate on the same
// data bytes. Legacy metadata remains isolated from the explicitly enabled v2
// coherent domain.
class SharedMemoryCoherenceBackend final : public CoherenceMemoryBackend {
public:
    explicit SharedMemoryCoherenceBackend(SharedMemoryManager &manager) noexcept;

    std::array<std::byte, 64> readLine(std::uint64_t address) override;
    void writeLine(std::uint64_t address, std::span<const std::byte, 64> data) override;

private:
    SharedMemoryManager *manager_;
};

enum class ServerClientProtocol {
    LegacyV1,
    CoherenceV2,
    Closed,
    Error,
};

// Peeks without consuming bytes. When v2 is disabled, every live connection
// remains on the legacy path and no protocol inference is attempted.
ServerClientProtocol detectServerClientProtocol(int fd, bool coherence_v2_enabled) noexcept;

} // namespace cxlmemsim
