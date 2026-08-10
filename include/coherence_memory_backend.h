#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>

namespace cxlmemsim {

class CoherenceMemoryBackend {
public:
    virtual ~CoherenceMemoryBackend() = default;

    // Operations on disjoint cache lines may execute concurrently. Implementations must be thread-safe and must not
    // reenter the transaction engine from either operation.
    virtual std::array<std::byte, 64> readLine(std::uint64_t address) = 0;
    virtual void writeLine(std::uint64_t address, std::span<const std::byte, 64> data) = 0;
};

} // namespace cxlmemsim
