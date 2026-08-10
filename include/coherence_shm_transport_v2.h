#pragma once

#include "coherence_protocol_v2.h"
#include "coherence_server_v2.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace cxlmemsim {

enum class CoherenceShmReceiveResult { Frame, Timeout, Closed, Error };

class CoherenceShmTransportV2;

class CoherenceShmChannelV2 {
public:
    CoherenceShmChannelV2() noexcept;
    ~CoherenceShmChannelV2();
    CoherenceShmChannelV2(CoherenceShmChannelV2 &&) noexcept;
    CoherenceShmChannelV2 &operator=(CoherenceShmChannelV2 &&) noexcept;
    CoherenceShmChannelV2(const CoherenceShmChannelV2 &) = delete;
    CoherenceShmChannelV2 &operator=(const CoherenceShmChannelV2 &) = delete;

    bool send(const protocol_v2::CoherenceFrame &frame, std::chrono::milliseconds timeout);
    CoherenceShmReceiveResult receive(protocol_v2::CoherenceFrame &frame, std::chrono::milliseconds timeout);
    void close() noexcept;
    std::uint64_t connectionId() const noexcept;
    explicit operator bool() const noexcept;

private:
    struct Impl;
    explicit CoherenceShmChannelV2(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
    friend class CoherenceShmTransportV2;
};

// A separate shared-memory object and layout keep protocol v2 explicitly
// isolated from the legacy ShmCommunicationManager request/response ABI.
class CoherenceShmTransportV2 {
public:
    static std::unique_ptr<CoherenceShmTransportV2> createServer(const std::string &name,
                                                                 std::size_t channel_count = 16);
    static std::unique_ptr<CoherenceShmTransportV2> openClient(const std::string &name);
    ~CoherenceShmTransportV2();

    CoherenceShmTransportV2(const CoherenceShmTransportV2 &) = delete;
    CoherenceShmTransportV2 &operator=(const CoherenceShmTransportV2 &) = delete;

    std::optional<CoherenceShmChannelV2> connect(std::chrono::milliseconds timeout);
    std::optional<CoherenceShmChannelV2> accept(std::chrono::milliseconds timeout);
    void close() noexcept;

private:
    struct Impl;
    explicit CoherenceShmTransportV2(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

bool serveCoherenceV2ShmChannel(CoherenceServerV2 &server, CoherenceShmChannelV2 &channel,
                                std::string transport_name = "shm");

} // namespace cxlmemsim
