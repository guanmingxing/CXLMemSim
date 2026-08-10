#pragma once

#include "coherence_protocol_v2.h"
#include "coherence_server_v2.h"

#include <functional>
#include <memory>
#include <span>
#include <string>

#ifdef HAS_RDMA
class RDMAConnection;
#endif

namespace cxlmemsim {

static_assert(protocol_v2::kFrameSize == 192);
using MutableCoherenceRdmaFrame = std::span<std::uint8_t, protocol_v2::kFrameSize>;
using ConstCoherenceRdmaFrame = std::span<const std::uint8_t, protocol_v2::kFrameSize>;

enum class CoherenceRdmaReceiveResult { Frame, Closed, Error };

// One callback invocation transfers exactly one protocol-v2 frame. The
// implementation must permit one send and one receive to execute concurrently.
// It must not expose the same registered/raw buffer to those operations.
struct CoherenceRdmaCallbacks {
    std::function<CoherenceRdmaReceiveResult(MutableCoherenceRdmaFrame)> receive;
    std::function<bool(ConstCoherenceRdmaFrame)> send;
    std::function<void()> close;
};

class CoherenceRdmaByteTransport {
public:
    virtual ~CoherenceRdmaByteTransport() = default;

    virtual CoherenceRdmaReceiveResult receive(MutableCoherenceRdmaFrame frame) = 0;
    virtual bool send(ConstCoherenceRdmaFrame frame) = 0;
    virtual void close() noexcept = 0;
};

// Callback transport used by fake/loopback tests and by hardware backends that
// already own their queue pairs, completion handling, and registered buffers.
class CallbackCoherenceRdmaByteTransport final : public CoherenceRdmaByteTransport {
public:
    explicit CallbackCoherenceRdmaByteTransport(CoherenceRdmaCallbacks callbacks);

    CoherenceRdmaReceiveResult receive(MutableCoherenceRdmaFrame frame) override;
    bool send(ConstCoherenceRdmaFrame frame) override;
    void close() noexcept override;

private:
    CoherenceRdmaCallbacks callbacks_;
};

// Serves a single multiplexed duplex connection. Ordinary responses and
// unsolicited snoops share one serialized outbound stream; requests and snoop
// ACKs share one inbound stream. The caller retains transport ownership.
bool serveCoherenceV2Rdma(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection,
                          CoherenceRdmaByteTransport &transport, std::string transport_name = "rdma");

#ifdef HAS_RDMA
// RDMAConnection's legacy RDMAMessage methods use one internal raw buffer and
// are therefore not wired here. A verbs backend must provide these frame ops
// using distinct registered TX/RX buffers and permit send/receive concurrency.
struct CoherenceRdmaConnectionFrameOps {
    std::function<CoherenceRdmaReceiveResult(::RDMAConnection &, MutableCoherenceRdmaFrame)> receive_frame;
    std::function<bool(::RDMAConnection &, ConstCoherenceRdmaFrame)> send_frame;
    std::function<void(::RDMAConnection &)> close;
};

std::unique_ptr<CoherenceRdmaByteTransport>
makeCoherenceRdmaConnectionTransportV2(::RDMAConnection &connection, CoherenceRdmaConnectionFrameOps operations);
#endif

} // namespace cxlmemsim
