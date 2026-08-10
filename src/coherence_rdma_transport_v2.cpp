#include "coherence_rdma_transport_v2.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace cxlmemsim {

namespace {

class ConnectionLease {
public:
    ConnectionLease(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection) noexcept
        : server_(&server), connection_(connection) {}
    ConnectionLease(const ConnectionLease &) = delete;
    ConnectionLease &operator=(const ConnectionLease &) = delete;
    ~ConnectionLease() { detach(); }
    void detach() noexcept {
        if (server_ != nullptr) {
            (void)server_->detachConnection(connection_);
            server_ = nullptr;
        }
    }

private:
    CoherenceServerV2 *server_;
    CoherenceServerV2::ConnectionId connection_;
};

struct OutboundState {
    explicit OutboundState(CoherenceRdmaByteTransport &transport_value) : transport(&transport_value) {}

    bool send(const protocol_v2::CoherenceFrame &frame) {
        std::lock_guard lock(mutex);
        tx_frame = protocol_v2::encodeFrame(frame);
        if (!transport->send(tx_frame)) {
            failed.store(true, std::memory_order_release);
            return false;
        }
        return true;
    }

    CoherenceRdmaByteTransport *transport;
    std::mutex mutex;
    protocol_v2::EncodedFrame tx_frame{};
    std::atomic<bool> failed{};
};

} // namespace

CallbackCoherenceRdmaByteTransport::CallbackCoherenceRdmaByteTransport(CoherenceRdmaCallbacks callbacks)
    : callbacks_(std::move(callbacks)) {}

CoherenceRdmaReceiveResult CallbackCoherenceRdmaByteTransport::receive(MutableCoherenceRdmaFrame frame) {
    if (!callbacks_.receive)
        return CoherenceRdmaReceiveResult::Error;
    try {
        return callbacks_.receive(frame);
    } catch (...) {
        return CoherenceRdmaReceiveResult::Error;
    }
}

bool CallbackCoherenceRdmaByteTransport::send(ConstCoherenceRdmaFrame frame) {
    if (!callbacks_.send)
        return false;
    try {
        return callbacks_.send(frame);
    } catch (...) {
        return false;
    }
}

void CallbackCoherenceRdmaByteTransport::close() noexcept {
    if (!callbacks_.close)
        return;
    try {
        callbacks_.close();
    } catch (...) {
    }
}

bool serveCoherenceV2Rdma(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection,
                          CoherenceRdmaByteTransport &transport, std::string transport_name) {
    auto outbound = std::make_shared<OutboundState>(transport);
    const auto sender = [outbound](const protocol_v2::CoherenceFrame &frame) { return outbound->send(frame); };
    if (!server.attachConnection(connection, std::move(transport_name), sender))
        return false;
    ConnectionLease connection_lease(server, connection);

    class DispatchWorker {
    public:
        DispatchWorker(CoherenceServerV2 &server_value, CoherenceServerV2::ConnectionId connection_value,
                       ResponseSender sender_value, CoherenceRdmaByteTransport &transport_value)
            : server_(server_value), connection_(connection_value), sender_(std::move(sender_value)),
              transport_(transport_value), thread_([this] { run(); }) {}
        ~DispatchWorker() { stop(); }
        bool enqueue(const protocol_v2::CoherenceFrame &frame) {
            std::lock_guard lock(mutex_);
            if (stopping_ || queue_.size() >= 64)
                return false;
            queue_.push_back(frame);
            changed_.notify_one();
            return true;
        }
        void stop() {
            {
                std::lock_guard lock(mutex_);
                stopping_ = true;
                queue_.clear();
            }
            changed_.notify_one();
            if (thread_.joinable())
                thread_.join();
        }
        bool failed() const noexcept { return failed_.load(std::memory_order_acquire); }

    private:
        void run() {
            for (;;) {
                protocol_v2::CoherenceFrame frame;
                {
                    std::unique_lock lock(mutex_);
                    changed_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
                    if (stopping_)
                        return;
                    frame = queue_.front();
                    queue_.pop_front();
                }
                const auto result = server_.dispatch(connection_, frame);
                if (result.delivery_failed) {
                    failed_.store(true, std::memory_order_release);
                    transport_.close();
                    return;
                }
                if (result.response && !result.response_via_sender && !sender_(*result.response)) {
                    failed_.store(true, std::memory_order_release);
                    transport_.close();
                    return;
                }
                if (result.close_connection) {
                    transport_.close();
                    return;
                }
            }
        }
        CoherenceServerV2 &server_;
        CoherenceServerV2::ConnectionId connection_;
        ResponseSender sender_;
        CoherenceRdmaByteTransport &transport_;
        std::mutex mutex_;
        std::condition_variable changed_;
        std::deque<protocol_v2::CoherenceFrame> queue_;
        bool stopping_{};
        std::atomic<bool> failed_{};
        std::thread thread_;
    } worker(server, connection, sender, transport);

    // RX and TX storage are intentionally distinct. RX may remain posted while
    // a server worker publishes a snoop through OutboundState::tx_frame.
    protocol_v2::EncodedFrame rx_frame{};
    bool success = true;
    for (;;) {
        const auto received = transport.receive(rx_frame);
        if (received == CoherenceRdmaReceiveResult::Closed)
            break;
        if (received == CoherenceRdmaReceiveResult::Error) {
            success = false;
            break;
        }

        protocol_v2::CoherenceFrame frame{};
        if (!protocol_v2::decodeFrame(rx_frame, frame)) {
            success = false;
            break;
        }
        const auto opcode = protocol_v2::opcode(frame);
        if (opcode != protocol_v2::Opcode::Register && opcode != protocol_v2::Opcode::SnoopAck) {
            if (!worker.enqueue(frame)) {
                success = false;
                transport.close();
                break;
            }
            continue;
        }
        const auto result = server.dispatch(connection, frame);
        if (result.delivery_failed) {
            success = false;
            transport.close();
            break;
        }
        if (result.response && !result.response_via_sender && !sender(*result.response)) {
            success = false;
            break;
        }
        if (outbound->failed.load(std::memory_order_acquire)) {
            success = false;
            break;
        }
        if (result.close_connection) {
            transport.close();
            break;
        }
    }
    connection_lease.detach();
    worker.stop();
    return success && !worker.failed() && !outbound->failed.load(std::memory_order_acquire);
}

#ifdef HAS_RDMA
std::unique_ptr<CoherenceRdmaByteTransport>
makeCoherenceRdmaConnectionTransportV2(::RDMAConnection &connection, CoherenceRdmaConnectionFrameOps operations) {
    auto shared_operations = std::make_shared<CoherenceRdmaConnectionFrameOps>(std::move(operations));
    CoherenceRdmaCallbacks callbacks{
        .receive =
            [&connection, shared_operations](MutableCoherenceRdmaFrame frame) {
                if (!shared_operations->receive_frame)
                    return CoherenceRdmaReceiveResult::Error;
                return shared_operations->receive_frame(connection, frame);
            },
        .send =
            [&connection, shared_operations](ConstCoherenceRdmaFrame frame) {
                return shared_operations->send_frame && shared_operations->send_frame(connection, frame);
            },
        .close =
            [&connection, shared_operations] {
                if (shared_operations->close)
                    shared_operations->close(connection);
            },
    };
    return std::make_unique<CallbackCoherenceRdmaByteTransport>(std::move(callbacks));
}
#endif

} // namespace cxlmemsim
