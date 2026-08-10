#include "coherence_tcp_transport_v2.h"

#include "coherence_protocol_v2.h"

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <sys/socket.h>
#include <thread>

namespace cxlmemsim {

namespace {

enum class ReadResult { Complete, Eof, Error };

ReadResult readAll(int fd, std::span<std::uint8_t> bytes) noexcept {
    while (!bytes.empty()) {
        const auto count = ::recv(fd, bytes.data(), bytes.size(), 0);
        if (count == 0)
            return ReadResult::Eof;
        if (count < 0) {
            if (errno == EINTR)
                continue;
            return ReadResult::Error;
        }
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
    return ReadResult::Complete;
}

bool writeAll(int fd, std::span<const std::uint8_t> bytes) noexcept {
    while (!bytes.empty()) {
        const auto count = ::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL);
        if (count < 0 && errno == EINTR)
            continue;
        if (count <= 0)
            return false;
        bytes = bytes.subspan(static_cast<std::size_t>(count));
    }
    return true;
}

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

} // namespace

bool serveCoherenceV2Stream(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection, int fd,
                            std::string transport_name) {
    if (fd < 0)
        return false;
    auto send_mutex = std::make_shared<std::mutex>();
    const auto sender = [fd, send_mutex](const protocol_v2::CoherenceFrame &frame) {
        const auto encoded = protocol_v2::encodeFrame(frame);
        std::lock_guard lock(*send_mutex);
        return writeAll(fd, encoded);
    };
    if (!server.attachConnection(connection, std::move(transport_name), sender))
        return false;
    ConnectionLease connection_lease(server, connection);

    class DispatchWorker {
    public:
        DispatchWorker(CoherenceServerV2 &server_value, CoherenceServerV2::ConnectionId connection_value,
                       ResponseSender sender_value, int socket)
            : server_(server_value), connection_(connection_value), sender_(std::move(sender_value)), fd_(socket),
              thread_([this] { run(); }) {}
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
                    (void)::shutdown(fd_, SHUT_RDWR);
                    return;
                }
                if (result.response && !result.response_via_sender && !sender_(*result.response)) {
                    failed_.store(true, std::memory_order_release);
                    (void)::shutdown(fd_, SHUT_RDWR);
                    return;
                }
                if (result.close_connection) {
                    (void)::shutdown(fd_, SHUT_RDWR);
                    return;
                }
            }
        }
        CoherenceServerV2 &server_;
        CoherenceServerV2::ConnectionId connection_;
        ResponseSender sender_;
        int fd_;
        std::mutex mutex_;
        std::condition_variable changed_;
        std::deque<protocol_v2::CoherenceFrame> queue_;
        bool stopping_{};
        std::atomic<bool> failed_{};
        std::thread thread_;
    } worker(server, connection, sender, fd);

    bool success = true;
    for (;;) {
        protocol_v2::EncodedFrame encoded{};
        const auto read = readAll(fd, encoded);
        if (read == ReadResult::Eof)
            break;
        if (read == ReadResult::Error) {
            success = false;
            break;
        }

        protocol_v2::CoherenceFrame frame{};
        if (!protocol_v2::decodeFrame(encoded, frame)) {
            success = false;
            break;
        }
        const auto opcode = protocol_v2::opcode(frame);
        if (opcode != protocol_v2::Opcode::Register && opcode != protocol_v2::Opcode::SnoopAck) {
            if (!worker.enqueue(frame)) {
                success = false;
                (void)::shutdown(fd, SHUT_RDWR);
                break;
            }
            continue;
        }
        const auto result = server.dispatch(connection, frame);
        if (result.delivery_failed) {
            success = false;
            (void)::shutdown(fd, SHUT_RDWR);
            break;
        }
        if (result.response && !result.response_via_sender && !sender(*result.response)) {
            success = false;
            break;
        }
        if (result.close_connection) {
            (void)::shutdown(fd, SHUT_RDWR);
            break;
        }
    }
    connection_lease.detach();
    worker.stop();
    return success && !worker.failed();
}

} // namespace cxlmemsim
