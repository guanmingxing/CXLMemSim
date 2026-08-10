#include "coherence_shm_transport_v2.h"

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace cxlmemsim {

namespace {

constexpr std::uint64_t kShmMagic = 0x32564d48534c5843ULL; // CXLSHMV2
constexpr std::uint32_t kShmVersion = 2;
constexpr std::size_t kMaximumChannels = protocol_v2::kMaximumHosts;
constexpr std::size_t kRingCapacity = 16;
constexpr std::uint32_t kClaiming = 1U << 0;
constexpr std::uint32_t kReady = 1U << 1;
constexpr std::uint32_t kServerAttached = 1U << 2;
constexpr std::uint32_t kClientClosed = 1U << 3;
constexpr std::uint32_t kServerClosed = 1U << 4;

struct alignas(64) FrameRing {
    std::atomic<std::uint64_t> producer{};
    std::atomic<std::uint64_t> consumer{};
    std::array<protocol_v2::EncodedFrame, kRingCapacity> frames{};
};

struct alignas(64) ChannelSlot {
    std::atomic<std::uint32_t> flags{};
    std::atomic<std::uint64_t> generation{};
    FrameRing client_to_server;
    FrameRing server_to_client;
};

struct SharedRegion {
    std::uint64_t magic{kShmMagic};
    std::uint32_t version{kShmVersion};
    std::uint32_t channel_count{};
    std::atomic<std::uint64_t> next_generation{1};
    std::array<ChannelSlot, kMaximumChannels> channels{};
};

static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);

struct Mapping {
    int fd{-1};
    SharedRegion *region{};
    std::string name;
    bool owner{};
    std::atomic<bool> closed{};

    ~Mapping() {
        if (region != nullptr)
            ::munmap(region, sizeof(SharedRegion));
        if (fd >= 0)
            ::close(fd);
        if (owner && !name.empty())
            ::shm_unlink(name.c_str());
    }
};

bool validName(const std::string &name) {
    return name.size() > 1 && name.front() == '/' && name.find('/', 1) == std::string::npos;
}

bool expired(std::chrono::steady_clock::time_point deadline) { return std::chrono::steady_clock::now() >= deadline; }

void pause() { std::this_thread::sleep_for(std::chrono::microseconds(50)); }

void resetRing(FrameRing &ring) {
    ring.consumer.store(0, std::memory_order_relaxed);
    ring.producer.store(0, std::memory_order_relaxed);
    for (auto &frame : ring.frames)
        frame.fill(0);
}

void maybeRelease(ChannelSlot &slot) {
    auto flags = slot.flags.load(std::memory_order_acquire);
    if ((flags & (kClientClosed | kServerClosed)) != (kClientClosed | kServerClosed))
        return;
    resetRing(slot.client_to_server);
    resetRing(slot.server_to_client);
    slot.generation.store(0, std::memory_order_relaxed);
    slot.flags.store(0, std::memory_order_release);
}

class ConnectionLease {
public:
    ConnectionLease(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection) noexcept
        : server_(&server), connection_(connection) {}
    ~ConnectionLease() { detach(); }
    void detach() noexcept {
        if (server_ != nullptr) {
            (void)server_->detachConnection(connection_);
            server_ = nullptr;
        }
    }
    ConnectionLease(const ConnectionLease &) = delete;
    ConnectionLease &operator=(const ConnectionLease &) = delete;

private:
    CoherenceServerV2 *server_;
    CoherenceServerV2::ConnectionId connection_;
};

} // namespace

struct CoherenceShmChannelV2::Impl {
    Impl(std::shared_ptr<Mapping> mapping_value, std::size_t slot_index_value, std::uint64_t generation_value,
         bool server_side_value)
        : mapping(std::move(mapping_value)), slot_index(slot_index_value), generation(generation_value),
          server_side(server_side_value) {}

    std::shared_ptr<Mapping> mapping;
    std::size_t slot_index{};
    std::uint64_t generation{};
    bool server_side{};
    bool locally_closed{};
    std::mutex outbound_mutex;

    ChannelSlot &slot() const { return mapping->region->channels[slot_index]; }
    FrameRing &outbound() const { return server_side ? slot().server_to_client : slot().client_to_server; }
    FrameRing &inbound() const { return server_side ? slot().client_to_server : slot().server_to_client; }
    std::uint32_t ownClosedFlag() const { return server_side ? kServerClosed : kClientClosed; }
    std::uint32_t peerClosedFlag() const { return server_side ? kClientClosed : kServerClosed; }
};

struct CoherenceShmTransportV2::Impl {
    std::shared_ptr<Mapping> mapping;
    bool server{};
};

CoherenceShmChannelV2::CoherenceShmChannelV2() noexcept = default;
CoherenceShmChannelV2::CoherenceShmChannelV2(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
CoherenceShmChannelV2::~CoherenceShmChannelV2() { close(); }
CoherenceShmChannelV2::CoherenceShmChannelV2(CoherenceShmChannelV2 &&) noexcept = default;
CoherenceShmChannelV2 &CoherenceShmChannelV2::operator=(CoherenceShmChannelV2 &&) noexcept = default;

CoherenceShmChannelV2::operator bool() const noexcept { return impl_ != nullptr && !impl_->locally_closed; }

std::uint64_t CoherenceShmChannelV2::connectionId() const noexcept {
    if (impl_ == nullptr || impl_->generation == 0)
        return 0;
    return (impl_->generation << 8U) | (impl_->slot_index + 1U);
}

bool CoherenceShmChannelV2::send(const protocol_v2::CoherenceFrame &frame, std::chrono::milliseconds timeout) {
    if (impl_ == nullptr)
        return false;
    std::lock_guard lock(impl_->outbound_mutex);
    if (!*this || impl_->mapping->closed.load(std::memory_order_acquire))
        return false;
    const auto encoded = protocol_v2::encodeFrame(frame);
    auto &ring = impl_->outbound();
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const auto flags = impl_->slot().flags.load(std::memory_order_acquire);
        if ((flags & impl_->peerClosedFlag()) != 0 || (flags & kReady) == 0)
            return false;
        const auto producer = ring.producer.load(std::memory_order_relaxed);
        const auto consumer = ring.consumer.load(std::memory_order_acquire);
        if (producer - consumer < kRingCapacity) {
            ring.frames[producer % kRingCapacity] = encoded;
            ring.producer.store(producer + 1, std::memory_order_release);
            return true;
        }
        if (expired(deadline))
            return false;
        pause();
    }
}

CoherenceShmReceiveResult CoherenceShmChannelV2::receive(protocol_v2::CoherenceFrame &frame,
                                                         std::chrono::milliseconds timeout) {
    if (!*this || impl_->mapping->closed.load(std::memory_order_acquire))
        return CoherenceShmReceiveResult::Closed;
    auto &ring = impl_->inbound();
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        const auto consumer = ring.consumer.load(std::memory_order_relaxed);
        const auto producer = ring.producer.load(std::memory_order_acquire);
        if (consumer != producer) {
            const auto encoded = ring.frames[consumer % kRingCapacity];
            ring.consumer.store(consumer + 1, std::memory_order_release);
            return protocol_v2::decodeFrame(encoded, frame) ? CoherenceShmReceiveResult::Frame
                                                            : CoherenceShmReceiveResult::Error;
        }
        const auto flags = impl_->slot().flags.load(std::memory_order_acquire);
        if ((flags & impl_->peerClosedFlag()) != 0 || (flags & kReady) == 0)
            return CoherenceShmReceiveResult::Closed;
        if (expired(deadline))
            return CoherenceShmReceiveResult::Timeout;
        pause();
    }
}

void CoherenceShmChannelV2::close() noexcept {
    if (impl_ == nullptr || impl_->locally_closed)
        return;
    impl_->locally_closed = true;
    auto &slot = impl_->slot();
    slot.flags.fetch_or(impl_->ownClosedFlag(), std::memory_order_acq_rel);
    maybeRelease(slot);
}

CoherenceShmTransportV2::CoherenceShmTransportV2(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}
CoherenceShmTransportV2::~CoherenceShmTransportV2() { close(); }

std::unique_ptr<CoherenceShmTransportV2> CoherenceShmTransportV2::createServer(const std::string &name,
                                                                               std::size_t channel_count) {
    if (!validName(name) || channel_count == 0 || channel_count > kMaximumChannels)
        return nullptr;
    const int fd = ::shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0)
        return nullptr;
    if (::ftruncate(fd, sizeof(SharedRegion)) != 0) {
        ::close(fd);
        ::shm_unlink(name.c_str());
        return nullptr;
    }
    void *memory = ::mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (memory == MAP_FAILED) {
        ::close(fd);
        ::shm_unlink(name.c_str());
        return nullptr;
    }
    auto mapping = std::make_shared<Mapping>();
    mapping->fd = fd;
    mapping->region = ::new (memory) SharedRegion{};
    mapping->region->channel_count = static_cast<std::uint32_t>(channel_count);
    mapping->name = name;
    mapping->owner = true;
    return std::unique_ptr<CoherenceShmTransportV2>(
        new CoherenceShmTransportV2(std::make_unique<Impl>(Impl{std::move(mapping), true})));
}

std::unique_ptr<CoherenceShmTransportV2> CoherenceShmTransportV2::openClient(const std::string &name) {
    if (!validName(name))
        return nullptr;
    const int fd = ::shm_open(name.c_str(), O_RDWR, 0600);
    if (fd < 0)
        return nullptr;
    struct stat attributes{};
    if (::fstat(fd, &attributes) != 0 || attributes.st_size != static_cast<off_t>(sizeof(SharedRegion))) {
        ::close(fd);
        return nullptr;
    }
    void *memory = ::mmap(nullptr, sizeof(SharedRegion), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (memory == MAP_FAILED) {
        ::close(fd);
        return nullptr;
    }
    auto mapping = std::make_shared<Mapping>();
    mapping->fd = fd;
    mapping->region = static_cast<SharedRegion *>(memory);
    mapping->name = name;
    if (mapping->region->magic != kShmMagic || mapping->region->version != kShmVersion ||
        mapping->region->channel_count == 0 || mapping->region->channel_count > kMaximumChannels) {
        return nullptr;
    }
    return std::unique_ptr<CoherenceShmTransportV2>(
        new CoherenceShmTransportV2(std::make_unique<Impl>(Impl{std::move(mapping), false})));
}

std::optional<CoherenceShmChannelV2> CoherenceShmTransportV2::connect(std::chrono::milliseconds timeout) {
    if (impl_ == nullptr || impl_->server || impl_->mapping->closed.load(std::memory_order_acquire))
        return std::nullopt;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        auto &region = *impl_->mapping->region;
        for (std::size_t index = 0; index < region.channel_count; ++index) {
            auto &slot = region.channels[index];
            std::uint32_t expected = 0;
            if (!slot.flags.compare_exchange_strong(expected, kClaiming, std::memory_order_acq_rel))
                continue;
            resetRing(slot.client_to_server);
            resetRing(slot.server_to_client);
            auto generation = region.next_generation.fetch_add(1, std::memory_order_relaxed);
            if (generation == 0)
                generation = region.next_generation.fetch_add(1, std::memory_order_relaxed);
            slot.generation.store(generation, std::memory_order_relaxed);
            slot.flags.store(kReady, std::memory_order_release);
            return CoherenceShmChannelV2(
                std::make_unique<CoherenceShmChannelV2::Impl>(impl_->mapping, index, generation, false));
        }
        if (expired(deadline))
            return std::nullopt;
        pause();
    }
}

std::optional<CoherenceShmChannelV2> CoherenceShmTransportV2::accept(std::chrono::milliseconds timeout) {
    if (impl_ == nullptr || !impl_->server || impl_->mapping->closed.load(std::memory_order_acquire))
        return std::nullopt;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        auto &region = *impl_->mapping->region;
        for (std::size_t index = 0; index < region.channel_count; ++index) {
            auto &slot = region.channels[index];
            std::uint32_t expected = kReady;
            if (!slot.flags.compare_exchange_strong(expected, kReady | kServerAttached, std::memory_order_acq_rel))
                continue;
            const auto generation = slot.generation.load(std::memory_order_acquire);
            if (generation == 0) {
                slot.flags.fetch_or(kServerClosed, std::memory_order_release);
                maybeRelease(slot);
                continue;
            }
            return CoherenceShmChannelV2(
                std::make_unique<CoherenceShmChannelV2::Impl>(impl_->mapping, index, generation, true));
        }
        if (expired(deadline))
            return std::nullopt;
        pause();
    }
}

void CoherenceShmTransportV2::close() noexcept {
    if (impl_ == nullptr)
        return;
    impl_->mapping->closed.store(true, std::memory_order_release);
}

bool serveCoherenceV2ShmChannel(CoherenceServerV2 &server, CoherenceShmChannelV2 &channel, std::string transport_name) {
    const auto connection = channel.connectionId();
    if (connection == 0)
        return false;
    const auto sender = [&channel](const protocol_v2::CoherenceFrame &frame) {
        return channel.send(frame, std::chrono::seconds(1));
    };
    if (!server.attachConnection(connection, std::move(transport_name), sender))
        return false;
    ConnectionLease lease(server, connection);

    class DispatchWorker {
    public:
        DispatchWorker(CoherenceServerV2 &server_value, CoherenceServerV2::ConnectionId connection_value,
                       ResponseSender sender_value)
            : server_(server_value), connection_(connection_value), sender_(std::move(sender_value)),
              thread_([this] { run(); }) {}
        ~DispatchWorker() { stop(); }
        bool enqueue(const protocol_v2::CoherenceFrame &frame) {
            std::lock_guard lock(mutex_);
            if (stopping_ || finished_.load(std::memory_order_acquire) || queue_.size() >= 64)
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
        bool finished() const noexcept { return finished_.load(std::memory_order_acquire); }
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
                    finish(true);
                    return;
                }
                if (result.response && !result.response_via_sender && !sender_(*result.response)) {
                    finish(true);
                    return;
                }
                if (result.close_connection) {
                    finish(false);
                    return;
                }
            }
        }
        void finish(bool failed) {
            std::lock_guard lock(mutex_);
            if (failed)
                failed_.store(true, std::memory_order_release);
            finished_.store(true, std::memory_order_release);
        }
        CoherenceServerV2 &server_;
        CoherenceServerV2::ConnectionId connection_;
        ResponseSender sender_;
        std::mutex mutex_;
        std::condition_variable changed_;
        std::deque<protocol_v2::CoherenceFrame> queue_;
        bool stopping_{};
        std::atomic<bool> finished_{};
        std::atomic<bool> failed_{};
        std::thread thread_;
    } worker(server, connection, sender);

    bool success = true;
    for (;;) {
        if (worker.finished())
            break;
        protocol_v2::CoherenceFrame frame{};
        const auto result = channel.receive(frame, std::chrono::milliseconds(100));
        if (worker.finished())
            break;
        if (result == CoherenceShmReceiveResult::Timeout) {
            continue;
        }
        if (result == CoherenceShmReceiveResult::Closed)
            break;
        if (result == CoherenceShmReceiveResult::Error) {
            success = false;
            break;
        }
        const auto opcode = protocol_v2::opcode(frame);
        if (opcode != protocol_v2::Opcode::Register && opcode != protocol_v2::Opcode::SnoopAck) {
            if (!worker.enqueue(frame)) {
                success = false;
                break;
            }
            continue;
        }
        const auto dispatch = server.dispatch(connection, frame);
        if (dispatch.delivery_failed) {
            success = false;
            break;
        }
        if (dispatch.response && !dispatch.response_via_sender && !sender(*dispatch.response)) {
            success = false;
            break;
        }
        if (dispatch.close_connection) {
            break;
        }
    }
    lease.detach();
    worker.stop();
    if (worker.finished())
        channel.close();
    return success && !worker.failed();
}

} // namespace cxlmemsim
