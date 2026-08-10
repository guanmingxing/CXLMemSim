#include "coherence_endpoint_cache.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <map>
#include <mutex>
#include <span>

using namespace cxlmemsim;
using namespace cxlmemsim::mesi_v2;
using namespace cxlmemsim::protocol_v2;

namespace {

constexpr std::uint64_t kLineA = 0x1000;
constexpr std::uint64_t kLineB = 0x2000;
constexpr std::uint64_t kOldValue = 0x1111111111111111ULL;
constexpr std::uint64_t kNewValue = 0x2222222222222222ULL;
constexpr std::uint64_t kLineBValue = 0xbbbbbbbbbbbbbbbbULL;
constexpr auto kWait = std::chrono::seconds(5);

class BlockingMemoryBackend final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t address) override {
        std::lock_guard lock(mutex_);
        return lines_[address];
    }

    void writeLine(std::uint64_t address, std::span<const std::byte, kLineSize> data) override {
        std::unique_lock lock(mutex_);
        if (block_armed_ && address == blocked_address_) {
            write_blocked_ = true;
            changed_.notify_all();
            changed_.wait(lock, [&] { return !block_armed_; });
        }
        std::copy(data.begin(), data.end(), lines_[address].begin());
    }

    void blockNextWrite(std::uint64_t address) {
        std::lock_guard lock(mutex_);
        blocked_address_ = address;
        block_armed_ = true;
        write_blocked_ = false;
    }

    bool waitForBlockedWrite() {
        std::unique_lock lock(mutex_);
        return changed_.wait_for(lock, kWait, [&] { return write_blocked_; });
    }

    void releaseBlockedWrite() {
        std::lock_guard lock(mutex_);
        block_armed_ = false;
        changed_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable changed_;
    std::map<std::uint64_t, std::array<std::byte, kLineSize>> lines_;
    std::uint64_t blocked_address_{};
    bool block_armed_{};
    bool write_blocked_{};
};

template <typename T> Status storeScalar(CoherenceEndpointCache &cache, std::uint64_t address, T value) {
    std::array<std::byte, sizeof(T)> data{};
    std::memcpy(data.data(), &value, sizeof(value));
    return cache.store(address, data);
}

template <typename T> std::pair<Status, T> loadScalar(CoherenceEndpointCache &cache, std::uint64_t address) {
    std::array<std::byte, sizeof(T)> data{};
    const auto status = cache.load(address, data);
    T value{};
    std::memcpy(&value, data.data(), sizeof(value));
    return {status, value};
}

template <typename T> bool waitReady(std::future<T> &future, const char *operation) {
    if (future.wait_for(kWait) == std::future_status::ready)
        return true;
    std::cerr << "FAIL: timed out waiting for " << operation << '\n';
    return false;
}

int runEvictionRace() {
    BlockingMemoryBackend memory;
    MesiDirectory directory;
    InProcessCoherenceTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kWait);
    CoherenceEndpointCache endpoint(engine, {0, 101, 1, EndpointWritePolicy::WriteBack});
    transport.bindEngine(engine);

    if (!engine.bindSession(endpoint.endpointId(), endpoint.sessionId()) || !transport.registerEndpoint(endpoint)) {
        std::cerr << "FAIL: could not register endpoint\n";
        return 1;
    }
    if (storeScalar(endpoint, kLineA, kOldValue) != Status::Ok) {
        std::cerr << "FAIL: could not install dirty line A\n";
        return 1;
    }

    memory.blockNextWrite(kLineA);
    auto insert_b = std::async(std::launch::async, [&] { return storeScalar(endpoint, kLineB, kLineBValue); });
    if (!memory.waitForBlockedWrite()) {
        memory.releaseBlockedWrite();
        waitReady(insert_b, "line B insertion cleanup");
        std::cerr << "FAIL: timed out waiting for line A PUTM eviction\n";
        return 1;
    }

    std::promise<void> newer_store_started;
    auto newer_store_started_future = newer_store_started.get_future();
    auto store_newer_a = std::async(std::launch::async, [&] {
        newer_store_started.set_value();
        return storeScalar(endpoint, kLineA, kNewValue);
    });
    if (newer_store_started_future.wait_for(kWait) != std::future_status::ready) {
        memory.releaseBlockedWrite();
        waitReady(insert_b, "line B insertion cleanup");
        waitReady(store_newer_a, "newer line A store cleanup");
        std::cerr << "FAIL: timed out starting concurrent line A store\n";
        return 1;
    }

    // The buggy implementation completes this M-hit while eviction retains an older snapshot. A corrected
    // implementation may serialize it behind eviction, so release the backend after a bounded observation window.
    const bool newer_store_completed_while_putm_blocked =
        store_newer_a.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    memory.releaseBlockedWrite();

    if (!waitReady(insert_b, "line B insertion") || !waitReady(store_newer_a, "newer line A store"))
        return 1;
    const auto insert_b_status = insert_b.get();
    const auto newer_store_status = store_newer_a.get();
    if (insert_b_status != Status::Ok || newer_store_status != Status::Ok) {
        std::cerr << "FAIL: concurrent stores returned status B=" << static_cast<unsigned>(insert_b_status)
                  << " A=" << static_cast<unsigned>(newer_store_status) << '\n';
        return 1;
    }

    const auto [load_status, observed] = loadScalar<std::uint64_t>(endpoint, kLineA);
    if (load_status != Status::Ok) {
        std::cerr << "FAIL: reacquiring line A returned status " << static_cast<unsigned>(load_status) << '\n';
        return 1;
    }
    if (observed != kNewValue) {
        std::cerr << "FAIL: line A expected 0x" << std::hex << kNewValue << " after eviction race, observed 0x"
                  << observed << std::dec << "; newer store completed while PUTM blocked=" << std::boolalpha
                  << newer_store_completed_while_putm_blocked << '\n';
        return 1;
    }
    return 0;
}

} // namespace

int main() { return runEvictionRace(); }
