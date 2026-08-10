#include "coherence_endpoint_cache.h"
#include "coherence_memory_backend.h"
#include "coherence_transport.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <vector>

using namespace cxlmemsim;
using namespace cxlmemsim::mesi_v2;
using namespace cxlmemsim::protocol_v2;

namespace {

std::atomic<int> failures{};

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __func__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            failures.fetch_add(1, std::memory_order_relaxed);                                                          \
        }                                                                                                              \
    } while (false)

constexpr std::uint64_t kDataLine = 0x1000;
constexpr std::uint64_t kFlagLine = 0x2000;
constexpr std::uint64_t kMergeLine = 0x3000;
constexpr std::uint64_t kAtomicLine = 0x4000;
constexpr std::uint64_t kEvictLineA = 0x5000;
constexpr std::uint64_t kEvictLineB = 0x6000;

std::uint64_t holder(std::uint16_t endpoint) { return std::uint64_t{1} << endpoint; }

class TestMemory final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t address) override {
        std::lock_guard lock(mutex_);
        ++reads_;
        return lines_[address];
    }

    void writeLine(std::uint64_t address, std::span<const std::byte, kLineSize> data) override {
        std::lock_guard lock(mutex_);
        ++writes_;
        std::copy(data.begin(), data.end(), lines_[address].begin());
    }

    template <typename T> void seed(std::uint64_t address, std::size_t offset, T value) {
        std::lock_guard lock(mutex_);
        std::memcpy(lines_[address].data() + offset, &value, sizeof(value));
    }

    template <typename T> T scalar(std::uint64_t address, std::size_t offset = 0) const {
        std::lock_guard lock(mutex_);
        T value{};
        const auto found = lines_.find(address);
        if (found != lines_.end())
            std::memcpy(&value, found->second.data() + offset, sizeof(value));
        return value;
    }

    std::size_t writes() const {
        std::lock_guard lock(mutex_);
        return writes_;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::uint64_t, std::array<std::byte, kLineSize>> lines_;
    std::size_t reads_{};
    std::size_t writes_{};
};

template <typename T> Status storeScalar(CoherenceEndpointCache &cache, std::uint64_t address, T value) {
    const auto bytes = std::as_bytes(std::span{&value, std::size_t{1}});
    return cache.store(address, bytes);
}

template <typename T> std::pair<Status, T> loadScalar(CoherenceEndpointCache &cache, std::uint64_t address) {
    T value{};
    const auto status = cache.load(address, std::as_writable_bytes(std::span{&value, std::size_t{1}}));
    return {status, value};
}

class Type2Domain {
public:
    explicit Type2Domain(std::size_t cache_lines = 4)
        : engine(directory, memory, transport, std::chrono::milliseconds(100)),
          host(engine, {0, 101, cache_lines, EndpointWritePolicy::WriteBack}),
          device(engine, {1, 202, cache_lines, EndpointWritePolicy::WriteBack}) {
        transport.bindEngine(engine);
        CHECK(engine.bindSession(0, 101));
        CHECK(engine.bindSession(1, 202));
        CHECK(transport.registerEndpoint(host));
        CHECK(transport.registerEndpoint(device));
    }

    TestMemory memory;
    MesiDirectory directory;
    InProcessCoherenceTransport transport;
    MesiTransactionEngine engine;
    CoherenceEndpointCache host;
    CoherenceEndpointCache device;
};

void checkDirectory(const MesiDirectory &directory, std::uint64_t address, MesiState state,
                    std::optional<std::uint16_t> owner, std::uint64_t sharers, bool server_current) {
    const auto snapshot = directory.inspect(address);
    CHECK(snapshot.has_value());
    if (!snapshot)
        return;
    CHECK(snapshot->state == state);
    CHECK(snapshot->owner == owner);
    CHECK(snapshot->sharers == sharers);
    CHECK(snapshot->server_copy_current == server_current);
    CHECK(isValidSnapshot(*snapshot));
}

void testNegativeLegacySplitShadowExposesForbiddenOutcome() {
    struct SplitShadow {
        std::uint64_t host{};
        std::uint64_t device{};
    } shadow;

    shadow.host = 0xfeedface;
    const auto forbidden = shadow.device != shadow.host ? 1U : 0U;
    CHECK(forbidden == 1);
}

void testHostWriteDeviceReadReturnsDirtyOwnerData() {
    Type2Domain domain;
    domain.memory.seed<std::uint64_t>(kDataLine, 0, 0);

    CHECK(storeScalar(domain.host, kDataLine, std::uint64_t{0x1122334455667788}) == Status::Ok);
    CHECK(domain.memory.scalar<std::uint64_t>(kDataLine) == 0);

    const auto [status, observed] = loadScalar<std::uint64_t>(domain.device, kDataLine);
    CHECK(status == Status::Ok);
    CHECK(observed == 0x1122334455667788ULL);
    CHECK(domain.memory.scalar<std::uint64_t>(kDataLine) == observed);
    checkDirectory(domain.directory, kDataLine, MesiState::S, std::nullopt, holder(0) | holder(1), true);

    const auto host = domain.host.counters();
    const auto device = domain.device.counters();
    CHECK(host.snoop_data_downgrade == 1);
    CHECK(host.snoop_acks == 1);
    CHECK(device.gets >= 1);
}

void testDeviceWriteHostReadReturnsDirtyOwnerData() {
    Type2Domain domain;
    domain.memory.seed<std::uint64_t>(kDataLine, 0, 0);

    CHECK(storeScalar(domain.device, kDataLine, std::uint64_t{0xa5a5a5a55a5a5a5a}) == Status::Ok);
    const auto [status, observed] = loadScalar<std::uint64_t>(domain.host, kDataLine);

    CHECK(status == Status::Ok);
    CHECK(observed == 0xa5a5a5a55a5a5a5aULL);
    CHECK(domain.device.counters().snoop_data_downgrade == 1);
    CHECK(domain.host.counters().gets >= 1);
}

void testMessagePassingNeverObservesFlagWithOldData() {
    Type2Domain domain(8);
    domain.memory.seed<std::uint64_t>(kDataLine, 0, 0);
    domain.memory.seed<std::uint64_t>(kFlagLine, 0, 0);
    std::atomic<std::size_t> forbidden{};
    std::barrier phase(2);

    auto writer = std::async(std::launch::async, [&] {
        for (std::uint64_t iteration = 1; iteration <= 128; ++iteration) {
            phase.arrive_and_wait();
            CHECK(storeScalar(domain.host, kDataLine, iteration) == Status::Ok);
            CHECK(storeScalar(domain.host, kFlagLine, std::uint64_t{1}) == Status::Ok);
            phase.arrive_and_wait();
        }
    });
    auto reader = std::async(std::launch::async, [&] {
        for (std::uint64_t iteration = 1; iteration <= 128; ++iteration) {
            phase.arrive_and_wait();
            std::uint64_t flag{};
            for (std::size_t attempt = 0; attempt < 10000 && flag != 1; ++attempt) {
                const auto [status, value] = loadScalar<std::uint64_t>(domain.device, kFlagLine);
                CHECK(status == Status::Ok);
                flag = value;
            }
            const auto [data_status, data] = loadScalar<std::uint64_t>(domain.device, kDataLine);
            CHECK(flag == 1);
            CHECK(data_status == Status::Ok);
            if (flag == 1 && data != iteration) {
                std::cerr << "MP forbidden iteration=" << iteration << " data=" << data << '\n';
                forbidden.fetch_add(1, std::memory_order_relaxed);
            }
            const auto reset_status = storeScalar(domain.device, kFlagLine, std::uint64_t{0});
            if (reset_status != Status::Ok) {
                const auto snapshot = domain.directory.inspect(kFlagLine);
                std::cerr << "MP reset failed iteration=" << iteration << " status=" << toString(reset_status)
                          << " cache_present=" << domain.device.contains(kFlagLine)
                          << " directory_state=" << (snapshot ? static_cast<unsigned>(snapshot->state) : 99U)
                          << " epoch=" << (snapshot ? snapshot->epoch : 0) << '\n';
            }
            CHECK(reset_status == Status::Ok);
            phase.arrive_and_wait();
        }
    });
    writer.get();
    reader.get();

    CHECK(forbidden.load(std::memory_order_relaxed) == 0);
    const auto transitions = domain.directory.transitionCounters();
    CHECK(transitions.gets > 0);
    CHECK(transitions.getm > 0);
    CHECK(transitions.upgrade > 0);
    CHECK(domain.transport.counters().snoop_acks > 0);
}

void testOwnershipTransferPreservesPartialCacheLineStores() {
    Type2Domain domain;
    domain.memory.seed<std::uint64_t>(kMergeLine, 0, 0);
    domain.memory.seed<std::uint64_t>(kMergeLine, 8, 0);

    CHECK(storeScalar(domain.host, kMergeLine, std::uint64_t{0x1111111111111111}) == Status::Ok);
    CHECK(storeScalar(domain.device, kMergeLine + 8, std::uint64_t{0x2222222222222222}) == Status::Ok);

    const auto [low_status, low] = loadScalar<std::uint64_t>(domain.host, kMergeLine);
    const auto [high_status, high] = loadScalar<std::uint64_t>(domain.host, kMergeLine + 8);
    CHECK(low_status == Status::Ok);
    CHECK(high_status == Status::Ok);
    CHECK(low == 0x1111111111111111ULL);
    CHECK(high == 0x2222222222222222ULL);
    CHECK(domain.host.counters().snoop_data_inv > 0);
    CHECK(domain.device.counters().snoop_data_downgrade > 0);
}

void testCrossEndpointFetchAddIsLinearizable() {
    Type2Domain domain;
    domain.memory.seed<std::uint64_t>(kAtomicLine, 0, 0);
    constexpr std::size_t kIterations = 64;
    std::array<std::uint64_t, kIterations> host_old{};
    std::array<std::uint64_t, kIterations> device_old{};

    auto host_worker = std::async(std::launch::async, [&] {
        for (std::size_t index = 0; index < kIterations; ++index) {
            const auto result = domain.host.fetchAdd(kAtomicLine, 1);
            if (result.status != Status::Ok) {
                const auto snapshot = domain.directory.inspect(kAtomicLine);
                std::cerr << "host FAA failed iteration=" << index << " status=" << toString(result.status)
                          << " transition_status=" << static_cast<unsigned>(result.transition.status)
                          << " transition_state=" << static_cast<unsigned>(result.transition.snapshot.state)
                          << " transition_owner="
                          << (result.transition.snapshot.owner
                                  ? static_cast<unsigned>(*result.transition.snapshot.owner)
                                  : 99U)
                          << " transition_epoch=" << result.transition.snapshot.epoch
                          << " cache_present=" << domain.host.contains(kAtomicLine)
                          << " directory_state=" << (snapshot ? static_cast<unsigned>(snapshot->state) : 99U)
                          << " directory_owner="
                          << (snapshot && snapshot->owner ? static_cast<unsigned>(*snapshot->owner) : 99U)
                          << " directory_epoch=" << (snapshot ? snapshot->epoch : 0)
                          << " host_rejected=" << domain.host.counters().rejected_snoops
                          << " device_rejected=" << domain.device.counters().rejected_snoops
                          << " transport_failures=" << domain.transport.counters().send_failures << '\n';
            }
            CHECK(result.status == Status::Ok);
            host_old[index] = result.old_value;
        }
    });
    auto device_worker = std::async(std::launch::async, [&] {
        for (std::size_t index = 0; index < kIterations; ++index) {
            const auto result = domain.device.fetchAdd(kAtomicLine, 1);
            if (result.status != Status::Ok) {
                const auto snapshot = domain.directory.inspect(kAtomicLine);
                std::cerr << "device FAA failed iteration=" << index << " status=" << toString(result.status)
                          << " transition_status=" << static_cast<unsigned>(result.transition.status)
                          << " transition_state=" << static_cast<unsigned>(result.transition.snapshot.state)
                          << " transition_owner="
                          << (result.transition.snapshot.owner
                                  ? static_cast<unsigned>(*result.transition.snapshot.owner)
                                  : 99U)
                          << " transition_epoch=" << result.transition.snapshot.epoch
                          << " cache_present=" << domain.device.contains(kAtomicLine)
                          << " directory_state=" << (snapshot ? static_cast<unsigned>(snapshot->state) : 99U)
                          << " directory_owner="
                          << (snapshot && snapshot->owner ? static_cast<unsigned>(*snapshot->owner) : 99U)
                          << " directory_epoch=" << (snapshot ? snapshot->epoch : 0)
                          << " host_rejected=" << domain.host.counters().rejected_snoops
                          << " device_rejected=" << domain.device.counters().rejected_snoops
                          << " transport_failures=" << domain.transport.counters().send_failures << '\n';
            }
            CHECK(result.status == Status::Ok);
            device_old[index] = result.old_value;
        }
    });
    host_worker.get();
    device_worker.get();

    std::vector<std::uint64_t> observed;
    observed.insert(observed.end(), host_old.begin(), host_old.end());
    observed.insert(observed.end(), device_old.begin(), device_old.end());
    std::sort(observed.begin(), observed.end());
    for (std::size_t index = 0; index < observed.size(); ++index)
        CHECK(observed[index] == index);

    const auto [status, final_value] = loadScalar<std::uint64_t>(domain.host, kAtomicLine);
    CHECK(status == Status::Ok);
    CHECK(final_value == 2 * kIterations);

    const auto cas = domain.device.compareExchange(kAtomicLine, 2 * kIterations, 0x9999);
    CHECK(cas.status == Status::Ok);
    CHECK(cas.old_value == 2 * kIterations);
    const auto [cas_status, cas_value] = loadScalar<std::uint64_t>(domain.host, kAtomicLine);
    CHECK(cas_status == Status::Ok);
    CHECK(cas_value == 0x9999);
    CHECK(domain.directory.transitionCounters().atomic == 2 * kIterations + 1);
}

void testDirtySnoopCompletionCanBeReplayedAfterLocalInvalidation() {
    Type2Domain domain;
    constexpr std::uint64_t kValue = 0xdecafbad12345678ULL;
    CHECK(storeScalar(domain.host, kDataLine, kValue) == Status::Ok);
    const auto before = domain.directory.inspect(kDataLine);
    CHECK(before.has_value());

    auto snoop = initializeFrame(Opcode::SnpDataInv);
    setSrcHost(snoop, kServerHost);
    setDstHost(snoop, domain.host.endpointId());
    setSessionId(snoop, domain.host.sessionId());
    setSnoopId(snoop, 1001);
    setAddress(snoop, kDataLine);
    setEpoch(snoop, before ? before->epoch + 1 : 1);

    const auto first = domain.host.processSnoop(snoop);
    CHECK(first.has_value());
    CHECK(!domain.host.contains(kDataLine));
    CHECK(first && status(*first) == Status::Ok);
    CHECK(first && payloadLength(*first) == kLineSize);

    const auto duplicate = domain.host.processSnoop(snoop);
    CHECK(duplicate.has_value());
    CHECK(first && duplicate && encodeFrame(*first) == encodeFrame(*duplicate));

    auto replacement = snoop;
    setSnoopId(replacement, 1002);
    setEpoch(replacement, epoch(snoop) + 1);
    const auto replay = domain.host.processSnoop(replacement);
    CHECK(replay.has_value());
    CHECK(replay && status(*replay) == Status::Ok);
    CHECK(replay && payloadLength(*replay) == kLineSize);
    std::uint64_t replayed_value{};
    if (replay)
        std::memcpy(&replayed_value, replay->data.data(), sizeof(replayed_value));
    CHECK(replayed_value == kValue);

    auto unknown = replacement;
    setAddress(unknown, kEvictLineA);
    CHECK(!domain.host.processSnoop(unknown).has_value());
}

void testBoundedWriteBackCacheEvictsDirtyLine() {
    Type2Domain domain(1);
    CHECK(storeScalar(domain.host, kEvictLineA, std::uint64_t{0xaaaa}) == Status::Ok);
    CHECK(storeScalar(domain.host, kEvictLineB, std::uint64_t{0xbbbb}) == Status::Ok);

    CHECK(!domain.host.contains(kEvictLineA));
    CHECK(domain.host.contains(kEvictLineB));
    CHECK(domain.memory.scalar<std::uint64_t>(kEvictLineA) == 0xaaaa);
    CHECK(domain.host.counters().evictions == 1);
    CHECK(domain.host.counters().writebacks == 1);
    checkDirectory(domain.directory, kEvictLineA, MesiState::I, std::nullopt, 0, true);
}

class PartialAckTransport final : public CoherenceTransport {
public:
    void bindEngine(MesiTransactionEngine &engine) { engine_ = &engine; }

    void registerEndpoint(CoherenceEndpointCache &endpoint) { endpoints_[endpoint.endpointId()] = &endpoint; }

    void dropAckFrom(std::uint16_t endpoint) { dropped_ = endpoint; }

    bool sendToHost(std::uint16_t endpoint, const CoherenceFrame &snoop) override {
        const auto found = endpoints_.find(endpoint);
        if (found == endpoints_.end())
            return false;
        const auto ack = found->second->processSnoop(snoop);
        if (!ack)
            return false;
        if (endpoint == dropped_)
            return true;
        const auto disposition = engine_->handleSnoopAck(*ack);
        return disposition == AckDisposition::Accepted || disposition == AckDisposition::Deferred;
    }

private:
    MesiTransactionEngine *engine_{};
    std::map<std::uint16_t, CoherenceEndpointCache *> endpoints_;
    std::uint16_t dropped_{MesiDirectory::kMaximumHosts};
};

void testPartialAckFailsClosedAndCommitsOnlyAcknowledgedInvalidation() {
    TestMemory memory;
    MesiDirectory directory;
    PartialAckTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(20));
    CoherenceEndpointCache host(engine, {0, 101, 4, EndpointWritePolicy::WriteBack});
    CoherenceEndpointCache device(engine, {1, 202, 4, EndpointWritePolicy::WriteBack});
    transport.bindEngine(engine);
    transport.registerEndpoint(host);
    transport.registerEndpoint(device);
    CHECK(engine.bindSession(0, 101));
    CHECK(engine.bindSession(1, 202));
    CHECK(engine.bindSession(2, 303));

    CHECK(loadScalar<std::uint64_t>(host, kDataLine).first == Status::Ok);
    CHECK(loadScalar<std::uint64_t>(device, kDataLine).first == Status::Ok);
    transport.dropAckFrom(1);

    const auto result = engine.getm(kDataLine, {2, 303, 1});
    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    checkDirectory(directory, kDataLine, MesiState::S, std::nullopt, holder(1), true);
    CHECK(!host.contains(kDataLine));
    CHECK(!device.contains(kDataLine));
    CHECK(engine.auditCounters().partial_ack == 1);
}

void testWriteThroughPolicyMakesServerCopyCurrent() {
    TestMemory memory;
    MesiDirectory directory;
    InProcessCoherenceTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(100));
    CoherenceEndpointCache endpoint(engine, {0, 101, 2, EndpointWritePolicy::WriteThrough});
    transport.bindEngine(engine);
    CHECK(engine.bindSession(0, 101));
    CHECK(transport.registerEndpoint(endpoint));

    CHECK(storeScalar(endpoint, kDataLine, std::uint64_t{0xcafe}) == Status::Ok);
    CHECK(memory.scalar<std::uint64_t>(kDataLine) == 0xcafe);
    CHECK(!endpoint.contains(kDataLine));
    CHECK(endpoint.counters().writebacks == 1);
    checkDirectory(directory, kDataLine, MesiState::I, std::nullopt, 0, true);
}

void testWriteThroughPolicyCoversAtomics() {
    TestMemory memory;
    MesiDirectory directory;
    InProcessCoherenceTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(100));
    CoherenceEndpointCache endpoint(engine, {0, 101, 2, EndpointWritePolicy::WriteThrough});
    transport.bindEngine(engine);
    CHECK(engine.bindSession(0, 101));
    CHECK(transport.registerEndpoint(endpoint));
    memory.seed<std::uint64_t>(kAtomicLine, 0, 10);

    const auto faa = endpoint.fetchAdd(kAtomicLine, 5);
    CHECK(faa.status == Status::Ok);
    CHECK(faa.old_value == 10);
    CHECK(memory.scalar<std::uint64_t>(kAtomicLine) == 15);
    CHECK(!endpoint.contains(kAtomicLine));
    checkDirectory(directory, kAtomicLine, MesiState::I, std::nullopt, 0, true);

    const auto cas = endpoint.compareExchange(kAtomicLine, 15, 42);
    CHECK(cas.status == Status::Ok);
    CHECK(cas.old_value == 15);
    CHECK(memory.scalar<std::uint64_t>(kAtomicLine) == 42);
    CHECK(!endpoint.contains(kAtomicLine));
    CHECK(endpoint.counters().writebacks == 2);
}

} // namespace

int main() {
    testNegativeLegacySplitShadowExposesForbiddenOutcome();
    testHostWriteDeviceReadReturnsDirtyOwnerData();
    testDeviceWriteHostReadReturnsDirtyOwnerData();
    testMessagePassingNeverObservesFlagWithOldData();
    testOwnershipTransferPreservesPartialCacheLineStores();
    testCrossEndpointFetchAddIsLinearizable();
    testDirtySnoopCompletionCanBeReplayedAfterLocalInvalidation();
    testBoundedWriteBackCacheEvictsDirtyLine();
    testPartialAckFailsClosedAndCommitsOnlyAcknowledgedInvalidation();
    testWriteThroughPolicyMakesServerCopyCurrent();
    testWriteThroughPolicyCoversAtomics();

    if (failures.load(std::memory_order_relaxed) != 0) {
        std::cerr << failures.load(std::memory_order_relaxed) << " Type-2 coherence litmus checks failed\n";
        return 1;
    }
    std::cout << "Type-2 coherence litmus: PASS (forbidden outcomes=0)\n";
    return 0;
}
