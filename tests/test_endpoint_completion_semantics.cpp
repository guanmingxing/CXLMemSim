#include "coherence_endpoint_cache.h"

#include "coherence_memory_backend.h"
#include "coherence_transport.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <span>

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

constexpr std::uint64_t kFirstLine = 0x1000;
constexpr std::uint64_t kLineStride = kLineSize;
constexpr std::uint64_t kSessionId = 101;
constexpr std::size_t kCurrentMinimumCompletionCapacity = 16;

class TestMemory final : public CoherenceMemoryBackend {
public:
    std::array<std::byte, kLineSize> readLine(std::uint64_t address) override {
        std::lock_guard lock(mutex_);
        return lines_[address];
    }

    void writeLine(std::uint64_t address, std::span<const std::byte, kLineSize> data) override {
        std::lock_guard lock(mutex_);
        std::copy(data.begin(), data.end(), lines_[address].begin());
    }

    void seed(std::uint64_t address, std::uint64_t value) {
        std::lock_guard lock(mutex_);
        std::memcpy(lines_[address].data(), &value, sizeof(value));
    }

private:
    std::mutex mutex_;
    std::map<std::uint64_t, std::array<std::byte, kLineSize>> lines_;
};

class CompletionHarness {
public:
    explicit CompletionHarness(std::size_t cache_lines = 1)
        : engine(directory, memory, transport, std::chrono::milliseconds(100)),
          endpoint(engine, {0, kSessionId, cache_lines, EndpointWritePolicy::WriteBack}) {
        transport.bindEngine(engine);
        CHECK(engine.bindSession(endpoint.endpointId(), endpoint.sessionId()));
        CHECK(transport.registerEndpoint(endpoint));
    }

    TestMemory memory;
    MesiDirectory directory;
    InProcessCoherenceTransport transport;
    MesiTransactionEngine engine;
    CoherenceEndpointCache endpoint;
};

Status storeScalar(CoherenceEndpointCache &endpoint, std::uint64_t address, std::uint64_t value) {
    return endpoint.store(address, std::as_bytes(std::span{&value, std::size_t{1}}));
}

Status loadScalar(CoherenceEndpointCache &endpoint, std::uint64_t address, std::uint64_t &value) {
    return endpoint.load(address, std::as_writable_bytes(std::span{&value, std::size_t{1}}));
}

CoherenceFrame makeSnoop(const CoherenceEndpointCache &endpoint, Opcode opcode_value, std::uint64_t snoop_id,
                         std::uint64_t address, std::uint64_t target_epoch) {
    auto snoop = initializeFrame(opcode_value);
    setSrcHost(snoop, kServerHost);
    setDstHost(snoop, endpoint.endpointId());
    setSessionId(snoop, endpoint.sessionId());
    setSnoopId(snoop, snoop_id);
    setAddress(snoop, address);
    setEpoch(snoop, target_epoch);
    return snoop;
}

std::uint64_t scalarPayload(const CoherenceFrame &frame) {
    std::uint64_t value{};
    std::memcpy(&value, frame.data.data(), sizeof(value));
    return value;
}

void checkSuccessfulAck(const PreparedSnoopAck &ack, std::uint64_t snoop_id, std::uint64_t target_epoch,
                        LineState post_state, std::size_t payload_length) {
    CHECK(ack.has_value());
    if (!ack)
        return;
    CHECK(status(*ack) == Status::Ok);
    CHECK(snoopId(*ack) == snoop_id);
    CHECK(epoch(*ack) == target_epoch);
    CHECK(lineState(*ack) == post_state);
    CHECK(payloadLength(*ack) == payload_length);
}

template <typename Endpoint> bool reportsFenced(const Endpoint &endpoint) {
    if constexpr (requires(const Endpoint &candidate) {
                      { candidate.fenced() } -> std::convertible_to<bool>;
                  }) {
        return endpoint.fenced();
    } else {
        std::cerr << "required API missing: CoherenceEndpointCache::fenced()\n";
        return false;
    }
}

void testLostDirtyAckReplacementUsesSameTargetEpoch() {
    CompletionHarness harness;
    constexpr std::uint64_t kValue = 0x1122334455667788ULL;
    CHECK(storeScalar(harness.endpoint, kFirstLine, kValue) == Status::Ok);
    const auto before = harness.directory.inspect(kFirstLine);
    CHECK(before.has_value());
    const auto target_epoch = before ? before->epoch + 1 : 1;

    const auto first_snoop = makeSnoop(harness.endpoint, Opcode::SnpDataInv, 1001, kFirstLine, target_epoch);
    const auto first = harness.endpoint.processSnoop(first_snoop);
    checkSuccessfulAck(first, 1001, target_epoch, LineState::I, kLineSize);
    CHECK(!harness.endpoint.contains(kFirstLine));
    CHECK(first && scalarPayload(*first) == kValue);

    const auto replacement_snoop = makeSnoop(harness.endpoint, Opcode::SnpDataInv, 1002, kFirstLine, target_epoch);
    const auto replacement = harness.endpoint.processSnoop(replacement_snoop);
    checkSuccessfulAck(replacement, 1002, target_epoch, LineState::I, kLineSize);
    CHECK(replacement && scalarPayload(*replacement) == kValue);
    CHECK(first && replacement && first->data == replacement->data);
}

void testDirtyDowngradeCompletionSatisfiesSameEpochInvalidate() {
    CompletionHarness harness;
    constexpr std::uint64_t kValue = 0xa5a55a5adeadbeefULL;
    CHECK(storeScalar(harness.endpoint, kFirstLine, kValue) == Status::Ok);
    const auto before = harness.directory.inspect(kFirstLine);
    CHECK(before.has_value());
    const auto target_epoch = before ? before->epoch + 1 : 1;

    const auto downgrade_snoop = makeSnoop(harness.endpoint, Opcode::SnpDataDowngrade, 2001, kFirstLine, target_epoch);
    const auto downgrade = harness.endpoint.processSnoop(downgrade_snoop);
    checkSuccessfulAck(downgrade, 2001, target_epoch, LineState::S, kLineSize);
    CHECK(harness.endpoint.contains(kFirstLine));
    CHECK(downgrade && scalarPayload(*downgrade) == kValue);

    const auto invalidate_snoop = makeSnoop(harness.endpoint, Opcode::SnpDataInv, 2002, kFirstLine, target_epoch);
    const auto invalidate = harness.endpoint.processSnoop(invalidate_snoop);
    checkSuccessfulAck(invalidate, 2002, target_epoch, LineState::I, kLineSize);
    CHECK(!harness.endpoint.contains(kFirstLine));
    CHECK(invalidate && scalarPayload(*invalidate) == kValue);
    CHECK(downgrade && invalidate && downgrade->data == invalidate->data);
}

void testCleanDowngradeAndInvalidationSemanticReplacement() {
    {
        CompletionHarness harness;
        harness.memory.seed(kFirstLine, 0x1111);
        std::uint64_t observed{};
        CHECK(loadScalar(harness.endpoint, kFirstLine, observed) == Status::Ok);
        const auto before = harness.directory.inspect(kFirstLine);
        CHECK(before.has_value());
        const auto target_epoch = before ? before->epoch + 1 : 1;

        const auto downgrade_snoop = makeSnoop(harness.endpoint, Opcode::SnpDowngrade, 3001, kFirstLine, target_epoch);
        const auto downgrade = harness.endpoint.processSnoop(downgrade_snoop);
        checkSuccessfulAck(downgrade, 3001, target_epoch, LineState::S, 0);
        CHECK(harness.endpoint.contains(kFirstLine));

        const auto invalidate_snoop = makeSnoop(harness.endpoint, Opcode::SnpInv, 3002, kFirstLine, target_epoch);
        const auto invalidate = harness.endpoint.processSnoop(invalidate_snoop);
        checkSuccessfulAck(invalidate, 3002, target_epoch, LineState::I, 0);
        CHECK(!harness.endpoint.contains(kFirstLine));
    }

    {
        CompletionHarness harness;
        harness.memory.seed(kFirstLine, 0x2222);
        std::uint64_t observed{};
        CHECK(loadScalar(harness.endpoint, kFirstLine, observed) == Status::Ok);
        const auto before = harness.directory.inspect(kFirstLine);
        CHECK(before.has_value());
        const auto target_epoch = before ? before->epoch + 1 : 1;

        const auto first_snoop = makeSnoop(harness.endpoint, Opcode::SnpInv, 3101, kFirstLine, target_epoch);
        const auto first = harness.endpoint.processSnoop(first_snoop);
        checkSuccessfulAck(first, 3101, target_epoch, LineState::I, 0);
        CHECK(!harness.endpoint.contains(kFirstLine));

        const auto replacement_snoop = makeSnoop(harness.endpoint, Opcode::SnpInv, 3102, kFirstLine, target_epoch);
        const auto replacement = harness.endpoint.processSnoop(replacement_snoop);
        checkSuccessfulAck(replacement, 3102, target_epoch, LineState::I, 0);
        CHECK(!harness.endpoint.contains(kFirstLine));
    }
}

void testCompletionCapacityExhaustionRetainsOldestAndFencesEndpoint() {
    CompletionHarness harness;
    constexpr std::uint64_t kFirstSnoopId = 4001;
    std::uint64_t oldest_target_epoch{};
    std::uint64_t oldest_value{};

    for (std::size_t index = 0; index < kCurrentMinimumCompletionCapacity; ++index) {
        const auto address = kFirstLine + index * kLineStride;
        const auto value = 0xc000000000000000ULL + index;
        CHECK(storeScalar(harness.endpoint, address, value) == Status::Ok);
        const auto before = harness.directory.inspect(address);
        CHECK(before.has_value());
        const auto target_epoch = before ? before->epoch + 1 : 1;
        const auto snoop =
            makeSnoop(harness.endpoint, Opcode::SnpDataInv, kFirstSnoopId + index, address, target_epoch);
        const auto ack = harness.endpoint.processSnoop(snoop);
        checkSuccessfulAck(ack, kFirstSnoopId + index, target_epoch, LineState::I, kLineSize);
        CHECK(ack && scalarPayload(*ack) == value);
        if (index == 0) {
            oldest_target_epoch = target_epoch;
            oldest_value = value;
        }
    }

    const auto overflow_address = kFirstLine + kCurrentMinimumCompletionCapacity * kLineStride;
    constexpr std::uint64_t kOverflowValue = 0xd00dd00dd00dd00dULL;
    CHECK(storeScalar(harness.endpoint, overflow_address, kOverflowValue) == Status::Ok);
    const auto overflow_before = harness.directory.inspect(overflow_address);
    CHECK(overflow_before.has_value());
    const auto overflow_target = overflow_before ? overflow_before->epoch + 1 : 1;
    const auto overflow_snoop =
        makeSnoop(harness.endpoint, Opcode::SnpDataInv, kFirstSnoopId + kCurrentMinimumCompletionCapacity,
                  overflow_address, overflow_target);
    CHECK(!harness.endpoint.processSnoop(overflow_snoop).has_value());
    CHECK(harness.endpoint.contains(overflow_address));

    CHECK(reportsFenced(harness.endpoint));

    const auto oldest_replacement =
        makeSnoop(harness.endpoint, Opcode::SnpDataInv, 5001, kFirstLine, oldest_target_epoch);
    const auto replay = harness.endpoint.processSnoop(oldest_replacement);
    checkSuccessfulAck(replay, 5001, oldest_target_epoch, LineState::I, kLineSize);
    CHECK(replay && scalarPayload(*replay) == oldest_value);

    constexpr auto kNewLine = kFirstLine + (kCurrentMinimumCompletionCapacity + 2) * kLineStride;
    CHECK(storeScalar(harness.endpoint, kNewLine, 0xfeedface) == Status::HostFenced);
}

} // namespace

int main() {
    testLostDirtyAckReplacementUsesSameTargetEpoch();
    testDirtyDowngradeCompletionSatisfiesSameEpochInvalidate();
    testCleanDowngradeAndInvalidationSemanticReplacement();
    testCompletionCapacityExhaustionRetainsOldestAndFencesEndpoint();

    if (failures.load(std::memory_order_relaxed) != 0) {
        std::cerr << failures.load(std::memory_order_relaxed) << " endpoint completion semantic checks failed\n";
        return 1;
    }
    std::cout << "Endpoint completion semantics: PASS\n";
    return 0;
}
