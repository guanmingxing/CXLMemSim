#include "coherence_memory_backend.h"
#include "coherence_protocol_v2.h"
#include "coherence_transport.h"
#include "mesi_directory.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
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

constexpr std::uint64_t kLineA = 0x1000;
constexpr std::uint64_t kLineB = 0x2000;
constexpr std::uint64_t kLineC = 0x3000;

std::uint64_t holder(std::uint16_t host) { return std::uint64_t{1} << host; }

std::array<std::byte, kLineSize> bytes(std::uint64_t value) {
    std::array<std::byte, kLineSize> line{};
    std::memcpy(line.data(), &value, sizeof(value));
    return line;
}

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

    void seed(std::uint64_t address, const std::array<std::byte, kLineSize> &line) {
        std::lock_guard lock(mutex_);
        lines_[address] = line;
    }

    std::array<std::byte, kLineSize> inspect(std::uint64_t address) {
        std::lock_guard lock(mutex_);
        return lines_[address];
    }

private:
    std::mutex mutex_;
    std::map<std::uint64_t, std::array<std::byte, kLineSize>> lines_;
};

class AckingTransport final : public CoherenceTransport {
public:
    void bind(MesiTransactionEngine &engine) { engine_ = &engine; }
    void setDirty(std::uint16_t host, std::array<std::byte, kLineSize> data) { dirty_[host] = data; }

    bool sendToHost(std::uint16_t host, const CoherenceFrame &snoop) override {
        snoops.push_back(snoop);
        auto ack = initializeFrame(Opcode::SnoopAck);
        setSrcHost(ack, host);
        setDstHost(ack, kServerHost);
        setSessionId(ack, sessionId(snoop));
        setSnoopId(ack, snoopId(snoop));
        setAddress(ack, address(snoop));
        setEpoch(ack, epoch(snoop));
        setStatus(ack, Status::Ok);
        setAckStrength(ack, AckStrength::MODEL);
        const auto op = opcode(snoop);
        const bool downgrade = op == Opcode::SnpDowngrade || op == Opcode::SnpDataDowngrade;
        setLineState(ack, downgrade ? LineState::S : LineState::I);
        if (op == Opcode::SnpDataInv || op == Opcode::SnpDataDowngrade) {
            setPayloadLength(ack, kLineSize);
            const auto found = dirty_.find(host);
            if (found == dirty_.end())
                return false;
            std::transform(found->second.begin(), found->second.end(), ack.data.begin(),
                           [](std::byte value) { return std::to_integer<std::uint8_t>(value); });
        }
        const auto disposition = engine_->handleSnoopAck(ack);
        return disposition == AckDisposition::Accepted || disposition == AckDisposition::Deferred;
    }

    std::vector<CoherenceFrame> snoops;

private:
    MesiTransactionEngine *engine_{};
    std::map<std::uint16_t, std::array<std::byte, kLineSize>> dirty_;
};

TransactionRequest reported(std::uint16_t host, std::uint64_t session, std::uint64_t request_id, LineState state,
                            std::uint64_t installed_epoch) {
    TransactionRequest request{host, session, request_id};
    request.local_state = state;
    request.installed_epoch = installed_epoch;
    return request;
}

void bind(MesiTransactionEngine &engine, std::initializer_list<std::pair<std::uint16_t, std::uint64_t>> sessions) {
    for (const auto &[host, session] : sessions)
        CHECK(engine.bindSession(host, session));
}

void testReportedEpochValidationAndLaggingSharedUpgrade() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 0).committed());
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    CHECK(directory.gets(kLineB, 0).committed());
    TestMemory memory;
    AckingTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(100));
    transport.bind(engine);
    bind(engine, {{0, 100}, {1, 101}, {2, 102}});

    const auto future_epoch = engine.upgrade(kLineB, reported(0, 100, 1, LineState::E, 2));
    CHECK(future_epoch.status == Status::StaleEpoch);
    CHECK(!future_epoch.granted);
    CHECK(transport.snoops.empty());

    const auto lagging = engine.upgrade(kLineA, reported(0, 100, 2, LineState::S, 2));
    CHECK(lagging.status == Status::Ok);
    CHECK(lagging.granted);
    CHECK(lagging.transition.snapshot.state == MesiState::M);
    CHECK(lagging.transition.snapshot.epoch == 4);
    CHECK(transport.snoops.size() == 2);
}

void testInvalidReportedStateRecoversPhantomSharedHolder() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 0).committed());
    CHECK(directory.gets(kLineA, 1).committed());
    TestMemory memory;
    memory.seed(kLineA, bytes(0x1234));
    AckingTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(100));
    transport.bind(engine);
    bind(engine, {{0, 100}, {1, 101}});

    const auto result = engine.getm(kLineA, reported(0, 100, 1, LineState::I, 0));
    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.transition.snapshot.state == MesiState::M);
    CHECK(result.transition.snapshot.owner == 0);
    CHECK(result.transition.snapshot.epoch == 3);
    CHECK(transport.snoops.size() == 2);
    CHECK(std::any_of(transport.snoops.begin(), transport.snoops.end(),
                      [](const auto &snoop) { return dstHost(snoop) == 0 && opcode(snoop) == Opcode::SnpInv; }));
}

void testInvalidReportedStateRecoversPhantomModifiedOwner() {
    constexpr std::uint64_t kDirtyValue = 0xdecafbad12345678ULL;
    MesiDirectory directory;
    CHECK(directory.getm(kLineC, 0).committed());
    TestMemory memory;
    memory.seed(kLineC, bytes(0));
    AckingTransport transport;
    transport.setDirty(0, bytes(kDirtyValue));
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(100));
    transport.bind(engine);
    bind(engine, {{0, 100}});

    const auto result = engine.getm(kLineC, reported(0, 100, 1, LineState::I, 0));
    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.transition.snapshot.state == MesiState::M);
    CHECK(result.transition.snapshot.owner == 0);
    CHECK(result.transition.snapshot.epoch == 2);
    CHECK(result.data == bytes(kDirtyValue));
    CHECK(memory.inspect(kLineC) == bytes(kDirtyValue));
    CHECK(transport.snoops.size() == 1);
    CHECK(opcode(transport.snoops.front()) == Opcode::SnpDataInv);
    CHECK(dstHost(transport.snoops.front()) == 0);
    CHECK(epoch(transport.snoops.front()) == 2);
}

} // namespace

int main() {
    testReportedEpochValidationAndLaggingSharedUpgrade();
    testInvalidReportedStateRecoversPhantomSharedHolder();
    testInvalidReportedStateRecoversPhantomModifiedOwner();
    if (failures.load(std::memory_order_relaxed) != 0) {
        std::cerr << failures.load(std::memory_order_relaxed) << " reported-state recovery checks failed\n";
        return 1;
    }
    std::cout << "MESI reported-state recovery: PASS\n";
    return 0;
}
