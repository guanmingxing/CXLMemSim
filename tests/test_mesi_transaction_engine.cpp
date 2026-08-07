#include "coherency_engine.h"
#include "mesi_transaction_engine.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <utility>
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
constexpr auto kTestTimeout = std::chrono::seconds(5);

std::uint64_t holder(std::uint16_t host) { return std::uint64_t{1} << host; }

std::array<std::byte, 64> bytes(std::uint8_t value) {
    std::array<std::byte, 64> line{};
    line.fill(static_cast<std::byte>(value));
    return line;
}

struct EventLog {
    void push(std::string event) {
        std::lock_guard lock(mutex);
        events.push_back(std::move(event));
    }

    std::size_t indexOf(const std::string &event) const {
        std::lock_guard lock(mutex);
        const auto found = std::find(events.begin(), events.end(), event);
        return found == events.end() ? events.size() : static_cast<std::size_t>(found - events.begin());
    }

    mutable std::mutex mutex;
    std::vector<std::string> events;
};

class FakeMemoryBackend final : public CoherenceMemoryBackend {
public:
    explicit FakeMemoryBackend(EventLog *events = nullptr) : events_(events) {}

    std::array<std::byte, 64> readLine(std::uint64_t address) override {
        std::unique_lock lock(mutex_);
        ++reads_;
        read_entered_ = true;
        operation_cv_.notify_all();
        operation_cv_.wait(lock, [&] { return !block_read_; });
        if (throw_read_)
            throw std::runtime_error("injected read failure");
        if (events_)
            events_->push("read:" + std::to_string(address));
        return lines_[address];
    }

    void writeLine(std::uint64_t address, std::span<const std::byte, 64> data) override {
        std::unique_lock lock(mutex_);
        ++writes_;
        write_entered_ = true;
        operation_cv_.notify_all();
        operation_cv_.wait(lock, [&] { return !block_write_; });
        if (throw_write_)
            throw std::runtime_error("injected write failure");
        std::copy(data.begin(), data.end(), lines_[address].begin());
        if (events_)
            events_->push("write:" + std::to_string(address));
    }

    void seed(std::uint64_t address, const std::array<std::byte, 64> &data) {
        std::lock_guard lock(mutex_);
        lines_[address] = data;
    }

    std::array<std::byte, 64> line(std::uint64_t address) const {
        std::lock_guard lock(mutex_);
        const auto found = lines_.find(address);
        return found == lines_.end() ? std::array<std::byte, 64>{} : found->second;
    }

    std::size_t reads() const {
        std::lock_guard lock(mutex_);
        return reads_;
    }

    std::size_t writes() const {
        std::lock_guard lock(mutex_);
        return writes_;
    }

    void setThrowRead(bool value) {
        std::lock_guard lock(mutex_);
        throw_read_ = value;
    }

    void setThrowWrite(bool value) {
        std::lock_guard lock(mutex_);
        throw_write_ = value;
    }

    void blockReads() {
        std::lock_guard lock(mutex_);
        block_read_ = true;
        read_entered_ = false;
    }

    void blockWrites() {
        std::lock_guard lock(mutex_);
        block_write_ = true;
        write_entered_ = false;
    }

    void waitForRead() {
        std::unique_lock lock(mutex_);
        if (!operation_cv_.wait_for(lock, kTestTimeout, [&] { return read_entered_; })) {
            std::cerr << "memory read wait timed out\n";
            std::_Exit(EXIT_FAILURE);
        }
    }

    void waitForWrite() {
        std::unique_lock lock(mutex_);
        if (!operation_cv_.wait_for(lock, kTestTimeout, [&] { return write_entered_; })) {
            std::cerr << "memory write wait timed out\n";
            std::_Exit(EXIT_FAILURE);
        }
    }

    void releaseReads() {
        std::lock_guard lock(mutex_);
        block_read_ = false;
        operation_cv_.notify_all();
    }

    void releaseWrites() {
        std::lock_guard lock(mutex_);
        block_write_ = false;
        operation_cv_.notify_all();
    }

private:
    EventLog *events_;
    mutable std::mutex mutex_;
    std::condition_variable operation_cv_;
    std::map<std::uint64_t, std::array<std::byte, 64>> lines_;
    std::size_t reads_{};
    std::size_t writes_{};
    bool throw_read_{};
    bool throw_write_{};
    bool block_read_{};
    bool block_write_{};
    bool read_entered_{};
    bool write_entered_{};
};

class FakeTransport final : public CoherenceTransport {
public:
    bool sendToHost(std::uint16_t host, const CoherenceFrame &frame) override {
        std::function<void(std::uint16_t, const CoherenceFrame &)> callback;
        bool throw_after_callback = false;
        std::size_t send_number = 0;
        {
            std::unique_lock lock(mutex_);
            sent_.emplace_back(host, frame);
            send_number = sent_.size();
            sent_cv_.notify_all();
            sent_cv_.wait(lock, [&] { return blocked_send_ != send_number; });
            if (throw_on_send_)
                throw std::runtime_error("injected transport failure");
            callback = callback_;
            throw_after_callback = throw_after_callback_;
        }
        if (callback)
            callback(host, frame);
        {
            std::unique_lock lock(mutex_);
            callback_completed_.push_back(send_number);
            sent_cv_.notify_all();
            sent_cv_.wait(lock, [&] { return blocked_after_callback_send_ != send_number; });
        }
        if (throw_after_callback)
            throw std::runtime_error("injected post-callback transport failure");
        return send_success_.load();
    }

    void onSend(std::function<void(std::uint16_t, const CoherenceFrame &)> callback) {
        std::lock_guard lock(mutex_);
        callback_ = std::move(callback);
    }

    void setSendSuccess(bool success) { send_success_.store(success); }

    void setThrowOnSend(bool value) {
        std::lock_guard lock(mutex_);
        throw_on_send_ = value;
    }

    void setThrowAfterCallback(bool value) {
        std::lock_guard lock(mutex_);
        throw_after_callback_ = value;
    }

    void blockSend(std::size_t send_number) {
        std::lock_guard lock(mutex_);
        blocked_send_ = send_number;
    }

    void blockAfterCallback(std::size_t send_number) {
        std::lock_guard lock(mutex_);
        blocked_after_callback_send_ = send_number;
    }

    void releaseSend() {
        std::lock_guard lock(mutex_);
        blocked_send_ = 0;
        sent_cv_.notify_all();
    }

    void releaseAfterCallback() {
        std::lock_guard lock(mutex_);
        blocked_after_callback_send_ = 0;
        sent_cv_.notify_all();
    }

    void waitForCallback(std::size_t send_number) const {
        std::unique_lock lock(mutex_);
        if (!sent_cv_.wait_for(lock, kTestTimeout, [&] {
                return std::find(callback_completed_.begin(), callback_completed_.end(), send_number) !=
                       callback_completed_.end();
            })) {
            std::cerr << "transport callback wait timed out\n";
            std::_Exit(EXIT_FAILURE);
        }
    }

    std::vector<std::pair<std::uint16_t, CoherenceFrame>> waitFor(std::size_t count) const {
        std::unique_lock lock(mutex_);
        if (!sent_cv_.wait_for(lock, kTestTimeout, [&] { return sent_.size() >= count; })) {
            std::cerr << "transport wait timed out\n";
            std::_Exit(EXIT_FAILURE);
        }
        return sent_;
    }

    std::vector<std::pair<std::uint16_t, CoherenceFrame>> sent() const {
        std::lock_guard lock(mutex_);
        return sent_;
    }

private:
    mutable std::mutex mutex_;
    mutable std::condition_variable sent_cv_;
    std::vector<std::pair<std::uint16_t, CoherenceFrame>> sent_;
    std::function<void(std::uint16_t, const CoherenceFrame &)> callback_;
    std::vector<std::size_t> callback_completed_;
    std::atomic<bool> send_success_{true};
    bool throw_on_send_{};
    bool throw_after_callback_{};
    std::size_t blocked_send_{};
    std::size_t blocked_after_callback_send_{};
};

CoherenceFrame ackFor(const CoherenceFrame &snoop, const std::array<std::byte, 64> &dirty = {}) {
    auto ack = initializeFrame(Opcode::SnoopAck);
    setSrcHost(ack, dstHost(snoop));
    setDstHost(ack, srcHost(snoop));
    setSessionId(ack, sessionId(snoop));
    setSnoopId(ack, snoopId(snoop));
    setAddress(ack, address(snoop));
    setEpoch(ack, epoch(snoop));
    setStatus(ack, Status::Ok);
    setAckStrength(ack, AckStrength::MODEL);
    const bool downgrade = opcode(snoop) == Opcode::SnpDowngrade || opcode(snoop) == Opcode::SnpDataDowngrade;
    setLineState(ack, downgrade ? LineState::S : LineState::I);
    const bool returns_data = opcode(snoop) == Opcode::SnpDataInv || opcode(snoop) == Opcode::SnpDataDowngrade;
    if (returns_data) {
        setPayloadLength(ack, kLineSize);
        std::transform(dirty.begin(), dirty.end(), ack.data.begin(),
                       [](std::byte byte) { return static_cast<std::uint8_t>(byte); });
    }
    return ack;
}

template <typename Future> auto ready(Future &future, const char *context) {
    if (future.wait_for(kTestTimeout) != std::future_status::ready) {
        std::cerr << context << " timed out\n";
        std::_Exit(EXIT_FAILURE);
    }
    return future.get();
}

template <typename Function> std::optional<TransactionResult> transactionWithoutThrow(Function &&function) {
    try {
        return std::forward<Function>(function)();
    } catch (const std::exception &error) {
        std::cerr << "unexpected transaction exception: " << error.what() << '\n';
        failures.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }
}

void checkNoPending(MesiDirectory &directory, std::uint64_t address) {
    auto line = directory.lockLine(address);
    CHECK(line.has_value());
    CHECK(line && line->pendingTransaction() == nullptr);
}

void bind(MesiTransactionEngine &engine, std::initializer_list<std::pair<std::uint16_t, std::uint64_t>> sessions) {
    for (const auto &[host, session] : sessions)
        CHECK(engine.bindSession(host, session));
}

void checkState(const MesiDirectory &directory, std::uint64_t address, MesiState state,
                std::optional<std::uint16_t> owner, std::uint64_t sharers, std::uint64_t epoch, bool server_current) {
    const auto snapshot = directory.inspect(address);
    CHECK(snapshot.has_value());
    if (!snapshot)
        return;
    CHECK(snapshot->state == state);
    CHECK(snapshot->owner == owner);
    CHECK(snapshot->sharers == sharers);
    CHECK(snapshot->epoch == epoch);
    CHECK(snapshot->server_copy_current == server_current);
    CHECK(isValidSnapshot(*snapshot));
}

void testGetsSnoopsModifiedOwnerForDataAndDowngrades() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto dirty = bytes(0xa5);

    auto future = std::async(std::launch::async, [&] { return engine.gets(kLineA, {2, 102, 9001}); });
    const auto sent = transport.waitFor(1);
    CHECK(sent[0].first == 1);
    CHECK(opcode(sent[0].second) == Opcode::SnpDataDowngrade);
    CHECK(epoch(sent[0].second) == 1);
    auto ack = ackFor(sent[0].second, dirty);
    setEpoch(ack, 1);
    const auto disposition = engine.handleSnoopAck(ack);
    CHECK(disposition == AckDisposition::Deferred);
    if (disposition != AckDisposition::Deferred)
        (void)engine.progress(MesiTransactionEngine::TimePoint::max());

    const auto result = ready(future, "GETS modified owner");
    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == dirty);
    CHECK(memory.line(kLineA) == dirty);
    CHECK(memory.writes() == 1);
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
}

void testGetmInvalidatesAllSharedHoldersBeforeGrant() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x33));
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9002}); });
    const auto sent = transport.waitFor(2);
    CHECK(opcode(sent[0].second) == Opcode::SnpInv);
    CHECK(opcode(sent[1].second) == Opcode::SnpInv);
    CHECK(future.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    CHECK(engine.handleSnoopAck(ackFor(sent[0].second)) == AckDisposition::Accepted);
    CHECK(future.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    CHECK(engine.handleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Accepted);

    const auto result = ready(future, "GETM shared holders");
    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == bytes(0x33));
    checkState(directory, kLineA, MesiState::M, 3, 0, 3, false);
}

void testGetmTakesDirtyDataBeforeGrantingNewOwner() {
    EventLog events;
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory(&events);
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto dirty = bytes(0x7c);

    auto future = std::async(std::launch::async, [&] {
        auto result = engine.getm(kLineA, {2, 102, 9003});
        events.push("grant-returned");
        return result;
    });
    const auto snoop = transport.waitFor(1)[0].second;
    CHECK(opcode(snoop) == Opcode::SnpDataInv);
    CHECK(engine.handleSnoopAck(ackFor(snoop, dirty)) == AckDisposition::Deferred);
    const auto result = ready(future, "GETM dirty owner");

    CHECK(result.granted);
    CHECK(result.data == dirty);
    CHECK(events.indexOf("write:" + std::to_string(kLineA)) < events.indexOf("grant-returned"));
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);
}

void testUpgradeInvalidatesOtherSharersWithoutRefetchingData() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.upgrade(kLineA, {1, 101, 9004}); });
    const auto sent = transport.waitFor(1);
    CHECK(sent[0].first == 2);
    CHECK(opcode(sent[0].second) == Opcode::SnpInv);
    CHECK(engine.handleSnoopAck(ackFor(sent[0].second)) == AckDisposition::Accepted);
    const auto result = ready(future, "UPGRADE");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(memory.reads() == 0);
    CHECK(memory.writes() == 0);
    checkState(directory, kLineA, MesiState::M, 1, 0, 3, false);
}

void testDuplicateAndStaleAcksCannotMutateEpoch() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}, {4, 104}});

    auto first = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9005}); });
    const auto sent = transport.waitFor(2);
    const auto first_ack = ackFor(sent[0].second);
    CHECK(engine.handleSnoopAck(first_ack) == AckDisposition::Accepted);
    CHECK(engine.handleSnoopAck(first_ack) == AckDisposition::Duplicate);
    auto wrong_epoch = ackFor(sent[1].second);
    setEpoch(wrong_epoch, epoch(wrong_epoch) + 1);
    CHECK(engine.handleSnoopAck(wrong_epoch) == AckDisposition::Stale);
    CHECK(first.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(engine.handleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Accepted);
    CHECK(ready(first, "first epoch transaction").granted);
    checkState(directory, kLineA, MesiState::M, 3, 0, 3, false);

    auto second = std::async(std::launch::async, [&] { return engine.gets(kLineA, {4, 104, 9006}); });
    const auto later = transport.waitFor(3);
    CHECK(epoch(later[2].second) == 3);
    CHECK(engine.handleSnoopAck(first_ack) == AckDisposition::Stale);
    CHECK(second.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(engine.handleSnoopAck(ackFor(later[2].second, bytes(0x44))) == AckDisposition::Deferred);
    CHECK(ready(second, "later epoch transaction").granted);
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(3) | holder(4), 4, true);
}

void testSynchronousGrantWaitsForEveryAckEffect() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9007}); });
    const auto sent = transport.waitFor(2);
    CHECK(engine.handleSnoopAck(ackFor(sent[0].second)) == AckDisposition::Accepted);
    CHECK(future.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(engine.handleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Accepted);
    const auto result = ready(future, "synchronous ordering");
    CHECK(result.granted);
    CHECK(result.transition.committed());
    CHECK(result.transition.snapshot.state == MesiState::M);
    CHECK(result.transition.snapshot.owner == 3);
}

void testInvalidRequesterRolesAreRejectedBeforeSnoops() {
    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).committed());
        CHECK(directory.gets(kLineA, 2).committed());
        FakeMemoryBackend memory;
        FakeTransport transport;
        MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
        bind(engine, {{1, 101}, {2, 102}});
        transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
            CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
        });

        const auto result = engine.getm(kLineA, {1, 101, 9013});
        CHECK(result.status == Status::InvalidState);
        CHECK(!result.granted);
        CHECK(transport.sent().empty());
        checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
    }
    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).committed());
        CHECK(directory.gets(kLineA, 2).committed());
        FakeMemoryBackend memory;
        FakeTransport transport;
        MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
        bind(engine, {{1, 101}, {2, 102}, {3, 103}});
        transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
            CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
        });

        const auto result = engine.upgrade(kLineA, {3, 103, 9014});
        CHECK(result.status == Status::InvalidState);
        CHECK(!result.granted);
        CHECK(transport.sent().empty());
        checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
    }
}

void testTimeoutWithZeroAcksPreservesPriorState() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.gets(kLineA, {2, 102, 9008}); });
    (void)transport.waitFor(1);
    CHECK(engine.progress(MesiTransactionEngine::TimePoint::max()) == 1);
    const auto result = ready(future, "zero ACK timeout");
    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    CHECK(memory.writes() == 0);
    checkState(directory, kLineA, MesiState::M, 1, 0, 1, false);
}

void testPartialTimeoutCommitsOnlyAcknowledgedInvalidations() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    CHECK(directory.gets(kLineA, 3).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}, {4, 104}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {4, 104, 9009}); });
    const auto sent = transport.waitFor(3);
    CHECK(engine.handleSnoopAck(ackFor(sent[0].second)) == AckDisposition::Accepted);
    CHECK(engine.progress(MesiTransactionEngine::TimePoint::max()) == 1);
    const auto result = ready(future, "partial timeout");

    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    const auto removed = holder(sent[0].first);
    CHECK((result.transition.snapshot.sharers & removed) == 0);
    CHECK(result.transition.snapshot.sharers == ((holder(1) | holder(2) | holder(3)) & ~removed));
    CHECK(result.transition.snapshot.epoch == 4);
    CHECK(engine.handleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Stale);
    CHECK(directory.inspect(kLineA)->epoch == 4);
}

void testRequesterDisconnectAfterSnoopsLeavesLegalStableState() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9010}); });
    const auto sent = transport.waitFor(2);
    CHECK(engine.handleSnoopAck(ackFor(sent[0].second)) == AckDisposition::Accepted);
    CHECK(engine.notifyDisconnect(3, 103) == 1);
    const auto result = ready(future, "requester disconnect");

    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    const auto snapshot = directory.inspect(kLineA);
    CHECK(snapshot.has_value());
    CHECK(snapshot && isValidSnapshot(*snapshot));
    CHECK(snapshot && snapshot->state == MesiState::S);
    CHECK(snapshot && snapshot->sharers == ((holder(1) | holder(2)) & ~holder(sent[0].first)));
    CHECK(snapshot && snapshot->epoch == 3);
}

void testConcurrentDisjointLinesProgressIndependently() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    CHECK(directory.getm(kLineB, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}, {4, 104}});

    auto line_a = std::async(std::launch::async, [&] { return engine.gets(kLineA, {3, 103, 9011}); });
    auto line_b = std::async(std::launch::async, [&] { return engine.gets(kLineB, {4, 104, 9012}); });
    const auto sent = transport.waitFor(2);
    CHECK(address(sent[0].second) != address(sent[1].second));
    CHECK(line_a.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    CHECK(line_b.wait_for(std::chrono::milliseconds(10)) == std::future_status::timeout);
    for (const auto &[host, snoop] : sent) {
        (void)host;
        CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(static_cast<std::uint8_t>(address(snoop) >> 8)))) ==
              AckDisposition::Deferred);
    }
    CHECK(ready(line_a, "disjoint line A").granted);
    CHECK(ready(line_b, "disjoint line B").granted);
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(3), 2, true);
    checkState(directory, kLineB, MesiState::S, std::nullopt, holder(2) | holder(4), 2, true);
}

void testCoherencyEngineStrictOperationsUseTransactionEngine() {
    CoherencyEngine engine(0, nullptr, nullptr);
    FakeMemoryBackend memory;
    FakeTransport transport;
    engine.configureStrictV2(memory, transport, kTestTimeout);
    bind(engine.strictV2TransactionEngine(), {{1, 101}, {2, 102}, {3, 103}});
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.strictV2HandleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });
    CHECK(engine.strictV2Gets(kLineA, 1).committed());
    CHECK(engine.strictV2Gets(kLineA, 2).committed());
    const auto result = engine.strictV2Getm(kLineA, 3);
    CHECK(result.committed());
    checkState(engine.strictV2Directory(), kLineA, MesiState::M, 3, 0, 3, false);
    CHECK(transport.sent().size() == 3);
}

void testStrictWrapperNeverReportsTimeoutAsSuccess() {
    {
        CoherencyEngine engine(0, nullptr, nullptr);
        FakeMemoryBackend memory;
        FakeTransport transport;
        engine.configureStrictV2(memory, transport, kTestTimeout);
        bind(engine.strictV2TransactionEngine(), {{1, 101}, {2, 102}});
        CHECK(engine.strictV2Getm(kLineA, 1).committed());

        auto future = std::async(std::launch::async, [&] { return engine.strictV2Gets(kLineA, 2); });
        (void)transport.waitFor(1);
        CHECK(engine.strictV2Progress(MesiTransactionEngine::TimePoint::max()) == 1);
        const auto result = ready(future, "strict wrapper zero-ACK timeout");

        CHECK(!result.succeeded());
        checkState(engine.strictV2Directory(), kLineA, MesiState::M, 1, 0, 1, false);
    }
    {
        CoherencyEngine engine(0, nullptr, nullptr);
        FakeMemoryBackend memory;
        FakeTransport transport;
        engine.configureStrictV2(memory, transport, kTestTimeout);
        bind(engine.strictV2TransactionEngine(), {{1, 101}, {2, 102}, {3, 103}});
        CHECK(engine.strictV2Gets(kLineA, 1).committed());
        transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
            CHECK(engine.strictV2HandleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
        });
        CHECK(engine.strictV2Gets(kLineA, 2).committed());
        transport.onSend({});

        auto future = std::async(std::launch::async, [&] { return engine.strictV2Getm(kLineA, 3); });
        const auto sent = transport.waitFor(3);
        CHECK(engine.strictV2HandleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Accepted);
        CHECK(engine.strictV2Progress(MesiTransactionEngine::TimePoint::max()) == 1);
        const auto result = ready(future, "strict wrapper partial timeout");

        CHECK(!result.succeeded());
        const auto remaining = (holder(1) | holder(2)) & ~holder(sent[1].first);
        checkState(engine.strictV2Directory(), kLineA, MesiState::S, std::nullopt, remaining, 3, true);
    }
}

void testRequesterDisconnectAfterFinalAckBeforeCommitCannotGrant() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});
    std::atomic<std::size_t> acknowledged{};
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
        if (acknowledged.fetch_add(1) + 1 == 2)
            CHECK(engine.notifyDisconnect(3, 103) == 1);
    });

    const auto result = engine.getm(kLineA, {3, 103, 9100});

    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    CHECK(transport.sent().size() == 2);
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 3, true);
}

void testUndispatchedSnoopAckCannotCompleteTransaction() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    transport.blockSend(1);
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout, 500);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9200}); });
    const auto first_snoop = transport.waitFor(1)[0].second;
    auto later_snoop = first_snoop;
    setDstHost(later_snoop, 2);
    setSessionId(later_snoop, 102);
    setSnoopId(later_snoop, snoopId(first_snoop) + 1);

    CHECK(engine.handleSnoopAck(ackFor(first_snoop)) == AckDisposition::Accepted);
    const auto early_disposition = engine.handleSnoopAck(ackFor(later_snoop));
    CHECK(early_disposition == AckDisposition::Stale);
    CHECK(future.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);

    transport.releaseSend();
    if (early_disposition == AckDisposition::Stale) {
        const auto sent = transport.waitFor(2);
        CHECK(sent[1].first == 2);
        CHECK(snoopId(sent[1].second) == snoopId(later_snoop));
        CHECK(engine.handleSnoopAck(ackFor(sent[1].second)) == AckDisposition::Accepted);
    }
    const auto result = ready(future, "undispatched snoop ACK");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(transport.sent().size() == 2);
    checkState(directory, kLineA, MesiState::M, 3, 0, 3, false);
}

void testRequesterDisconnectDuringSnoopPrefetchFailsAdmission() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    memory.blockReads();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9201}); });
    memory.waitForRead();
    (void)engine.notifyDisconnect(3, 103);
    memory.releaseReads();
    const auto result = ready(future, "requester disconnect during snoop prefetch");

    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    CHECK(transport.sent().empty());
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
    checkNoPending(directory, kLineA);
}

void testRequesterDisconnectDuringDirectReadFailsCommit() {
    MesiDirectory directory;
    FakeMemoryBackend memory;
    memory.blockReads();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}});

    auto future = std::async(std::launch::async, [&] { return engine.gets(kLineA, {1, 101, 9202}); });
    memory.waitForRead();
    (void)engine.notifyDisconnect(1, 101);
    memory.releaseReads();
    const auto result = ready(future, "requester disconnect during direct read");

    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    CHECK(transport.sent().empty());
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 0, true);
    checkNoPending(directory, kLineA);
}

void testTimeoutDuringBlockedSendStopsLaterSnoops() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    transport.blockSend(1);
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9101}); });
    (void)transport.waitFor(1);
    CHECK(engine.progress(MesiTransactionEngine::TimePoint::max()) == 1);
    transport.releaseSend();
    const auto result = ready(future, "timeout during blocked send");

    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    CHECK(transport.sent().size() == 1);
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
    checkNoPending(directory, kLineA);
}

void testNaturalDeadlineDuringBlockedSendStopsLaterSnoops() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    transport.blockSend(1);
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(30), 600);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {3, 103, 9203}); });
    const auto first_snoop = transport.waitFor(1)[0].second;
    auto later_snoop = first_snoop;
    setDstHost(later_snoop, 2);
    setSessionId(later_snoop, 102);
    setSnoopId(later_snoop, snoopId(first_snoop) + 1);
    CHECK(engine.handleSnoopAck(ackFor(first_snoop)) == AckDisposition::Accepted);

    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    transport.releaseSend();
    const auto result = ready(future, "natural deadline during blocked send");

    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    CHECK(transport.sent().size() == 1);
    CHECK(engine.handleSnoopAck(ackFor(later_snoop)) == AckDisposition::Stale);
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(2), 3, true);
    checkNoPending(directory, kLineA);
}

void testAcceptedFinalAckBeforeDeadlineSurvivesSuccessfulSlowSendReturn() {
    constexpr auto snoop_timeout = std::chrono::milliseconds(500);
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x6a));
    FakeTransport transport;
    transport.blockAfterCallback(1);
    MesiTransactionEngine engine(directory, memory, transport, snoop_timeout);
    bind(engine, {{1, 101}, {2, 102}});
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9207}); });
    transport.waitForCallback(1);
    CHECK(future.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    std::this_thread::sleep_for(snoop_timeout + std::chrono::milliseconds(50));
    transport.releaseAfterCallback();
    const auto result = ready(future, "accepted final ACK before slow successful send return");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == bytes(0x6a));
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);
    checkNoPending(directory, kLineA);
}

void checkFinalSynchronousAckSendFailure(bool throw_after_callback) {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    transport.setSendSuccess(false);
    transport.setThrowAfterCallback(throw_after_callback);
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });

    const auto result = transactionWithoutThrow([&] { return engine.getm(kLineA, {2, 102, 9204}); });

    CHECK(result && result->status == Status::IoError);
    CHECK(result && !result->granted);
    CHECK(transport.sent().size() == 1);
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 2, true);
    checkNoPending(directory, kLineA);
}

void testFinalSynchronousAckCannotHideFalseSendResult() { checkFinalSynchronousAckSendFailure(false); }

void testFinalSynchronousAckCannotHideThrownSendFailure() { checkFinalSynchronousAckSendFailure(true); }

void testRegisteredDirectDataGrantRequiresConfiguredBackend() {
    {
        MesiDirectory directory;
        MesiTransactionEngine engine(directory);
        bind(engine, {{1, 101}});

        const auto result = engine.gets(kLineA, {1, 101, 9205});

        CHECK(result.status == Status::IoError);
        CHECK(!result.granted);
        checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 0, true);
    }
    {
        MesiDirectory directory;
        MesiTransactionEngine engine(directory);
        bind(engine, {{1, 101}});

        const auto result = engine.getm(kLineA, {1, 101, 9206});

        CHECK(result.status == Status::IoError);
        CHECK(!result.granted);
        checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 0, true);
    }
}

void testConfiguredStrictV2RejectsSessionZeroBeforeBackendAccess() {
    MesiDirectory directory;
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x71));
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);

    const auto result = engine.gets(kLineA, {1, 0, 9208});

    CHECK(result.status == Status::StaleSession);
    CHECK(!result.granted);
    CHECK(memory.reads() == 0);
    CHECK(memory.writes() == 0);
    CHECK(transport.sent().empty());
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 0, true);

    CoherencyEngine wrapper(0, nullptr, nullptr);
    wrapper.configureStrictV2(memory, transport, kTestTimeout);
    CHECK(!wrapper.strictV2Gets(kLineB, 2).succeeded());
    CHECK(memory.reads() == 0);
    CHECK(transport.sent().empty());
    checkState(wrapper.strictV2Directory(), kLineB, MesiState::I, std::nullopt, 0, 0, true);
}

void testSessionGenerationCannotBeReplacedUntilDisconnect() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x72));
    FakeTransport transport;
    transport.blockAfterCallback(1);
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    CHECK(engine.bindSession(2, 102));
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9209}); });
    transport.waitForCallback(1);
    CHECK(!engine.bindSession(2, 202));
    CHECK(engine.sessionFor(2) == 102);
    CHECK(engine.bindSession(2, 102));
    transport.releaseAfterCallback();
    const auto result = ready(future, "old session transaction after rejected replacement");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == bytes(0x72));
    CHECK(engine.sessionFor(2) == 102);
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);

    CHECK(engine.notifyDisconnect(2, 102) == 0);
    CHECK(engine.sessionFor(2) == 0);
    CHECK(engine.bindSession(2, 202));
    CHECK(engine.sessionFor(2) == 202);
}

void testUnconfiguredStrictWrapperPreservesMetadataOnlyCompatibility() {
    CoherencyEngine engine(0, nullptr, nullptr);

    CHECK(engine.strictV2Gets(kLineA, 1).committed());
    CHECK(engine.strictV2Upgrade(kLineA, 1).committed());
    CHECK(engine.strictV2Getm(kLineB, 2).committed());
    checkState(engine.strictV2Directory(), kLineA, MesiState::M, 1, 0, 2, false);
    checkState(engine.strictV2Directory(), kLineB, MesiState::M, 2, 0, 1, false);
}

void testDependencyFailuresReturnIoErrorAndClearPending() {
    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).committed());
        FakeMemoryBackend memory;
        FakeTransport transport;
        transport.setThrowOnSend(true);
        MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
        bind(engine, {{1, 101}, {2, 102}});

        const auto result = transactionWithoutThrow([&] { return engine.getm(kLineA, {2, 102, 9102}); });
        CHECK(result && result->status == Status::IoError);
        CHECK(result && !result->granted);
        checkState(directory, kLineA, MesiState::E, 1, 0, 1, true);
        checkNoPending(directory, kLineA);
    }
    {
        MesiDirectory directory;
        FakeMemoryBackend memory;
        memory.setThrowRead(true);
        FakeTransport transport;
        MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
        bind(engine, {{1, 101}});

        const auto result = transactionWithoutThrow([&] { return engine.gets(kLineA, {1, 101, 9103}); });
        CHECK(result && result->status == Status::IoError);
        CHECK(result && !result->granted);
        checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 0, true);
        checkNoPending(directory, kLineA);

        memory.setThrowRead(false);
        const auto retry = engine.gets(kLineA, {1, 101, 9104});
        CHECK(retry.status == Status::Ok);
        CHECK(retry.granted);
    }
    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).committed());
        CHECK(directory.gets(kLineA, 2).committed());
        FakeMemoryBackend memory;
        memory.setThrowRead(true);
        FakeTransport transport;
        MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
        bind(engine, {{1, 101}, {2, 102}, {3, 103}});
        transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
            CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
        });

        const auto result = transactionWithoutThrow([&] { return engine.getm(kLineA, {3, 103, 9105}); });
        CHECK(result && result->status == Status::IoError);
        CHECK(result && !result->granted);
        CHECK(transport.sent().empty());
        checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
        checkNoPending(directory, kLineA);
    }
}

void testDirtyAckPersistenceFailureIsRetryableBeforeAcceptance() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x11));
    memory.setThrowWrite(true);
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto dirty = bytes(0xaa);

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9106}); });
    const auto snoop = transport.waitFor(1)[0].second;
    const auto ack = ackFor(snoop, dirty);
    CHECK(engine.handleSnoopAck(ack) == AckDisposition::Deferred);
    memory.waitForWrite();
    CHECK(future.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(memory.line(kLineA) == bytes(0x11));

    memory.setThrowWrite(false);
    CHECK(engine.handleSnoopAck(ack) == AckDisposition::Deferred);
    const auto result = ready(future, "retry dirty ACK persistence");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == dirty);
    CHECK(memory.line(kLineA) == dirty);
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);
    checkNoPending(directory, kLineA);
}

void testConflictingDirtyPayloadReplayIsInvalidAndOriginalCompletes() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x12));
    memory.setThrowWrite(true);
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto original = bytes(0xab);

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9119}); });
    const auto snoop = transport.waitFor(1)[0].second;
    const auto original_ack = ackFor(snoop, original);
    CHECK(engine.handleSnoopAck(original_ack) == AckDisposition::Deferred);
    memory.waitForWrite();
    CHECK(memory.writes() == 1);
    CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(0xcd))) == AckDisposition::Invalid);

    memory.setThrowWrite(false);
    CHECK(engine.handleSnoopAck(original_ack) == AckDisposition::Deferred);
    const auto result = ready(future, "original dirty payload replay");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == original);
    CHECK(memory.line(kLineA) == original);
    CHECK(memory.writes() == 2);
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);
    checkNoPending(directory, kLineA);
}

void testAcceptedCleanInvalidationsUsePrefetchedDataWithoutRollback() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x5d));
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});
    transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        memory.setThrowRead(true);
        CHECK(engine.handleSnoopAck(ackFor(snoop)) == AckDisposition::Accepted);
    });

    const auto result = engine.getm(kLineA, {3, 103, 9111});

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(result.data == bytes(0x5d));
    CHECK(memory.reads() == 1);
    CHECK(transport.sent().size() == 2);
    checkState(directory, kLineA, MesiState::M, 3, 0, 3, false);
}

void testDirtyWriteIsDurableBeforeOwnerChangeBecomesObservable() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.blockWrites();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9107}); });
    const auto snoop = transport.waitFor(1)[0].second;
    auto ack_future = std::async(std::launch::async, [&] { return engine.handleSnoopAck(ackFor(snoop, bytes(0xbb))); });
    memory.waitForWrite();
    auto observation = std::async(std::launch::async, [&] { return directory.inspect(kLineA); });
    CHECK(observation.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(future.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    CHECK(ack_future.wait_for(std::chrono::milliseconds(20)) == std::future_status::ready);
    memory.releaseWrites();

    CHECK(ack_future.get() == AckDisposition::Deferred);
    const auto result = ready(future, "blocked dirty write");
    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(memory.line(kLineA) == bytes(0xbb));
    const auto observed = ready(observation, "post-write directory observation");
    CHECK(observed && observed->state == MesiState::M && observed->owner == 2);
}

void testTimeoutDuringDirtyPersistenceReconcilesAcceptedEffect() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.blockWrites();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto dirty = bytes(0xd1);

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9112}); });
    const auto snoop = transport.waitFor(1)[0].second;
    auto ack_future = std::async(std::launch::async, [&] { return engine.handleSnoopAck(ackFor(snoop, dirty)); });
    memory.waitForWrite();
    CHECK(engine.progress(MesiTransactionEngine::TimePoint::max()) == 1);
    memory.releaseWrites();

    CHECK(ready(ack_future, "timeout dirty persistence ACK") == AckDisposition::Deferred);
    const auto result = ready(future, "timeout dirty persistence transaction");
    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    CHECK(memory.line(kLineA) == dirty);
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 2, true);
    checkNoPending(directory, kLineA);
}

void testDisconnectDuringDirtyPersistenceReconcilesAcceptedEffect() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.blockWrites();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    const auto dirty = bytes(0xd2);

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9113}); });
    const auto snoop = transport.waitFor(1)[0].second;
    auto ack_future = std::async(std::launch::async, [&] { return engine.handleSnoopAck(ackFor(snoop, dirty)); });
    memory.waitForWrite();
    CHECK(engine.notifyDisconnect(2, 102) == 1);
    memory.releaseWrites();

    CHECK(ready(ack_future, "disconnect dirty persistence ACK") == AckDisposition::Deferred);
    const auto result = ready(future, "disconnect dirty persistence transaction");
    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    CHECK(memory.line(kLineA) == dirty);
    checkState(directory, kLineA, MesiState::I, std::nullopt, 0, 2, true);
    checkNoPending(directory, kLineA);
}

void testDirtyPersistenceFailureThenTimeoutReleasesLine() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x31));
    memory.setThrowWrite(true);
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, std::chrono::milliseconds(50));
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9114}); });
    const auto snoop = transport.waitFor(1)[0].second;
    CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(0xe1))) == AckDisposition::Deferred);
    memory.waitForWrite();
    const auto result = ready(future, "dirty persistence failure then timeout");

    CHECK(result.status == Status::CoherenceTimeout);
    CHECK(!result.granted);
    CHECK(memory.line(kLineA) == bytes(0x31));
    checkState(directory, kLineA, MesiState::M, 1, 0, 1, false);
    checkNoPending(directory, kLineA);
}

void testDirtyPersistenceFailureThenDisconnectReleasesLine() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x32));
    memory.setThrowWrite(true);
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9115}); });
    const auto snoop = transport.waitFor(1)[0].second;
    CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(0xe2))) == AckDisposition::Deferred);
    memory.waitForWrite();
    CHECK(engine.notifyDisconnect(2, 102) == 1);
    const auto result = ready(future, "dirty persistence failure then disconnect");

    CHECK(result.status == Status::HostFenced);
    CHECK(!result.granted);
    CHECK(memory.line(kLineA) == bytes(0x32));
    checkState(directory, kLineA, MesiState::M, 1, 0, 1, false);
    checkNoPending(directory, kLineA);
}

void testSendFailureWithDeferredDirtyPersistenceReleasesLine() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend memory;
    memory.seed(kLineA, bytes(0x33));
    memory.setThrowWrite(true);
    FakeTransport transport;
    transport.blockSend(1);
    transport.setSendSuccess(false);
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9116}); });
    const auto snoop = transport.waitFor(1)[0].second;
    CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(0xe3))) == AckDisposition::Deferred);
    transport.releaseSend();
    memory.waitForWrite();
    const auto result = ready(future, "send failure with deferred dirty persistence");

    CHECK(result.status == Status::IoError);
    CHECK(!result.granted);
    CHECK(memory.line(kLineA) == bytes(0x33));
    CHECK(memory.writes() == 1);
    checkState(directory, kLineA, MesiState::M, 1, 0, 1, false);
    checkNoPending(directory, kLineA);
}

void testBlockedDirtyPersistenceDoesNotBlockDisjointAckHandling() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    CHECK(directory.gets(kLineB, 3).committed());
    CHECK(directory.gets(kLineB, 4).committed());
    FakeMemoryBackend memory;
    memory.blockWrites();
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}, {3, 103}, {4, 104}});

    auto line_a = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9117}); });
    const auto snoop_a = transport.waitFor(1)[0].second;
    auto ack_a = std::async(std::launch::async, [&] { return engine.handleSnoopAck(ackFor(snoop_a, bytes(0xe4))); });
    memory.waitForWrite();
    CHECK(ack_a.wait_for(std::chrono::milliseconds(20)) == std::future_status::ready);

    auto line_b = std::async(std::launch::async, [&] { return engine.upgrade(kLineB, {3, 103, 9118}); });
    const auto sent = transport.waitFor(2);
    const auto snoop_b = address(sent[0].second) == kLineB ? sent[0].second : sent[1].second;
    CHECK(engine.handleSnoopAck(ackFor(snoop_b)) == AckDisposition::Accepted);
    const auto result_b = ready(line_b, "disjoint ACK while dirty persistence blocked");
    CHECK(result_b.status == Status::Ok);
    CHECK(result_b.granted);
    CHECK(line_a.wait_for(std::chrono::milliseconds(20)) == std::future_status::timeout);
    checkState(directory, kLineB, MesiState::M, 3, 0, 3, false);

    memory.releaseWrites();
    CHECK(ack_a.get() == AckDisposition::Deferred);
    const auto result_a = ready(line_a, "dirty line after disjoint completion");
    CHECK(result_a.status == Status::Ok);
    CHECK(result_a.granted);
    checkState(directory, kLineA, MesiState::M, 2, 0, 2, false);
}

void testTransactionUsesOneDependencySnapshotAcrossReconfigure() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 1).committed());
    FakeMemoryBackend first_memory;
    FakeMemoryBackend second_memory;
    FakeTransport first_transport;
    FakeTransport second_transport;
    first_transport.blockSend(1);
    MesiTransactionEngine engine(directory, first_memory, first_transport, kTestTimeout);
    bind(engine, {{1, 101}, {2, 102}});
    first_transport.onSend([&](std::uint16_t, const CoherenceFrame &snoop) {
        CHECK(engine.handleSnoopAck(ackFor(snoop, bytes(0xcc))) == AckDisposition::Deferred);
    });

    auto future = std::async(std::launch::async, [&] { return engine.getm(kLineA, {2, 102, 9108}); });
    (void)first_transport.waitFor(1);
    engine.configure(second_memory, second_transport, kTestTimeout);
    first_transport.releaseSend();
    const auto result = ready(future, "dependency snapshot reconfigure");

    CHECK(result.status == Status::Ok);
    CHECK(result.granted);
    CHECK(first_memory.line(kLineA) == bytes(0xcc));
    CHECK(second_memory.line(kLineA) == bytes(0x00));
    CHECK(second_transport.sent().empty());
}

void testSnoopIdExhaustionFailsClosedWithoutReuse() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).committed());
    CHECK(directory.gets(kLineA, 2).committed());
    FakeMemoryBackend memory;
    FakeTransport transport;
    MesiTransactionEngine engine(directory, memory, transport, kTestTimeout, std::numeric_limits<std::uint64_t>::max());
    bind(engine, {{1, 101}, {2, 102}, {3, 103}});

    const auto first = engine.getm(kLineA, {3, 103, 9109});
    const auto second = engine.getm(kLineA, {3, 103, 9110});

    CHECK(first.status == Status::IoError);
    CHECK(!first.granted);
    CHECK(second.status == Status::IoError);
    CHECK(!second.granted);
    CHECK(transport.sent().empty());
    checkState(directory, kLineA, MesiState::S, std::nullopt, holder(1) | holder(2), 2, true);
}

} // namespace

int main() {
    testGetsSnoopsModifiedOwnerForDataAndDowngrades();
    testGetmInvalidatesAllSharedHoldersBeforeGrant();
    testGetmTakesDirtyDataBeforeGrantingNewOwner();
    testUpgradeInvalidatesOtherSharersWithoutRefetchingData();
    testDuplicateAndStaleAcksCannotMutateEpoch();
    testSynchronousGrantWaitsForEveryAckEffect();
    testInvalidRequesterRolesAreRejectedBeforeSnoops();
    testTimeoutWithZeroAcksPreservesPriorState();
    testPartialTimeoutCommitsOnlyAcknowledgedInvalidations();
    testRequesterDisconnectAfterSnoopsLeavesLegalStableState();
    testConcurrentDisjointLinesProgressIndependently();
    testCoherencyEngineStrictOperationsUseTransactionEngine();
    testStrictWrapperNeverReportsTimeoutAsSuccess();
    testRequesterDisconnectAfterFinalAckBeforeCommitCannotGrant();
    testUndispatchedSnoopAckCannotCompleteTransaction();
    testRequesterDisconnectDuringSnoopPrefetchFailsAdmission();
    testRequesterDisconnectDuringDirectReadFailsCommit();
    testTimeoutDuringBlockedSendStopsLaterSnoops();
    testNaturalDeadlineDuringBlockedSendStopsLaterSnoops();
    testAcceptedFinalAckBeforeDeadlineSurvivesSuccessfulSlowSendReturn();
    testFinalSynchronousAckCannotHideFalseSendResult();
    testFinalSynchronousAckCannotHideThrownSendFailure();
    testRegisteredDirectDataGrantRequiresConfiguredBackend();
    testConfiguredStrictV2RejectsSessionZeroBeforeBackendAccess();
    testSessionGenerationCannotBeReplacedUntilDisconnect();
    testUnconfiguredStrictWrapperPreservesMetadataOnlyCompatibility();
    testDependencyFailuresReturnIoErrorAndClearPending();
    testDirtyAckPersistenceFailureIsRetryableBeforeAcceptance();
    testConflictingDirtyPayloadReplayIsInvalidAndOriginalCompletes();
    testAcceptedCleanInvalidationsUsePrefetchedDataWithoutRollback();
    testDirtyWriteIsDurableBeforeOwnerChangeBecomesObservable();
    testTimeoutDuringDirtyPersistenceReconcilesAcceptedEffect();
    testDisconnectDuringDirtyPersistenceReconcilesAcceptedEffect();
    testDirtyPersistenceFailureThenTimeoutReleasesLine();
    testDirtyPersistenceFailureThenDisconnectReleasesLine();
    testSendFailureWithDeferredDirtyPersistenceReleasesLine();
    testBlockedDirtyPersistenceDoesNotBlockDisjointAckHandling();
    testTransactionUsesOneDependencySnapshotAcrossReconfigure();
    testSnoopIdExhaustionFailsClosedWithoutReuse();

    const auto failure_count = failures.load(std::memory_order_relaxed);
    if (failure_count != 0) {
        std::cerr << failure_count << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "MESI transaction engine tests passed\n";
    return EXIT_SUCCESS;
}
