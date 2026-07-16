#include "coherence_protocol_v2.h"

#include <array>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string_view>

namespace protocol = cxlmemsim::protocol_v2;

namespace {

int failures = 0;

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __FILE__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            ++failures;                                                                                                \
        }                                                                                                              \
    } while (false)

template <typename T> void putLe(std::array<std::uint8_t, protocol::kFrameSize> &bytes, std::size_t offset, T value) {
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        bytes[offset + index] = static_cast<std::uint8_t>(value >> (index * 8U));
    }
}

std::array<std::uint8_t, protocol::kFrameSize> goldenBase(protocol::Opcode opcode, std::uint16_t host_id) {
    std::array<std::uint8_t, protocol::kFrameSize> bytes{};
    bytes[0] = 'C';
    bytes[1] = 'X';
    bytes[2] = 'V';
    bytes[3] = '2';
    putLe(bytes, 4, std::uint16_t{2});
    putLe(bytes, 6, static_cast<std::uint16_t>(opcode));
    putLe(bytes, 12, host_id);
    return bytes;
}

void checkGolden(const protocol::CoherenceFrame &frame,
                 const std::array<std::uint8_t, protocol::kFrameSize> &expected) {
    CHECK(std::memcmp(&frame, expected.data(), expected.size()) == 0);
}

protocol::CoherenceFrame identified(protocol::Opcode opcode) {
    auto frame = protocol::initializeFrame(opcode);
    protocol::setHostId(frame, 3);
    protocol::setSessionId(frame, 0x1122334455667788ULL);
    protocol::setRequestId(frame, 0x0102030405060708ULL);
    return frame;
}

void testAbiAndAccessors() {
    static_assert(sizeof(protocol::CoherenceFrame) == 192);
    static_assert(offsetof(protocol::CoherenceFrame, version_le) == 4);
    static_assert(offsetof(protocol::CoherenceFrame, capabilities_le) == 64);
    static_assert(offsetof(protocol::CoherenceFrame, payload) == 80);
    static_assert(offsetof(protocol::CoherenceFrame, reserved) == 144);

    auto frame = identified(protocol::Opcode::Gets);
    protocol::setFlags(frame, 0x01020304U);
    protocol::setAddress(frame, 0x8899aabbccddeeffULL);
    CHECK(protocol::opcode(frame) == protocol::Opcode::Gets);
    CHECK(protocol::flags(frame) == 0x01020304U);
    CHECK(protocol::address(frame) == 0x8899aabbccddeeffULL);
    const auto *bytes = reinterpret_cast<const std::uint8_t *>(&frame);
    CHECK(bytes[8] == 0x04 && bytes[11] == 0x01);
    CHECK(bytes[40] == 0xff && bytes[47] == 0x88);
}

void testGoldenFrames() {
    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setRequestId(registration, 0x0102030405060708ULL);
    protocol::setCapabilities(registration, 3);
    protocol::setCacheCapacity(registration, 0x00010000U);
    protocol::setCacheWays(registration, 8);
    auto register_golden = goldenBase(protocol::Opcode::Register, protocol::kNoOwner);
    putLe(register_golden, 24, 0x0102030405060708ULL);
    putLe(register_golden, 64, 3ULL);
    putLe(register_golden, 72, 0x00010000U);
    putLe(register_golden, 76, std::uint16_t{8});
    checkGolden(registration, register_golden);
    CHECK(protocol::validateFrame(registration));

    auto gets = identified(protocol::Opcode::Gets);
    protocol::setAddress(gets, 0x1000);
    protocol::setEpoch(gets, 9);
    auto gets_golden = goldenBase(protocol::Opcode::Gets, 3);
    putLe(gets_golden, 16, 0x1122334455667788ULL);
    putLe(gets_golden, 24, 0x0102030405060708ULL);
    putLe(gets_golden, 40, 0x1000ULL);
    putLe(gets_golden, 48, 9ULL);
    checkGolden(gets, gets_golden);
    CHECK(protocol::validateFrame(gets));

    auto snoop = identified(protocol::Opcode::SnpDataInv);
    protocol::setSnoopId(snoop, 0xaabbccddeeff0011ULL);
    protocol::setAddress(snoop, 0x2040);
    protocol::setPayloadLength(snoop, 64);
    for (std::size_t i = 0; i < 64; ++i) {
        snoop.payload[i] = static_cast<std::uint8_t>(i);
    }
    auto snoop_golden = goldenBase(protocol::Opcode::SnpDataInv, 3);
    putLe(snoop_golden, 16, 0x1122334455667788ULL);
    putLe(snoop_golden, 24, 0x0102030405060708ULL);
    putLe(snoop_golden, 32, 0xaabbccddeeff0011ULL);
    putLe(snoop_golden, 40, 0x2040ULL);
    putLe(snoop_golden, 78, std::uint16_t{64});
    for (std::size_t i = 0; i < 64; ++i) {
        snoop_golden[80 + i] = static_cast<std::uint8_t>(i);
    }
    checkGolden(snoop, snoop_golden);
    CHECK(protocol::validateFrame(snoop));

    auto ack = identified(protocol::Opcode::SnoopAck);
    protocol::setSnoopId(ack, 99);
    protocol::setAddress(ack, 0x3000);
    protocol::setCapabilities(ack, static_cast<std::uint64_t>(protocol::Capability::NATIVE_FLUSH));
    protocol::setAckStrength(ack, protocol::AckStrength::NATIVE);
    protocol::setLineState(ack, protocol::LineState::M);
    auto ack_golden = goldenBase(protocol::Opcode::SnoopAck, 3);
    putLe(ack_golden, 16, 0x1122334455667788ULL);
    putLe(ack_golden, 24, 0x0102030405060708ULL);
    putLe(ack_golden, 32, 99ULL);
    putLe(ack_golden, 40, 0x3000ULL);
    putLe(ack_golden, 64, 2ULL);
    putLe(ack_golden, 78, std::uint16_t{2});
    ack_golden[80] = 2;
    ack_golden[81] = 3;
    checkGolden(ack, ack_golden);
    CHECK(protocol::validateFrame(ack));

    auto heartbeat = identified(protocol::Opcode::Heartbeat);
    protocol::setEpoch(heartbeat, 7);
    protocol::setResponseWatermark(heartbeat, 42);
    auto heartbeat_golden = goldenBase(protocol::Opcode::Heartbeat, 3);
    putLe(heartbeat_golden, 16, 0x1122334455667788ULL);
    putLe(heartbeat_golden, 24, 0x0102030405060708ULL);
    putLe(heartbeat_golden, 48, 7ULL);
    putLe(heartbeat_golden, 56, 42ULL);
    checkGolden(heartbeat, heartbeat_golden);
    CHECK(protocol::validateFrame(heartbeat));
}

void expectError(protocol::CoherenceFrame frame, protocol::ValidationError expected) {
    const auto result = protocol::validateFrame(frame);
    CHECK(!result);
    CHECK(result.error == expected);
    CHECK(!protocol::toString(result.error).empty());
}

void testValidationErrors() {
    auto base = identified(protocol::Opcode::Gets);
    protocol::setAddress(base, 0x1000);

    auto frame = base;
    frame.magic[0] = 'B';
    expectError(frame, protocol::ValidationError::BadMagic);
    frame = base;
    frame.version_le = 3;
    expectError(frame, protocol::ValidationError::BadVersion);
    frame = base;
    protocol::setOpcode(frame, static_cast<protocol::Opcode>(999));
    expectError(frame, protocol::ValidationError::UnknownOpcode);
    frame = base;
    protocol::setFlags(frame, 0x80000000U);
    expectError(frame, protocol::ValidationError::UnknownFlags);
    frame = base;
    protocol::setHostId(frame, 64);
    expectError(frame, protocol::ValidationError::InvalidHost);
    frame = base;
    protocol::setPayloadLength(frame, 65);
    expectError(frame, protocol::ValidationError::InvalidPayloadLength);
    frame = base;
    protocol::setPayloadLength(frame, 1);
    expectError(frame, protocol::ValidationError::InvalidPayloadLength);
    frame = base;
    protocol::setAddress(frame, 0x1001);
    expectError(frame, protocol::ValidationError::UnalignedAddress);
    frame = base;
    frame.reserved[47] = 1;
    expectError(frame, protocol::ValidationError::NonzeroReserved);
    frame = base;
    protocol::setSessionId(frame, 0);
    expectError(frame, protocol::ValidationError::InvalidIdentity);
    frame = base;
    protocol::setStatus(frame, protocol::Status::Busy);
    expectError(frame, protocol::ValidationError::InvalidStatus);

    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setRequestId(registration, 1);
    protocol::setCapabilities(registration, 1);
    protocol::setCacheCapacity(registration, 1000);
    protocol::setCacheWays(registration, 8);
    expectError(registration, protocol::ValidationError::InvalidCacheGeometry);
    protocol::setCacheCapacity(registration, 4096);
    protocol::setCapabilities(registration, 0);
    expectError(registration, protocol::ValidationError::InvalidCapabilities);

    auto ack = identified(protocol::Opcode::SnoopAck);
    protocol::setSnoopId(ack, 1);
    protocol::setAddress(ack, 0x1000);
    protocol::setPayloadLength(ack, 2);
    ack.payload[0] = 3;
    expectError(ack, protocol::ValidationError::InvalidAckStrength);
    ack.payload[0] = 1;
    ack.payload[1] = 4;
    expectError(ack, protocol::ValidationError::InvalidLineState);

    CHECK(protocol::toString(protocol::Opcode::Getm) == std::string_view("GETM"));
    CHECK(protocol::toString(protocol::Status::Busy) == std::string_view("BUSY"));
}

} // namespace

int main() {
    testAbiAndAccessors();
    testGoldenFrames();
    testValidationErrors();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
