#include "coherence_protocol_v2.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
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

using Bytes = protocol::EncodedFrame;

template <typename T> void putLe(Bytes &bytes, std::size_t offset, T value) {
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        bytes[offset + index] = static_cast<std::uint8_t>(value >> (index * 8U));
    }
}

Bytes goldenBase(protocol::Opcode type, std::uint16_t src_host, std::uint16_t dst_host) {
    Bytes bytes{};
    putLe(bytes, 0, protocol::kMagic);
    putLe(bytes, 4, protocol::kProtocolVersion);
    putLe(bytes, 6, static_cast<std::uint16_t>(type));
    putLe(bytes, 16, src_host);
    putLe(bytes, 18, dst_host);
    return bytes;
}

void expectEncoding(const protocol::CoherenceFrame &frame, const Bytes &golden) {
    const auto encoded = protocol::encodeFrame(frame);
    CHECK(encoded == golden);
    protocol::CoherenceFrame decoded{};
    CHECK(protocol::decodeFrame(encoded, decoded));
    CHECK(protocol::encodeFrame(decoded) == golden);
}

protocol::CoherenceFrame command(protocol::Opcode type) {
    auto frame = protocol::initializeFrame(type);
    protocol::setSrcHost(frame, 3);
    protocol::setDstHost(frame, protocol::kServerHost);
    protocol::setSessionId(frame, 0x1122334455667788ULL);
    protocol::setRequestId(frame, 0x0102030405060708ULL);
    return frame;
}

protocol::CoherenceFrame snoop(protocol::Opcode type) {
    auto frame = protocol::initializeFrame(type);
    protocol::setSrcHost(frame, protocol::kServerHost);
    protocol::setDstHost(frame, 3);
    protocol::setSessionId(frame, 0x1122334455667788ULL);
    protocol::setSnoopId(frame, 0xaabbccddeeff0011ULL);
    protocol::setAddress(frame, 0x2040);
    protocol::setEpoch(frame, 9);
    return frame;
}

void testExactAbiAndValues() {
    static_assert(sizeof(protocol::CoherenceFrame) == 192);
    static_assert(offsetof(protocol::CoherenceFrame, magic) == 0);
    static_assert(offsetof(protocol::CoherenceFrame, version) == 4);
    static_assert(offsetof(protocol::CoherenceFrame, type) == 6);
    static_assert(offsetof(protocol::CoherenceFrame, flags) == 8);
    static_assert(offsetof(protocol::CoherenceFrame, status) == 12);
    static_assert(offsetof(protocol::CoherenceFrame, ack_strength) == 14);
    static_assert(offsetof(protocol::CoherenceFrame, state) == 15);
    static_assert(offsetof(protocol::CoherenceFrame, src_host) == 16);
    static_assert(offsetof(protocol::CoherenceFrame, dst_host) == 18);
    static_assert(offsetof(protocol::CoherenceFrame, payload_len) == 20);
    static_assert(offsetof(protocol::CoherenceFrame, reserved0) == 22);
    static_assert(offsetof(protocol::CoherenceFrame, request_id) == 24);
    static_assert(offsetof(protocol::CoherenceFrame, snoop_id) == 32);
    static_assert(offsetof(protocol::CoherenceFrame, session_id) == 40);
    static_assert(offsetof(protocol::CoherenceFrame, addr) == 48);
    static_assert(offsetof(protocol::CoherenceFrame, epoch) == 56);
    static_assert(offsetof(protocol::CoherenceFrame, capabilities) == 64);
    static_assert(offsetof(protocol::CoherenceFrame, expected) == 72);
    static_assert(offsetof(protocol::CoherenceFrame, value) == 80);
    static_assert(offsetof(protocol::CoherenceFrame, old_value) == 88);
    static_assert(offsetof(protocol::CoherenceFrame, size) == 96);
    static_assert(offsetof(protocol::CoherenceFrame, reserved1) == 100);
    static_assert(offsetof(protocol::CoherenceFrame, data) == 104);
    static_assert(offsetof(protocol::CoherenceFrame, reserved) == 168);

    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Register) == 0x0001);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Unregister) == 0x0002);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Gets) == 0x0003);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Getm) == 0x0004);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Upgrade) == 0x0005);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Puts) == 0x0006);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Putm) == 0x0007);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::AtomicFaa) == 0x0008);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::AtomicCas) == 0x0009);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Fence) == 0x000a);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::SnoopAck) == 0x000b);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Heartbeat) == 0x000c);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::Response) == 0x8001);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::SnpInv) == 0x8101);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::SnpDowngrade) == 0x8102);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::SnpDataInv) == 0x8103);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::SnpDataDowngrade) == 0x8104);
    CHECK(static_cast<std::uint16_t>(protocol::Opcode::HostFence) == 0x8105);
    CHECK(static_cast<std::uint16_t>(protocol::Status::Ok) == 0);
    CHECK(static_cast<std::uint16_t>(protocol::Status::BadProtocol) == 1);
    CHECK(static_cast<std::uint16_t>(protocol::Status::ProtocolRequired) == 2);
    CHECK(static_cast<std::uint16_t>(protocol::Status::DuplicateHost) == 3);
    CHECK(static_cast<std::uint16_t>(protocol::Status::StaleSession) == 4);
    CHECK(static_cast<std::uint16_t>(protocol::Status::StaleEpoch) == 5);
    CHECK(static_cast<std::uint16_t>(protocol::Status::StaleRequest) == 6);
    CHECK(static_cast<std::uint16_t>(protocol::Status::InvalidState) == 7);
    CHECK(static_cast<std::uint16_t>(protocol::Status::CoherenceTimeout) == 8);
    CHECK(static_cast<std::uint16_t>(protocol::Status::HostFenced) == 9);
    CHECK(static_cast<std::uint16_t>(protocol::Status::NoCapability) == 10);
    CHECK(static_cast<std::uint16_t>(protocol::Status::IoError) == 11);
}

void testGoldenFrames() {
    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setSrcHost(registration, 3);
    protocol::setDstHost(registration, protocol::kServerHost);
    protocol::setSize(registration, 64);
    protocol::setValue(registration, 0x10000);
    protocol::setExpected(registration, 8);
    protocol::setCapabilities(registration, static_cast<std::uint64_t>(protocol::Capability::MODEL_SNOOP));
    auto register_golden = goldenBase(protocol::Opcode::Register, 3, protocol::kServerHost);
    putLe(register_golden, 64, 1ULL);
    putLe(register_golden, 72, 8ULL);
    putLe(register_golden, 80, 0x10000ULL);
    putLe(register_golden, 96, 64U);
    expectEncoding(registration, register_golden);
    CHECK(protocol::validateFrame(registration));

    auto gets = command(protocol::Opcode::Gets);
    protocol::setAddress(gets, 0x1000);
    auto gets_golden = goldenBase(protocol::Opcode::Gets, 3, protocol::kServerHost);
    putLe(gets_golden, 24, 0x0102030405060708ULL);
    putLe(gets_golden, 40, 0x1122334455667788ULL);
    putLe(gets_golden, 48, 0x1000ULL);
    expectEncoding(gets, gets_golden);
    CHECK(protocol::validateFrame(gets));

    auto data_snoop = snoop(protocol::Opcode::SnpDataInv);
    auto snoop_golden = goldenBase(protocol::Opcode::SnpDataInv, protocol::kServerHost, 3);
    putLe(snoop_golden, 32, 0xaabbccddeeff0011ULL);
    putLe(snoop_golden, 40, 0x1122334455667788ULL);
    putLe(snoop_golden, 48, 0x2040ULL);
    putLe(snoop_golden, 56, 9ULL);
    expectEncoding(data_snoop, snoop_golden);
    CHECK(protocol::validateFrame(data_snoop));

    auto ack = protocol::initializeFrame(protocol::Opcode::SnoopAck);
    protocol::setSrcHost(ack, 3);
    protocol::setDstHost(ack, protocol::kServerHost);
    protocol::setSessionId(ack, 0x1122334455667788ULL);
    protocol::setSnoopId(ack, 99);
    protocol::setAddress(ack, 0x3000);
    protocol::setEpoch(ack, 7);
    protocol::setAckStrength(ack, protocol::AckStrength::MODEL);
    protocol::setLineState(ack, protocol::LineState::I);
    auto ack_golden = goldenBase(protocol::Opcode::SnoopAck, 3, protocol::kServerHost);
    ack_golden[14] = 1;
    putLe(ack_golden, 32, 99ULL);
    putLe(ack_golden, 40, 0x1122334455667788ULL);
    putLe(ack_golden, 48, 0x3000ULL);
    putLe(ack_golden, 56, 7ULL);
    expectEncoding(ack, ack_golden);
    CHECK(protocol::validateFrame(ack));
    protocol::setPayloadLength(ack, 64);
    for (std::size_t i = 0; i < 64; ++i) {
        ack.data[i] = static_cast<std::uint8_t>(i);
        ack_golden[104 + i] = static_cast<std::uint8_t>(i);
    }
    putLe(ack_golden, 20, std::uint16_t{64});
    expectEncoding(ack, ack_golden);
    CHECK(protocol::validateFrame(ack));

    auto heartbeat = command(protocol::Opcode::Heartbeat);
    protocol::setOldValue(heartbeat, 42);
    auto heartbeat_golden = goldenBase(protocol::Opcode::Heartbeat, 3, protocol::kServerHost);
    putLe(heartbeat_golden, 24, 0x0102030405060708ULL);
    putLe(heartbeat_golden, 40, 0x1122334455667788ULL);
    putLe(heartbeat_golden, 88, 42ULL);
    expectEncoding(heartbeat, heartbeat_golden);
    CHECK(protocol::validateFrame(heartbeat));
}

void expectError(protocol::CoherenceFrame frame, protocol::ValidationError expected) {
    const auto result = protocol::validateFrame(frame);
    CHECK(!result);
    CHECK(result.error == expected);
    CHECK(result.status == protocol::Status::BadProtocol);
    CHECK(!protocol::toString(result.error).empty());
}

void testValidationAndSemantics() {
    auto base = command(protocol::Opcode::Gets);
    protocol::setAddress(base, 0x1000);
    auto frame = base;
    protocol::setFlags(frame, 1);
    expectError(frame, protocol::ValidationError::NonzeroFlags);
    frame = base;
    protocol::setReserved0(frame, 1);
    expectError(frame, protocol::ValidationError::NonzeroReserved0);
    frame = base;
    protocol::setReserved1(frame, 1);
    expectError(frame, protocol::ValidationError::NonzeroReserved1);
    frame = base;
    frame.reserved[0] = 1;
    expectError(frame, protocol::ValidationError::NonzeroReserved);
    frame = base;
    frame.data[63] = 1;
    expectError(frame, protocol::ValidationError::NonzeroUnusedData);
    frame = base;
    protocol::setAckStrength(frame, protocol::AckStrength::MODEL);
    expectError(frame, protocol::ValidationError::UnexpectedAckStrength);
    frame = base;
    protocol::setSrcHost(frame, protocol::kServerHost);
    expectError(frame, protocol::ValidationError::InvalidDirection);

    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setSrcHost(registration, 3);
    protocol::setDstHost(registration, protocol::kServerHost);
    protocol::setSize(registration, 64);
    protocol::setValue(registration, 4096);
    protocol::setExpected(registration, 8);
    protocol::setCapabilities(registration, 1);
    CHECK(protocol::validateFrame(registration));
    protocol::setRequestId(registration, 1);
    expectError(registration, protocol::ValidationError::InvalidRequestId);

    auto atomic = command(protocol::Opcode::AtomicFaa);
    protocol::setAddress(atomic, 0x1008);
    protocol::setSize(atomic, 8);
    protocol::setValue(atomic, 5);
    CHECK(protocol::validateFrame(atomic));
    protocol::setAddress(atomic, 0x1004);
    expectError(atomic, protocol::ValidationError::UnalignedAddress);

    auto heartbeat = command(protocol::Opcode::Heartbeat);
    protocol::setEpoch(heartbeat, 1);
    expectError(heartbeat, protocol::ValidationError::UnexpectedEpoch);

    auto data_snoop = snoop(protocol::Opcode::SnpDataDowngrade);
    protocol::setPayloadLength(data_snoop, 64);
    expectError(data_snoop, protocol::ValidationError::InvalidPayloadLength);
    data_snoop = snoop(protocol::Opcode::SnpDataDowngrade);
    protocol::setValue(data_snoop, 1);
    expectError(data_snoop, protocol::ValidationError::UnexpectedValue);

    auto ack = protocol::initializeFrame(protocol::Opcode::SnoopAck);
    protocol::setSrcHost(ack, 3);
    protocol::setDstHost(ack, protocol::kServerHost);
    protocol::setSessionId(ack, 1);
    protocol::setSnoopId(ack, 1);
    protocol::setAddress(ack, 0x1000);
    protocol::setCapabilities(ack, 1);
    expectError(ack, protocol::ValidationError::UnexpectedCapabilities);

    auto fence = command(protocol::Opcode::Fence);
    protocol::setLineState(fence, protocol::LineState::S);
    expectError(fence, protocol::ValidationError::UnexpectedState);

    auto response = protocol::initializeFrame(protocol::Opcode::Response);
    protocol::setSrcHost(response, protocol::kServerHost);
    protocol::setDstHost(response, 3);
    protocol::setSessionId(response, 1);
    protocol::setRequestId(response, 1);
    protocol::setPayloadLength(response, 64);
    for (auto &byte : response.data) {
        byte = 0xaa;
    }
    protocol::setOldValue(response, 17);
    CHECK(protocol::validateFrame(response));

    CHECK(protocol::toString(protocol::Opcode::AtomicFaa) == std::string_view("ATOMIC_FAA"));
    CHECK(protocol::toString(protocol::Status::CoherenceTimeout) == std::string_view("COHERENCE_TIMEOUT"));
    CHECK(protocol::toString(protocol::AckStrength::NATIVE) == std::string_view("NATIVE"));
    CHECK(protocol::toString(protocol::LineState::M) == std::string_view("M"));
}

} // namespace

int main() {
    testExactAbiAndValues();
    testGoldenFrames();
    testValidationAndSemantics();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
