#include "coherence_protocol_v2.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
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
    auto acked_snoop = protocol::initializeFrame(protocol::Opcode::SnpInv);
    protocol::setSrcHost(acked_snoop, protocol::kServerHost);
    protocol::setDstHost(acked_snoop, 3);
    protocol::setSessionId(acked_snoop, 0x1122334455667788ULL);
    protocol::setSnoopId(acked_snoop, 99);
    protocol::setAddress(acked_snoop, 0x3000);
    protocol::setEpoch(acked_snoop, 7);
    CHECK(protocol::validateSnoopAck(ack, acked_snoop, protocol::AckStrength::MODEL));
    protocol::setPayloadLength(ack, 64);
    for (std::size_t i = 0; i < 64; ++i) {
        ack.data[i] = static_cast<std::uint8_t>(i);
        ack_golden[104 + i] = static_cast<std::uint8_t>(i);
    }
    putLe(ack_golden, 20, std::uint16_t{64});
    expectEncoding(ack, ack_golden);
    protocol::setOpcode(acked_snoop, protocol::Opcode::SnpDataInv);
    CHECK(protocol::validateSnoopAck(ack, acked_snoop, protocol::AckStrength::MODEL));

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

void fillLine(protocol::CoherenceFrame &frame) {
    protocol::setPayloadLength(frame, protocol::kLineSize);
    for (std::size_t index = 0; index < frame.data.size(); ++index) {
        frame.data[index] = static_cast<std::uint8_t>(index + 1);
    }
}

void expectResponseError(const protocol::CoherenceFrame &response, const protocol::CoherenceFrame &request,
                         protocol::ValidationError expected) {
    const auto result = protocol::validateResponse(response, request);
    CHECK(!result);
    CHECK(result.error == expected);
}

void expectAckError(const protocol::CoherenceFrame &ack, const protocol::CoherenceFrame &request,
                    protocol::AckStrength strength, protocol::ValidationError expected) {
    const auto result = protocol::validateSnoopAck(ack, request, strength);
    CHECK(!result);
    CHECK(result.error == expected);
}

protocol::CoherenceFrame responseFor(const protocol::CoherenceFrame &request, protocol::LineState state,
                                     std::uint64_t response_epoch) {
    auto response = protocol::initializeFrame(protocol::Opcode::Response);
    protocol::setSrcHost(response, protocol::kServerHost);
    protocol::setDstHost(response, protocol::srcHost(request));
    protocol::setSessionId(response, protocol::sessionId(request));
    protocol::setRequestId(response, protocol::requestId(request));
    protocol::setAddress(response, protocol::address(request));
    protocol::setLineState(response, state);
    protocol::setEpoch(response, response_epoch);
    return response;
}

protocol::CoherenceFrame ackFor(const protocol::CoherenceFrame &request, protocol::LineState state) {
    auto ack = protocol::initializeFrame(protocol::Opcode::SnoopAck);
    protocol::setSrcHost(ack, protocol::dstHost(request));
    protocol::setDstHost(ack, protocol::srcHost(request));
    protocol::setSessionId(ack, protocol::sessionId(request));
    protocol::setSnoopId(ack, protocol::snoopId(request));
    protocol::setAddress(ack, protocol::address(request));
    protocol::setEpoch(ack, protocol::epoch(request));
    protocol::setAckStrength(ack, protocol::AckStrength::MODEL);
    protocol::setLineState(ack, state);
    return ack;
}

void testRegisterResponseValidation() {
    auto request = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setSrcHost(request, 3);
    protocol::setDstHost(request, protocol::kServerHost);
    protocol::setCapabilities(request, 1);
    protocol::setExpected(request, 8);
    protocol::setValue(request, 4096);
    protocol::setSize(request, 64);

    auto response = responseFor(request, protocol::LineState::I, 0);
    protocol::setSessionId(response, 42);
    protocol::setCapabilities(response, 1);
    protocol::setExpected(response, 8);
    protocol::setValue(response, 4096);
    protocol::setOldValue(response, 250);
    protocol::setSize(response, 64);
    protocol::setAckStrength(response, protocol::AckStrength::MODEL);
    CHECK(protocol::validateResponse(response, request));
    CHECK(!protocol::validateFrame(response));

    auto bad = response;
    protocol::setRequestId(bad, 1);
    expectResponseError(bad, request, protocol::ValidationError::InvalidRequestId);
    bad = response;
    protocol::setSessionId(bad, 0);
    expectResponseError(bad, request, protocol::ValidationError::InvalidSessionId);
    bad = response;
    protocol::setExpected(bad, 4);
    expectResponseError(bad, request, protocol::ValidationError::InvalidCacheGeometry);
    bad = response;
    protocol::setCapabilities(bad, 3);
    expectResponseError(bad, request, protocol::ValidationError::InvalidCapabilities);
    bad = response;
    protocol::setAckStrength(bad, protocol::AckStrength::NONE);
    expectResponseError(bad, request, protocol::ValidationError::UnexpectedAckStrength);
    bad = response;
    protocol::setOldValue(bad, 0);
    expectResponseError(bad, request, protocol::ValidationError::UnexpectedOldValue);
    bad = response;
    protocol::setAddress(bad, 64);
    expectResponseError(bad, request, protocol::ValidationError::UnexpectedAddress);

    auto resume = request;
    protocol::setSessionId(resume, 41);
    bad = response;
    protocol::setSessionId(bad, 42);
    expectResponseError(bad, resume, protocol::ValidationError::InvalidSessionId);
}

void testContextualResponses() {
    for (const auto op : {protocol::Opcode::Gets, protocol::Opcode::Getm}) {
        auto request = command(op);
        protocol::setAddress(request, 0x1000);
        auto response =
            responseFor(request, op == protocol::Opcode::Gets ? protocol::LineState::E : protocol::LineState::M, 7);
        fillLine(response);
        CHECK(protocol::validateResponse(response, request));
        auto bad = response;
        protocol::setPayloadLength(bad, 0);
        bad.data.fill(0);
        expectResponseError(bad, request, protocol::ValidationError::InvalidPayloadLength);
    }

    for (const auto op : {protocol::Opcode::AtomicFaa, protocol::Opcode::AtomicCas}) {
        auto request = command(op);
        protocol::setAddress(request, 0x1038);
        protocol::setSize(request, 8);
        protocol::setValue(request, 9);
        if (op == protocol::Opcode::AtomicCas)
            protocol::setExpected(request, 4);
        auto response = responseFor(request, protocol::LineState::M, 8);
        fillLine(response);
        protocol::setOldValue(response, 4);
        CHECK(protocol::validateResponse(response, request));
        auto bad = response;
        protocol::setLineState(bad, protocol::LineState::E);
        expectResponseError(bad, request, protocol::ValidationError::UnexpectedState);
    }

    for (const auto op : {protocol::Opcode::Upgrade, protocol::Opcode::Puts, protocol::Opcode::Putm,
                          protocol::Opcode::Fence, protocol::Opcode::Heartbeat, protocol::Opcode::Unregister}) {
        auto request = command(op);
        if (op == protocol::Opcode::Upgrade || op == protocol::Opcode::Puts || op == protocol::Opcode::Putm) {
            protocol::setAddress(request, 0x1000);
            protocol::setEpoch(request, 2);
            protocol::setLineState(request,
                                   op == protocol::Opcode::Upgrade ? protocol::LineState::E : protocol::LineState::S);
        }
        if (op == protocol::Opcode::Putm) {
            protocol::setLineState(request, protocol::LineState::M);
            fillLine(request);
        }
        auto response = responseFor(
            request, op == protocol::Opcode::Upgrade ? protocol::LineState::M : protocol::LineState::I,
            op == protocol::Opcode::Upgrade || op == protocol::Opcode::Puts || op == protocol::Opcode::Putm ? 3 : 0);
        CHECK(protocol::validateResponse(response, request));
        auto bad = response;
        fillLine(bad);
        expectResponseError(bad, request, protocol::ValidationError::InvalidPayloadLength);
    }

    auto request = command(protocol::Opcode::Gets);
    protocol::setAddress(request, 0x1000);
    auto failure = responseFor(request, protocol::LineState::I, 0);
    protocol::setStatus(failure, protocol::Status::CoherenceTimeout);
    CHECK(protocol::validateResponse(failure, request));
    auto bad = failure;
    protocol::setEpoch(bad, 3);
    expectResponseError(bad, request, protocol::ValidationError::UnexpectedEpoch);
    bad = failure;
    protocol::setOldValue(bad, 2);
    expectResponseError(bad, request, protocol::ValidationError::UnexpectedOldValue);
    protocol::setStatus(bad, protocol::Status::StaleRequest);
    CHECK(protocol::validateResponse(bad, request));

    bad = responseFor(request, protocol::LineState::E, 3);
    fillLine(bad);
    protocol::setDstHost(bad, 4);
    expectResponseError(bad, request, protocol::ValidationError::InvalidDestinationHost);
    auto malformed_request = request;
    protocol::setFlags(malformed_request, 1);
    auto valid_response = responseFor(request, protocol::LineState::E, 3);
    fillLine(valid_response);
    expectResponseError(valid_response, malformed_request, protocol::ValidationError::ContextRequired);
}

void testContextualSnoopAcks() {
    for (const auto op : {protocol::Opcode::SnpInv, protocol::Opcode::SnpDowngrade, protocol::Opcode::SnpDataInv,
                          protocol::Opcode::SnpDataDowngrade}) {
        auto request = snoop(op);
        const auto post_state = op == protocol::Opcode::SnpDowngrade || op == protocol::Opcode::SnpDataDowngrade
                                    ? protocol::LineState::S
                                    : protocol::LineState::I;
        auto ack = ackFor(request, post_state);
        if (op == protocol::Opcode::SnpDataInv || op == protocol::Opcode::SnpDataDowngrade)
            fillLine(ack);
        CHECK(protocol::validateSnoopAck(ack, request, protocol::AckStrength::MODEL));
        CHECK(!protocol::validateFrame(ack));
        auto bad = ack;
        protocol::setLineState(bad, protocol::LineState::M);
        expectAckError(bad, request, protocol::AckStrength::MODEL, protocol::ValidationError::UnexpectedState);
        bad = ack;
        protocol::setEpoch(bad, 10);
        expectAckError(bad, request, protocol::AckStrength::MODEL, protocol::ValidationError::UnexpectedEpoch);
    }

    auto request = snoop(protocol::Opcode::SnpDataInv);
    auto failed = ackFor(request, protocol::LineState::I);
    protocol::setStatus(failed, protocol::Status::StaleEpoch);
    CHECK(protocol::validateSnoopAck(failed, request, protocol::AckStrength::MODEL));
    auto bad = failed;
    fillLine(bad);
    expectAckError(bad, request, protocol::AckStrength::MODEL, protocol::ValidationError::InvalidPayloadLength);
    bad = failed;
    protocol::setAckStrength(bad, protocol::AckStrength::NONE);
    expectAckError(bad, request, protocol::AckStrength::MODEL, protocol::ValidationError::UnexpectedAckStrength);
    auto malformed_snoop = request;
    protocol::setLineState(malformed_snoop, protocol::LineState::S);
    expectAckError(failed, malformed_snoop, protocol::AckStrength::MODEL, protocol::ValidationError::ContextRequired);
}

void testRequestStateEpochMatrixAndUnusedFields() {
    auto check_line = [](protocol::Opcode op, protocol::LineState state, std::uint64_t request_epoch, bool valid) {
        auto frame = command(op);
        protocol::setAddress(frame, 0x1000);
        protocol::setLineState(frame, state);
        protocol::setEpoch(frame, request_epoch);
        if (op == protocol::Opcode::Putm)
            fillLine(frame);
        CHECK(static_cast<bool>(protocol::validateFrame(frame)) == valid);
    };
    for (const auto state :
         {protocol::LineState::I, protocol::LineState::S, protocol::LineState::E, protocol::LineState::M}) {
        check_line(protocol::Opcode::Gets, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::I);
        check_line(protocol::Opcode::Getm, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::I || state == protocol::LineState::S);
        check_line(protocol::Opcode::Upgrade, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::E);
        check_line(protocol::Opcode::Puts, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::S || state == protocol::LineState::E);
        check_line(protocol::Opcode::Putm, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::M);
    }

    for (const auto state :
         {protocol::LineState::I, protocol::LineState::S, protocol::LineState::E, protocol::LineState::M}) {
        auto atomic = command(protocol::Opcode::AtomicFaa);
        protocol::setAddress(atomic, 0x1038);
        protocol::setSize(atomic, 8);
        protocol::setValue(atomic, 1);
        protocol::setLineState(atomic, state);
        protocol::setEpoch(atomic, state == protocol::LineState::I ? 0 : 4);
        CHECK(protocol::validateFrame(atomic));
    }
    auto atomic = command(protocol::Opcode::AtomicFaa);
    protocol::setAddress(atomic, 0x103c);
    protocol::setSize(atomic, 8);
    CHECK(!protocol::validateFrame(atomic));

    auto line_snoop = snoop(protocol::Opcode::SnpInv);
    protocol::setLineState(line_snoop, protocol::LineState::S);
    CHECK(!protocol::validateFrame(line_snoop));
    protocol::setLineState(line_snoop, protocol::LineState::I);
    protocol::setEpoch(line_snoop, 0);
    CHECK(!protocol::validateFrame(line_snoop));

    auto host_fence = snoop(protocol::Opcode::HostFence);
    protocol::setAddress(host_fence, 0);
    protocol::setEpoch(host_fence, 0);
    CHECK(protocol::validateFrame(host_fence));
    protocol::setEpoch(host_fence, 1);
    CHECK(!protocol::validateFrame(host_fence));

    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setSrcHost(registration, 3);
    protocol::setDstHost(registration, protocol::kServerHost);
    protocol::setSize(registration, 64);
    protocol::setValue(registration, std::numeric_limits<std::uint64_t>::max());
    protocol::setExpected(registration, std::numeric_limits<std::uint64_t>::max() / 64 + 1);
    protocol::setCapabilities(registration, 1);
    CHECK(!protocol::validateFrame(registration));
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
    expectError(ack, protocol::ValidationError::ContextRequired);

    auto fence = command(protocol::Opcode::Fence);
    protocol::setLineState(fence, protocol::LineState::S);
    expectError(fence, protocol::ValidationError::UnexpectedState);

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
    testRegisterResponseValidation();
    testContextualResponses();
    testContextualSnoopAcks();
    testRequestStateEpochMatrixAndUnusedFields();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
