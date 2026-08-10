#include "coherence_protocol_v2.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <span>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>

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

bool parseFixtureHex(std::string_view name, std::size_t line_number, std::string_view hex, Bytes &bytes,
                     std::ostream &diagnostics) {
    if (hex.size() != protocol::kFrameSize * 2) {
        diagnostics << "fixture '" << name << "' on line " << line_number << " has " << hex.size()
                    << " hex characters; expected " << protocol::kFrameSize * 2 << '\n';
        return false;
    }

    const auto nibble = [](char value) -> int {
        if (value >= '0' && value <= '9')
            return value - '0';
        if (value >= 'a' && value <= 'f')
            return value - 'a' + 10;
        if (value >= 'A' && value <= 'F')
            return value - 'A' + 10;
        return -1;
    };

    Bytes parsed{};
    for (std::size_t index = 0; index < parsed.size(); ++index) {
        const int high = nibble(hex[index * 2]);
        const int low = nibble(hex[index * 2 + 1]);
        if (high < 0 || low < 0) {
            diagnostics << "fixture '" << name << "' on line " << line_number << " has non-hex character at column "
                        << (index * 2 + (high < 0 ? 1 : 2)) << '\n';
            return false;
        }
        parsed[index] = static_cast<std::uint8_t>((high << 4) | low);
    }
    bytes = parsed;
    return true;
}

std::unordered_map<std::string, Bytes> loadGoldenFrames() {
    std::ifstream input(COHERENCE_PROTOCOL_V2_FIXTURE_PATH);
    CHECK(input.is_open());
    std::unordered_map<std::string, Bytes> fixtures;
    std::string line;
    std::size_t line_number = 0;
    while (std::getline(input, line)) {
        ++line_number;
        const auto separator = line.find('\t');
        if (separator == std::string::npos) {
            std::cerr << "fixture line " << line_number << " has no tab separator\n";
            ++failures;
            continue;
        }
        const auto name = line.substr(0, separator);
        const auto hex = line.substr(separator + 1);
        Bytes bytes{};
        if (!parseFixtureHex(name, line_number, hex, bytes, std::cerr)) {
            ++failures;
            continue;
        }
        if (!fixtures.emplace(name, bytes).second) {
            std::cerr << "duplicate fixture '" << name << "' on line " << line_number << '\n';
            ++failures;
        }
    }
    return fixtures;
}

void expectEncoding(const protocol::CoherenceFrame &frame, const Bytes &golden) {
    const auto encoded = protocol::encodeFrame(frame);
    CHECK(encoded == golden);
    protocol::CoherenceFrame decoded{};
    CHECK(protocol::decodeFrame(encoded, decoded));
    CHECK(protocol::encodeFrame(decoded) == golden);
}

void testFixtureHexParsingRejectsInvalidInput() {
    Bytes bytes{};
    std::ostringstream diagnostics;
    std::string invalid_hex(protocol::kFrameSize * 2, '0');
    invalid_hex[17] = 'g';
    CHECK(!parseFixtureHex("BAD_CHARACTER", 7, invalid_hex, bytes, diagnostics));
    CHECK(diagnostics.str().find("BAD_CHARACTER") != std::string::npos);
    CHECK(diagnostics.str().find("line 7") != std::string::npos);

    diagnostics.str({});
    diagnostics.clear();
    CHECK(!parseFixtureHex("BAD_LENGTH", 11, std::string(protocol::kFrameSize * 2 - 1, '0'), bytes, diagnostics));
    CHECK(diagnostics.str().find("BAD_LENGTH") != std::string::npos);
    CHECK(diagnostics.str().find("line 11") != std::string::npos);
}

void testDecodeRejectsShortFrameWithoutModifyingOutput() {
    auto output = protocol::initializeFrame(protocol::Opcode::Heartbeat);
    protocol::setSrcHost(output, 3);
    protocol::setDstHost(output, protocol::kServerHost);
    protocol::setSessionId(output, 0x1122334455667788ULL);
    protocol::setRequestId(output, 0x0102030405060708ULL);
    protocol::setOldValue(output, 42);
    const auto original = protocol::encodeFrame(output);
    const std::array<std::uint8_t, protocol::kFrameSize - 1> short_frame{};

    CHECK(!protocol::decodeFrame(std::span<const std::uint8_t>(short_frame), output));
    CHECK(protocol::encodeFrame(output) == original);
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
    CHECK(protocol::kKnownCapabilities == 3);
    CHECK(protocol::kSupportedCapabilities == 1);
}

void testGoldenFrames() {
    const auto fixtures = loadGoldenFrames();
    constexpr std::array required_names{"REGISTER", "GETS", "SNP_DATA_INV", "SNOOP_ACK", "HEARTBEAT"};
    for (const auto *name : required_names) {
        if (!fixtures.contains(name)) {
            std::cerr << "required fixture '" << name << "' is missing\n";
            ++failures;
        }
    }
    if (std::any_of(required_names.begin(), required_names.end(),
                    [&fixtures](const auto *name) { return !fixtures.contains(name); }))
        return;

    auto registration = protocol::initializeFrame(protocol::Opcode::Register);
    protocol::setSrcHost(registration, 3);
    protocol::setDstHost(registration, protocol::kServerHost);
    protocol::setSize(registration, 64);
    protocol::setValue(registration, 0x10000);
    protocol::setExpected(registration, 8);
    protocol::setCapabilities(registration, static_cast<std::uint64_t>(protocol::Capability::MODEL_SNOOP));
    expectEncoding(registration, fixtures.find("REGISTER")->second);
    CHECK(protocol::validateFrame(registration));

    auto gets = command(protocol::Opcode::Gets);
    protocol::setAddress(gets, 0x1000);
    expectEncoding(gets, fixtures.find("GETS")->second);
    CHECK(protocol::validateFrame(gets));

    auto data_snoop = snoop(protocol::Opcode::SnpDataInv);
    expectEncoding(data_snoop, fixtures.find("SNP_DATA_INV")->second);
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
    auto acked_snoop = protocol::initializeFrame(protocol::Opcode::SnpInv);
    protocol::setSrcHost(acked_snoop, protocol::kServerHost);
    protocol::setDstHost(acked_snoop, 3);
    protocol::setSessionId(acked_snoop, 0x1122334455667788ULL);
    protocol::setSnoopId(acked_snoop, 99);
    protocol::setAddress(acked_snoop, 0x3000);
    protocol::setEpoch(acked_snoop, 7);
    CHECK(protocol::validateSnoopAck(ack, acked_snoop,
                                     {protocol::AckStrength::MODEL, protocol::LineState::I, protocol::LineState::S}));
    protocol::setPayloadLength(ack, 64);
    for (std::size_t i = 0; i < 64; ++i) {
        ack.data[i] = static_cast<std::uint8_t>(i);
    }
    expectEncoding(ack, fixtures.find("SNOOP_ACK")->second);
    protocol::setOpcode(acked_snoop, protocol::Opcode::SnpDataInv);
    CHECK(protocol::validateSnoopAck(ack, acked_snoop,
                                     {protocol::AckStrength::MODEL, protocol::LineState::I, protocol::LineState::M}));

    auto heartbeat = command(protocol::Opcode::Heartbeat);
    protocol::setOldValue(heartbeat, 42);
    expectEncoding(heartbeat, fixtures.find("HEARTBEAT")->second);
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
    if (result.error != expected) {
        std::cerr << __func__ << ": response error=" << protocol::toString(result.error)
                  << " expected=" << protocol::toString(expected) << '\n';
        ++failures;
    }
}

void expectAckError(const protocol::CoherenceFrame &ack, const protocol::CoherenceFrame &request,
                    protocol::SnoopAckContext context, protocol::ValidationError expected) {
    const auto result = protocol::validateSnoopAck(ack, request, context);
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
    auto resumed = response;
    protocol::setSessionId(resumed, 41);
    CHECK(protocol::validateResponse(resumed, resume));
    bad = response;
    protocol::setSessionId(bad, 42);
    expectResponseError(bad, resume, protocol::ValidationError::InvalidSessionId);

    for (const auto request_session : {0ULL, 41ULL}) {
        auto failed_request = request;
        protocol::setSessionId(failed_request, request_session);
        auto failed = responseFor(failed_request, protocol::LineState::I, 0);
        protocol::setStatus(failed, protocol::Status::NoCapability);
        CHECK(protocol::validateResponse(failed, failed_request));
    }

    auto native_request = request;
    protocol::setCapabilities(native_request, protocol::kKnownCapabilities);
    CHECK(protocol::validateFrame(native_request));
    CHECK(protocol::validateResponse(response, native_request));
    auto unknown_request = native_request;
    protocol::setCapabilities(unknown_request, protocol::kKnownCapabilities | (1ULL << 2));
    expectError(unknown_request, protocol::ValidationError::InvalidCapabilities);
    auto native_response = response;
    protocol::setCapabilities(native_response, protocol::kKnownCapabilities);
    protocol::setAckStrength(native_response, protocol::AckStrength::NATIVE);
    expectResponseError(native_response, native_request, protocol::ValidationError::InvalidCapabilities);
    protocol::setStatus(native_response, protocol::Status::NoCapability);
    protocol::setAckStrength(native_response, protocol::AckStrength::NONE);
    protocol::setSessionId(native_response, 0);
    expectResponseError(native_response, native_request, protocol::ValidationError::UnexpectedState);

    auto stale_register = responseFor(request, protocol::LineState::I, 0);
    protocol::setStatus(stale_register, protocol::Status::StaleRequest);
    expectResponseError(stale_register, request, protocol::ValidationError::InvalidRequestId);
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
                          protocol::Opcode::AtomicFaa, protocol::Opcode::AtomicCas}) {
        auto request = command(op);
        protocol::setAddress(request,
                             op == protocol::Opcode::AtomicFaa || op == protocol::Opcode::AtomicCas ? 0x1038 : 0x1000);
        protocol::setEpoch(request, 9);
        if (op == protocol::Opcode::Upgrade || op == protocol::Opcode::Puts)
            protocol::setLineState(request, protocol::LineState::S);
        else
            protocol::setLineState(request, protocol::LineState::M);
        if (op == protocol::Opcode::Putm)
            fillLine(request);
        if (op == protocol::Opcode::AtomicFaa || op == protocol::Opcode::AtomicCas) {
            protocol::setSize(request, 8);
            protocol::setValue(request, 3);
            if (op == protocol::Opcode::AtomicCas)
                protocol::setExpected(request, 2);
        }
        auto response =
            responseFor(request,
                        op == protocol::Opcode::Puts || op == protocol::Opcode::Putm ? protocol::LineState::I
                                                                                     : protocol::LineState::M,
                        10);
        if (op == protocol::Opcode::AtomicFaa || op == protocol::Opcode::AtomicCas)
            fillLine(response);
        CHECK(protocol::validateResponse(response, request));
        for (const auto invalid_epoch : {9ULL, 8ULL}) {
            auto bad = response;
            protocol::setEpoch(bad, invalid_epoch);
            expectResponseError(bad, request, protocol::ValidationError::UnexpectedEpoch);
        }
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

    for (const auto op : {protocol::Opcode::Upgrade, protocol::Opcode::Puts, protocol::Opcode::Putm,
                          protocol::Opcode::AtomicFaa, protocol::Opcode::AtomicCas}) {
        const bool atomic = op == protocol::Opcode::AtomicFaa || op == protocol::Opcode::AtomicCas;
        auto request = command(op);
        protocol::setAddress(request, atomic ? 0x1038 : 0x1000);
        protocol::setEpoch(request, 9);
        protocol::setLineState(request, op == protocol::Opcode::Putm ? protocol::LineState::M : protocol::LineState::E);
        if (op == protocol::Opcode::Putm)
            fillLine(request);
        if (atomic) {
            protocol::setSize(request, 8);
            protocol::setValue(request, 3);
            if (op == protocol::Opcode::AtomicCas)
                protocol::setExpected(request, 2);
        }
        auto response =
            responseFor(request,
                        op == protocol::Opcode::Puts || op == protocol::Opcode::Putm ? protocol::LineState::I
                                                                                     : protocol::LineState::M,
                        11);
        if (atomic)
            fillLine(response);
        expectResponseError(response, request, protocol::ValidationError::UnexpectedEpoch);
    }

    for (const auto state : {protocol::LineState::I, protocol::LineState::S}) {
        auto request = command(protocol::Opcode::AtomicFaa);
        protocol::setAddress(request, 0x1038);
        protocol::setSize(request, 8);
        protocol::setValue(request, 3);
        protocol::setLineState(request, state);
        protocol::setEpoch(request, state == protocol::LineState::I ? 0 : 9);
        auto response = responseFor(request, protocol::LineState::M, 12);
        fillLine(response);
        CHECK(protocol::validateResponse(response, request));
    }

    for (const auto op : {protocol::Opcode::AtomicFaa, protocol::Opcode::AtomicCas}) {
        auto request = command(op);
        protocol::setAddress(request, 0x1038);
        protocol::setSize(request, 8);
        protocol::setValue(request, 3);
        protocol::setLineState(request, protocol::LineState::M);
        protocol::setEpoch(request, 9);
        if (op == protocol::Opcode::AtomicCas)
            protocol::setExpected(request, 2);
        auto response = responseFor(request, protocol::LineState::M, 11);
        fillLine(response);
        expectResponseError(response, request, protocol::ValidationError::UnexpectedEpoch);
    }

    auto lagging_puts = command(protocol::Opcode::Puts);
    protocol::setAddress(lagging_puts, 0x1000);
    protocol::setLineState(lagging_puts, protocol::LineState::S);
    protocol::setEpoch(lagging_puts, 9);
    CHECK(protocol::validateResponse(responseFor(lagging_puts, protocol::LineState::I, 12), lagging_puts));

    auto lagging_upgrade = command(protocol::Opcode::Upgrade);
    protocol::setAddress(lagging_upgrade, 0x1000);
    protocol::setLineState(lagging_upgrade, protocol::LineState::S);
    protocol::setEpoch(lagging_upgrade, 9);
    CHECK(protocol::validateResponse(responseFor(lagging_upgrade, protocol::LineState::M, 12), lagging_upgrade));

    auto max_epoch_upgrade = command(protocol::Opcode::Upgrade);
    protocol::setAddress(max_epoch_upgrade, 0x1000);
    protocol::setLineState(max_epoch_upgrade, protocol::LineState::E);
    protocol::setEpoch(max_epoch_upgrade, std::numeric_limits<std::uint64_t>::max());
    expectResponseError(responseFor(max_epoch_upgrade, protocol::LineState::M, 0), max_epoch_upgrade,
                        protocol::ValidationError::UnexpectedEpoch);

    for (const auto op : {protocol::Opcode::Fence, protocol::Opcode::Heartbeat, protocol::Opcode::Unregister}) {
        auto request = command(op);
        auto response = responseFor(request, protocol::LineState::I, 1);
        expectResponseError(response, request, protocol::ValidationError::UnexpectedEpoch);
    }

    for (const auto op :
         {protocol::Opcode::Gets, protocol::Opcode::Getm, protocol::Opcode::Upgrade, protocol::Opcode::Puts,
          protocol::Opcode::Putm, protocol::Opcode::AtomicFaa, protocol::Opcode::AtomicCas, protocol::Opcode::Fence,
          protocol::Opcode::Heartbeat, protocol::Opcode::Unregister}) {
        auto request = command(op);
        auto unchanged_state = protocol::LineState::I;
        std::uint64_t unchanged_epoch = 0;
        if (op == protocol::Opcode::Getm) {
            protocol::setAddress(request, 0x1000);
        } else if (op == protocol::Opcode::Upgrade) {
            protocol::setAddress(request, 0x1000);
            unchanged_state = protocol::LineState::E;
            unchanged_epoch = 12;
        } else if (op == protocol::Opcode::Puts) {
            protocol::setAddress(request, 0x1000);
            unchanged_state = protocol::LineState::S;
            unchanged_epoch = 13;
        } else if (op == protocol::Opcode::Putm) {
            protocol::setAddress(request, 0x1000);
            unchanged_state = protocol::LineState::M;
            unchanged_epoch = 14;
            fillLine(request);
        } else if (op == protocol::Opcode::Gets) {
            protocol::setAddress(request, 0x1000);
        } else if (op == protocol::Opcode::AtomicFaa || op == protocol::Opcode::AtomicCas) {
            protocol::setAddress(request, 0x1038);
            protocol::setSize(request, 8);
            protocol::setValue(request, 9);
            if (op == protocol::Opcode::AtomicCas)
                protocol::setExpected(request, 4);
            unchanged_state = protocol::LineState::S;
            unchanged_epoch = 15;
        }
        protocol::setLineState(request, unchanged_state);
        protocol::setEpoch(request, unchanged_epoch);

        for (const auto failure_status : {protocol::Status::StaleSession, protocol::Status::StaleEpoch,
                                          protocol::Status::InvalidState, protocol::Status::CoherenceTimeout}) {
            auto failure = responseFor(request, unchanged_state, unchanged_epoch);
            protocol::setStatus(failure, failure_status);
            CHECK(protocol::validateResponse(failure, request));
            auto bad = failure;
            protocol::setLineState(bad, unchanged_state == protocol::LineState::I ? protocol::LineState::S
                                                                                  : protocol::LineState::I);
            expectResponseError(bad, request, protocol::ValidationError::UnexpectedState);
        }
        auto stale = responseFor(request, unchanged_state, unchanged_epoch);
        protocol::setStatus(stale, protocol::Status::StaleRequest);
        protocol::setOldValue(stale, protocol::requestId(request) + 1);
        CHECK(protocol::validateResponse(stale, request));
        for (const auto invalid_floor :
             std::array<std::uint64_t, 3>{protocol::requestId(request), protocol::requestId(request) - 1, 0}) {
            auto bad = stale;
            protocol::setOldValue(bad, invalid_floor);
            expectResponseError(bad, request, protocol::ValidationError::UnexpectedOldValue);
        }
    }

    auto partial_upgrade = command(protocol::Opcode::Upgrade);
    protocol::setAddress(partial_upgrade, 0x1000);
    protocol::setLineState(partial_upgrade, protocol::LineState::S);
    protocol::setEpoch(partial_upgrade, 3);
    for (const auto failure_status :
         {protocol::Status::CoherenceTimeout, protocol::Status::HostFenced, protocol::Status::IoError}) {
        auto committed = responseFor(partial_upgrade, protocol::LineState::S, 4);
        protocol::setStatus(committed, failure_status);
        CHECK(protocol::validateResponse(committed, partial_upgrade));
    }

    auto invalid_failure = responseFor(partial_upgrade, protocol::LineState::S, 4);
    protocol::setStatus(invalid_failure, protocol::Status::StaleEpoch);
    expectResponseError(invalid_failure, partial_upgrade, protocol::ValidationError::UnexpectedEpoch);
    protocol::setStatus(invalid_failure, protocol::Status::CoherenceTimeout);
    protocol::setLineState(invalid_failure, protocol::LineState::M);
    expectResponseError(invalid_failure, partial_upgrade, protocol::ValidationError::UnexpectedState);

    auto partial_puts = command(protocol::Opcode::Puts);
    protocol::setAddress(partial_puts, 0x1000);
    protocol::setLineState(partial_puts, protocol::LineState::S);
    protocol::setEpoch(partial_puts, 3);
    auto invalid_puts_failure = responseFor(partial_puts, protocol::LineState::I, 4);
    protocol::setStatus(invalid_puts_failure, protocol::Status::CoherenceTimeout);
    expectResponseError(invalid_puts_failure, partial_puts, protocol::ValidationError::UnexpectedState);

    auto request = command(protocol::Opcode::Gets);
    protocol::setAddress(request, 0x1000);

    auto bad = responseFor(request, protocol::LineState::E, 3);
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
        const auto failure_state = op == protocol::Opcode::SnpInv         ? protocol::LineState::S
                                   : op == protocol::Opcode::SnpDowngrade ? protocol::LineState::E
                                                                          : protocol::LineState::M;
        const protocol::SnoopAckContext context{protocol::AckStrength::MODEL, post_state, failure_state};
        CHECK(protocol::validateSnoopAck(ack, request, context));
        CHECK(!protocol::validateFrame(ack));
        auto bad = ack;
        protocol::setLineState(bad, protocol::LineState::M);
        expectAckError(bad, request, context, protocol::ValidationError::UnexpectedState);
        bad = ack;
        protocol::setEpoch(bad, 10);
        expectAckError(bad, request, context, protocol::ValidationError::UnexpectedEpoch);

        for (const auto failure_status :
             {protocol::Status::StaleSession, protocol::Status::StaleEpoch, protocol::Status::InvalidState}) {
            auto failed = ackFor(request, failure_state);
            protocol::setStatus(failed, failure_status);
            CHECK(protocol::validateSnoopAck(failed, request, context));
        }

        for (std::uint16_t raw_status = 1; raw_status <= 11; ++raw_status) {
            const auto failure_status = static_cast<protocol::Status>(raw_status);
            const bool permitted =
                failure_status == protocol::Status::BadProtocol || failure_status == protocol::Status::StaleSession ||
                failure_status == protocol::Status::StaleEpoch || failure_status == protocol::Status::InvalidState ||
                failure_status == protocol::Status::HostFenced || failure_status == protocol::Status::IoError;
            auto failed = ackFor(request, failure_state);
            protocol::setStatus(failed, failure_status);
            if (permitted)
                CHECK(protocol::validateSnoopAck(failed, request, context));
            else
                expectAckError(failed, request, context, protocol::ValidationError::UnexpectedStatus);
        }

        for (const auto illegal_state :
             {protocol::LineState::I, protocol::LineState::S, protocol::LineState::E, protocol::LineState::M}) {
            const bool legal = op == protocol::Opcode::SnpInv
                                   ? illegal_state == protocol::LineState::S || illegal_state == protocol::LineState::E
                               : op == protocol::Opcode::SnpDowngrade ? illegal_state == protocol::LineState::E
                                                                      : illegal_state == protocol::LineState::M;
            if (legal)
                continue;
            auto failed = ackFor(request, illegal_state);
            protocol::setStatus(failed, protocol::Status::InvalidState);
            expectAckError(failed, request, {protocol::AckStrength::MODEL, post_state, illegal_state},
                           protocol::ValidationError::ContextRequired);
        }
    }

    auto inv = snoop(protocol::Opcode::SnpInv);
    auto failed_inv = ackFor(inv, protocol::LineState::E);
    protocol::setStatus(failed_inv, protocol::Status::InvalidState);
    CHECK(protocol::validateSnoopAck(failed_inv, inv,
                                     {protocol::AckStrength::MODEL, protocol::LineState::I, protocol::LineState::E}));

    auto request = snoop(protocol::Opcode::SnpDataInv);
    const protocol::SnoopAckContext context{protocol::AckStrength::MODEL, protocol::LineState::I,
                                            protocol::LineState::M};
    auto failed = ackFor(request, protocol::LineState::M);
    protocol::setStatus(failed, protocol::Status::StaleEpoch);
    CHECK(protocol::validateSnoopAck(failed, request, context));
    auto bad = failed;
    fillLine(bad);
    expectAckError(bad, request, context, protocol::ValidationError::InvalidPayloadLength);
    bad = failed;
    protocol::setAckStrength(bad, protocol::AckStrength::NONE);
    expectAckError(bad, request, context, protocol::ValidationError::UnexpectedAckStrength);
    bad = failed;
    protocol::setAckStrength(bad, protocol::AckStrength::NATIVE);
    expectAckError(bad, request, {protocol::AckStrength::NATIVE, protocol::LineState::I, protocol::LineState::M},
                   protocol::ValidationError::UnexpectedAckStrength);
    bad = failed;
    protocol::setSnoopId(bad, protocol::snoopId(request) + 1);
    expectAckError(bad, request, context, protocol::ValidationError::InvalidSnoopId);
    bad = failed;
    protocol::setSessionId(bad, protocol::sessionId(request) + 1);
    expectAckError(bad, request, context, protocol::ValidationError::InvalidSessionId);
    bad = failed;
    protocol::setAddress(bad, protocol::address(request) + protocol::kLineSize);
    expectAckError(bad, request, context, protocol::ValidationError::UnexpectedAddress);
    bad = failed;
    protocol::setOldValue(bad, 1);
    expectAckError(bad, request, context, protocol::ValidationError::UnexpectedValue);
    auto malformed_snoop = request;
    protocol::setLineState(malformed_snoop, protocol::LineState::S);
    expectAckError(failed, malformed_snoop, context, protocol::ValidationError::ContextRequired);

    auto host_fence = snoop(protocol::Opcode::HostFence);
    protocol::setAddress(host_fence, 0);
    protocol::setEpoch(host_fence, 0);
    const protocol::SnoopAckContext fence_context{protocol::AckStrength::MODEL, protocol::LineState::I,
                                                  protocol::LineState::I};
    auto fence_ack = ackFor(host_fence, protocol::LineState::I);
    CHECK(protocol::validateSnoopAck(fence_ack, host_fence, fence_context));
    for (std::uint16_t raw_status = 1; raw_status <= 11; ++raw_status) {
        const auto failure_status = static_cast<protocol::Status>(raw_status);
        const bool permitted =
            failure_status == protocol::Status::BadProtocol || failure_status == protocol::Status::StaleSession ||
            failure_status == protocol::Status::HostFenced || failure_status == protocol::Status::IoError;
        auto failed_fence = fence_ack;
        protocol::setStatus(failed_fence, failure_status);
        if (permitted)
            CHECK(protocol::validateSnoopAck(failed_fence, host_fence, fence_context));
        else
            expectAckError(failed_fence, host_fence, fence_context, protocol::ValidationError::UnexpectedStatus);
    }
    for (const auto illegal_state : {protocol::LineState::S, protocol::LineState::E, protocol::LineState::M}) {
        auto failed_fence = ackFor(host_fence, illegal_state);
        protocol::setStatus(failed_fence, protocol::Status::HostFenced);
        expectAckError(failed_fence, host_fence, {protocol::AckStrength::MODEL, protocol::LineState::I, illegal_state},
                       protocol::ValidationError::ContextRequired);
    }

    expectAckError(failed, request, {protocol::AckStrength::NONE, protocol::LineState::I, protocol::LineState::M},
                   protocol::ValidationError::UnexpectedAckStrength);
    expectAckError(failed, request, {protocol::AckStrength::MODEL, protocol::LineState::S, protocol::LineState::M},
                   protocol::ValidationError::ContextRequired);
    auto successful = ackFor(request, protocol::LineState::I);
    fillLine(successful);
    expectAckError(successful, request,
                   {protocol::AckStrength::MODEL, protocol::LineState::I, static_cast<protocol::LineState>(4)},
                   protocol::ValidationError::ContextRequired);
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
                   state == protocol::LineState::I);
        check_line(protocol::Opcode::Upgrade, state, state == protocol::LineState::I ? 0 : 2,
                   state == protocol::LineState::S || state == protocol::LineState::E);
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
    testFixtureHexParsingRejectsInvalidInput();
    testDecodeRejectsShortFrameWithoutModifyingOutput();
    testGoldenFrames();
    testValidationAndSemantics();
    testRegisterResponseValidation();
    testContextualResponses();
    testContextualSnoopAcks();
    testRequestStateEpochMatrixAndUnusedFields();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
