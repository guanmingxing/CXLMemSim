#include "coherence_protocol_v2.h"

#include <algorithm>
#include <array>

namespace cxlmemsim::protocol_v2 {
namespace {

template <typename T> void putLe(EncodedFrame &bytes, std::size_t offset, T value) noexcept {
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        bytes[offset + index] = static_cast<std::uint8_t>(value >> (index * 8U));
    }
}

template <typename T> T getLe(std::span<const std::uint8_t> bytes, std::size_t offset) noexcept {
    T value = 0;
    for (std::size_t index = 0; index < sizeof(T); ++index) {
        value |= static_cast<T>(bytes[offset + index]) << (index * 8U);
    }
    return value;
}

bool knownOpcode(Opcode op) noexcept {
    switch (op) {
    case Opcode::Register:
    case Opcode::Unregister:
    case Opcode::Gets:
    case Opcode::Getm:
    case Opcode::Upgrade:
    case Opcode::Puts:
    case Opcode::Putm:
    case Opcode::AtomicFaa:
    case Opcode::AtomicCas:
    case Opcode::Fence:
    case Opcode::SnoopAck:
    case Opcode::Heartbeat:
    case Opcode::Response:
    case Opcode::SnpInv:
    case Opcode::SnpDowngrade:
    case Opcode::SnpDataInv:
    case Opcode::SnpDataDowngrade:
    case Opcode::HostFence:
        return true;
    }
    return false;
}

bool isSnoop(Opcode op) noexcept {
    return op == Opcode::SnpInv || op == Opcode::SnpDowngrade || op == Opcode::SnpDataInv ||
           op == Opcode::SnpDataDowngrade || op == Opcode::HostFence;
}

bool isAtomic(Opcode op) noexcept { return op == Opcode::AtomicFaa || op == Opcode::AtomicCas; }
bool isLineCommand(Opcode op) noexcept {
    return op == Opcode::Gets || op == Opcode::Getm || op == Opcode::Upgrade || op == Opcode::Puts ||
           op == Opcode::Putm;
}
ValidationResult bad(ValidationError error) noexcept { return {error, Status::BadProtocol}; }
bool endpoint(std::uint16_t host) noexcept { return host < kMaximumHosts; }

bool permittedSnoopFailure(Status failure, Opcode snoop_op) noexcept {
    if (failure == Status::BadProtocol || failure == Status::StaleSession || failure == Status::HostFenced ||
        failure == Status::IoError)
        return true;
    return snoop_op != Opcode::HostFence && (failure == Status::StaleEpoch || failure == Status::InvalidState);
}

bool zeroScalars(const CoherenceFrame &frame) noexcept {
    return capabilities(frame) == 0 && expected(frame) == 0 && value(frame) == 0 && oldValue(frame) == 0 &&
           size(frame) == 0;
}

ValidationResult validateEnvelope(const CoherenceFrame &frame) noexcept {
    if (magic(frame) != kMagic)
        return bad(ValidationError::BadMagic);
    if (version(frame) != kProtocolVersion)
        return bad(ValidationError::BadVersion);
    const auto op = opcode(frame);
    if (!knownOpcode(op))
        return bad(ValidationError::UnknownOpcode);
    if (flags(frame) != 0)
        return bad(ValidationError::NonzeroFlags);
    if (static_cast<std::uint16_t>(status(frame)) > 11)
        return bad(ValidationError::InvalidStatus);
    if (static_cast<std::uint8_t>(ackStrength(frame)) > 2)
        return bad(ValidationError::InvalidAckStrength);
    if (static_cast<std::uint8_t>(lineState(frame)) > 3)
        return bad(ValidationError::InvalidLineState);
    if (reserved0(frame) != 0)
        return bad(ValidationError::NonzeroReserved0);
    if (reserved1(frame) != 0)
        return bad(ValidationError::NonzeroReserved1);
    if (std::any_of(frame.reserved.begin(), frame.reserved.end(), [](auto byte) { return byte != 0; }))
        return bad(ValidationError::NonzeroReserved);
    if (payloadLength(frame) > kLineSize)
        return bad(ValidationError::InvalidPayloadLength);
    if (std::any_of(frame.data.begin() + payloadLength(frame), frame.data.end(), [](auto byte) { return byte != 0; }))
        return bad(ValidationError::NonzeroUnusedData);
    if (srcHost(frame) != kServerHost && !endpoint(srcHost(frame)))
        return bad(ValidationError::InvalidSourceHost);
    if (dstHost(frame) != kServerHost && !endpoint(dstHost(frame)))
        return bad(ValidationError::InvalidDestinationHost);
    const bool server_to_endpoint = op == Opcode::Response || isSnoop(op);
    if (server_to_endpoint ? (srcHost(frame) != kServerHost || !endpoint(dstHost(frame)))
                           : (!endpoint(srcHost(frame)) || dstHost(frame) != kServerHost))
        return bad(ValidationError::InvalidDirection);
    return {ValidationError::None, Status::Ok};
}

bool validRequestState(Opcode op, LineState state, std::uint64_t request_epoch) noexcept {
    if (op == Opcode::Gets)
        return state == LineState::I && request_epoch == 0;
    if (op == Opcode::Getm)
        return state == LineState::I && request_epoch == 0;
    if (op == Opcode::Upgrade)
        return (state == LineState::S || state == LineState::E) && request_epoch != 0;
    if (op == Opcode::Puts)
        return (state == LineState::S || state == LineState::E) && request_epoch != 0;
    if (op == Opcode::Putm)
        return state == LineState::M && request_epoch != 0;
    if (isAtomic(op))
        return (state == LineState::I && request_epoch == 0) || (state != LineState::I && request_epoch != 0);
    return true;
}

} // namespace

EncodedFrame encodeFrame(const CoherenceFrame &frame) noexcept {
    EncodedFrame bytes{};
#define PUT(member, offset) putLe(bytes, offset, frame.member)
    PUT(magic, 0);
    PUT(version, 4);
    PUT(type, 6);
    PUT(flags, 8);
    PUT(status, 12);
    bytes[14] = frame.ack_strength;
    bytes[15] = frame.state;
    PUT(src_host, 16);
    PUT(dst_host, 18);
    PUT(payload_len, 20);
    PUT(reserved0, 22);
    PUT(request_id, 24);
    PUT(snoop_id, 32);
    PUT(session_id, 40);
    PUT(addr, 48);
    PUT(epoch, 56);
    PUT(capabilities, 64);
    PUT(expected, 72);
    PUT(value, 80);
    PUT(old_value, 88);
    PUT(size, 96);
    PUT(reserved1, 100);
#undef PUT
    std::copy(frame.data.begin(), frame.data.end(), bytes.begin() + 104);
    std::copy(frame.reserved.begin(), frame.reserved.end(), bytes.begin() + 168);
    return bytes;
}

bool decodeFrame(std::span<const std::uint8_t> bytes, CoherenceFrame &frame) noexcept {
    if (bytes.size() != kFrameSize)
        return false;

    CoherenceFrame decoded{};
#define GET(member, offset, type) decoded.member = getLe<type>(bytes, offset)
    GET(magic, 0, std::uint32_t);
    GET(version, 4, std::uint16_t);
    GET(type, 6, std::uint16_t);
    GET(flags, 8, std::uint32_t);
    GET(status, 12, std::uint16_t);
    decoded.ack_strength = bytes[14];
    decoded.state = bytes[15];
    GET(src_host, 16, std::uint16_t);
    GET(dst_host, 18, std::uint16_t);
    GET(payload_len, 20, std::uint16_t);
    GET(reserved0, 22, std::uint16_t);
    GET(request_id, 24, std::uint64_t);
    GET(snoop_id, 32, std::uint64_t);
    GET(session_id, 40, std::uint64_t);
    GET(addr, 48, std::uint64_t);
    GET(epoch, 56, std::uint64_t);
    GET(capabilities, 64, std::uint64_t);
    GET(expected, 72, std::uint64_t);
    GET(value, 80, std::uint64_t);
    GET(old_value, 88, std::uint64_t);
    GET(size, 96, std::uint32_t);
    GET(reserved1, 100, std::uint32_t);
#undef GET
    std::copy_n(bytes.begin() + 104, kLineSize, decoded.data.begin());
    std::copy_n(bytes.begin() + 168, decoded.reserved.size(), decoded.reserved.begin());
    frame = decoded;
    return true;
}

CoherenceFrame initializeFrame(Opcode type) noexcept {
    CoherenceFrame frame{};
    frame.magic = kMagic;
    frame.version = kProtocolVersion;
    setOpcode(frame, type);
    return frame;
}

#define ACCESSOR(type, name, setter, member)                                                                           \
    type name(const CoherenceFrame &frame) noexcept { return static_cast<type>(frame.member); }                        \
    void setter(CoherenceFrame &frame, type value) noexcept {                                                          \
        frame.member = static_cast<decltype(frame.member)>(value);                                                     \
    }
ACCESSOR(std::uint32_t, magic, setMagic, magic)
ACCESSOR(std::uint16_t, version, setVersion, version)
ACCESSOR(Opcode, opcode, setOpcode, type)
ACCESSOR(std::uint32_t, flags, setFlags, flags)
ACCESSOR(Status, status, setStatus, status)
ACCESSOR(AckStrength, ackStrength, setAckStrength, ack_strength)
ACCESSOR(LineState, lineState, setLineState, state)
ACCESSOR(std::uint16_t, srcHost, setSrcHost, src_host)
ACCESSOR(std::uint16_t, dstHost, setDstHost, dst_host)
ACCESSOR(std::uint16_t, payloadLength, setPayloadLength, payload_len)
ACCESSOR(std::uint16_t, reserved0, setReserved0, reserved0)
ACCESSOR(std::uint64_t, requestId, setRequestId, request_id)
ACCESSOR(std::uint64_t, snoopId, setSnoopId, snoop_id)
ACCESSOR(std::uint64_t, sessionId, setSessionId, session_id)
ACCESSOR(std::uint64_t, address, setAddress, addr)
ACCESSOR(std::uint64_t, epoch, setEpoch, epoch)
ACCESSOR(std::uint64_t, capabilities, setCapabilities, capabilities)
ACCESSOR(std::uint64_t, expected, setExpected, expected)
ACCESSOR(std::uint64_t, value, setValue, value)
ACCESSOR(std::uint64_t, oldValue, setOldValue, old_value)
ACCESSOR(std::uint32_t, size, setSize, size)
ACCESSOR(std::uint32_t, reserved1, setReserved1, reserved1)
#undef ACCESSOR

ValidationResult validateFrame(const CoherenceFrame &frame) noexcept {
    if (const auto result = validateEnvelope(frame); !result)
        return result;
    const auto op = opcode(frame);
    if (op == Opcode::Register) {
        if (requestId(frame) != 0)
            return bad(ValidationError::InvalidRequestId);
        if (snoopId(frame) != 0)
            return bad(ValidationError::InvalidSnoopId);
        if (status(frame) != Status::Ok)
            return bad(ValidationError::UnexpectedStatus);
        if (ackStrength(frame) != AckStrength::NONE)
            return bad(ValidationError::UnexpectedAckStrength);
        if (lineState(frame) != LineState::I)
            return bad(ValidationError::UnexpectedState);
        if (address(frame) != 0)
            return bad(ValidationError::UnexpectedAddress);
        if (epoch(frame) != 0)
            return bad(ValidationError::UnexpectedEpoch);
        if (payloadLength(frame) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (size(frame) != kLineSize || value(frame) == 0 || expected(frame) == 0 || value(frame) % kLineSize != 0 ||
            (value(frame) / kLineSize) % expected(frame) != 0)
            return bad(ValidationError::InvalidCacheGeometry);
        if ((capabilities(frame) & ~kKnownCapabilities) != 0 ||
            (capabilities(frame) & static_cast<std::uint64_t>(Capability::MODEL_SNOOP)) == 0)
            return bad(ValidationError::InvalidCapabilities);
        if (oldValue(frame) != 0)
            return bad(ValidationError::UnexpectedOldValue);
        return {ValidationError::None, Status::Ok};
    }
    if (sessionId(frame) == 0)
        return bad(ValidationError::InvalidSessionId);
    if (op == Opcode::Response || op == Opcode::SnoopAck)
        return bad(ValidationError::ContextRequired);
    if (isSnoop(op)) {
        if (requestId(frame) != 0)
            return bad(ValidationError::InvalidRequestId);
        if (snoopId(frame) == 0)
            return bad(ValidationError::InvalidSnoopId);
        if (status(frame) != Status::Ok)
            return bad(ValidationError::UnexpectedStatus);
        if (payloadLength(frame) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (ackStrength(frame) != AckStrength::NONE)
            return bad(ValidationError::UnexpectedAckStrength);
        if (!zeroScalars(frame))
            return bad(ValidationError::UnexpectedValue);
        if (lineState(frame) != LineState::I)
            return bad(ValidationError::UnexpectedState);
        if (op == Opcode::HostFence) {
            if (address(frame) != 0)
                return bad(ValidationError::UnexpectedAddress);
            if (epoch(frame) != 0)
                return bad(ValidationError::UnexpectedEpoch);
        } else {
            if (address(frame) % kLineSize != 0)
                return bad(ValidationError::UnalignedAddress);
            if (epoch(frame) == 0)
                return bad(ValidationError::UnexpectedEpoch);
        }
        return {ValidationError::None, Status::Ok};
    }
    if (requestId(frame) == 0)
        return bad(ValidationError::InvalidRequestId);
    if (snoopId(frame) != 0)
        return bad(ValidationError::InvalidSnoopId);
    if (status(frame) != Status::Ok)
        return bad(ValidationError::UnexpectedStatus);
    if (ackStrength(frame) != AckStrength::NONE)
        return bad(ValidationError::UnexpectedAckStrength);
    if (op == Opcode::Heartbeat) {
        if (address(frame) != 0)
            return bad(ValidationError::UnexpectedAddress);
        if (epoch(frame) != 0)
            return bad(ValidationError::UnexpectedEpoch);
        if (lineState(frame) != LineState::I)
            return bad(ValidationError::UnexpectedState);
        if (payloadLength(frame) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (capabilities(frame) || expected(frame) || value(frame) || size(frame))
            return bad(ValidationError::UnexpectedValue);
        return {ValidationError::None, Status::Ok};
    }
    if (isAtomic(op)) {
        if (size(frame) != 8)
            return bad(ValidationError::UnexpectedSize);
        if (address(frame) % 8 != 0 || (address(frame) & (kLineSize - 1)) > kLineSize - size(frame))
            return bad(ValidationError::UnalignedAddress);
        if (payloadLength(frame) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (op == Opcode::AtomicFaa && expected(frame) != 0)
            return bad(ValidationError::UnexpectedExpected);
        if (capabilities(frame) || oldValue(frame))
            return bad(ValidationError::UnexpectedCapabilities);
        if (!validRequestState(op, lineState(frame), epoch(frame)))
            return lineState(frame) == LineState::I ? bad(ValidationError::UnexpectedEpoch)
                                                    : bad(ValidationError::UnexpectedState);
        return {ValidationError::None, Status::Ok};
    }
    if (op == Opcode::Fence || op == Opcode::Unregister) {
        if (lineState(frame) != LineState::I)
            return bad(ValidationError::UnexpectedState);
        if (address(frame) != 0)
            return bad(ValidationError::UnexpectedAddress);
        if (epoch(frame) != 0)
            return bad(ValidationError::UnexpectedEpoch);
    }
    if (isLineCommand(op) && address(frame) % kLineSize != 0)
        return bad(ValidationError::UnalignedAddress);
    if (isLineCommand(op) && !validRequestState(op, lineState(frame), epoch(frame)))
        return bad(ValidationError::UnexpectedState);
    const auto required_payload = op == Opcode::Putm ? kLineSize : 0;
    if (payloadLength(frame) != required_payload)
        return bad(ValidationError::InvalidPayloadLength);
    if (capabilities(frame) || expected(frame) || value(frame) || oldValue(frame) || size(frame))
        return bad(ValidationError::UnexpectedValue);
    return {ValidationError::None, Status::Ok};
}

ValidationResult validateResponse(const CoherenceFrame &response, const CoherenceFrame &request) noexcept {
    if (const auto result = validateEnvelope(response); !result)
        return result;
    if (const auto result = validateFrame(request); !result)
        return bad(ValidationError::ContextRequired);
    if (opcode(response) != Opcode::Response)
        return bad(ValidationError::UnknownOpcode);
    if (opcode(request) == Opcode::Response || opcode(request) == Opcode::SnoopAck || isSnoop(opcode(request)))
        return bad(ValidationError::ContextRequired);
    if (srcHost(response) != dstHost(request))
        return bad(ValidationError::InvalidSourceHost);
    if (dstHost(response) != srcHost(request))
        return bad(ValidationError::InvalidDestinationHost);
    if (requestId(response) != requestId(request))
        return bad(ValidationError::InvalidRequestId);
    if (snoopId(response) != 0)
        return bad(ValidationError::InvalidSnoopId);
    if (address(response) != address(request))
        return bad(ValidationError::UnexpectedAddress);

    const auto request_op = opcode(request);
    if (request_op == Opcode::Register) {
        if (requestId(response) != 0)
            return bad(ValidationError::InvalidRequestId);
        if (status(response) != Status::Ok) {
            if (status(response) == Status::StaleRequest)
                return bad(ValidationError::InvalidRequestId);
            if (sessionId(response) != sessionId(request))
                return bad(ValidationError::InvalidSessionId);
            if (lineState(response) != LineState::I || epoch(response) != 0 || payloadLength(response) != 0 ||
                ackStrength(response) != AckStrength::NONE || !zeroScalars(response))
                return bad(ValidationError::UnexpectedState);
            return {ValidationError::None, Status::Ok};
        }
        if (sessionId(response) == 0)
            return bad(ValidationError::InvalidSessionId);
        if (sessionId(request) != 0 && sessionId(response) != sessionId(request))
            return bad(ValidationError::InvalidSessionId);
        if (lineState(response) != LineState::I)
            return bad(ValidationError::UnexpectedState);
        if (epoch(response) != 0)
            return bad(ValidationError::UnexpectedEpoch);
        if (payloadLength(response) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (size(response) != size(request) || value(response) != value(request) ||
            expected(response) != expected(request))
            return bad(ValidationError::InvalidCacheGeometry);
        if (capabilities(response) == 0 || (capabilities(response) & ~kSupportedCapabilities) != 0 ||
            (capabilities(response) & ~capabilities(request)) != 0 ||
            (capabilities(response) & static_cast<std::uint64_t>(Capability::MODEL_SNOOP)) == 0)
            return bad(ValidationError::InvalidCapabilities);
        if (ackStrength(response) != AckStrength::MODEL)
            return bad(ValidationError::UnexpectedAckStrength);
        if (oldValue(response) == 0)
            return bad(ValidationError::UnexpectedOldValue);
        return {ValidationError::None, Status::Ok};
    }

    if (sessionId(response) != sessionId(request) || sessionId(response) == 0)
        return bad(ValidationError::InvalidSessionId);
    if (ackStrength(response) != AckStrength::NONE || capabilities(response) != 0 || expected(response) != 0 ||
        value(response) != 0 || size(response) != 0)
        return bad(ValidationError::UnexpectedValue);
    if (status(response) != Status::Ok) {
        const bool state_changed = lineState(response) != lineState(request);
        const bool epoch_changed = epoch(response) != epoch(request);
        if (state_changed || epoch_changed) {
            if (state_changed && !epoch_changed)
                return bad(ValidationError::UnexpectedState);
            const auto failure_status = status(response);
            const bool post_snoop_status = failure_status == Status::CoherenceTimeout ||
                                           failure_status == Status::HostFenced || failure_status == Status::IoError;
            bool post_snoop_state = false;
            switch (request_op) {
            case Opcode::Gets:
                post_snoop_state = lineState(request) == LineState::I && lineState(response) == LineState::S;
                break;
            case Opcode::Getm:
            case Opcode::AtomicFaa:
            case Opcode::AtomicCas:
                post_snoop_state = lineState(response) == LineState::I || lineState(response) == LineState::S;
                break;
            case Opcode::Upgrade:
                post_snoop_state = lineState(request) == LineState::S && lineState(response) == LineState::S;
                break;
            default:
                break;
            }
            if (!post_snoop_status || !post_snoop_state) {
                if (state_changed)
                    return bad(ValidationError::UnexpectedState);
                return bad(ValidationError::UnexpectedEpoch);
            }
            if (epoch(response) <= epoch(request))
                return bad(ValidationError::UnexpectedEpoch);
        }
        if (payloadLength(response) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (status(response) == Status::StaleRequest ? oldValue(response) <= requestId(request)
                                                     : oldValue(response) != 0)
            return bad(ValidationError::UnexpectedOldValue);
        return {ValidationError::None, Status::Ok};
    }

    const bool data_response = request_op == Opcode::Gets || request_op == Opcode::Getm || isAtomic(request_op);
    if (payloadLength(response) != (data_response ? kLineSize : 0))
        return bad(ValidationError::InvalidPayloadLength);
    if (epoch(response) == 0 && (isLineCommand(request_op) || isAtomic(request_op)))
        return bad(ValidationError::UnexpectedEpoch);
    const bool state_changing_response = (request_op == Opcode::Getm && lineState(request) == LineState::S) ||
                                         request_op == Opcode::Upgrade || request_op == Opcode::Puts ||
                                         request_op == Opcode::Putm || isAtomic(request_op);
    if (state_changing_response && epoch(response) <= epoch(request))
        return bad(ValidationError::UnexpectedEpoch);
    const bool exact_owner_epoch =
        (request_op == Opcode::Upgrade && lineState(request) == LineState::E) || request_op == Opcode::Putm ||
        (request_op == Opcode::Puts && lineState(request) == LineState::E) ||
        (isAtomic(request_op) && (lineState(request) == LineState::E || lineState(request) == LineState::M));
    if (exact_owner_epoch && epoch(response) - epoch(request) != 1)
        return bad(ValidationError::UnexpectedEpoch);
    if ((request_op == Opcode::Fence || request_op == Opcode::Heartbeat || request_op == Opcode::Unregister) &&
        epoch(response) != 0)
        return bad(ValidationError::UnexpectedEpoch);
    LineState required_state = LineState::I;
    if (request_op == Opcode::Gets) {
        if (lineState(response) != LineState::S && lineState(response) != LineState::E)
            return bad(ValidationError::UnexpectedState);
    } else {
        if (request_op == Opcode::Getm || request_op == Opcode::Upgrade || isAtomic(request_op))
            required_state = LineState::M;
        if (lineState(response) != required_state)
            return bad(ValidationError::UnexpectedState);
    }
    if (isAtomic(request_op) ? false : oldValue(response) != 0)
        return bad(ValidationError::UnexpectedOldValue);
    return {ValidationError::None, Status::Ok};
}

ValidationResult validateSnoopAck(const CoherenceFrame &ack, const CoherenceFrame &snoop,
                                  SnoopAckContext context) noexcept {
    if (const auto result = validateEnvelope(ack); !result)
        return result;
    if (const auto result = validateFrame(snoop); !result)
        return bad(ValidationError::ContextRequired);
    if (opcode(ack) != Opcode::SnoopAck || !isSnoop(opcode(snoop)))
        return bad(ValidationError::ContextRequired);
    if (srcHost(ack) != dstHost(snoop) || dstHost(ack) != srcHost(snoop))
        return bad(ValidationError::InvalidDirection);
    if (requestId(ack) != 0)
        return bad(ValidationError::InvalidRequestId);
    if (snoopId(ack) == 0 || snoopId(ack) != snoopId(snoop))
        return bad(ValidationError::InvalidSnoopId);
    if (sessionId(ack) == 0 || sessionId(ack) != sessionId(snoop))
        return bad(ValidationError::InvalidSessionId);
    if (address(ack) != address(snoop))
        return bad(ValidationError::UnexpectedAddress);
    if (epoch(ack) != epoch(snoop))
        return bad(ValidationError::UnexpectedEpoch);
    if (context.negotiated_strength != AckStrength::MODEL || ackStrength(ack) != AckStrength::MODEL)
        return bad(ValidationError::UnexpectedAckStrength);
    if (!zeroScalars(ack))
        return bad(ValidationError::UnexpectedValue);
    const bool success = status(ack) == Status::Ok;
    if (!success && !permittedSnoopFailure(status(ack), opcode(snoop)))
        return bad(ValidationError::UnexpectedStatus);
    const bool data_snoop = opcode(snoop) == Opcode::SnpDataInv || opcode(snoop) == Opcode::SnpDataDowngrade;
    if (payloadLength(ack) != (success && data_snoop ? kLineSize : 0))
        return bad(ValidationError::InvalidPayloadLength);
    const auto required_success_state =
        opcode(snoop) == Opcode::SnpDowngrade || opcode(snoop) == Opcode::SnpDataDowngrade ? LineState::S
                                                                                           : LineState::I;
    const bool valid_failure_state =
        opcode(snoop) == Opcode::SnpInv ? context.failure_state == LineState::S || context.failure_state == LineState::E
        : opcode(snoop) == Opcode::SnpDowngrade ? context.failure_state == LineState::E
        : opcode(snoop) == Opcode::SnpDataInv || opcode(snoop) == Opcode::SnpDataDowngrade
            ? context.failure_state == LineState::M
            : context.failure_state == LineState::I;
    if (static_cast<std::uint8_t>(context.failure_state) > static_cast<std::uint8_t>(LineState::M) ||
        context.success_state != required_success_state || !valid_failure_state)
        return bad(ValidationError::ContextRequired);
    const auto expected_state = success ? context.success_state : context.failure_state;
    if (lineState(ack) != expected_state)
        return bad(ValidationError::UnexpectedState);
    return {ValidationError::None, Status::Ok};
}

std::string_view toString(Opcode value) noexcept {
    switch (value) {
#define CASE(value, text)                                                                                              \
    case Opcode::value:                                                                                                \
        return text
        CASE(Register, "REGISTER");
        CASE(Unregister, "UNREGISTER");
        CASE(Gets, "GETS");
        CASE(Getm, "GETM");
        CASE(Upgrade, "UPGRADE");
        CASE(Puts, "PUTS");
        CASE(Putm, "PUTM");
        CASE(AtomicFaa, "ATOMIC_FAA");
        CASE(AtomicCas, "ATOMIC_CAS");
        CASE(Fence, "FENCE");
        CASE(SnoopAck, "SNOOP_ACK");
        CASE(Heartbeat, "HEARTBEAT");
        CASE(Response, "RESPONSE");
        CASE(SnpInv, "SNP_INV");
        CASE(SnpDowngrade, "SNP_DOWNGRADE");
        CASE(SnpDataInv, "SNP_DATA_INV");
        CASE(SnpDataDowngrade, "SNP_DATA_DOWNGRADE");
        CASE(HostFence, "HOST_FENCE");
#undef CASE
    }
    return "UNKNOWN_OPCODE";
}

std::string_view toString(Status value) noexcept {
    static constexpr std::array names{"OK",
                                      "BAD_PROTOCOL",
                                      "PROTOCOL_REQUIRED",
                                      "DUPLICATE_HOST",
                                      "STALE_SESSION",
                                      "STALE_EPOCH",
                                      "STALE_REQUEST",
                                      "INVALID_STATE",
                                      "COHERENCE_TIMEOUT",
                                      "HOST_FENCED",
                                      "NO_CAPABILITY",
                                      "IO_ERROR"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "UNKNOWN_STATUS";
}
std::string_view toString(AckStrength value) noexcept {
    static constexpr std::array names{"NONE", "MODEL", "NATIVE"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "UNKNOWN_ACK_STRENGTH";
}
std::string_view toString(LineState value) noexcept {
    static constexpr std::array names{"I", "S", "E", "M"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "UNKNOWN_STATE";
}
std::string_view toString(ValidationError value) noexcept {
    static constexpr std::array names{"none",
                                      "bad magic",
                                      "bad version",
                                      "unknown opcode",
                                      "nonzero flags",
                                      "invalid status",
                                      "invalid ACK strength",
                                      "invalid line state",
                                      "invalid source host",
                                      "invalid destination host",
                                      "invalid direction",
                                      "invalid request ID",
                                      "invalid snoop ID",
                                      "invalid session ID",
                                      "invalid payload length",
                                      "unaligned address",
                                      "nonzero reserved0",
                                      "nonzero reserved1",
                                      "nonzero reserved",
                                      "nonzero unused data",
                                      "invalid cache geometry",
                                      "invalid capabilities",
                                      "unexpected status",
                                      "unexpected ACK strength",
                                      "unexpected state",
                                      "unexpected address",
                                      "unexpected epoch",
                                      "unexpected capabilities",
                                      "unexpected expected",
                                      "unexpected value",
                                      "unexpected old value",
                                      "unexpected size",
                                      "context required"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "unknown validation error";
}

} // namespace cxlmemsim::protocol_v2
