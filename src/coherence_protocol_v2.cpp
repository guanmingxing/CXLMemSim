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

template <typename T> T getLe(const EncodedFrame &bytes, std::size_t offset) noexcept {
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

bool decodeFrame(const EncodedFrame &bytes, CoherenceFrame &frame) noexcept {
    frame = {};
#define GET(member, offset, type) frame.member = getLe<type>(bytes, offset)
    GET(magic, 0, std::uint32_t);
    GET(version, 4, std::uint16_t);
    GET(type, 6, std::uint16_t);
    GET(flags, 8, std::uint32_t);
    GET(status, 12, std::uint16_t);
    frame.ack_strength = bytes[14];
    frame.state = bytes[15];
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
    std::copy_n(bytes.begin() + 104, kLineSize, frame.data.begin());
    std::copy_n(bytes.begin() + 168, frame.reserved.size(), frame.reserved.begin());
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
        if (size(frame) != kLineSize || value(frame) == 0 || expected(frame) == 0 ||
            value(frame) % (expected(frame) * kLineSize) != 0)
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
    if (op == Opcode::Response) {
        if (requestId(frame) == 0)
            return bad(ValidationError::InvalidRequestId);
        if (snoopId(frame) != 0)
            return bad(ValidationError::InvalidSnoopId);
        if (payloadLength(frame) != 0 && payloadLength(frame) != kLineSize)
            return bad(ValidationError::InvalidPayloadLength);
        return {ValidationError::None, Status::Ok};
    }
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
        if (capabilities(frame) || expected(frame) || value(frame) || oldValue(frame) || size(frame))
            return bad(ValidationError::UnexpectedValue);
        if (op != Opcode::HostFence && address(frame) % kLineSize != 0)
            return bad(ValidationError::UnalignedAddress);
        if (op == Opcode::HostFence && address(frame) != 0)
            return bad(ValidationError::UnexpectedAddress);
        return {ValidationError::None, Status::Ok};
    }
    if (op == Opcode::SnoopAck) {
        if (requestId(frame) != 0)
            return bad(ValidationError::InvalidRequestId);
        if (snoopId(frame) == 0)
            return bad(ValidationError::InvalidSnoopId);
        if (payloadLength(frame) != 0 && payloadLength(frame) != kLineSize)
            return bad(ValidationError::InvalidPayloadLength);
        if (address(frame) % kLineSize != 0)
            return bad(ValidationError::UnalignedAddress);
        if (capabilities(frame) != 0)
            return bad(ValidationError::UnexpectedCapabilities);
        if (expected(frame) || value(frame) || oldValue(frame) || size(frame))
            return bad(ValidationError::UnexpectedValue);
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
        if (address(frame) % 8 != 0)
            return bad(ValidationError::UnalignedAddress);
        if (payloadLength(frame) != 0)
            return bad(ValidationError::InvalidPayloadLength);
        if (op == Opcode::AtomicFaa && expected(frame) != 0)
            return bad(ValidationError::UnexpectedExpected);
        if (capabilities(frame) || oldValue(frame))
            return bad(ValidationError::UnexpectedCapabilities);
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
    const auto required_payload = op == Opcode::Putm ? kLineSize : 0;
    if (payloadLength(frame) != required_payload)
        return bad(ValidationError::InvalidPayloadLength);
    if (capabilities(frame) || expected(frame) || value(frame) || oldValue(frame) || size(frame))
        return bad(ValidationError::UnexpectedValue);
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
                                      "unexpected size"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "unknown validation error";
}

} // namespace cxlmemsim::protocol_v2
