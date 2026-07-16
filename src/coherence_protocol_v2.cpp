#include "coherence_protocol_v2.h"

#include <algorithm>
#include <bit>
#include <cstring>
#include <type_traits>

namespace cxlmemsim::protocol_v2 {
namespace {

template <typename T> constexpr T byteSwap(T value) noexcept {
    static_assert(std::is_unsigned_v<T>);
    if constexpr (sizeof(T) == 2) {
        return static_cast<T>((value >> 8U) | (value << 8U));
    } else if constexpr (sizeof(T) == 4) {
        return static_cast<T>(((value & 0x000000ffU) << 24U) | ((value & 0x0000ff00U) << 8U) |
                              ((value & 0x00ff0000U) >> 8U) | ((value & 0xff000000U) >> 24U));
    } else {
        return static_cast<T>(((value & 0x00000000000000ffULL) << 56U) | ((value & 0x000000000000ff00ULL) << 40U) |
                              ((value & 0x0000000000ff0000ULL) << 24U) | ((value & 0x00000000ff000000ULL) << 8U) |
                              ((value & 0x000000ff00000000ULL) >> 8U) | ((value & 0x0000ff0000000000ULL) >> 24U) |
                              ((value & 0x00ff000000000000ULL) >> 40U) | ((value & 0xff00000000000000ULL) >> 56U));
    }
}

template <typename T> constexpr T hostToLe(T value) noexcept {
    if constexpr (std::endian::native == std::endian::little) {
        return value;
    }
    return byteSwap(value);
}

template <typename T> constexpr T leToHost(T value) noexcept { return hostToLe(value); }

bool knownOpcode(Opcode value) noexcept { return value >= Opcode::Register && value <= Opcode::HostFenceAck; }

bool lineOperation(Opcode value) noexcept {
    return value == Opcode::Gets || value == Opcode::Getm || value == Opcode::Upgrade || value == Opcode::Puts ||
           value == Opcode::Putm || value == Opcode::AtomicCompareExchange || value == Opcode::AtomicFetchAdd ||
           value == Opcode::SnpInv || value == Opcode::SnpDataInv || value == Opcode::SnpDataDowngrade ||
           value == Opcode::SnoopAck;
}

bool responseOpcode(Opcode value) noexcept { return value == Opcode::RegisterResponse || value == Opcode::Response; }
bool snoopOpcode(Opcode value) noexcept {
    return value == Opcode::SnpInv || value == Opcode::SnpDataInv || value == Opcode::SnpDataDowngrade ||
           value == Opcode::SnoopAck;
}

bool requiresEmptyPayload(Opcode value) noexcept {
    return value == Opcode::Register || value == Opcode::RegisterResponse || value == Opcode::Gets ||
           value == Opcode::Getm || value == Opcode::Upgrade || value == Opcode::Puts || value == Opcode::Fence ||
           value == Opcode::Heartbeat || value == Opcode::Unregister || value == Opcode::SnpInv ||
           value == Opcode::HostFence || value == Opcode::HostFenceAck;
}

ValidationResult invalid(ValidationError error) noexcept { return {error, Status::InvalidFrame}; }

} // namespace

CoherenceFrame initializeFrame(Opcode value) noexcept {
    CoherenceFrame frame{};
    std::copy(kMagic.begin(), kMagic.end(), frame.magic);
    frame.version_le = hostToLe(kProtocolVersion);
    setOpcode(frame, value);
    setHostId(frame, kNoOwner);
    return frame;
}

#define DEFINE_ACCESSORS(name, setter, member, type)                                                                   \
    type name(const CoherenceFrame &frame) noexcept { return leToHost(frame.member); }                                 \
    void setter(CoherenceFrame &frame, type value) noexcept { frame.member = hostToLe(value); }

Opcode opcode(const CoherenceFrame &frame) noexcept { return static_cast<Opcode>(leToHost(frame.opcode_le)); }
void setOpcode(CoherenceFrame &frame, Opcode value) noexcept {
    frame.opcode_le = hostToLe(static_cast<std::uint16_t>(value));
}
DEFINE_ACCESSORS(flags, setFlags, flags_le, std::uint32_t)
DEFINE_ACCESSORS(hostId, setHostId, host_id_le, std::uint16_t)
Status status(const CoherenceFrame &frame) noexcept { return static_cast<Status>(leToHost(frame.status_le)); }
void setStatus(CoherenceFrame &frame, Status value) noexcept {
    frame.status_le = hostToLe(static_cast<std::uint16_t>(value));
}
DEFINE_ACCESSORS(sessionId, setSessionId, session_id_le, std::uint64_t)
DEFINE_ACCESSORS(requestId, setRequestId, request_id_le, std::uint64_t)
DEFINE_ACCESSORS(snoopId, setSnoopId, snoop_id_le, std::uint64_t)
DEFINE_ACCESSORS(address, setAddress, address_le, std::uint64_t)
DEFINE_ACCESSORS(epoch, setEpoch, epoch_le, std::uint64_t)
DEFINE_ACCESSORS(responseWatermark, setResponseWatermark, response_watermark_le, std::uint64_t)
DEFINE_ACCESSORS(capabilities, setCapabilities, capabilities_le, std::uint64_t)
DEFINE_ACCESSORS(cacheCapacity, setCacheCapacity, cache_capacity_le, std::uint32_t)
DEFINE_ACCESSORS(cacheWays, setCacheWays, cache_ways_le, std::uint16_t)
DEFINE_ACCESSORS(payloadLength, setPayloadLength, payload_length_le, std::uint16_t)
#undef DEFINE_ACCESSORS

AckStrength ackStrength(const CoherenceFrame &frame) noexcept { return static_cast<AckStrength>(frame.payload[0]); }
void setAckStrength(CoherenceFrame &frame, AckStrength value) noexcept {
    frame.payload[0] = static_cast<std::uint8_t>(value);
    if (payloadLength(frame) < 1) {
        setPayloadLength(frame, 1);
    }
}
LineState lineState(const CoherenceFrame &frame) noexcept { return static_cast<LineState>(frame.payload[1]); }
void setLineState(CoherenceFrame &frame, LineState value) noexcept {
    frame.payload[1] = static_cast<std::uint8_t>(value);
    if (payloadLength(frame) < 2) {
        setPayloadLength(frame, 2);
    }
}

ValidationResult validateFrame(const CoherenceFrame &frame) noexcept {
    if (!std::equal(kMagic.begin(), kMagic.end(), frame.magic)) {
        return invalid(ValidationError::BadMagic);
    }
    if (leToHost(frame.version_le) != kProtocolVersion) {
        return invalid(ValidationError::BadVersion);
    }
    const Opcode op = opcode(frame);
    if (!knownOpcode(op)) {
        return {ValidationError::UnknownOpcode, Status::Unsupported};
    }
    if ((flags(frame) & ~kKnownFlags) != 0) {
        return {ValidationError::UnknownFlags, Status::Unsupported};
    }
    if (hostId(frame) >= kMaximumHosts && hostId(frame) != kNoOwner) {
        return invalid(ValidationError::InvalidHost);
    }
    if ((!responseOpcode(op) && status(frame) != Status::Ok) ||
        static_cast<std::uint16_t>(status(frame)) > static_cast<std::uint16_t>(Status::InternalError)) {
        return invalid(ValidationError::InvalidStatus);
    }
    if (payloadLength(frame) > kLineSize) {
        return invalid(ValidationError::InvalidPayloadLength);
    }
    if ((requiresEmptyPayload(op) && payloadLength(frame) != 0) ||
        (op == Opcode::AtomicCompareExchange && payloadLength(frame) != 16) ||
        (op == Opcode::AtomicFetchAdd && payloadLength(frame) != 8)) {
        return invalid(ValidationError::InvalidPayloadLength);
    }
    if (std::any_of(std::begin(frame.reserved), std::end(frame.reserved),
                    [](std::uint8_t byte) { return byte != 0; })) {
        return invalid(ValidationError::NonzeroReserved);
    }
    if (lineOperation(op) && address(frame) % kLineSize != 0) {
        return invalid(ValidationError::UnalignedAddress);
    }

    const bool is_register = op == Opcode::Register;
    if (is_register) {
        if (hostId(frame) != kNoOwner || sessionId(frame) != 0 || requestId(frame) == 0 || snoopId(frame) != 0) {
            return invalid(ValidationError::InvalidIdentity);
        }
        const auto capacity = cacheCapacity(frame);
        const auto ways = cacheWays(frame);
        if (capacity == 0 || ways == 0 || capacity % (static_cast<std::uint32_t>(ways) * kLineSize) != 0) {
            return invalid(ValidationError::InvalidCacheGeometry);
        }
        const auto caps = capabilities(frame);
        if ((caps & ~kKnownCapabilities) != 0 || (caps & static_cast<std::uint64_t>(Capability::MODEL_SNOOP)) == 0) {
            return invalid(ValidationError::InvalidCapabilities);
        }
    } else {
        if (hostId(frame) >= kMaximumHosts || sessionId(frame) == 0 || requestId(frame) == 0) {
            return invalid(ValidationError::InvalidIdentity);
        }
        if (snoopOpcode(op) != (snoopId(frame) != 0)) {
            return invalid(ValidationError::InvalidIdentity);
        }
    }

    if (op == Opcode::SnoopAck) {
        if (payloadLength(frame) != 2 || static_cast<std::uint8_t>(ackStrength(frame)) > 2) {
            return invalid(ValidationError::InvalidAckStrength);
        }
        if (static_cast<std::uint8_t>(lineState(frame)) > 3) {
            return invalid(ValidationError::InvalidLineState);
        }
        if (ackStrength(frame) == AckStrength::NATIVE &&
            (capabilities(frame) & static_cast<std::uint64_t>(Capability::NATIVE_FLUSH)) == 0) {
            return invalid(ValidationError::InvalidCapabilities);
        }
    } else if (op == Opcode::SnpDataInv || op == Opcode::SnpDataDowngrade || op == Opcode::Putm) {
        if (payloadLength(frame) != kLineSize) {
            return invalid(ValidationError::InvalidPayloadLength);
        }
    }
    return {ValidationError::None, Status::Ok};
}

std::string_view toString(Opcode value) noexcept {
    static constexpr std::array names{
        "unknown", "REGISTER",     "REGISTER_RESPONSE",  "GETS",      "GETM",       "UPGRADE",       "PUTS",
        "PUTM",    "ATOMIC_CAS",   "ATOMIC_FETCH_ADD",   "FENCE",     "HEARTBEAT",  "UNREGISTER",    "RESPONSE",
        "SNP_INV", "SNP_DATA_INV", "SNP_DATA_DOWNGRADE", "SNOOP_ACK", "HOST_FENCE", "HOST_FENCE_ACK"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : names[0];
}

std::string_view toString(Status value) noexcept {
    static constexpr std::array names{"OK",    "INVALID_FRAME",  "UNSUPPORTED",       "BUSY",
                                      "RETRY", "NOT_REGISTERED", "PERMISSION_DENIED", "INTERNAL_ERROR"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "UNKNOWN_STATUS";
}

std::string_view toString(ValidationError value) noexcept {
    static constexpr std::array names{"none",
                                      "bad magic",
                                      "bad version",
                                      "unknown opcode",
                                      "unknown flags",
                                      "invalid host",
                                      "invalid status",
                                      "invalid identity",
                                      "invalid payload length",
                                      "unaligned address",
                                      "nonzero reserved",
                                      "invalid cache geometry",
                                      "invalid capabilities",
                                      "invalid ACK strength",
                                      "invalid line state"};
    const auto index = static_cast<std::size_t>(value);
    return index < names.size() ? names[index] : "unknown validation error";
}

} // namespace cxlmemsim::protocol_v2
