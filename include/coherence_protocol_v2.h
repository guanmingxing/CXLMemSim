#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace cxlmemsim::protocol_v2 {

inline constexpr std::uint32_t kMagic = 0x32565843U;
inline constexpr std::uint16_t kProtocolVersion = 2;
inline constexpr std::size_t kLineSize = 64;
inline constexpr std::size_t kFrameSize = 192;
inline constexpr std::uint16_t kMaximumHosts = 64;
inline constexpr std::uint16_t kServerHost = 0xffff;

enum class Opcode : std::uint16_t {
    Register = 0x0001,
    Unregister = 0x0002,
    Gets = 0x0003,
    Getm = 0x0004,
    Upgrade = 0x0005,
    Puts = 0x0006,
    Putm = 0x0007,
    AtomicFaa = 0x0008,
    AtomicCas = 0x0009,
    Fence = 0x000a,
    SnoopAck = 0x000b,
    Heartbeat = 0x000c,
    Response = 0x8001,
    SnpInv = 0x8101,
    SnpDowngrade = 0x8102,
    SnpDataInv = 0x8103,
    SnpDataDowngrade = 0x8104,
    HostFence = 0x8105,
};

enum class Status : std::uint16_t {
    Ok = 0,
    BadProtocol = 1,
    ProtocolRequired = 2,
    DuplicateHost = 3,
    StaleSession = 4,
    StaleEpoch = 5,
    StaleRequest = 6,
    InvalidState = 7,
    CoherenceTimeout = 8,
    HostFenced = 9,
    NoCapability = 10,
    IoError = 11,
};

enum class Capability : std::uint64_t { MODEL_SNOOP = 1ULL << 0, NATIVE_FLUSH = 1ULL << 1 };
inline constexpr std::uint64_t kKnownCapabilities = 0x3ULL;
enum class AckStrength : std::uint8_t { NONE = 0, MODEL = 1, NATIVE = 2 };
enum class LineState : std::uint8_t { I = 0, S = 1, E = 2, M = 3 };

// Host-order logical representation. Wire I/O must use encodeFrame/decodeFrame.
struct CoherenceFrame {
    std::uint32_t magic;
    std::uint16_t version;
    std::uint16_t type;
    std::uint32_t flags;
    std::uint16_t status;
    std::uint8_t ack_strength;
    std::uint8_t state;
    std::uint16_t src_host;
    std::uint16_t dst_host;
    std::uint16_t payload_len;
    std::uint16_t reserved0;
    std::uint64_t request_id;
    std::uint64_t snoop_id;
    std::uint64_t session_id;
    std::uint64_t addr;
    std::uint64_t epoch;
    std::uint64_t capabilities;
    std::uint64_t expected;
    std::uint64_t value;
    std::uint64_t old_value;
    std::uint32_t size;
    std::uint32_t reserved1;
    std::array<std::uint8_t, kLineSize> data;
    std::array<std::uint8_t, 24> reserved;
};

static_assert(sizeof(CoherenceFrame) == kFrameSize);
#define ASSERT_OFFSET(field, offset) static_assert(offsetof(CoherenceFrame, field) == offset)
ASSERT_OFFSET(magic, 0);
ASSERT_OFFSET(version, 4);
ASSERT_OFFSET(type, 6);
ASSERT_OFFSET(flags, 8);
ASSERT_OFFSET(status, 12);
ASSERT_OFFSET(ack_strength, 14);
ASSERT_OFFSET(state, 15);
ASSERT_OFFSET(src_host, 16);
ASSERT_OFFSET(dst_host, 18);
ASSERT_OFFSET(payload_len, 20);
ASSERT_OFFSET(reserved0, 22);
ASSERT_OFFSET(request_id, 24);
ASSERT_OFFSET(snoop_id, 32);
ASSERT_OFFSET(session_id, 40);
ASSERT_OFFSET(addr, 48);
ASSERT_OFFSET(epoch, 56);
ASSERT_OFFSET(capabilities, 64);
ASSERT_OFFSET(expected, 72);
ASSERT_OFFSET(value, 80);
ASSERT_OFFSET(old_value, 88);
ASSERT_OFFSET(size, 96);
ASSERT_OFFSET(reserved1, 100);
ASSERT_OFFSET(data, 104);
ASSERT_OFFSET(reserved, 168);
#undef ASSERT_OFFSET

using EncodedFrame = std::array<std::uint8_t, kFrameSize>;
EncodedFrame encodeFrame(const CoherenceFrame &frame) noexcept;
bool decodeFrame(const EncodedFrame &bytes, CoherenceFrame &frame) noexcept;
CoherenceFrame initializeFrame(Opcode type) noexcept;

#define DECLARE_ACCESSORS(type, name, setter)                                                                          \
    type name(const CoherenceFrame &frame) noexcept;                                                                   \
    void setter(CoherenceFrame &frame, type value) noexcept
DECLARE_ACCESSORS(std::uint32_t, magic, setMagic);
DECLARE_ACCESSORS(std::uint16_t, version, setVersion);
DECLARE_ACCESSORS(Opcode, opcode, setOpcode);
DECLARE_ACCESSORS(std::uint32_t, flags, setFlags);
DECLARE_ACCESSORS(Status, status, setStatus);
DECLARE_ACCESSORS(AckStrength, ackStrength, setAckStrength);
DECLARE_ACCESSORS(LineState, lineState, setLineState);
DECLARE_ACCESSORS(std::uint16_t, srcHost, setSrcHost);
DECLARE_ACCESSORS(std::uint16_t, dstHost, setDstHost);
DECLARE_ACCESSORS(std::uint16_t, payloadLength, setPayloadLength);
DECLARE_ACCESSORS(std::uint16_t, reserved0, setReserved0);
DECLARE_ACCESSORS(std::uint64_t, requestId, setRequestId);
DECLARE_ACCESSORS(std::uint64_t, snoopId, setSnoopId);
DECLARE_ACCESSORS(std::uint64_t, sessionId, setSessionId);
DECLARE_ACCESSORS(std::uint64_t, address, setAddress);
DECLARE_ACCESSORS(std::uint64_t, epoch, setEpoch);
DECLARE_ACCESSORS(std::uint64_t, capabilities, setCapabilities);
DECLARE_ACCESSORS(std::uint64_t, expected, setExpected);
DECLARE_ACCESSORS(std::uint64_t, value, setValue);
DECLARE_ACCESSORS(std::uint64_t, oldValue, setOldValue);
DECLARE_ACCESSORS(std::uint32_t, size, setSize);
DECLARE_ACCESSORS(std::uint32_t, reserved1, setReserved1);
#undef DECLARE_ACCESSORS

enum class ValidationError {
    None,
    BadMagic,
    BadVersion,
    UnknownOpcode,
    NonzeroFlags,
    InvalidStatus,
    InvalidAckStrength,
    InvalidLineState,
    InvalidSourceHost,
    InvalidDestinationHost,
    InvalidDirection,
    InvalidRequestId,
    InvalidSnoopId,
    InvalidSessionId,
    InvalidPayloadLength,
    UnalignedAddress,
    NonzeroReserved0,
    NonzeroReserved1,
    NonzeroReserved,
    NonzeroUnusedData,
    InvalidCacheGeometry,
    InvalidCapabilities,
    UnexpectedStatus,
    UnexpectedAckStrength,
    UnexpectedState,
    UnexpectedAddress,
    UnexpectedEpoch,
    UnexpectedCapabilities,
    UnexpectedExpected,
    UnexpectedValue,
    UnexpectedOldValue,
    UnexpectedSize,
    ContextRequired,
};

struct ValidationResult {
    ValidationError error;
    Status status;
    constexpr explicit operator bool() const noexcept { return error == ValidationError::None; }
};

ValidationResult validateFrame(const CoherenceFrame &frame) noexcept;
ValidationResult validateResponse(const CoherenceFrame &response, const CoherenceFrame &request) noexcept;
ValidationResult validateSnoopAck(const CoherenceFrame &ack, const CoherenceFrame &snoop,
                                  AckStrength negotiated_strength) noexcept;
std::string_view toString(Opcode value) noexcept;
std::string_view toString(Status value) noexcept;
std::string_view toString(AckStrength value) noexcept;
std::string_view toString(LineState value) noexcept;
std::string_view toString(ValidationError value) noexcept;

} // namespace cxlmemsim::protocol_v2
