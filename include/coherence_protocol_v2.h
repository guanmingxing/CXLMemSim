#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace cxlmemsim::protocol_v2 {

inline constexpr std::array<std::uint8_t, 4> kMagic{'C', 'X', 'V', '2'};
inline constexpr std::uint16_t kProtocolVersion = 2;
inline constexpr std::size_t kLineSize = 64;
inline constexpr std::size_t kFrameSize = 192;
inline constexpr std::uint16_t kMaximumHosts = 64;
inline constexpr std::uint16_t kNoOwner = 0xffff;

enum class Opcode : std::uint16_t {
    Register = 1,
    RegisterResponse = 2,
    Gets = 3,
    Getm = 4,
    Upgrade = 5,
    Puts = 6,
    Putm = 7,
    AtomicCompareExchange = 8,
    AtomicFetchAdd = 9,
    Fence = 10,
    Heartbeat = 11,
    Unregister = 12,
    Response = 13,
    SnpInv = 14,
    SnpDataInv = 15,
    SnpDataDowngrade = 16,
    SnoopAck = 17,
    HostFence = 18,
    HostFenceAck = 19,
};

enum class Status : std::uint16_t {
    Ok = 0,
    InvalidFrame = 1,
    Unsupported = 2,
    Busy = 3,
    Retry = 4,
    NotRegistered = 5,
    PermissionDenied = 6,
    InternalError = 7,
};

enum class FrameFlag : std::uint32_t {
    None = 0,
    HasData = 1U << 0,
    AckRequired = 1U << 1,
    Writeback = 1U << 2,
};
inline constexpr std::uint32_t kKnownFlags = 0x00000007U;

enum class Capability : std::uint64_t { MODEL_SNOOP = 1ULL << 0, NATIVE_FLUSH = 1ULL << 1 };
inline constexpr std::uint64_t kKnownCapabilities = 0x3ULL;

enum class AckStrength : std::uint8_t { NONE = 0, MODEL = 1, NATIVE = 2 };
enum class LineState : std::uint8_t { I = 0, S = 1, E = 2, M = 3 };

struct CoherenceFrame {
    std::uint8_t magic[4];
    std::uint16_t version_le;
    std::uint16_t opcode_le;
    std::uint32_t flags_le;
    std::uint16_t host_id_le;
    std::uint16_t status_le;
    std::uint64_t session_id_le;
    std::uint64_t request_id_le;
    std::uint64_t snoop_id_le;
    std::uint64_t address_le;
    std::uint64_t epoch_le;
    std::uint64_t response_watermark_le;
    std::uint64_t capabilities_le;
    std::uint32_t cache_capacity_le;
    std::uint16_t cache_ways_le;
    std::uint16_t payload_length_le;
    std::uint8_t payload[kLineSize];
    std::uint8_t reserved[48];
};

static_assert(sizeof(CoherenceFrame) == kFrameSize);
static_assert(offsetof(CoherenceFrame, magic) == 0);
static_assert(offsetof(CoherenceFrame, version_le) == 4);
static_assert(offsetof(CoherenceFrame, opcode_le) == 6);
static_assert(offsetof(CoherenceFrame, flags_le) == 8);
static_assert(offsetof(CoherenceFrame, host_id_le) == 12);
static_assert(offsetof(CoherenceFrame, status_le) == 14);
static_assert(offsetof(CoherenceFrame, session_id_le) == 16);
static_assert(offsetof(CoherenceFrame, request_id_le) == 24);
static_assert(offsetof(CoherenceFrame, snoop_id_le) == 32);
static_assert(offsetof(CoherenceFrame, address_le) == 40);
static_assert(offsetof(CoherenceFrame, epoch_le) == 48);
static_assert(offsetof(CoherenceFrame, response_watermark_le) == 56);
static_assert(offsetof(CoherenceFrame, capabilities_le) == 64);
static_assert(offsetof(CoherenceFrame, cache_capacity_le) == 72);
static_assert(offsetof(CoherenceFrame, cache_ways_le) == 76);
static_assert(offsetof(CoherenceFrame, payload_length_le) == 78);
static_assert(offsetof(CoherenceFrame, payload) == 80);
static_assert(offsetof(CoherenceFrame, reserved) == 144);

CoherenceFrame initializeFrame(Opcode opcode) noexcept;

Opcode opcode(const CoherenceFrame &frame) noexcept;
void setOpcode(CoherenceFrame &frame, Opcode value) noexcept;
std::uint32_t flags(const CoherenceFrame &frame) noexcept;
void setFlags(CoherenceFrame &frame, std::uint32_t value) noexcept;
std::uint16_t hostId(const CoherenceFrame &frame) noexcept;
void setHostId(CoherenceFrame &frame, std::uint16_t value) noexcept;
Status status(const CoherenceFrame &frame) noexcept;
void setStatus(CoherenceFrame &frame, Status value) noexcept;
std::uint64_t sessionId(const CoherenceFrame &frame) noexcept;
void setSessionId(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t requestId(const CoherenceFrame &frame) noexcept;
void setRequestId(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t snoopId(const CoherenceFrame &frame) noexcept;
void setSnoopId(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t address(const CoherenceFrame &frame) noexcept;
void setAddress(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t epoch(const CoherenceFrame &frame) noexcept;
void setEpoch(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t responseWatermark(const CoherenceFrame &frame) noexcept;
void setResponseWatermark(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint64_t capabilities(const CoherenceFrame &frame) noexcept;
void setCapabilities(CoherenceFrame &frame, std::uint64_t value) noexcept;
std::uint32_t cacheCapacity(const CoherenceFrame &frame) noexcept;
void setCacheCapacity(CoherenceFrame &frame, std::uint32_t value) noexcept;
std::uint16_t cacheWays(const CoherenceFrame &frame) noexcept;
void setCacheWays(CoherenceFrame &frame, std::uint16_t value) noexcept;
std::uint16_t payloadLength(const CoherenceFrame &frame) noexcept;
void setPayloadLength(CoherenceFrame &frame, std::uint16_t value) noexcept;

AckStrength ackStrength(const CoherenceFrame &frame) noexcept;
void setAckStrength(CoherenceFrame &frame, AckStrength value) noexcept;
LineState lineState(const CoherenceFrame &frame) noexcept;
void setLineState(CoherenceFrame &frame, LineState value) noexcept;

enum class ValidationError {
    None,
    BadMagic,
    BadVersion,
    UnknownOpcode,
    UnknownFlags,
    InvalidHost,
    InvalidStatus,
    InvalidIdentity,
    InvalidPayloadLength,
    UnalignedAddress,
    NonzeroReserved,
    InvalidCacheGeometry,
    InvalidCapabilities,
    InvalidAckStrength,
    InvalidLineState,
};

struct ValidationResult {
    ValidationError error;
    Status status;
    constexpr explicit operator bool() const noexcept { return error == ValidationError::None; }
};

ValidationResult validateFrame(const CoherenceFrame &frame) noexcept;
std::string_view toString(Opcode value) noexcept;
std::string_view toString(Status value) noexcept;
std::string_view toString(ValidationError value) noexcept;

} // namespace cxlmemsim::protocol_v2
