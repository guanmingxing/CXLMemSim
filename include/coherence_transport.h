#pragma once

#include "coherence_protocol_v2.h"

#include <cstdint>

namespace cxlmemsim {

class CoherenceTransport {
public:
    virtual ~CoherenceTransport() = default;
    virtual bool sendToHost(std::uint16_t host_id, const protocol_v2::CoherenceFrame &frame) = 0;
};

} // namespace cxlmemsim
