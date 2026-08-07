#pragma once

#include "coherence_protocol_v2.h"

#include <cstdint>

namespace cxlmemsim {

class CoherenceTransport {
public:
    virtual ~CoherenceTransport() = default;

    // Calls for disjoint cache lines may execute concurrently, so implementations must be thread-safe. This call may
    // synchronously deliver the matching ACK to MesiTransactionEngine::handleSnoopAck() before returning. That matching
    // ACK is the only transaction-engine reentry permitted: callbacks must not start another acquisition for this or
    // any other cache line, because doing so can invert line-lock ordering.
    virtual bool sendToHost(std::uint16_t host_id, const protocol_v2::CoherenceFrame &frame) = 0;
};

} // namespace cxlmemsim
