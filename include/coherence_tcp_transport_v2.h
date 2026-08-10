#pragma once

#include "coherence_server_v2.h"

#include <string>

namespace cxlmemsim {

// Serves one already-connected SOCK_STREAM fd until EOF or protocol-directed
// close. The caller retains ownership of fd. Outbound frames are serialized so
// response publication and snoops may safely originate on different threads.
bool serveCoherenceV2Stream(CoherenceServerV2 &server, CoherenceServerV2::ConnectionId connection, int fd,
                            std::string transport_name = "tcp");

} // namespace cxlmemsim
