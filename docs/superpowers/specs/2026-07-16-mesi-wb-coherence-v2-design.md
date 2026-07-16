# MESI-WB Coherence Protocol v2 Design

## Objective

Add an explicitly enabled, directory-based MESI write-back coherence protocol to
CXLMemSim. The server maintains authoritative per-cache-line coherence state,
QEMU Type-3 and QEMUless PGAS clients maintain bounded endpoint caches, and the
server sends asynchronous snoops that endpoints must acknowledge before a
conflicting operation can receive permission.

Protocol v2 must provide identical coherence semantics over TCP, RDMA, and
shared memory. Protocol v1 remains the default legacy behavior so existing
experiments and their wire formats continue to run unchanged.

This design models CXL-style host coherence. It does not claim that a model-level
snoop ACK has invalidated a physical guest CPU cache. Native CPU cache
flush/invalidation requires a future guest agent or equivalent platform support
and is negotiated separately from the model-level mechanism.

## Current-State Constraints

The existing repository has multiple independent metadata paths:

- `CoherencyEngine` is a MOESI-oriented latency model whose remote coherence
  operations do not implement a synchronous snoop/ACK transaction.
- `main_server.cc` has separate TCP/SHM `CachelineMetadata` logic.
- PGAS shared-memory entries contain another state/version representation and
  currently bypass a strict central directory.
- the current TCP, RDMA, and SHM request formats are synchronous request/response
  structures without registration, stable host identity, request IDs, snoop
  IDs, or asynchronous server-to-client messages;
- the current RDMA path uses a single synchronous request/response buffer and
  cannot receive unsolicited snoops;
- QEMU Type-3 uses a process-global synchronous client and derives its SHM slot
  from the process ID;
- QEMUless ranks use an explicit SHM slot, but the slot identifies a mailbox,
  not a coherence host.

Protocol v2 removes these duplicated correctness decisions from the active v2
path. Storage and latency models may provide bytes and timing, but only
`CoherencyEngine` may change v2 MESI directory state.

## Scope

The initial endpoint implementations are:

1. QEMU CXL Type-3 device-model accesses that traverse
   `cxl_type3_read()`/`cxl_type3_write()`.
2. QEMUless PGAS accesses made through the Splash/CXLMemSim backend API.

The server supports TCP, RDMA RC SEND/RECV, and shared-memory transports. A
single server may host registered endpoint sessions using different transports.

The following are deferred:

- QEMU Type-1 and Type-2 endpoint caches;
- SCC region selection and lazy invalidation tokens;
- a native guest cache-flush agent and `NATIVE_FLUSH` execution;
- hardware CXL.cache, physical CPU cache, or PCIe/CXL electrical behavior;
- more than 64 simultaneously configured hosts;
- persistence ordering beyond the explicit v2 `FENCE` contract.

## Compatibility and Activation

The server exposes an explicit coherence mode:

```text
--coherence=legacy
--coherence=mesi-wb
```

`legacy` is the default. It preserves all v1 wire structures, PGAS slots, SHM
rings, counters, and experiment behavior.

`mesi-wb` activates protocol v2. In this mode:

- every coherence endpoint must complete a v2 `REGISTER` handshake before
  issuing memory operations;
- a v1 client is rejected with `PROTOCOL_REQUIRED` or `BAD_PROTOCOL`;
- every data access uses the v2 directory and endpoint-cache path;
- no access may silently fall back to a legacy local-memory operation after a
  v2 protocol error.

The server must never infer v2 from a packet that resembles a v2 frame. Both
server mode and endpoint mode are explicit.

## Architecture

### CoherencyEngine

The existing public `CoherencyEngine` is refactored rather than replaced with a
second top-level engine. Internally it has an explicit legacy path and a strict
v2 MESI transaction core.

The v2 core owns:

- the sharded sparse directory;
- stable MESI state transitions;
- per-line epochs;
- pending snoop transactions;
- permission grants;
- atomic serialization;
- timeout reconciliation;
- directory and protocol counters.

The engine does not own sockets, RDMA queue pairs, SHM mappings, or endpoint
cache replacement policy.

### EndpointSessionRegistry

An `EndpointSessionRegistry` maps a configured host ID to its current session,
transport adapter, capabilities, ACK strength, and lifecycle state. It is the
only path used by the engine to send a snoop.

A session has:

- `host_id`: configured `uint16_t` coherence identity;
- `session_id`: server-issued reconnect generation;
- transport kind and transport-local connection handle;
- negotiated capabilities;
- negotiated ACK strength;
- lifecycle state: `ACTIVE`, `OFFLINE_RETAINED`, `FENCED`, or `CLOSED`;
- heartbeat and diagnostic timestamps.

An abrupt disconnect changes an active session to `OFFLINE_RETAINED`. The host
remains coherence-relevant because it may own dirty data or continue using clean
cached lines. A new session with the same host ID is rejected until the old
session is gracefully closed or explicitly evicted.

The same endpoint process may resume an `OFFLINE_RETAINED` session by sending
`REGISTER` with the prior nonzero session ID, the same host ID, and identical
cache geometry/capabilities. The server rebinds the transport to that session
and replays unacknowledged responses. A restarted endpoint that lost its cache
and session state must not resume; it requires administrative fencing/eviction.

### Transport Adapters

TCP, RDMA, and SHM adapters carry the same `CoherenceFrame` and expose the same
transport-neutral operations:

- receive endpoint commands and ACKs;
- send correlated responses;
- send unsolicited snoops;
- report disconnect and transport errors;
- wake pending transactions during shutdown or host fencing.

Transport code cannot inspect or modify MESI state.

### Endpoint Cache

QEMU Type-3 and QEMUless each use the same logical bounded, set-associative,
write-back cache model:

- 64-byte lines;
- default capacity 256 KiB per host/rank;
- default four-way associativity;
- LRU replacement within each set;
- stable states `I`, `S`, `E`, and `M`;
- a transient per-line gate for an in-progress request, eviction, or snoop.

The two endpoint implementations may use language-appropriate code, but must
pass the same golden transition traces and wire-frame fixtures.

## Host Identity and Registration

Host identity and transport mailbox identity are separate.

- QEMU configures `cxl-host-id=<uint16>`.
- QEMUless configures `CXL_SHM_HOST_ID`; when unset, the MPI world rank is used.
- `CXL_SHM_SLOT` remains a transport mailbox index only.
- the configured server maximum defaults to 64 hosts.

The registration sequence is:

1. The transport connection is established.
2. The endpoint sends `REGISTER` with protocol version, requested host ID,
   capabilities, and cache geometry. The accepting adapter supplies transport
   identity out of band.
3. The server rejects unsupported protocol, duplicate live host ID, invalid host
   range, or insufficient capabilities.
4. The server creates a fresh nonzero `session_id`, or reuses the matching
   retained ID for a valid resume, and replies with accepted capabilities, ACK
   strength, line size, accepted cache geometry, and timeout.
5. Only frames carrying the accepted host ID and session ID are admitted.

The initial capability set includes:

- `MODEL_SNOOP = 1 << 0`: endpoint cache state and dirty data can be changed by
  snoops;
- `NATIVE_FLUSH = 1 << 1`: reserved for a future native guest
  flush/invalidation agent.

The initial implementation requires `MODEL_SNOOP`. It records ACK strength in
traces and counters and must not advertise `NATIVE_FLUSH` unless that mechanism
actually ran.

ACK strength is encoded as `NONE = 0`, `MODEL = 1`, and `NATIVE = 2`. Stable
line state is encoded as `I = 0`, `S = 1`, `E = 2`, and `M = 3`.

Graceful disconnect blocks new application operations, executes the endpoint
`FENCE` sequence so all local `M` lines complete `PUTM`, invalidates all local
clean lines, and then sends `UNREGISTER`. The session registry maintains a
reverse holder index.
`UNREGISTER` waits for earlier requests and snoops, removes every remaining
clean `S`/`E` holding under the corresponding line locks, advances each changed
line's epoch, and closes the session. It is rejected if the session still owns
an `M` line. Only then may the endpoint disconnect without administrative
eviction.

## Directory Representation

The memory pool may be hundreds of GiB, so a flat line array is prohibited. A
512 GiB pool contains 8,589,934,592 64-byte lines.

The v2 directory is a sparse map with 256 shards by default. A line entry is
allocated only after the line is touched. Each entry contains:

- aligned 64-byte line address;
- stable MESI state;
- owner host ID or no-owner sentinel;
- `std::bitset<64>` sharer set;
- committed 64-bit epoch;
- whether the server copy is current;
- a per-entry transaction mutex;
- pending-transaction reference and diagnostic counters.

The wire representation of sharers is a 64-bit bitmap. Host IDs are encoded as
`uint16_t`, but configured v2 hosts must be in `[0, max_hosts)`, where
`max_hosts <= 64`.

The shard lock protects only lookup or insertion and obtaining a stable entry
reference. It is released before the entry transaction mutex is acquired.

The reverse holder index is derived bookkeeping, not a second authority. A
committed directory transition updates the relevant per-session holder index
before releasing the line lock. Cleanup snapshots an index without holding any
line lock, releases the index lock, then locks and revalidates one directory
line at a time.

## Stable-State Invariants

The server maintains these exact invariants:

| State | Owner | Sharers | Authoritative data |
| --- | --- | --- | --- |
| `I` | none | empty | server |
| `S` | none | one or more | server |
| `E` | exactly one | empty | server |
| `M` | exactly one | empty | owner endpoint |

`S` intentionally permits one sharer. This is required after a downgrade whose
requester disconnects before installation and after a timed-out invalidation
transaction in which some sharers already acknowledged.

At most one endpoint may hold `E` or `M`. No owner may coexist with a sharer.
No permission response may be sent before every required snoop effect for that
grant is committed.

The existing MOESI `O` state is not used in protocol v2.

## Data Ownership

The server storage backend holds the current line bytes in `I`, `S`, and `E`.
An `E` owner has a clean copy identical to the server copy.

In `M`, the owner endpoint has the latest bytes and the server copy is stale and
non-authoritative. Operations that remove or downgrade an `M` owner use
`SNP_DATA_INV` or `SNP_DATA_DOWNGRADE`; the ACK carries the complete 64-byte
line. A dirty eviction and `FENCE` use `PUTM`, also carrying the complete line.

The server commits returned data before granting a new owner or returning a
read. Partial writes always acquire and merge against a complete line.

## MESI Operations

### GETS

- `I -> E`: return the full line and make the requester the clean exclusive
  owner.
- `S -> S`: return the server line and add the requester to the sharer set.
- `E -> S`: send `SNP_DOWNGRADE` to the owner. After a matching ACK, remove the
  owner, add the old owner and requester as sharers, and return the server line.
- `M -> S`: send `SNP_DATA_DOWNGRADE`. After a matching ACK, commit the returned
  line to server storage, remove the owner, add the old owner and requester as
  sharers, and return the latest line.

If the requester disappears after the old owner has acknowledged but before the
response is installed, only the old owner remains in `S`.

### GETM and UPGRADE

- `I -> M`: return the complete server line and grant `M`.
- `S -> M`: send `SNP_INV` to every sharer except the requester. Grant `M` only
  after all required matching ACKs. A requester already in `S` removes its own
  shared copy as part of the grant.
- same-owner `E -> M`: the endpoint sends explicit `UPGRADE`; after validation,
  the server changes the owner state to `M`.
- another-host `E -> M`: send `SNP_INV` to the `E` owner, then grant the complete
  server line to the requester.
- same-owner `M`: no server request is needed for an ordinary write hit.
- another-host `M -> M`: send `SNP_DATA_INV`, commit the returned line, and then
  grant the latest complete line to the requester.

There is no silent local `E -> M` transition.

If a requester reports `I` because it has an unresolved snoop-completion record
but the directory still records that same host, the server reconciles from the
directory state:

- a phantom clean `S` or `E` holding by that requester may be removed or reused
  while processing the new request because server data is current;
- a phantom `M` owner must receive a data snoop, even when owner and requester
  are the same session, so the saved dirty line is committed before permission
  is regranted.

### PUTS and PUTM

`PUTS` removes a clean `S` or `E` copy:

- removing the last `S` sharer produces `I`;
- removing an `E` owner produces `I`;
- a host not recorded as a holder receives `INVALID_STATE`.

`PUTM` is valid only from the current `M` owner. It commits the complete line to
server storage, removes the owner, and produces `I`.

### FENCE

An endpoint `FENCE` writes back every local `M` line with `PUTM` and waits for
all responses. Clean `S` and `E` lines may remain cached. The endpoint then sends
the protocol `FENCE` command; the server responds only after all earlier
requests from that session are complete.

## Endpoint Access Rules

Endpoint reads behave as follows:

- `S`, `E`, or `M` hit: read the local line;
- `I` or miss: send `GETS`, install the returned line in the granted state, and
  then read.

Endpoint writes behave as follows:

- `M` hit: merge locally and remain `M`;
- `E` hit: send explicit `UPGRADE`, then merge locally;
- `S`, `I`, or miss: send `GETM`, install the complete line as `M`, then merge.

An access spanning line boundaries is split into increasing-address line
operations. Each partial write first obtains the complete line and merges only
its requested byte range.

LRU eviction behaves as follows:

- `S` or `E` victim: send `PUTS` and wait for success before reusing the slot;
- `M` victim: send full-line `PUTM` and wait for success before reuse;
- `I` victim: reuse locally.

A one-line cache configuration is supported for deterministic eviction tests.

QEMU v2 accesses must not use the current SHM local `address_space_read/write`
fallback after joining MESI-WB. QEMUless code outside the registered backend API
is outside the coherence model.

## Endpoint Snoop Rules

The endpoint progress path validates destination host, session ID, snoop ID,
line address, and target epoch before changing local state.

- `SNP_INV` accepts local `S` or `E`, changes it to `I`, and ACKs.
- `SNP_DOWNGRADE` accepts local `E`, changes it to `S`, and ACKs.
- `SNP_DATA_INV` accepts local `M`, returns the complete dirty line, changes it
  to `I`, and ACKs.
- `SNP_DATA_DOWNGRADE` accepts local `M`, returns the complete dirty line,
  changes it to `S`, and ACKs.
- `HOST_FENCE` blocks new application operations, resolves or cancels local
  transients, and acknowledges the administrative fence.

A duplicate snoop with the same session ID, snoop ID, and epoch returns the
cached completion without applying the transition twice. If an ACK was delayed
or lost, a replacement snoop for the same line may replay an already-satisfied
invalidation/downgrade and its saved dirty data at the replacement target epoch.
This semantic completion is keyed by session, line, pre-state, post-state, and
saved data, not only by snoop ID.

The endpoint retains an unsafe-to-forget semantic completion until it observes a
new permission response proving that the server has consumed or superseded it,
or until the session is closed. In particular, a late `SNP_DATA_INV` may leave
the endpoint locally `I` while the server still records it as `M`; a later
`SNP_DATA_INV` with a newer snoop ID or epoch must return the saved dirty line
again. The table is bounded, but an entry needed for correctness is never
evicted: exhaustion fences the endpoint and fails new work closed. A stale
session or epoch returns the corresponding error and has no additional state
effect.

While a semantic completion remains unresolved:

- a snoop-derived `S` line may satisfy reads because it contains the latest
  bytes and cannot coexist with a different writer without another snoop;
- writes, atomics, and upgrades treat the line as a miss and reacquire with
  `state = I`, `epoch = 0`;
- eviction may remove the ordinary cache line but must retain the completion
  record and any saved dirty data;
- a direct permission response for the line clears records that its committed
  epoch and returned state/data supersede.

Each endpoint has a local per-line gate. A snoop, application miss/upgrade, and
victim eviction for the same line cannot concurrently transfer different dirty
versions. The application thread never holds the endpoint-cache mutex while
waiting for a server response. It also does not retain exclusive ownership of
the per-line gate while blocked: the request leaves a transient record and
yields the gate so the progress thread can service a same-line snoop. The
progress thread atomically claims the transient, uses any semantic-completion
data, ACKs the snoop, and returns the gate before waking or completing the
application waiter. This handoff prevents a same-session reconciliation request
from deadlocking on its own snoop.

QEMU TCP uses one full-duplex connection with one RX dispatcher. Synchronous
requesters register a waiter by request ID and never call `recv()` directly.
QEMUless and QEMU each create a transport progress thread on connect and join it
on disconnect. The progress thread continues handling snoops while the
application thread is blocked in MPI, CUDA, or a protocol request.

## Atomics

`ATOMIC_FAA` and `ATOMIC_CAS` are serialized by the server under the target
line's transaction mutex.

The server first obtains exclusive control:

- an `M` owner receives `SNP_DATA_INV` so the latest 64-byte line is returned;
- an `E` owner receives `SNP_INV`;
- all `S` holders except the requester receive `SNP_INV`;
- `I` needs no snoop.

After all required ACKs match the reserved epoch, the server executes the
operation against the authoritative line, increments the committed epoch,
returns the old scalar value and complete updated line, and grants `M` to the
requester. FAA and CAS scalar operands must be naturally aligned and contained
within one cache line.

If coherence acquisition times out, the atomic update is not executed and the
requester receives no permission.

## Epochs, Transactions, and ACK Matching

Every line has a committed epoch. A transaction reserves
`target_epoch = committed_epoch + 1`. Every snoop and ACK in that transaction
carries the target epoch.

Every operation that commits a line-state, holder-set, or authoritative-data
change advances the epoch exactly once to its reserved target. A read by an
already recorded `S` holder changes nothing and returns the current epoch. Every
permission response carries the committed epoch, and the endpoint stores that
epoch with the installed line.

An endpoint's installed epoch may lag the server epoch while it remains a
sharer, because another sharer can be added or removed without changing this
endpoint's bytes or state. Request validation therefore follows these rules:

- a miss or local `I` request uses `epoch = 0`;
- `UPGRADE`, `PUTS`, `PUTM`, and a cache-resident `GETM` carry the endpoint's
  installed epoch and state;
- a request reconciling an unresolved snoop completion uses `state = I` and
  `epoch = 0`, while the completion record remains available to the progress
  thread for a server re-snoop;
- a recorded `S` holder is accepted when its nonzero epoch is not greater than
  the committed server epoch;
- an `E` or `M` owner must carry the exact committed epoch;
- a request carrying an epoch greater than the server epoch is rejected as
  `STALE_EPOCH`.

A snoop carries the reserved target epoch. An endpoint may accept a target
greater than its installed epoch when its local state or semantic-completion
record satisfies the requested transition. Applying the snoop stores the target
epoch in the local line or tombstone. An endpoint never infers server commit
merely because it sent the ACK.

A pending transaction records:

- requester host, session, and request ID;
- line address and operation;
- target epoch;
- generated snoop IDs;
- required and received ACK sets;
- returned dirty data, if any;
- deadline and failure cause.

An ACK affects a transaction only when host ID, session ID, snoop ID, line
address, and target epoch all match. ACK dispatch uses a pending-transaction
mutex and condition variable; it never acquires the line transaction mutex.

Timeout has one linearization point. While holding the pending-transaction
mutex, the request worker changes the transaction from `OPEN` to `CLOSED`,
snapshots the accepted ACK set and returned dirty data, and prevents the ACK
dispatcher from appending further effects. The worker then releases that mutex
and reconciles the snapshot while still owning the line transaction mutex. An
ACK dispatcher that observes `CLOSED` classifies the ACK as late and cannot
change the snapshot.

The request worker may hold the line transaction mutex while waiting because
same-line operations must serialize. It must not hold a shard lock, session
registry lock, transport send lock, storage global lock, or endpoint-cache lock
while waiting.

Different lines remain concurrent. Disconnect, shutdown, or administrative
fencing wakes affected pending waits.

## Timeout Reconciliation

Snoops are synchronous for a permission grant but their already acknowledged
endpoint effects cannot be rolled back. The timeout uses a monotonic deadline
configured by `--snoop-timeout-ms`, defaulting to 1000 ms.

On timeout:

1. Commit every matching ACK effect already received.
2. Commit dirty data returned by an acknowledged data snoop.
3. Remove only holders that acknowledged invalidation.
4. Retain every unacknowledged owner or sharer.
5. Do not add the requester and do not grant permission.
6. Return `COHERENCE_TIMEOUT` to the requester when the session is reachable.
7. Advance the committed epoch if any acknowledged state or data effect changed
   the committed line; otherwise leave it unchanged.

For a partially acknowledged `S` invalidation, the line remains `S` with the
unacknowledged sharers, even if only one remains. For a single-owner `E` or `M`
transaction, an unacknowledged owner remains the owner. A matching late ACK for
the expired transaction is rejected as stale and cannot mutate the line.

This is fail-closed per request: progress resumes only when a later request can
complete or an operator fences and evicts the failed host.

## Host Fencing and Eviction

The local administrative API is
`HOST_EVICT(host_id, confirm_host_stopped, force_data_loss)`. It first marks the
session `FENCED`, blocks new requests, and wakes pending transactions. When the
transport is usable, it sends `HOST_FENCE`; the endpoint blocks new application
operations, finishes or cancels transients, writes back all `M` lines,
invalidates clean lines, and returns a matching ACK. The server may remove clean
directory holdings only after that ACK.

While fenced, the server rejects new application operations but still admits
the bounded drain sequence: pending-response retries, `PUTM`, `FENCE`,
`SNOOP_ACK`, heartbeat watermark updates, and the final host-fence ACK. Cleanup
walks the reverse holder index in address order, acquires at most one line lock
at a time, and never waits while holding the session-registry lock.

A transport disconnect alone is not proof that a VM or process stopped reading
its clean cache. If `HOST_FENCE` cannot be acknowledged, normal eviction also
requires an explicit administrative assertion that the endpoint process/VM has
stopped. Without ACK or that assertion, `S` and `E` holdings remain and
conflicting grants fail closed.

Directory cleanup is line-by-line:

- `S` holder: remove the host and produce `I` if it was the last sharer;
- `E` owner: remove the clean owner and produce `I`;
- `M` owner: require dirty data from the host before eviction.

If an unreachable host owns `M`, normal eviction fails closed even after the
operator confirms that execution stopped. A separate `--force-data-loss`
administrative action may discard that owner, make the server's stale bytes the
fallback current copy, transition to `I`, advance the epoch, and emit a
persistent high-severity data-loss event. Forced data loss is counted in
machine-readable results and may not be reported as a coherent pass.

## Wire Protocol

Protocol v2 uses one fixed 192-byte little-endian frame. All implementations
must use explicit encoding/decoding or packed C-compatible fields plus
compile-time offset and size assertions. Native C++ object serialization is
forbidden.

The four wire bytes for `magic` are ASCII `CXV2`, represented as little-endian
`0x32565843`; `version` is `2`. Configured hosts use IDs 0 through 63, and
`0xffff` denotes the server endpoint in `src_host` or `dst_host`.

| Offset | Field | Type | Bytes |
| ---: | --- | --- | ---: |
| 0 | `magic` | `u32` | 4 |
| 4 | `version` | `u16` | 2 |
| 6 | `type` | `u16` | 2 |
| 8 | `flags` | `u32` | 4 |
| 12 | `status` | `u16` | 2 |
| 14 | `ack_strength` | `u8` | 1 |
| 15 | `state` | `u8` | 1 |
| 16 | `src_host` | `u16` | 2 |
| 18 | `dst_host` | `u16` | 2 |
| 20 | `payload_len` | `u16` | 2 |
| 22 | `reserved0` | `u16` | 2 |
| 24 | `request_id` | `u64` | 8 |
| 32 | `snoop_id` | `u64` | 8 |
| 40 | `session_id` | `u64` | 8 |
| 48 | `addr` | `u64` | 8 |
| 56 | `epoch` | `u64` | 8 |
| 64 | `capabilities` | `u64` | 8 |
| 72 | `expected` | `u64` | 8 |
| 80 | `value` | `u64` | 8 |
| 88 | `old_value` | `u64` | 8 |
| 96 | `size` | `u32` | 4 |
| 100 | `reserved1` | `u32` | 4 |
| 104 | `data` | `u8[64]` | 64 |
| 168 | `reserved` | `u8[24]` | 24 |

Reserved fields and `flags` must be zero in the initial v2 implementation. A
receiver rejects a nonzero value as `BAD_PROTOCOL` unless a negotiated future
capability explicitly defines that field. `payload_len` describes bytes valid
in `data`.

`GETS`, `GETM`, `UPGRADE`, `PUTS`, `PUTM`, and snoop frames carry an aligned
line address. `GETS` and `GETM` request and return a complete 64-byte line.
`PUTM` and data-snoop ACKs set `payload_len = 64`. `PUTS`, `UPGRADE`, and
non-data snoops set `payload_len = 0`. Atomic frames carry the exact naturally
aligned scalar byte address in `addr`, set `size = 8`, and are mapped to the
directory line by clearing the low six address bits.

For line commands, `state` is the sender's current local state. Misses use
`state = I` and `epoch = 0`; cache-resident commands use the installed epoch.
Responses place the granted state in `state` and the committed server epoch in
`epoch`. A request that is reconciling an unresolved snoop completion also uses
`state = I`, `epoch = 0`; the endpoint retains the separate completion record
for any re-snoop. Snoop requests and their ACKs carry the transaction target
epoch.

A new-session `REGISTER` request sets `session_id = 0`; a resume request carries
the prior session ID. Both set `request_id = 0`, `size = 64`, `value` to
endpoint cache capacity in bytes, `expected` to associativity, and
`capabilities` to the requested capability bitmap. A successful response echoes
accepted `size`/`value`/`expected`, sets `old_value` to the server snoop timeout
in milliseconds, and carries the new or resumed session ID.

Message type values are:

| Value | Type |
| ---: | --- |
| `0x0001` | `REGISTER` |
| `0x0002` | `UNREGISTER` |
| `0x0003` | `GETS` |
| `0x0004` | `GETM` |
| `0x0005` | `UPGRADE` |
| `0x0006` | `PUTS` |
| `0x0007` | `PUTM` |
| `0x0008` | `ATOMIC_FAA` |
| `0x0009` | `ATOMIC_CAS` |
| `0x000a` | `FENCE` |
| `0x000b` | `SNOOP_ACK` |
| `0x000c` | `HEARTBEAT` |
| `0x8001` | `RESPONSE` |
| `0x8101` | `SNP_INV` |
| `0x8102` | `SNP_DOWNGRADE` |
| `0x8103` | `SNP_DATA_INV` |
| `0x8104` | `SNP_DATA_DOWNGRADE` |
| `0x8105` | `HOST_FENCE` |

`request_id` correlates endpoint commands and responses. After registration,
endpoint command IDs start at 1 and increase by exactly one; they are never
reused within a session. `snoop_id` correlates unsolicited snoops and ACKs.
`session_id` rejects frames from old connections. `epoch` rejects stale
same-line work.

The server may execute different-line requests concurrently, but publishes
`RESPONSE` frames in increasing request-ID order per session. A completed later
request waits in the session response queue until all earlier IDs have a final
response. A `FENCE` with request ID `N` completes only after every admitted
request with an ID lower than `N` has completed or failed. Snoop frames and
their ACKs are independent of response ordering.

Each side keeps bounded idempotency state:

- the server pins every final response until the endpoint acknowledges
  consumption; an unacknowledged grant, `PUTS`, or `PUTM` response cannot age
  out while directory correctness or endpoint progress depends on it;
- periodic `HEARTBEAT` commands carry the highest contiguous consumed response
  ID in `old_value`; receipt advances the server's response-ACK watermark;
- if pinned responses reach the configured bound, the server applies
  backpressure and admits no new command for that session rather than evicting
  correctness state;
- after a response is acknowledged, its completion may be evicted and the
  replay floor advanced; a request below that floor returns `STALE_REQUEST` and
  reports the floor in `old_value`;
- a resumed session retries every request above its last consumed-response
  watermark with the original request IDs, and the server replays pinned
  completions;
- the endpoint records consumed request IDs, so replay of a response already
  installed locally only advances the watermark and never overwrites newer dirty
  endpoint data.

Snoop semantic completions follow the stricter retention rule in Endpoint Snoop
Rules.

An endpoint-side request timeout does not authorize a new request ID for the
same operation. The endpoint retries the original ID, resumes the retained
session, or fences itself; it does not abandon an operation whose final outcome
is still unknown.

Status values are:

| Value | Status |
| ---: | --- |
| `0` | `OK` |
| `1` | `BAD_PROTOCOL` |
| `2` | `PROTOCOL_REQUIRED` |
| `3` | `DUPLICATE_HOST` |
| `4` | `STALE_SESSION` |
| `5` | `STALE_EPOCH` |
| `6` | `STALE_REQUEST` |
| `7` | `INVALID_STATE` |
| `8` | `COHERENCE_TIMEOUT` |
| `9` | `HOST_FENCED` |
| `10` | `NO_CAPABILITY` |
| `11` | `IO_ERROR` |

### Frame Validation Matrix

All unused scalar fields and unused payload bytes must be zero. A malformed
frame has no directory or endpoint-cache effect. Before registration, the
adapter returns `BAD_PROTOCOL` when possible and closes the connection; after
registration it returns a correlated `BAD_PROTOCOL` response or snoop ACK and
may fence the session after repeated violations.

| Frame class | Required identifiers | Required data/state |
| --- | --- | --- |
| New `REGISTER` | endpoint `src_host`, server `dst_host`, `request_id = 0`, `snoop_id = 0`, `session_id = 0` | `state = I`, `epoch = 0`, `payload_len = 0`, geometry/capabilities as defined above |
| Resume `REGISTER` | same as new registration but with the prior nonzero `session_id` | geometry and capabilities must exactly match the retained session |
| Ordinary command | endpoint `src_host`, server `dst_host`, accepted session, next exact new `request_id` or retained duplicate, `snoop_id = 0` | `status = OK`, `ack_strength = NONE`; line state/epoch and payload follow the command rules |
| `HEARTBEAT` | ordinary-command identifiers | `addr = 0`, `epoch = 0`, `state = I`, `payload_len = 0`; `old_value` is consumed-response watermark |
| `SNOOP_ACK` | endpoint `src_host`, server `dst_host`, `request_id = 0`, nonzero matching `snoop_id`, accepted session and target epoch | negotiated ACK strength, resulting state, and `payload_len = 64` only for a successful data snoop |
| `RESPONSE` | server `src_host`, endpoint `dst_host`, original command `request_id`, `snoop_id = 0`, accepted session | final status, granted/resulting state and epoch; data length must match the original command |
| Snoop or `HOST_FENCE` | server `src_host`, target endpoint `dst_host`, `request_id = 0`, nonzero `snoop_id`, accepted session | `status = OK`; line snoops carry aligned line address and target epoch |

The receiver validates message type before accepting payload bytes. A successful
`GETS`, `GETM`, or atomic response carries 64 bytes. A successful `PUTM` request
or data-snoop ACK carries exactly 64 bytes. FAA uses `value`; CAS uses
`expected` and `value`; their response returns the prior scalar in `old_value`.
Any missing, oversized, or unexpected payload is `BAD_PROTOCOL` and cannot
satisfy a pending coherence transaction.

## Transport Mappings

### TCP

TCP uses one multiplexed full-duplex connection per endpoint. Each frame is
preceded by a four-byte little-endian length, which must equal 192 for v2. One RX
dispatcher routes `RESPONSE` by request ID and unsolicited snoops to the cache
handler. Writes are serialized per connection; reads are owned by the RX
dispatcher.

### RDMA

RDMA uses reliable-connected SEND/RECV with exactly one frame per SEND. The v2
path replaces the current single-buffer synchronous exchange with:

- a registered pool of receive frames;
- continuously pre-posted receives;
- a CQ progress thread;
- independent request and snoop dispatch;
- bounded send work requests and explicit backpressure;
- disconnect/error propagation into the session registry.

The protocol does not use one-sided RDMA writes for coherence commands because
the server and endpoint both need ordered, acknowledged message dispatch.

### Shared Memory

Each registered SHM endpoint has two bounded queues:

- endpoint-to-server command/ACK queue;
- server-to-endpoint response/snoop queue.

Queue entries contain one fixed v2 frame. Producers publish with release
semantics and consumers acquire before reading. Each direction has independent
head/tail ownership and wakeup signaling. Queue-full behavior applies bounded
backpressure; it does not overwrite an unread frame.

The v1 `ShmRingBuffer` and PGAS request slots remain unchanged in legacy mode.
V2 QEMU Type-3 and QEMUless use the same duplex SHM protocol and differ only in
their endpoint adapter.

## Configuration

Server settings:

```text
--coherence=legacy|mesi-wb
--max-hosts=64
--directory-shards=256
--snoop-timeout-ms=1000
```

QEMU Type-3 properties:

```text
cxl-host-id=<id>
cxl-coherence=legacy|mesi-wb
cxl-cache-size=<bytes>
cxl-cache-ways=<ways>
```

QEMUless environment:

```text
CXL_SHM_HOST_ID=<id>
CXL_COHERENCE_MODE=legacy|mesi-wb
CXL_ENDPOINT_CACHE_BYTES=262144
CXL_ENDPOINT_CACHE_WAYS=4
```

Transport selection remains explicit through the existing server/endpoint
transport configuration. Invalid cache geometry, duplicate host IDs, more than
64 configured hosts, or missing `MODEL_SNOOP` capability fail registration.

## Observability and Evidence

The server exports at least:

- registrations, active/offline/fenced sessions, and negotiated capabilities;
- `GETS`, `GETM`, `UPGRADE`, `PUTS`, `PUTM`, atomic, and fence counts;
- state-transition counts;
- snoops sent, ACKs received, duplicate ACKs, stale ACKs, and ACK-strength
  counts;
- coherence timeouts and per-host timeout attribution;
- endpoint cache hit/miss/eviction/writeback counters;
- host evictions and forced-data-loss events;
- sparse-directory entries and shard contention;
- current legacy CXL controller counters for compatibility.

Proof runs produce:

- `summary.json`;
- `directory-trace.jsonl`;
- per-endpoint cache traces;
- protocol frame trace with payload bytes redacted or sampled as configured;
- registration/capability records;
- exact commands, git SHAs, configuration, timeout values, and cleanup status;
- server and endpoint logs.

Trace records include host ID, session ID, request ID or snoop ID, line address,
old/new state, old/new epoch, ACK strength, status, and transport.

## Verification Strategy

### Deterministic Unit Tests

A reference MESI model drives tests for:

- every `GETS`, `GETM`, `UPGRADE`, `PUTS`, and `PUTM` transition;
- explicit `E -> M`;
- `M` data forwarding and writeback;
- partial writes and cross-line splitting;
- clean and dirty LRU eviction, including a one-line cache;
- FAA and CAS success/failure;
- duplicate request/snoop replay;
- lost grant, `PUTS`, and `PUTM` response replay across session resume;
- response-watermark backpressure and replay-floor behavior;
- stale session, snoop, and epoch rejection;
- partial multi-sharer timeout reconciliation;
- ACK-at-deadline linearization;
- requester disconnect after downgrade;
- same-session re-snoop while an application request is waiting;
- graceful unregister with clean-holder removal;
- reachable host-fence ACK and unreachable clean-host fail-closed behavior;
- abrupt owner disconnect and explicit host eviction;
- forced-data-loss reporting.

Every committed transition checks the stable-state invariants.

### Transport Conformance

The same scripted frame and MESI trace runs over:

1. loopback TCP;
2. v2 shared-memory duplex queues;
3. an in-memory RDMA adapter/CQ mock;
4. live RDMA when RDMA hardware and libraries are available.

Golden 192-byte frame fixtures are shared across the C++ server, QEMU C client,
and Splash/QEMUless client to detect ABI drift.

### Integration Tests

QEMU tests use two Type-3 endpoints with distinct host IDs over TCP and SHM.
Accesses observed at the Type-3 callbacks must produce endpoint-cache hits,
snoops, data forwarding, and invariant-clean directory traces. A guest workload
may drive those accesses, but its ordinary CPU loads are not an end-to-end
coherence oracle unless the mapping is verified uncached or `NATIVE_FLUSH` is
active.

QEMUless tests use multiple ranks with explicit host IDs and independent SHM
slots. The live target is eight ranks on four MIG instances, two ranks per MIG,
over TCP and SHM. It must preserve the passing legacy v1 MIG experiment while
adding a separate v2 coherence result.

RDMA completion is reported at two levels:

- build plus mock/conformance proof is always required;
- live RDMA proof is required only when a usable RDMA device and dependencies
  are present, and otherwise is reported explicitly as not live-verified.

ThreadSanitizer is used for transport-neutral and SHM tests where dependencies
permit it. Randomized traces compare the implementation against the reference
model and stop on the first invariant or data mismatch.

### Pass Criteria

A v2 proof passes only when:

- no directory invariant is violated;
- every granted permission followed all required matching ACKs;
- stale session/epoch messages caused no state change;
- every protocol read and atomic observed the latest committed or
  owner-supplied data;
- timeout tests show reconciled partial effects and no requester grant;
- all endpoint caches finish without unresolved transient lines;
- forced data loss is absent;
- the expected transport was actually exercised;
- the machine-readable artifact and traces agree.

A successful build, a legacy counter increment, or a model-level ACK alone is
not proof of hardware CPU cache coherence.

## QEMU Proof Boundary

The QEMU CXL fixed-memory-window callbacks route guest accesses through
`cxl_type3_read()` and `cxl_type3_write()`, and the P2P fallback path calls the
same functions. Protocol v2 can therefore prove coherence for accesses that
traverse those device-model callbacks.

The ACK proves that the QEMU endpoint cache model completed the requested state
transition and, for data snoops, returned its modeled dirty line. It does not
prove that QEMU caused a native guest CPU `clflush`, invalidated a physical CPU
cache, or implemented hardware CXL.cache. Those stronger claims require a
negotiated and measured `NATIVE_FLUSH` implementation.

Consequently, a v2 model-level pass may claim coherence only for
callback-observed transactions and endpoint-cache state. End-to-end claims
about repeated guest CPU loads require either a verified uncached mapping or
`NATIVE_FLUSH`; without one of those, guest result values are supporting
workload evidence rather than a cache-coherence proof.

## Repository Boundaries

CXLMemSim owns:

- the canonical v2 wire definition and golden fixtures;
- the refactored `CoherencyEngine`;
- endpoint-session registry and server transport adapters;
- v2 TCP, RDMA, and SHM server support;
- directory, timeout, eviction, observability, and server tests.

The nested QEMU checkout owns:

- Type-3 v2 configuration properties;
- the multiplexed client dispatcher and progress thread;
- the bounded Type-3 endpoint cache;
- v2 TCP/RDMA/SHM endpoint adapters;
- QEMU-focused transition and integration tests.

Splash owns:

- QEMUless host-ID configuration;
- the bounded PGAS endpoint cache;
- its progress thread and TCP/RDMA/SHM adapters;
- MPI/rank and MIG integration tests.

The server header is the canonical wire contract. QEMU and Splash maintain
C-compatible mirrors validated against the same golden encoded frames and
compile-time size/offset assertions. Cross-repository changes are developed in
isolated worktrees and merged without reverting unrelated user modifications.

## Acceptance Boundary

This feature is complete only when protocol v2 is explicitly selectable,
registration and host identity are enforced, all three transports implement the
duplex frame contract, QEMU Type-3 and QEMUless use bounded write-back endpoint
caches, server-side MESI transactions pass the invariant suite, and TCP/SHM live
artifacts demonstrate snoop/data/timeout behavior.

Live RDMA is part of the acceptance evidence only on a host with usable RDMA
hardware. Native guest CPU cache invalidation, Type-1/Type-2 endpoints, and SCC
remain separate future work and are not implied by a v2 pass.
