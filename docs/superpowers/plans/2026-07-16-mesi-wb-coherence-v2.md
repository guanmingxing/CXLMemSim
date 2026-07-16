# MESI-WB Coherence Protocol v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an explicitly enabled directory-based MESI write-back protocol whose server, QEMU Type-3 endpoint, and Splash QEMUless endpoint provide the same coherence semantics over TCP, shared memory, and RDMA while preserving the protocol-v1 legacy default.

**Architecture:** Add one fixed protocol-v2 frame and transport-neutral server interfaces. Refactor `CoherencyEngine` so its strict v2 core owns a 256-shard sparse directory, endpoint sessions, pending snoop transactions, timeout reconciliation, atomics, and fences; legacy methods remain unchanged. TCP, SHM, and RDMA adapters provide multiplexed duplex delivery only. QEMU and QEMUless endpoints each run a progress thread around a bounded 64-byte, four-way LRU write-back cache and acknowledge semantic snoop completion.

**Tech Stack:** C++20, C11, CMake/CTest, POSIX sockets and shared memory, Linux RDMA CM/libibverbs when available, QEMU Meson/qtest, MPI/Splash PGAS, Python 3 artifact validation.

---

## Global Execution Rules

- Work in isolated worktrees. Do not modify the dirty user checkouts.
- Use protocol v2 only when the server and endpoint are explicitly configured for `mesi-wb`.
- Preserve all v1 structs, SHM objects, RDMA messages, counters, and default CLI behavior.
- Use test-driven development for every production change: add the test, run it and observe the intended failure, implement the minimum behavior, rerun it, then run the relevant regression set.
- Use the fixed 64-byte line size and reject misaligned full-line operations at the ABI boundary.
- Do not let transport code mutate directory state or infer protocol mode.
- Keep server-side bytes authoritative in `I`, `S`, and `E`; the endpoint owner is authoritative in `M`.
- Commit each task separately with the subject shown at the end of that task.

## Task 1: Define and Validate the Protocol-v2 ABI

**Files:**
- Create: `include/coherence_protocol_v2.h`
- Create: `src/coherence_protocol_v2.cpp`
- Create: `tests/test_coherence_protocol_v2.cpp`
- Modify: `CMakeLists.txt`

- [ ] Add a failing ABI test that requires:
  - `sizeof(CoherenceFrame) == 192`;
  - magic bytes `CXV2`, protocol version 2, and little-endian encoding;
  - a 64-byte payload and explicit `payload_length`;
  - validation of host/session/request/snoop IDs, address alignment, flags, opcode, and status;
  - rejection of unknown mandatory flags, invalid payload lengths, and nonzero reserved bytes.
- [ ] Run:

```bash
cmake --build build-mesi-baseline -j4 --target test_coherence_protocol_v2
ctest --test-dir build-mesi-baseline -R test_coherence_protocol_v2 --output-on-failure
```

  Confirm that configuration or compilation fails because the ABI does not exist.
- [ ] Define the canonical wire types without compiler-dependent bit fields:

```cpp
namespace cxlmemsim::coherence::v2 {

constexpr std::array<std::byte, 4> kMagic{
    std::byte{'C'}, std::byte{'X'}, std::byte{'V'}, std::byte{'2'}};
constexpr std::uint16_t kVersion = 2;
constexpr std::size_t kLineSize = 64;
constexpr std::size_t kFrameSize = 192;

enum class Opcode : std::uint16_t {
    Register,
    RegisterResponse,
    Gets,
    Getm,
    Upgrade,
    Puts,
    Putm,
    AtomicCompareExchange,
    AtomicFetchAdd,
    Fence,
    Heartbeat,
    Unregister,
    Response,
    SnpInv,
    SnpDataInv,
    SnpDataDowngrade,
    SnoopAck,
    HostFence,
    HostFenceAck,
};

struct CoherenceFrame final {
    std::array<std::byte, 4> magic;
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
    std::array<std::byte, 64> payload;
    std::array<std::byte, 48> reserved;
};

static_assert(sizeof(CoherenceFrame) == kFrameSize);
}
```

- [ ] Implement explicit host-to-little-endian helpers, `validateFrame()`, opcode-specific validation, status-to-string helpers, and deterministic frame initialization that zeroes reserved fields.
- [ ] Add golden byte fixtures for `REGISTER`, `GETS`, `SNP_DATA_INV`, `SNOOP_ACK`, and `HEARTBEAT`. The fixtures will later be consumed by the C endpoint implementations.
- [ ] Rebuild and run the ABI test plus all existing CTest tests.
- [ ] Commit:

```bash
git add CMakeLists.txt include/coherence_protocol_v2.h src/coherence_protocol_v2.cpp tests/test_coherence_protocol_v2.cpp
git commit -m "protocol: define fixed MESI v2 frame"
```

## Task 2: Add Endpoint Registration, Resume, and Response Replay

**Files:**
- Create: `include/endpoint_session_registry.h`
- Create: `src/endpoint_session_registry.cpp`
- Create: `tests/test_endpoint_session_registry.cpp`
- Modify: `CMakeLists.txt`

- [ ] Add failing tests for fresh registration, duplicate active host rejection, host-range validation, missing `MODEL_SNOOP`, retained-session resume, mismatched resume rejection, response pinning, heartbeat watermark reclamation, and graceful close.
- [ ] Model transport identity without including socket/RDMA/SHM headers:

```cpp
using SessionId = std::uint64_t;
using ResponseSender =
    std::function<bool(const coherence::v2::CoherenceFrame &)>;

enum class SessionState { Active, OfflineRetained, Fenced, Closed };

struct RegistrationRequest {
    std::uint16_t host_id;
    SessionId requested_session_id;
    std::uint64_t capabilities;
    std::uint32_t cache_capacity;
    std::uint16_t cache_ways;
    std::string transport_name;
    ResponseSender sender;
};
```

- [ ] Run the focused test and confirm it fails before implementation.
- [ ] Implement a mutex-protected registry with:
  - maximum 64 configured hosts;
  - monotonically generated nonzero session IDs;
  - exact geometry/capability checks on resume;
  - `ACTIVE -> OFFLINE_RETAINED` on abrupt disconnect;
  - per-session pinned response map keyed by request ID;
  - replay in request-ID order after valid resume;
  - heartbeat response watermark reclamation;
  - reverse holder indexes for clean and modified lines;
  - no directory calls while holding the registry mutex.
- [ ] Add tests proving callbacks execute after registry locks are released and a reconnect can replay responses while another host registers.
- [ ] Run the new test and the full CTest suite.
- [ ] Commit:

```bash
git add CMakeLists.txt include/endpoint_session_registry.h src/endpoint_session_registry.cpp tests/test_endpoint_session_registry.cpp
git commit -m "server: add MESI endpoint session registry"
```

## Task 3: Refactor `CoherencyEngine` Around a Sparse Strict-MESI Directory

**Files:**
- Create: `include/mesi_directory.h`
- Create: `src/mesi_directory.cpp`
- Create: `tests/test_mesi_directory.cpp`
- Modify: `include/coherency_engine.h`
- Modify: `src/coherency_engine.cpp`
- Modify: `CMakeLists.txt`

- [ ] Add failing tests for all stable-state invariants:
  - untouched lines read as `I`, no owner, no sharers, server current;
  - `I -> E` on first `GETS`;
  - `E -> S` on a second reader;
  - `S -> S` when adding readers;
  - `I/E/S -> M` for `GETM`;
  - explicit `E -> M` `UPGRADE`;
  - no owner and sharer coexistence;
  - epoch increments exactly once per committed metadata transition;
  - sparse allocation and deterministic shard selection.
- [ ] Run the focused test and confirm it fails.
- [ ] Implement:

```cpp
enum class MesiState : std::uint8_t { I, S, E, M };

struct DirectorySnapshot {
    MesiState state;
    std::optional<std::uint16_t> owner;
    std::uint64_t sharers;
    std::uint64_t epoch;
    bool server_copy_current;
};

class MesiDirectory {
  public:
    explicit MesiDirectory(std::size_t shard_count = 256);
    std::shared_ptr<DirectoryEntry> getOrCreate(std::uint64_t line_address);
    std::optional<DirectorySnapshot> inspect(std::uint64_t line_address) const;
    std::size_t allocatedLineCount() const;
};
```

- [ ] Store each shard as `unordered_map<uint64_t, shared_ptr<DirectoryEntry>>`; release the shard lock before taking the entry transaction mutex.
- [ ] Add a strict v2 member owned by `CoherencyEngine` while preserving all existing public legacy methods and MOESI latency behavior. Do not reinterpret legacy `OWNED` as a v2 state.
- [ ] Add invariant validation in debug/test builds and transition counters for `GETS`, `GETM`, `UPGRADE`, `PUTS`, and `PUTM`.
- [ ] Run the focused test, existing coherence-related tests, and full CTest.
- [ ] Commit:

```bash
git add CMakeLists.txt include/mesi_directory.h src/mesi_directory.cpp tests/test_mesi_directory.cpp include/coherency_engine.h src/coherency_engine.cpp
git commit -m "coherence: add sharded sparse MESI directory"
```

## Task 4: Implement Pending Snoop Transactions and Hybrid Failure Reconciliation

**Files:**
- Create: `include/coherence_memory_backend.h`
- Create: `include/coherence_transport.h`
- Create: `include/mesi_transaction_engine.h`
- Create: `src/mesi_transaction_engine.cpp`
- Create: `tests/test_mesi_transaction_engine.cpp`
- Modify: `include/coherency_engine.h`
- Modify: `src/coherency_engine.cpp`
- Modify: `CMakeLists.txt`

- [ ] Build a deterministic fake memory backend and fake transport in the test. Add failing scenarios for:
  - `GETS` snooping an `M` owner for data and downgrading it;
  - `GETM` invalidating all `S` holders before grant;
  - `GETM` taking data from an `M` owner before grant;
  - `UPGRADE` invalidating other sharers without re-fetching data;
  - duplicate and stale ACK handling by snoop ID and epoch;
  - synchronous grant ordering after all required ACK effects commit;
  - timeout with zero ACKs preserving the prior state;
  - timeout after partial ACKs committing acknowledged invalidations and returning an error to the requester;
  - a requester disconnect after snoops leaving a legal stable state;
  - concurrent disjoint lines progressing independently.
- [ ] Run the focused test and observe failure.
- [ ] Define transport-neutral interfaces:

```cpp
class CoherenceTransport {
  public:
    virtual ~CoherenceTransport() = default;
    virtual bool sendToHost(std::uint16_t host_id,
                            const coherence::v2::CoherenceFrame &frame) = 0;
};

class CoherenceMemoryBackend {
  public:
    virtual ~CoherenceMemoryBackend() = default;
    virtual std::array<std::byte, 64> readLine(std::uint64_t address) = 0;
    virtual void writeLine(std::uint64_t address,
                           std::span<const std::byte, 64> data) = 0;
};
```

- [ ] Implement one pending transaction per line under the entry transaction mutex. Allocate monotonically increasing nonzero snoop IDs and record expected hosts, ACKed hosts, returned dirty data, requester request ID, starting epoch, and deadline.
- [ ] Implement hybrid ACK semantics:
  - each valid ACK effect is durable once accepted;
  - a timeout preserves non-ACKed holders but removes ACKed invalidated holders;
  - returned dirty data is written to the memory backend before owner removal;
  - the requester receives a timeout/error and no permission grant;
  - stale late ACKs are idempotently rejected or recognized without mutating a later epoch.
- [ ] Add a progress/timer API so timeout processing does not depend on a new application request.
- [ ] Route strict-v2 `CoherencyEngine` operations through this transaction engine.
- [ ] Run focused and full tests under normal build and ThreadSanitizer where available.
- [ ] Commit:

```bash
git add CMakeLists.txt include/coherence_memory_backend.h include/coherence_transport.h include/mesi_transaction_engine.h src/mesi_transaction_engine.cpp tests/test_mesi_transaction_engine.cpp include/coherency_engine.h src/coherency_engine.cpp
git commit -m "coherence: execute synchronous MESI snoop transactions"
```

## Task 5: Add Write-Back Operations, Atomics, Fences, and Host Eviction

**Files:**
- Create: `tests/test_mesi_writeback_and_atomics.cpp`
- Modify: `include/mesi_transaction_engine.h`
- Modify: `src/mesi_transaction_engine.cpp`
- Modify: `include/endpoint_session_registry.h`
- Modify: `src/endpoint_session_registry.cpp`
- Modify: `CMakeLists.txt`

- [ ] Add failing tests for:
  - clean `PUTS` from `S` and `E`;
  - dirty `PUTM` from `M`, including data-before-metadata commit order;
  - rejection of `PUTM` from a non-owner or wrong epoch;
  - compare-and-swap and fetch-and-add serialized after exclusive ownership;
  - atomic response data and memory-order epoch updates;
  - `FENCE` waiting for all earlier requests and all local `M` writebacks;
  - `UNREGISTER` removing clean holdings and rejecting a remaining `M`;
  - `HOST_FENCE` requiring ACK for clean-state removal;
  - explicit stopped-process assertion allowing clean-state administrative removal;
  - forced loss of an unreachable `M` owner returning and recording `DATA_LOSS`.
- [ ] Run the focused test and observe failure.
- [ ] Implement opcode handlers on the transaction engine. Atomics must acquire the line transaction mutex, reconcile any owner/sharers, modify the complete 64-byte line in the server backend, increment the epoch, and grant/retain the requested ownership.
- [ ] Implement per-session operation watermarks so `FENCE` waits for earlier requests without globally blocking unrelated hosts.
- [ ] Implement graceful unregister by snapshotting the reverse holder index, releasing the index lock, and revalidating one directory line at a time.
- [ ] Implement administrative host eviction with an auditable policy enum:

```cpp
enum class HostFailurePolicy {
    RequireFenceAck,
    AssertProcessStopped,
    ForceDataLoss,
};
```

- [ ] Add counters and structured records for timeout, partial ACK, forced clean removal, forced dirty loss, stale ACK, and invalid ownership.
- [ ] Run focused and full tests.
- [ ] Commit:

```bash
git add CMakeLists.txt tests/test_mesi_writeback_and_atomics.cpp include/mesi_transaction_engine.h src/mesi_transaction_engine.cpp include/endpoint_session_registry.h src/endpoint_session_registry.cpp
git commit -m "coherence: add writeback atomics and host fencing"
```

## Task 6: Build the Shared Endpoint Cache Reference Model

**Files:**
- Create: `include/endpoint_cache_v2.h`
- Create: `src/endpoint_cache_v2.cpp`
- Create: `tests/test_endpoint_cache_v2.cpp`
- Create: `tests/fixtures/mesi_v2_endpoint_traces.txt`
- Modify: `CMakeLists.txt`

- [ ] Add failing tests for a 256 KiB, four-way, 64-byte-line LRU cache:
  - read miss installs `S` or `E`;
  - write hit in `E` performs explicit `UPGRADE` before `M`;
  - write hit in `S` performs explicit `UPGRADE`;
  - write miss uses `GETM`;
  - dirty eviction emits `PUTM`, clean eviction emits `PUTS`;
  - snoop invalidation/downgrade returns data only when required;
  - a snoop racing a local miss/upgrade is serialized by the per-line transient gate;
  - duplicate snoops return a semantic completion tombstone;
  - bounded tombstone eviction never fabricates completion for an unknown snoop.
- [ ] Run the focused test and observe failure.
- [ ] Implement a transport-independent reference cache whose callbacks send commands and whose `handleSnoop()` method produces the exact ACK frame only after the cache state/data effect is complete.
- [ ] Store stable line state, tag, 64 bytes, LRU generation, committed epoch, and one transient operation marker. Do not hold a set lock while executing a transport callback.
- [ ] Add golden transition traces shared later by the C QEMUless and QEMU tests.
- [ ] Run focused and full tests, including a randomized trace that compares endpoint state with a simple reference oracle.
- [ ] Commit:

```bash
git add CMakeLists.txt include/endpoint_cache_v2.h src/endpoint_cache_v2.cpp tests/test_endpoint_cache_v2.cpp tests/fixtures/mesi_v2_endpoint_traces.txt
git commit -m "endpoint: add bounded MESI writeback cache model"
```

## Task 7: Add Multiplexed Duplex TCP and Server Dispatch

**Files:**
- Create: `include/coherence_tcp_transport.h`
- Create: `src/coherence_tcp_transport.cpp`
- Create: `include/coherence_server_v2.h`
- Create: `src/coherence_server_v2.cpp`
- Create: `tests/test_coherence_tcp_transport.cpp`
- Create: `tests/test_coherence_server_v2.cpp`
- Modify: `src/main_server.cc`
- Modify: `CMakeLists.txt`

- [ ] Add failing socket-pair tests for fragmented frame receive, multiple frames in one receive, concurrent unsolicited snoop and correlated response delivery, disconnect notification, and send serialization.
- [ ] Add failing server tests for mandatory `REGISTER`, explicit host/session validation, request-ID deduplication and pinned replay, protocol-v1 rejection on the v2 listener, and no fallback after protocol error.
- [ ] Run focused tests and observe failure.
- [ ] Implement one reader loop and a serialized writer queue per TCP session. Dispatch endpoint commands/ACKs to `CoherenceServerV2`; use the registry sender to enqueue responses and snoops.
- [ ] Add an owning `CoherenceServerV2` facade that wires the session registry, strict `CoherencyEngine`, memory backend, transport fanout, timeout progress thread, and structured metrics.
- [ ] Add server CLI options:

```text
--coherence legacy|mesi-wb
--coherence-port 9999
--max-hosts 64
--directory-shards 256
--snoop-timeout-ms 1000
--endpoint-cache-bytes 262144
--endpoint-cache-ways 4
```

  `legacy` remains the default and starts the existing server paths unchanged. `mesi-wb` starts only v2 endpoint listeners for coherent data access.
- [ ] Run TCP integration with two in-process endpoints: reader gets `E`, second reader causes `E -> S`, writer invalidates both, and the first reader later observes writer data.
- [ ] Run full CTest and a legacy demo smoke test.
- [ ] Commit:

```bash
git add CMakeLists.txt include/coherence_tcp_transport.h src/coherence_tcp_transport.cpp include/coherence_server_v2.h src/coherence_server_v2.cpp tests/test_coherence_tcp_transport.cpp tests/test_coherence_server_v2.cpp src/main_server.cc
git commit -m "server: add duplex TCP MESI v2 service"
```

## Task 8: Add Protocol-v2 Duplex Shared-Memory Queues

**Files:**
- Create: `include/coherence_shm_transport.h`
- Create: `src/coherence_shm_transport.cpp`
- Create: `tests/test_coherence_shm_transport.cpp`
- Modify: `src/main_server.cc`
- Modify: `CMakeLists.txt`

- [ ] Add failing multiprocess tests for explicit mailbox allocation, separate endpoint-to-server and server-to-endpoint fixed queues, asynchronous snoop delivery while a response is pending, queue-full backpressure, peer death detection, and cleanup/recreate.
- [ ] Run the focused test and observe failure.
- [ ] Define a v2-only shared-memory object name and layout. Do not reuse or resize `/cxlmemsim_comm`:

```cpp
struct alignas(64) CoherenceShmSlot {
    std::atomic<std::uint64_t> endpoint_head;
    std::atomic<std::uint64_t> endpoint_tail;
    std::atomic<std::uint64_t> server_head;
    std::atomic<std::uint64_t> server_tail;
    CoherenceFrame endpoint_to_server[kQueueDepth];
    CoherenceFrame server_to_endpoint[kQueueDepth];
};
```

- [ ] Use release/acquire publication, bounded wait with stop awareness, and explicit `CXL_SHM_SLOT` mailbox selection independent from coherence host ID.
- [ ] Add a server SHM progress loop that registers a sender for each active mailbox and reports disconnects to the session registry.
- [ ] Run a two-process coherence trace identical to the TCP trace and compare final bytes, directory snapshots, epochs, and counters.
- [ ] Run full CTest and `./run_demo.sh` to prove the legacy SHM object still works.
- [ ] Commit:

```bash
git add CMakeLists.txt include/coherence_shm_transport.h src/coherence_shm_transport.cpp tests/test_coherence_shm_transport.cpp src/main_server.cc
git commit -m "transport: add duplex SHM MESI v2 queues"
```

## Task 9: Add RDMA RC SEND/RECV Frame Transport

**Files:**
- Create: `include/coherence_rdma_transport.h`
- Create: `src/coherence_rdma_transport.cpp`
- Create: `tests/test_coherence_rdma_transport.cpp`
- Modify: `src/main_server.cc`
- Modify: `CMakeLists.txt`

- [ ] Add a failing transport-contract test using a fake verbs backend for receive-credit replenishment, send queueing, unsolicited snoops, completion errors, disconnect, and shutdown wakeup. Keep the test enabled when libibverbs hardware is absent.
- [ ] Run the focused test and observe failure.
- [ ] Implement protocol-v2 RDMA as RC SEND/RECV messages containing exactly one `CoherenceFrame`. Maintain multiple posted receive work requests and a completion progress thread; do not reuse the legacy single `RDMAMessage` request/response buffer.
- [ ] Keep all verbs types behind a private implementation so the server and unit tests depend only on `CoherenceTransport`.
- [ ] Add a compile-time no-RDMA implementation that returns a clear unsupported status without silently switching transport.
- [ ] When an RDMA device is available, run the same two-endpoint coherence trace and record adapter/device/port/GID details. Otherwise mark live RDMA proof `SKIPPED_NO_DEVICE` while retaining fake-backend semantic proof.
- [ ] Run full CTest with `CXLMEMSIM_ENABLE_RDMA=OFF` and, when libraries are installed, with it enabled.
- [ ] Commit:

```bash
git add CMakeLists.txt include/coherence_rdma_transport.h src/coherence_rdma_transport.cpp tests/test_coherence_rdma_transport.cpp src/main_server.cc
git commit -m "transport: add duplex RDMA MESI v2 frames"
```

## Task 10: Implement the Splash QEMUless Endpoint

**Worktree:** Create a clean sibling Splash worktree from the current `main` and a branch named `codex/mesi-wb-v2-qemuless-20260716`.

**Files in Splash:**
- Create: `src/libpgas/include/pgas/coherence_protocol_v2.h`
- Create: `src/libpgas/include/pgas/cxlmemsim_mesi_client.h`
- Create: `src/libpgas/src/cxlmemsim_mesi_client.c`
- Create: `src/libpgas/tests/test_cxlmemsim_mesi_client.c`
- Modify: `src/libpgas/src/cxlmemsim_client.c`
- Modify: `src/libpgas/src/cxl_backend_shmem.c`
- Modify: `src/libpgas/src/pgas_cxlmemsim.c`
- Modify: the owning Splash/libpgas build files discovered in the worktree

- [ ] Add failing C tests that consume CXLMemSim golden wire fixtures and endpoint traces, covering byte layout, registration, bounded cache replacement, explicit upgrade, dirty writeback, snoop completion, duplicate snoop tombstones, and progress-thread shutdown.
- [ ] Run the focused test and observe failure.
- [ ] Copy the canonical C-compatible constants/layout with `_Static_assert(sizeof(...) == 192)` and fixture comparison. Keep one checked-in fixture source of truth in CXLMemSim and validate the Splash copy in the integration harness.
- [ ] Implement a QEMUless client with:
  - explicit `CXL_COHERENCE=mesi-wb`;
  - `CXL_COHERENCE_TRANSPORT=tcp|shm|rdma`;
  - `CXL_SHM_HOST_ID`, defaulting to MPI rank;
  - `CXL_SHM_SLOT` used only for the SHM mailbox;
  - a 256 KiB, four-way cache by default;
  - a progress thread that receives responses and unsolicited snoops;
  - request-ID waiters independent from snoop handling;
  - graceful `FENCE` and `UNREGISTER`.
- [ ] Route PGAS loads/stores through the v2 endpoint only when explicitly enabled. Preserve current legacy SHM and counter paths otherwise.
- [ ] Add an in-process/mock transport test, then run against a local CXLMemSim server over TCP and SHM.
- [ ] Commit in the Splash worktree:

```bash
git add src/libpgas
git commit -m "pgas: add MESI writeback endpoint client"
```

## Task 11: Implement the QEMU Type-3 Endpoint

**Worktree:** Create a clean QEMU worktree from the CXLMemSim gitlink commit and a branch named `codex/mesi-wb-v2-qemu-20260716`.

**Files in QEMU:**
- Create: `hw/mem/cxl_memsim_v2.h`
- Create: `hw/mem/cxl_memsim_v2.c`
- Create: `tests/unit/test-cxl-memsim-v2.c`
- Modify: `hw/mem/cxl_type3.c`
- Modify: `hw/mem/meson.build`
- Modify: `tests/unit/meson.build`

- [ ] Add failing GLib unit tests using CXLMemSim golden fixtures and endpoint traces for frame layout, cache transitions, dirty eviction, duplicate snoops, and progress-thread lifecycle.
- [ ] Run the focused QEMU unit test and observe failure.
- [ ] Implement a per-device, not process-global, v2 endpoint object with:
  - `cxl-coherence=legacy|mesi-wb`;
  - `cxl-host-id=<uint16>`;
  - `cxl-coherence-transport=tcp|shm|rdma`;
  - configurable server address/port, SHM slot, cache bytes, and ways;
  - one progress thread and request waiter map;
  - bounded write-back cache and semantic snoop ACKs;
  - graceful realize/unrealize registration and fence.
- [ ] Modify `cxl_type3_read()` and `cxl_type3_write()` only at the existing CXLMemSim callback boundary. In legacy mode preserve the current global client behavior. In v2 mode require the per-device endpoint and never fall back to local backing memory after a protocol error.
- [ ] Add trace events stating `ack_strength=model`; do not advertise or claim native guest CPU cache invalidation.
- [ ] Run the focused unit test and the relevant QEMU CXL qtests/build target.
- [ ] Commit in the QEMU worktree:

```bash
git add hw/mem/cxl_memsim_v2.h hw/mem/cxl_memsim_v2.c hw/mem/cxl_type3.c hw/mem/meson.build tests/unit/test-cxl-memsim-v2.c tests/unit/meson.build
git commit -m "cxl/type3: add MESI writeback endpoint model"
```

## Task 12: Cross-Transport Correctness, Observability, and the 8-Rank MIG Experiment

**Files in CXLMemSim:**
- Create: `tests/test_mesi_v2_cross_transport.cpp`
- Create: `scripts/run_mesi_v2_correctness.sh`
- Create: `scripts/run_qemuless_mig_mesi_v2.sh`
- Create: `scripts/validate_mesi_v2_artifacts.py`
- Create: `docs/mesi_v2_experiment.md`
- Modify: `src/coherence_server_v2.cpp`
- Modify: `CMakeLists.txt`
- Modify: existing MIG summary scripts/tests as required by their current owning paths

- [ ] Add a failing cross-transport test that runs one deterministic trace through TCP and SHM adapters and requires byte-identical final memory, legal directory snapshots, matching epochs, and matching transition/snoop counters. Include RDMA when live or fake transport support is available.
- [ ] Add JSONL tracing for registration, request, snoop send, ACK, timeout, transition commit, response, fence, and host eviction. Include transport, host ID, session ID, request ID, snoop ID, address, old/new state, old/new epoch, ACK strength, and status.
- [ ] Add metrics for commands, state transitions, snoops by type, ACKs, duplicate/stale ACKs, partial timeouts, response replays, cache writebacks, forced host removal, and dirty data loss.
- [ ] Implement `scripts/run_mesi_v2_correctness.sh` to:
  - build an isolated Release configuration;
  - start a v2 server;
  - run the two-reader/one-writer trace over TCP and SHM;
  - conditionally run RDMA;
  - run a reconnect/replay trace;
  - run a partial-ACK timeout trace;
  - save commands, logs, JSONL traces, summary JSON, and environment metadata.
- [ ] Implement the approved QEMUless topology in `scripts/run_qemuless_mig_mesi_v2.sh`:
  - eight MPI ranks on one physical node;
  - four MIG devices, each assigned to exactly two ranks;
  - eight distinct coherence host IDs;
  - TCP and SHM runs required;
  - RDMA run conditional on a usable local RDMA device;
  - Splash oversubscription workload and parameters recorded verbatim;
  - per-rank logs and server trace preserved.
- [ ] Make artifact validation fail unless:
  - all eight ranks registered and completed;
  - the rank-to-MIG mapping is exactly two ranks per MIG;
  - the server observed nonzero `GETS`, `GETM`/`UPGRADE`, snoop, ACK, and writeback counts;
  - no directory invariant, protocol fallback, lost dirty data, or unacknowledged required snoop occurred;
  - TCP and SHM final checksums match;
  - every result names its proof boundary and records RDMA as pass or `SKIPPED_NO_DEVICE`.
- [ ] Run:

```bash
cmake -S . -B build-mesi-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-mesi-release -j4
ctest --test-dir build-mesi-release --output-on-failure
./run_demo.sh
./run_protocol_demo.sh
./scripts/run_mesi_v2_correctness.sh
./scripts/run_qemuless_mig_mesi_v2.sh
python3 scripts/validate_mesi_v2_artifacts.py <artifact-directory>
```

- [ ] Run `clang-format` on touched C/C++ files and the repository's available `clang-tidy` checks. Re-run focused tests after formatting.
- [ ] Write `docs/mesi_v2_experiment.md` with exact commands, commits for all three repositories, host/MIG topology, transport status, result tables, artifact paths, known limitations, and the explicit statement that QEMU proof covers callback-observed model coherence rather than native CPU cache coherence.
- [ ] Commit:

```bash
git add CMakeLists.txt src/coherence_server_v2.cpp tests/test_mesi_v2_cross_transport.cpp scripts/run_mesi_v2_correctness.sh scripts/run_qemuless_mig_mesi_v2.sh scripts/validate_mesi_v2_artifacts.py docs/mesi_v2_experiment.md
git commit -m "tests: prove MESI v2 across transports and MIG ranks"
```

## Task 13: Review and Integrate the Three Branches

- [ ] Run a specification-compliance review against
  `docs/superpowers/specs/2026-07-16-mesi-wb-coherence-v2-design.md`. Resolve every missing requirement or document a measured environmental blocker.
- [ ] Run a code-quality review focused on lock order, callback-under-lock bugs, frame parsing, stale ACK handling, disconnect races, bounded memory, and legacy-path regressions.
- [ ] Re-run all verification commands from Task 12 after review fixes.
- [ ] Merge the Splash endpoint branch into Splash `main` locally without discarding unrelated user changes. If the original checkout is dirty, merge only after proving the merge can preserve those changes.
- [ ] Merge the QEMU endpoint branch into the QEMU gitlink branch expected by CXLMemSim, update the CXLMemSim gitlink in the isolated integration branch, and preserve unrelated QEMU changes.
- [ ] Merge the CXLMemSim implementation branch into local `main` without reverting unrelated main-worktree changes. Resolve conflicts by retaining the user's changes and the verified v2 feature.
- [ ] Push the resulting branches only after all local merge and artifact validation gates pass.
- [ ] Record final commit IDs and artifact paths in the final report.
