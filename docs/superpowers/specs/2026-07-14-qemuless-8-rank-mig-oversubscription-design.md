# QEMULess 8-Rank MIG Oversubscription Design

## Objective

Build a reproducible, single-host QEMULess experiment that runs eight logical MPI nodes on four real MIG instances, with two ranks sharing each MIG instance. The experiment must execute CUDA work directly on the host, route checkpoint traffic through Splash's PGAS shared-memory backend into CXLMemSim, and produce machine-readable evidence for both GPU oversubscription and non-zero CXL activity.

The experiment does not launch QEMU and does not modify QEMU integration code.

## Hardware Contract

The current host exposes one NVIDIA RTX PRO 6000 Blackwell Server Edition. Its smallest available GPU instance profile is profile ID `14`, named `MIG 1g.24gb`, with a maximum of four instances. The experiment therefore uses four real MIG instances rather than software aliases.

The required eight-rank mapping is:

| MIG index | MPI ranks |
| --- | --- |
| 0 | 0, 4 |
| 1 | 1, 5 |
| 2 | 2, 6 |
| 3 | 3, 7 |

Equivalently, each local rank selects MIG index `rank % 4`. A launcher wrapper resolves the corresponding MIG UUID and exports it as `CUDA_VISIBLE_DEVICES` before starting the workload.

MIG configuration is a deliberate setup action because enabling or recreating MIG instances disrupts active GPU users. The setup path must refuse to modify MIG state while conflicting compute processes are present. A successful run leaves the four-instance MIG configuration in place instead of disabling MIG during cleanup.

## Architecture

One physical host runs the following components:

1. One CXLMemSim server in `pgas-shm` mode, using `/cxlmemsim_pgas` and a default configured capacity of 512 GiB (`524288` MiB).
2. One Open MPI job for the four-rank baseline, with one rank bound to each MIG instance.
3. One Open MPI job for the eight-rank oversubscription run, with two ranks bound to each MIG instance.
4. One Splash MPI/CUDA workload per rank. Each process runs CUDA kernels on its assigned MIG and uses Splash's shared-memory backend for checkpoint write/read traffic.

The Splash shared-memory protocol is structurally compatible with CXLMemSim's PGAS shared-memory request slots. The integration uses that backend directly rather than the existing TCP-only MPI CXLMemSim shim.

## Shared-Memory Slot Assignment

Splash currently derives a request slot from `pthread_self() % num_slots`. Thread identifiers are process-local, so separate MPI ranks can select the same slot and corrupt concurrent requests.

The SHM backend will accept an explicit client slot through configuration or environment. The launcher assigns slots from the MPI local rank:

- four-rank baseline: slots 0 through 3;
- eight-rank run: slots 0 through 7.

The backend validates that the selected slot is within the server-advertised slot count. It must fail connection setup on an invalid slot instead of falling back silently. Existing callers that do not provide a slot retain a backward-compatible automatic selection path, but the new launcher always provides one.

## Workload Data Flow

Each MPI rank performs the same deterministic sequence:

1. Initialize MPI and obtain world rank and world size.
2. Verify that exactly one CUDA device is visible to the process.
3. Record the visible MIG UUID, device name, and memory capacity.
4. Allocate configurable host and device vectors and initialize rank-specific input data.
5. Enter an MPI barrier so all ranks begin the measured CUDA section together.
6. Run a deterministic CUDA vector kernel for a configurable number of iterations.
7. Copy a configurable checkpoint chunk from device memory to host memory.
8. Write the checkpoint to a rank-owned CXL address range through the Splash SHM backend.
9. Clear the host checkpoint buffer, read the same bytes back through CXLMemSim, and restore them to device memory.
10. Run a validation kernel or copy the result back and verify the expected rank-specific checksum.
11. Emit one JSON object containing rank, MIG UUID, slot, CXL address range, CUDA timings, CXL byte counts, checksum, and pass/fail state.

Each rank owns a disjoint CXL address range derived from a fixed per-rank stride. All CXL transfers are split through the existing cache-line protocol, and the workload size defaults remain small enough for a practical proof run. Larger checkpoint sizes are command-line options rather than required defaults.

## Experiment Sequence

The top-level launcher performs these phases:

1. Capture host, CUDA driver, GPU, MIG, compiler, MPI, and git-state metadata.
2. Validate or create four `1g.24gb` MIG instances and record their UUIDs in stable index order.
3. Build the CXLMemSim server and the Splash QEMULess workload.
4. Start CXLMemSim in `pgas-shm` mode and wait for `/cxlmemsim_pgas` to become ready.
5. Run the four-rank baseline.
6. Run the eight-rank oversubscription case.
7. Stop the server cleanly so its controller summary is written to the artifact log.
8. Validate all rank records, MIG cardinalities, checksums, CXL byte counts, and server counters.
9. Write a consolidated `summary.json` and exit non-zero if any required check failed.

The launcher starts only CXLMemSim and host MPI/CUDA processes. QEMU binaries, disk images, and guest kernels are not part of the execution path.

## Evidence Contract

A successful artifact directory contains:

- `summary.json`: overall verdict, configuration, baseline and oversubscription metrics, and explicit validation checks;
- `ranks-baseline.jsonl`: four per-rank records;
- `ranks-oversub.jsonl`: eight per-rank records;
- `server.log`: CXLMemSim startup, request processing, and final controller summary;
- `mig-map.json`: ordered MIG UUIDs and rank assignments;
- `nvidia-smi-before.txt` and `nvidia-smi-after.txt`;
- `commands.log`: exact build and run commands;
- per-rank stdout/stderr logs when MPI output separation is available.

The final verdict is `pass` only when all of the following are true:

- the baseline has four passing rank records;
- the oversubscription run has eight passing rank records;
- the oversubscription records contain exactly four unique MIG UUIDs;
- each MIG UUID appears exactly twice in the oversubscription records;
- each rank used its expected SHM slot and a disjoint CXL range;
- every rank reports a valid CUDA checksum and non-zero CXL bytes written and read;
- CXLMemSim reports non-zero Load, Store, and Remote counters;
- the controller summary accounts for at least the eight oversubscription request slots;
- no QEMU command is launched by the harness.

The summary reports baseline versus oversubscribed wall time, aggregate kernel throughput, checkpoint throughput, and per-MIG rank occupancy. Performance changes are measurements, not pass thresholds; correctness and topology evidence determine pass/fail.

## Error Handling and Cleanup

The launcher fails before changing GPU state when prerequisites are missing or active GPU clients make MIG reconfiguration unsafe. It also fails on an unsupported MIG profile, fewer than four resulting MIG UUIDs, missing MPI/CUDA tools, build errors, server-ready timeout, invalid SHM slot, CUDA allocation or kernel errors, CXL request errors, checksum mismatch, malformed result records, or zero controller counters.

On failure after processes have started, the launcher terminates only the CXLMemSim and MPI processes it created, waits for them to exit, and preserves all available logs. It removes the experiment-owned PGAS shared-memory object when safe. It does not kill unrelated processes, remove user artifacts, or disable MIG automatically.

## Repository Boundaries

CXLMemSim owns the top-level orchestration, server invocation, artifact validation, and focused server-side regression coverage. Splash owns the MPI/CUDA workload, local-rank-to-MIG wrapper, deterministic SHM slot selection, and associated backend tests.

Implementation must preserve unrelated changes in both dirty worktrees. No QEMU source, QEMU launcher, guest library, or guest image changes are required.

## Testing Strategy

Testing is layered so failures are attributable:

1. A focused Splash backend test verifies explicit slot parsing, bounds checking, and backward-compatible fallback behavior.
2. A launcher mapping test uses fixture MIG UUIDs to verify four-rank and eight-rank assignments without changing live GPU state.
3. The Splash CUDA target is built with the installed CUDA toolchain and linked with MPI and the PGAS SHM backend.
4. A one-rank smoke run verifies CUDA execution and a SHM checkpoint round trip.
5. The four-rank baseline verifies one rank per MIG.
6. The eight-rank run verifies two ranks per MIG and all evidence-contract checks.
7. Existing focused CXLMemSim PGAS counter tests run alongside the new harness checks.

Live verification is complete only after the eight-rank run produces a passing `summary.json` and the server log contains non-zero controller counters.
