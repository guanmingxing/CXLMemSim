# QEMULess 8-Rank MIG Oversubscription Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run and validate four-rank baseline and eight-rank QEMULess CUDA jobs on four real MIG instances, with Splash checkpoint traffic passing through CXLMemSim PGAS shared memory.

**Architecture:** A host launcher configures four `MIG 1g.24gb` instances, starts one 512 GiB logical CXLMemSim server with PGAS request slots and a sparse streaming backing file, then launches Splash MPI/CUDA jobs. A rank wrapper maps `local_rank % 4` to a MIG UUID and `local_rank` to a unique SHM request slot; a result validator turns rank JSON files and the controller log into one pass/fail summary.

**Tech Stack:** C++17, CUDA Runtime API, Open MPI, C11, POSIX shared memory, Bash, Python 3 standard library, CMake/CTest.

## Global Constraints

- Run eight logical MPI nodes on one physical host.
- Use four real profile-ID `14` `MIG 1g.24gb` instances; do not simulate additional MIG devices.
- Map ranks 0/4, 1/5, 2/6, and 3/7 to MIG indices 0, 1, 2, and 3 respectively.
- Use direct host CUDA and Splash PGAS SHM; do not launch or modify QEMU.
- Keep CXLMemSim logical capacity at 512 GiB (`524288` MiB).
- Leave a successful four-instance MIG configuration enabled after the run.
- Refuse MIG reconfiguration while unrelated compute processes are active.
- Preserve all unrelated changes in both dirty repositories.
- A pass requires correct CUDA/checkpoint validation, exactly two ranks per MIG in the eight-rank run, non-zero CXL read/write bytes per rank, and non-zero server Load/Store/Remote counters.

---

### Task 1: Make Splash SHM Client Slots Deterministic and Unmap Exact Mappings

**Files:**
- Modify: `../Splash/src/libpgas/src/cxl_backend_shmem.c`
- Create: `../Splash/src/libpgas/tests/test_shmem_client_slot.c`
- Modify: `../Splash/CMakeLists.txt`

**Interfaces:**
- Consumes: environment variable `CXL_SHM_SLOT`, server header fields `num_slots` and `server_ready`.
- Produces: explicit per-process slot selection; exact `mapped_size` cleanup; CTest target `test_shmem_client_slot`.

- [ ] **Step 1: Write the failing SHM slot test**

Create a test that makes a minimal request-slot-only POSIX SHM object, connects a client with valid slot `7`, rejects slot `8` for an eight-slot header, rejects non-numeric input, and retains fallback behavior when the variable is absent:

```c
#define _GNU_SOURCE
#include "pgas/cxl_backend.h"
#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static const char *kName = "/splash_test_shmem_slot";

static void create_fixture(void) {
    shm_unlink(kName);
    int fd = shm_open(kName, O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(fd >= 0);
    size_t bytes = CXL_SHM_HEADER_SIZE(8);
    assert(ftruncate(fd, (off_t)bytes) == 0);
    cxl_shm_header_t *header = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(header != MAP_FAILED);
    memset(header, 0, bytes);
    header->magic = CXL_SHM_MAGIC;
    header->version = CXL_SHM_VERSION;
    header->num_slots = 8;
    header->memory_size = 512ULL * 1024 * 1024 * 1024;
    __atomic_store_n(&header->server_ready, 1, __ATOMIC_RELEASE);
    munmap(header, bytes);
    close(fd);
}

static int connect_client(const char *slot) {
    if (slot) setenv("CXL_SHM_SLOT", slot, 1);
    else unsetenv("CXL_SHM_SLOT");
    cxl_backend_config_t config = {0};
    config.type = CXL_BACKEND_SHMEM;
    strncpy(config.shmem.shm_name, kName, sizeof(config.shmem.shm_name) - 1);
    config.shmem.is_server = false;
    cxl_backend_t *backend = cxl_backend_create(CXL_BACKEND_SHMEM, &config);
    assert(backend != NULL);
    int rc = backend->ops->connect(backend);
    cxl_backend_destroy(backend);
    return rc;
}

int main(void) {
    create_fixture();
    assert(connect_client("7") == 0);
    assert(connect_client("8") != 0);
    assert(connect_client("invalid") != 0);
    assert(connect_client(NULL) == 0);
    shm_unlink(kName);
    return 0;
}
```

- [ ] **Step 2: Register and run the test to verify RED**

Add this target near the existing backend test in `../Splash/CMakeLists.txt`:

```cmake
enable_testing()
add_executable(test_shmem_client_slot src/libpgas/tests/test_shmem_client_slot.c)
target_include_directories(test_shmem_client_slot PRIVATE
    ${CMAKE_SOURCE_DIR}/src/libpgas/include
    ${CMAKE_SOURCE_DIR}/src/libpgas/include/pgas
)
target_link_libraries(test_shmem_client_slot cxl_backend pthread rt)
add_test(NAME test_shmem_client_slot COMMAND test_shmem_client_slot)
```

Run:

```bash
cmake -S ../Splash -B /tmp/splash-qemuless-build -DCMAKE_BUILD_TYPE=Release
cmake --build /tmp/splash-qemuless-build --target test_shmem_client_slot -j
ctest --test-dir /tmp/splash-qemuless-build -R test_shmem_client_slot --output-on-failure
```

Expected: FAIL because invalid `CXL_SHM_SLOT` values are ignored and the old disconnect path derives an incorrect unmap size from the advertised 512 GiB logical capacity.

- [ ] **Step 3: Implement deterministic slot parsing and exact unmap size**

Extend `shmem_priv_t` with `size_t mapped_size`. Set it to the exact `mmap()` length on both server and client paths. Add this helper and use it after the server-ready check:

```c
static int resolve_client_slot(uint32_t num_slots) {
    const char *value = getenv("CXL_SHM_SLOT");
    if (!value || value[0] == '\0') {
        return (int)((uintptr_t)pthread_self() % num_slots);
    }

    errno = 0;
    char *end = NULL;
    long slot = strtol(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || slot < 0 ||
        (unsigned long)slot >= num_slots) {
        fprintf(stderr, "cxl_shmem: Invalid CXL_SHM_SLOT=%s for %u slots\n", value, num_slots);
        return -1;
    }
    return (int)slot;
}
```

Replace the `pthread_self()` assignment with `resolve_client_slot()`. On failure, unmap `priv->mapped_size`, close the fd, clear pointers, and return `-1`. In `shmem_disconnect()`, use only:

```c
if (priv->header && priv->mapped_size > 0) {
    munmap(priv->header, priv->mapped_size);
    priv->header = NULL;
    priv->mapped_size = 0;
}
```

- [ ] **Step 4: Run focused and existing backend tests**

Run:

```bash
cmake --build /tmp/splash-qemuless-build --target test_shmem_client_slot test_cxl_backends -j
ctest --test-dir /tmp/splash-qemuless-build -R test_shmem_client_slot --output-on-failure
/tmp/splash-qemuless-build/test_cxl_backends
```

Expected: the slot test passes; the existing backend test exits zero.

- [ ] **Step 5: Commit the Splash backend change**

```bash
git -C ../Splash add src/libpgas/src/cxl_backend_shmem.c src/libpgas/tests/test_shmem_client_slot.c CMakeLists.txt
git -C ../Splash diff --cached --check
git -C ../Splash commit -m "pgas: assign deterministic SHM client slots"
```

### Task 2: Add the Splash MPI/CUDA Checkpoint Workload

**Files:**
- Create: `../Splash/src/qemuless_mig_oversubscribe.cu`
- Modify: `../Splash/CMakeLists.txt`

**Interfaces:**
- Consumes: `CUDA_VISIBLE_DEVICES`, `CXL_SHM_SLOT`, `--shm-name`, `--result-dir`, `--checkpoint-bytes`, and `--iterations`.
- Produces: target `qemuless_mig_oversubscribe`; one `rank-N.json` file per MPI rank; exit zero only when CUDA and CXL round-trip validation pass globally.

- [ ] **Step 1: Add a CUDA target that initially fails because the source is absent**

Add optional CUDA discovery and the target:

```cmake
include(CheckLanguage)
check_language(CUDA)
if(CMAKE_CUDA_COMPILER)
    enable_language(CUDA)
    find_package(CUDAToolkit REQUIRED)
    add_executable(qemuless_mig_oversubscribe src/qemuless_mig_oversubscribe.cu)
    target_include_directories(qemuless_mig_oversubscribe PRIVATE
        ${CMAKE_SOURCE_DIR}/src/libpgas/include
        ${CMAKE_SOURCE_DIR}/src/libpgas/include/pgas
    )
    target_link_libraries(qemuless_mig_oversubscribe
        MPI::MPI_CXX cxl_backend CUDA::cudart pthread rt
    )
    set_target_properties(qemuless_mig_oversubscribe PROPERTIES
        CUDA_STANDARD 17
        CUDA_STANDARD_REQUIRED ON
    )
endif()
```

Run:

```bash
cmake -S ../Splash -B /tmp/splash-qemuless-build \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CUDA_COMPILER=/usr/local/cuda-12.8/bin/nvcc
cmake --build /tmp/splash-qemuless-build --target qemuless_mig_oversubscribe -j
```

Expected: configure or build fails because `src/qemuless_mig_oversubscribe.cu` is absent.

- [ ] **Step 2: Implement the deterministic CUDA and CXL round-trip workload**

Implement these exact data contracts in `qemuless_mig_oversubscribe.cu`:

```cpp
struct Options {
    std::string shm_name = "/cxlmemsim_pgas";
    std::string result_dir;
    size_t elements = 1U << 20;
    size_t checkpoint_bytes = 256U << 10;
    int iterations = 100;
};

struct RankResult {
    int rank;
    int world_size;
    int slot;
    std::string mig_uuid;
    std::string device_name;
    uint64_t cxl_addr;
    size_t checkpoint_bytes;
    size_t cxl_bytes_written;
    size_t cxl_bytes_read;
    double kernel_ms;
    double checkpoint_write_ms;
    double checkpoint_read_ms;
    bool cuda_valid;
    bool checkpoint_valid;
};

__global__ void vector_add(const float *a, const float *b, float *c, size_t n) {
    size_t i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) c[i] = a[i] + b[i];
}
```

The program must:

```cpp
MPI_Init(&argc, &argv);
MPI_Comm_rank(MPI_COMM_WORLD, &rank);
MPI_Comm_size(MPI_COMM_WORLD, &world_size);
if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count != 1) local_ok = 0;
cudaGetDeviceProperties(&properties, 0);
cudaSetDevice(0);
```

Initialize `a[i] = rank + i * 0.001f` and `b[i] = 2.0f + i * 0.002f`, run `iterations` vector-add kernels after `MPI_Barrier`, and time them with CUDA events. Copy the first `checkpoint_bytes` of `c` to `expected`, create a `CXL_BACKEND_SHMEM` client, and use a 16 MiB per-rank stride:

```cpp
uint64_t cxl_addr = (uint64_t)rank * (16ULL << 20);
int write_rc = CXL_BACKEND_BULK_WRITE(backend, cxl_addr, expected.data(), checkpoint_bytes);
std::fill(restored.begin(), restored.end(), 0);
int read_rc = CXL_BACKEND_BULK_READ(backend, cxl_addr, restored.data(), checkpoint_bytes);
bool checkpoint_valid = write_rc == 0 && read_rc == 0 && restored == expected;
```

Validate at least indices `0`, `elements / 2`, and `elements - 1` against the CPU formula with relative tolerance `1e-5`. Read the assigned MIG UUID directly from `CUDA_VISIBLE_DEVICES` and the slot from `CXL_SHM_SLOT`. Write a complete JSON object to `result_dir/rank-<rank>.json` using `std::ofstream`; include numeric CXL read/write bytes equal to `checkpoint_bytes` only on successful operations. Use `MPI_Allreduce(MPI_MIN)` on `local_ok` and return non-zero if any rank fails.

- [ ] **Step 3: Build and inspect the workload CLI**

Run:

```bash
cmake --build /tmp/splash-qemuless-build --target qemuless_mig_oversubscribe -j
/tmp/splash-qemuless-build/qemuless_mig_oversubscribe --help
```

Expected: build succeeds and help lists `--shm-name`, `--result-dir`, `--checkpoint-bytes`, `--elements`, and `--iterations`.

- [ ] **Step 4: Commit the Splash workload**

```bash
git -C ../Splash add src/qemuless_mig_oversubscribe.cu CMakeLists.txt
git -C ../Splash diff --cached --check
git -C ../Splash commit -m "gpu: add QEMULess MIG checkpoint workload"
```

### Task 3: Add and Test the Local-Rank MIG Wrapper

**Files:**
- Create: `../Splash/script/qemuless_mig_rank_wrapper.sh`
- Create: `../Splash/script/test_qemuless_mig_rank_wrapper.sh`

**Interfaces:**
- Consumes: `MIG_UUID_FILE`, Open MPI local-rank environment, and workload command arguments.
- Produces: `CUDA_VISIBLE_DEVICES=<uuid[index]>`, `CXL_SHM_SLOT=<local_rank>`, then `exec` of the workload.

- [ ] **Step 1: Write the mapping test first**

The test creates four fixture UUIDs, invokes the wrapper for ranks 0 through 7 with a probe command, and compares exact output:

```bash
#!/usr/bin/env bash
set -euo pipefail
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
printf '%s\n' MIG-A MIG-B MIG-C MIG-D > "$tmp/uuids"
for rank in 0 1 2 3 4 5 6 7; do
  output=$(OMPI_COMM_WORLD_LOCAL_RANK=$rank MIG_UUID_FILE="$tmp/uuids" \
    ./script/qemuless_mig_rank_wrapper.sh \
    bash -c 'printf "%s %s" "$CUDA_VISIBLE_DEVICES" "$CXL_SHM_SLOT"')
  expected_uuid=$(sed -n "$((rank % 4 + 1))p" "$tmp/uuids")
  test "$output" = "$expected_uuid $rank"
done
```

Run `bash ../Splash/script/test_qemuless_mig_rank_wrapper.sh` and expect failure because the wrapper is absent.

- [ ] **Step 2: Implement strict rank-to-MIG mapping**

```bash
#!/usr/bin/env bash
set -euo pipefail
: "${MIG_UUID_FILE:?MIG_UUID_FILE is required}"
rank=${OMPI_COMM_WORLD_LOCAL_RANK:-${PMI_LOCAL_RANK:-}}
[[ "$rank" =~ ^[0-9]+$ ]] || { echo "invalid local rank: $rank" >&2; exit 2; }
(( rank < 8 )) || { echo "local rank $rank exceeds supported slots 0-7" >&2; exit 2; }
mapfile -t mig_uuids < "$MIG_UUID_FILE"
(( ${#mig_uuids[@]} == 4 )) || { echo "expected exactly four MIG UUIDs" >&2; exit 2; }
export CUDA_VISIBLE_DEVICES=${mig_uuids[$((rank % 4))]}
export CXL_SHM_SLOT=$rank
exec "$@"
```

- [ ] **Step 3: Run mapping and malformed-input tests**

Run:

```bash
bash ../Splash/script/test_qemuless_mig_rank_wrapper.sh
OMPI_COMM_WORLD_LOCAL_RANK=8 MIG_UUID_FILE=/tmp/nonexistent \
  ../Splash/script/qemuless_mig_rank_wrapper.sh true
```

Expected: the mapping test passes; rank 8 exits non-zero.

- [ ] **Step 4: Commit the wrapper**

```bash
git -C ../Splash add script/qemuless_mig_rank_wrapper.sh script/test_qemuless_mig_rank_wrapper.sh
git -C ../Splash commit -m "gpu: map local ranks onto four MIG instances"
```

### Task 4: Add Machine-Readable Result Validation

**Files:**
- Create: `tools/qemuless_mig/summarize_results.py`
- Create: `tests/test_qemuless_mig_summary.py`
- Modify: `CMakeLists.txt`

**Interfaces:**
- Consumes: baseline and oversubscription rank JSON directories, `server.log`, and four-line MIG UUID file.
- Produces: `summary.json`; exit zero only when the evidence contract passes.

- [ ] **Step 1: Write failing Python unittest fixtures**

Use `unittest.TemporaryDirectory` to generate four baseline records, eight oversubscription records with UUID mapping `rank % 4`, and a server log containing:

```text
Global Counter:
  Local: 0
  Remote: 4096
Switch id=0:
  Events:
    Load: 2048
    Store: 2048
Statistics:
  Number of Threads created: 8
```

The test invokes the summarizer with `subprocess.run()`, asserts exit zero and `verdict == "pass"`, then changes one oversubscription checksum to false and asserts non-zero exit with `verdict == "fail"`.

- [ ] **Step 2: Run the test to verify RED**

```bash
python3 -m unittest tests/test_qemuless_mig_summary.py -v
```

Expected: FAIL because `tools/qemuless_mig/summarize_results.py` does not exist.

- [ ] **Step 3: Implement strict summary validation**

The script accepts:

```text
--baseline-dir PATH --oversub-dir PATH --server-log PATH
--mig-uuid-file PATH --output PATH
```

Load `rank-*.json` in numeric rank order. Validate record counts 4 and 8, all `cuda_valid` and `checkpoint_valid`, positive `cxl_bytes_read` and `cxl_bytes_written`, slot equal to rank, disjoint CXL ranges, and exact UUID occupancy. Parse counters with anchored regular expressions:

```python
remote = int(re.search(r"^\s*Remote:\s+(\d+)\s*$", log, re.MULTILINE).group(1))
loads = [int(v) for v in re.findall(r"^\s*Load:\s+(\d+)\s*$", log, re.MULTILINE)]
stores = [int(v) for v in re.findall(r"^\s*Store:\s+(\d+)\s*$", log, re.MULTILINE)]
threads = int(re.search(r"Number of Threads created:\s*(\d+)", log).group(1))
```

Write a JSON document with `verdict`, `checks`, `baseline`, `oversubscription`, `controller`, and `errors`. Return `0` for pass and `1` for fail; malformed inputs must still produce a fail summary when `--output` is writable.

- [ ] **Step 4: Register and run the unittest**

Add:

```cmake
find_package(Python3 COMPONENTS Interpreter)
if(Python3_Interpreter_FOUND)
    add_test(NAME test_qemuless_mig_summary
        COMMAND ${Python3_EXECUTABLE} -m unittest tests/test_qemuless_mig_summary.py -v)
    set_tests_properties(test_qemuless_mig_summary PROPERTIES
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif()
```

Run:

```bash
python3 -m unittest tests/test_qemuless_mig_summary.py -v
cmake -S . -B /tmp/cxlmemsim-qemuless-build \
  -DCMAKE_BUILD_TYPE=Release -DCXLMEMSIM_BUILD_MICROBENCHMARKS=OFF
ctest --test-dir /tmp/cxlmemsim-qemuless-build -R test_qemuless_mig_summary --output-on-failure
```

Expected: both pass.

- [ ] **Step 5: Commit the validator**

```bash
git add tools/qemuless_mig/summarize_results.py tests/test_qemuless_mig_summary.py CMakeLists.txt
git diff --cached --check
git commit -m "tools: validate QEMULess MIG evidence"
```

### Task 5: Add the End-to-End MIG and MPI Launcher

**Files:**
- Create: `tools/qemuless_mig/run_qemuless_mig.sh`
- Create: `tests/test_qemuless_mig_launcher.sh`

**Interfaces:**
- Consumes: optional `--configure-mig`, `--artifact-dir`, `--splash-dir`, `--checkpoint-bytes`, and `--iterations`.
- Produces: a complete artifact directory and the exit status from `summarize_results.py`.

- [ ] **Step 1: Write a launcher dry-run test**

The test creates fixture `nvidia-smi`, `cmake`, and `mpirun` commands ahead of `PATH`, runs the launcher with `--dry-run --artifact-dir "$tmp/artifact"`, and asserts `commands.log` contains:

```text
--comm-mode=pgas-shm
--capacity=524288
mpirun --allow-run-as-root --oversubscribe --bind-to none -np 4
mpirun --allow-run-as-root --oversubscribe --bind-to none -np 8
```

It also asserts no command contains `qemu-system` and `mig-map.json` maps ranks by modulo four.

- [ ] **Step 2: Run the dry-run test to verify RED**

```bash
bash tests/test_qemuless_mig_launcher.sh
```

Expected: FAIL because the launcher is absent.

- [ ] **Step 3: Implement preflight, build, run, and cleanup phases**

Use `set -euo pipefail`, an owned PID variable, and an EXIT trap. Defaults are:

```bash
capacity_mb=524288
checkpoint_bytes=$((256 * 1024))
iterations=100
shm_name=/cxlmemsim_pgas
splash_dir=$(realpath "${repo_root}/../Splash")
cxl_build=/tmp/cxlmemsim-qemuless-build
splash_build=/tmp/splash-qemuless-build
artifact_dir="${repo_root}/artifact/qemuless_mig/$(date -u +%Y%m%dT%H%M%SZ)"
```

Before MIG changes, query active compute PIDs with:

```bash
nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits
```

If four MIG UUIDs are not already present and `--configure-mig` is absent, fail with an actionable message. If configuration is requested, require no active compute PIDs and run:

```bash
sudo nvidia-smi -i 0 -mig 1
sudo nvidia-smi mig -i 0 -dci || true
sudo nvidia-smi mig -i 0 -dgi || true
sudo nvidia-smi mig -i 0 -cgi 14,14,14,14 -C
```

Extract exactly four UUIDs matching `MIG-[^)]*` from `nvidia-smi -L`. Write the UUIDs in device order to `mig-uuids.txt`, then create `mig-map.json` with:

```bash
jq -Rn '[inputs] as $u | {migs:$u, ranks:[range(0;8) | {rank:., mig_index:(. % 4), mig_uuid:$u[. % 4]}]}' \
  < "$artifact_dir/mig-uuids.txt" > "$artifact_dir/mig-map.json"
```

Configure isolated builds with `/usr/local/cuda-12.8/bin/nvcc`. Start CXLMemSim with a sparse streaming file so 512 GiB logical capacity does not allocate a 1 TiB PGAS entry table:

```bash
"$cxl_build/cxlmemsim_server" \
  --comm-mode=pgas-shm \
  --pgas-shm-name="$shm_name" \
  --capacity="$capacity_mb" \
  --default_latency=100 \
  --ssd-backing-file="$artifact_dir/cxlmemsim.ssd" \
  --ssd-cache-mb=64 \
  --ssd-io-uring=false \
  --ssd-odirect=false
```

Wait up to 30 seconds for `/dev/shm/${shm_name#/}`. Launch baseline and oversubscription jobs with:

```bash
mpirun --allow-run-as-root --oversubscribe --bind-to none -np 4 \
  -x MIG_UUID_FILE="$artifact_dir/mig-uuids.txt" \
  "$splash_dir/script/qemuless_mig_rank_wrapper.sh" \
  "$splash_build/qemuless_mig_oversubscribe" \
  --shm-name "$shm_name" --result-dir "$artifact_dir/baseline" \
  --checkpoint-bytes "$checkpoint_bytes" --iterations "$iterations"

mpirun --allow-run-as-root --oversubscribe --bind-to none -np 8 \
  -x MIG_UUID_FILE="$artifact_dir/mig-uuids.txt" \
  "$splash_dir/script/qemuless_mig_rank_wrapper.sh" \
  "$splash_build/qemuless_mig_oversubscribe" \
  --shm-name "$shm_name" --result-dir "$artifact_dir/oversub" \
  --checkpoint-bytes "$checkpoint_bytes" --iterations "$iterations"
```

After each MPI phase, convert the rank files into the evidence-contract JSONL files:

```bash
: > "$artifact_dir/ranks-baseline.jsonl"
for result in "$artifact_dir"/baseline/rank-*.json; do
  jq -c . "$result" >> "$artifact_dir/ranks-baseline.jsonl"
done
: > "$artifact_dir/ranks-oversub.jsonl"
for result in "$artifact_dir"/oversub/rank-*.json; do
  jq -c . "$result" >> "$artifact_dir/ranks-oversub.jsonl"
done
```

Send SIGINT only to the recorded server PID, wait for its final summary, remove the experiment-owned sparse backing file and PGAS SHM object, then invoke the validator. Preserve all logs on every path. Record every executed command through a helper that appends shell-escaped arguments to `commands.log`; dry-run mode records the same commands without executing them.

- [ ] **Step 4: Run shell syntax and dry-run tests**

```bash
bash -n tools/qemuless_mig/run_qemuless_mig.sh
bash -n ../Splash/script/qemuless_mig_rank_wrapper.sh
bash tests/test_qemuless_mig_launcher.sh
```

Expected: all pass.

- [ ] **Step 5: Commit the launcher**

```bash
git add tools/qemuless_mig/run_qemuless_mig.sh tests/test_qemuless_mig_launcher.sh
git diff --cached --check
git commit -m "tools: run QEMULess MIG oversubscription"
```

### Task 6: Build and Run the Live Four-MIG Experiment

**Files:**
- Runtime artifacts only: the newest directory under `artifact/qemuless_mig/`.

**Interfaces:**
- Consumes: completed targets and permission to reconfigure GPU 0 MIG state.
- Produces: passing `summary.json`, rank evidence, MIG mapping, and server counters.

- [ ] **Step 1: Run all non-destructive tests**

```bash
cmake -S . -B /tmp/cxlmemsim-qemuless-build -DCMAKE_BUILD_TYPE=Release -DCXLMEMSIM_BUILD_MICROBENCHMARKS=OFF
cmake --build /tmp/cxlmemsim-qemuless-build --target cxlmemsim_server test_pgas_controller_counters -j
ctest --test-dir /tmp/cxlmemsim-qemuless-build -R "test_pgas_controller_counters|test_qemuless_mig_summary" --output-on-failure
cmake -S ../Splash -B /tmp/splash-qemuless-build -DCMAKE_BUILD_TYPE=Release -DCMAKE_CUDA_COMPILER=/usr/local/cuda-12.8/bin/nvcc
cmake --build /tmp/splash-qemuless-build --target test_shmem_client_slot qemuless_mig_oversubscribe -j
ctest --test-dir /tmp/splash-qemuless-build -R test_shmem_client_slot --output-on-failure
bash ../Splash/script/test_qemuless_mig_rank_wrapper.sh
bash tests/test_qemuless_mig_launcher.sh
```

Expected: all commands pass.

- [ ] **Step 2: Record preflight and configure four MIG instances**

Run:

```bash
nvidia-smi -L
nvidia-smi --query-compute-apps=pid,process_name,used_gpu_memory --format=csv,noheader
tools/qemuless_mig/run_qemuless_mig.sh --configure-mig
```

Expected: the launcher either safely creates four MIG instances and continues, or exits before mutation with the exact conflicting compute processes listed.

- [ ] **Step 3: Validate the live artifact**

Resolve and inspect the newest artifact directory:

```bash
artifact_dir=$(find artifact/qemuless_mig -mindepth 1 -maxdepth 1 -type d | sort | tail -n 1)
jq . "$artifact_dir/summary.json"
wc -l "$artifact_dir/ranks-baseline.jsonl"
wc -l "$artifact_dir/ranks-oversub.jsonl"
rg -n "Remote:|Load:|Store:|Number of Threads created" "$artifact_dir/server.log"
```

Expected: `verdict` is `pass`, line counts are 4 and 8, each MIG has two oversubscription ranks, every rank validates CUDA and checkpoint data, and controller counters are non-zero with at least eight recorded slots.

- [ ] **Step 4: Inspect repository scope and format touched files**

```bash
clang-format -i ../Splash/src/libpgas/src/cxl_backend_shmem.c ../Splash/src/libpgas/tests/test_shmem_client_slot.c ../Splash/src/qemuless_mig_oversubscribe.cu
git diff --check
git -C ../Splash diff --check
git status --short
git -C ../Splash status --short
```

Expected: no whitespace errors; only task files plus pre-existing unrelated changes remain.
