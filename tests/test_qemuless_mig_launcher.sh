#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
launcher="$repo_root/tools/qemuless_mig/run_qemuless_mig.sh"
splash_dir=$(cd "$repo_root/../Splash" && pwd -P)
tmp=$(mktemp -d)
fixture_bin="$tmp/bin"
artifact="$tmp/artifact"
fixture_calls="$tmp/fixture-calls.log"
mutation_log="$tmp/mutation.log"

trap 'if [[ "${KEEP_QEMULESS_TEST_TMP:-0}" == 1 ]]; then printf "fixture tmp: %s\\n" "$tmp" >&2; else rm -rf "$tmp"; fi' EXIT
mkdir -p "$fixture_bin"

cat > "$fixture_bin/nvidia-smi" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

case "${1:-}" in
    -L)
        cat <<'OUTPUT'
GPU 0: fixture GPU (UUID: GPU-00000000-0000-0000-0000-000000000000)
  MIG 1g.24gb Device 0: (UUID: MIG-00000000-0000-0000-0000-000000000001)
  MIG 1g.24gb Device 1: (UUID: MIG-00000000-0000-0000-0000-000000000002)
  MIG 1g.24gb Device 2: (UUID: MIG-00000000-0000-0000-0000-000000000003)
  MIG 1g.24gb Device 3: (UUID: MIG-00000000-0000-0000-0000-000000000004)
OUTPUT
        ;;
    mig)
        if [[ "${2:-}" == -i && "${4:-}" == -lgi ]]; then
            if [[ "${FIXTURE_LGI_EXIT:-0}" != 0 ]]; then
                printf '%s\n' "${FIXTURE_LGI_MESSAGE:-No GPU instances found: Not Found}"
                exit "$FIXTURE_LGI_EXIT"
            fi
            IFS=, read -r -a profile_ids <<< "${FIXTURE_PROFILE_IDS:-14,14,14,14}"
            printf '%s\n' '| GPU  Name        Profile ID  Instance ID  Placement |'
            for index in "${!profile_ids[@]}"; do
                printf '| 0    MIG 1g.24gb %-11s %s            %s:1      |\n' \
                    "${profile_ids[$index]}" "$index" "$index"
            done
        else
            printf 'unexpected mig invocation: %s\n' "$*" >&2
            exit 96
        fi
        ;;
    --query-compute-apps=pid)
        printf '%s\n' "${FIXTURE_COMPUTE_PIDS:-}"
        ;;
    *)
        printf 'fixture nvidia-smi snapshot\n'
        ;;
esac
EOF
cat > "$fixture_bin/cmake" <<'EOF'
#!/usr/bin/env bash
printf 'cmake %q\n' "$*" >> "${FIXTURE_MUTATION_LOG:?}"
exit 97
EOF
cat > "$fixture_bin/mpirun" <<'EOF'
#!/usr/bin/env bash
printf 'mpirun %q\n' "$*" >> "${FIXTURE_MUTATION_LOG:?}"
exit 97
EOF
cat > "$fixture_bin/sudo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

{
    printf 'sudo'
    printf ' %q' "$@"
    printf '\n'
} >> "${FIXTURE_MUTATION_LOG:?}"
if [[ "${FIXTURE_FAIL_DCI:-0}" == 1 && "$*" == *'mig -i 0 -dci'* ]]; then
    exit 98
fi
EOF
chmod +x "$fixture_bin/nvidia-smi" "$fixture_bin/cmake" "$fixture_bin/mpirun" "$fixture_bin/sudo"

PATH="$fixture_bin:$PATH" FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" "$launcher" \
    --dry-run --artifact-dir "$artifact" --splash-dir "$splash_dir"

test -s "$artifact/commands.log"
test -s "$artifact/nvidia-smi-before.txt"
test -s "$artifact/nvidia-smi-after.txt"
test -s "$artifact/mig-uuids.txt"
test -s "$artifact/mig-map.json"
test ! -e "$mutation_log"

grep -F -- 'nvidia-smi > '"$artifact"'/nvidia-smi-before.txt 2>&1' "$artifact/commands.log" >/dev/null
grep -F -- 'nvidia-smi mig -i 0 -lgi > '"$artifact"'/nvidia-smi-gi-before.txt' "$artifact/commands.log" >/dev/null
grep -F -- '--comm-mode=pgas-shm' "$artifact/commands.log" >/dev/null
grep -F -- '--capacity=524288' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-backing-file=' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-cache-mb=64' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-io-uring=false' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-odirect=false' "$artifact/commands.log" >/dev/null
grep -Eq -- '--pgas-shm-name=/cxlmemsim_pgas_[A-Za-z0-9_]+' "$artifact/commands.log"
shm_name=$(sed -n 's/.*--pgas-shm-name=\([^ ]*\).*/\1/p' "$artifact/commands.log" | head -n 1)
[[ "$shm_name" =~ ^/cxlmemsim_pgas_[A-Za-z0-9_]+$ ]]
[[ "$shm_name" != /cxlmemsim_pgas ]]
grep -F -- "--shm-name $shm_name" "$artifact/commands.log" >/dev/null
grep -F -- "env CCACHE_DISABLE=1 cmake --build /tmp/cxlmemsim-qemuless-build --target cxlmemsim_server -j" "$artifact/commands.log" >/dev/null
grep -F -- "env CCACHE_DISABLE=1 cmake --build /tmp/splash-qemuless-build --target qemuless_mig_oversubscribe -j" "$artifact/commands.log" >/dev/null
grep -F -- "2>&1 &" "$artifact/commands.log" >/dev/null
grep -F -- "> $artifact/baseline-mpirun.log 2>&1" "$artifact/commands.log" >/dev/null
grep -F -- ">> $artifact/ranks-baseline.jsonl" "$artifact/commands.log" >/dev/null
grep -F -- ">> $artifact/ranks-oversub.jsonl" "$artifact/commands.log" >/dev/null
grep -F -- 'rm -f -- '"$artifact"'/cxlmemsim.ssd' "$artifact/commands.log" >/dev/null
grep -F -- "rm -f -- /dev/shm/${shm_name#/}" "$artifact/commands.log" >/dev/null
! grep -Fqi 'qemu-system' "$artifact/commands.log"

grep -F -- 'setsid --wait' "$launcher" >/dev/null
grep -F -- 'terminate_mpi' "$launcher" >/dev/null
grep -F -- 'process_group_is_running' "$launcher" >/dev/null
grep -F -- "trap '' INT TERM" "$launcher" >/dev/null
grep -F -- 'jq -c . < "$result" >> "$output"' "$launcher" >/dev/null
grep -F -- 'QEMULESS_CXL_BUILD' "$launcher" >/dev/null
grep -F -- 'QEMULESS_SPLASH_BUILD' "$launcher" >/dev/null
grep -F -- 'QEMULESS_SHM_DIR' "$launcher" >/dev/null
grep -F -- 'QEMULESS_MPI_SIGNAL_TIMEOUT_SECONDS' "$launcher" >/dev/null
grep -F -- 'QEMULESS_SERVER_INT_TIMEOUT_SECONDS' "$launcher" >/dev/null
grep -F -- 'QEMULESS_SERVER_TERM_TIMEOUT_SECONDS' "$launcher" >/dev/null

jq -e '
    . as $map |
    .migs == [
        "MIG-00000000-0000-0000-0000-000000000001",
        "MIG-00000000-0000-0000-0000-000000000002",
        "MIG-00000000-0000-0000-0000-000000000003",
        "MIG-00000000-0000-0000-0000-000000000004"
    ] and
    (.ranks | length == 8) and
    all(.ranks[]; .mig_index == (.rank % 4) and .mig_uuid == $map.migs[.mig_index])
' "$artifact/mig-map.json" >/dev/null

expect_failure() {
    if PATH="$fixture_bin:$PATH" FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" "$@" >/dev/null 2>&1; then
        printf 'expected launcher failure: %q\n' "$*" >&2
        exit 1
    fi
}

expect_failure "$launcher" --dry-run --artifact-dir "$tmp/unknown" --unknown
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/missing-checkpoint" --checkpoint-bytes
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/zero-checkpoint" --checkpoint-bytes 0
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/large-checkpoint" --checkpoint-bytes 16777217
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/negative-iterations" --iterations -1
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/missing-splash" --splash-dir
expect_failure "$launcher" --dry-run --artifact-dir "$tmp/empty-artifact" --artifact-dir ''

expect_failure env FIXTURE_PROFILE_IDS=14,14,13,14 "$launcher" \
    --dry-run --artifact-dir "$tmp/profile-mismatch" --splash-dir "$splash_dir"

: > "$mutation_log"
expect_failure env FIXTURE_PROFILE_IDS=13 FIXTURE_COMPUTE_PIDS=4242 "$launcher" \
    --configure-mig --artifact-dir "$tmp/active-compute" --splash-dir "$splash_dir"
grep -F -- 'cmake ' "$mutation_log" >/dev/null && {
    printf 'active compute PID reached the build phase\n' >&2
    exit 1
}
test ! -s "$mutation_log"

: > "$mutation_log"
expect_failure env FIXTURE_LGI_EXIT=6 "$launcher" \
    --configure-mig --artifact-dir "$tmp/empty-gi-layout" --splash-dir "$splash_dir"
grep -F -- 'nvidia-smi -i 0 -mig 1' "$mutation_log" >/dev/null
grep -F -- 'mig -i 0 -cgi' "$mutation_log" >/dev/null

: > "$mutation_log"
expect_failure env FIXTURE_LGI_EXIT=6 FIXTURE_LGI_MESSAGE='No MIG-enabled devices found.' "$launcher" \
    --configure-mig --artifact-dir "$tmp/mig-disabled-layout" --splash-dir "$splash_dir"
grep -F -- 'nvidia-smi -i 0 -mig 1' "$mutation_log" >/dev/null

: > "$mutation_log"
expect_failure env FIXTURE_LGI_EXIT=6 FIXTURE_LGI_MESSAGE='No GPU instances found: Permission denied' "$launcher" \
    --configure-mig --artifact-dir "$tmp/lgi-permission-error" --splash-dir "$splash_dir"
test ! -s "$mutation_log"

: > "$mutation_log"
expect_failure env FIXTURE_PROFILE_IDS=13 FIXTURE_FAIL_DCI=1 "$launcher" \
    --configure-mig --artifact-dir "$tmp/dci-failure" --splash-dir "$splash_dir"
grep -F -- 'mig -i 0 -dci' "$mutation_log" >/dev/null
! grep -F -- 'mig -i 0 -dgi' "$mutation_log"
! grep -F -- 'mig -i 0 -cgi 14,14,14,14 -C' "$mutation_log"

lifecycle_bin="$tmp/lifecycle-bin"
lifecycle_cxl_build="$tmp/lifecycle-cxl-build"
lifecycle_splash_build="$tmp/lifecycle-splash-build"
lifecycle_shm="$tmp/lifecycle-shm"
lifecycle_artifact="$tmp/lifecycle-artifact"
lifecycle_mpi_pid="$tmp/lifecycle-mpi.pid"
lifecycle_rank_pid="$tmp/lifecycle-rank.pid"
lifecycle_server_pid="$tmp/lifecycle-server.pid"
lifecycle_signals="$tmp/lifecycle-signals.log"
mkdir -p "$lifecycle_bin" "$lifecycle_cxl_build" "$lifecycle_splash_build" "$lifecycle_shm"

cat > "$lifecycle_bin/cmake" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
cat > "$lifecycle_bin/mpirun" <<'EOF'
#!/usr/bin/env python3
import glob
import os
import signal
import struct
import time

paths = glob.glob(os.path.join(os.environ["QEMULESS_SHM_DIR"], "cxlmemsim_pgas_*"))
if len(paths) != 1:
    raise SystemExit(91)
with open(paths[0], "rb", buffering=0) as handle:
    header = struct.unpack("<QIII", handle.read(20))
if header != (0x43584C53484D454D, 1, 64, 1):
    raise SystemExit(92)
with open(os.environ["LIFECYCLE_MPI_PID"], "w", encoding="ascii") as handle:
    handle.write(f"{os.getpid()}\n")

def stop(signum, _frame):
    name = signal.Signals(signum).name.removeprefix("SIG")
    with open(os.environ["LIFECYCLE_SIGNALS"], "a", encoding="ascii") as handle:
        handle.write(f"mpi {name}\n")
    if os.environ.get("LIFECYCLE_STUBBORN") != "1":
        raise SystemExit(0)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
if os.environ.get("LIFECYCLE_ORPHAN_RANK") == "1" and os.fork() == 0:
    with open(os.environ["LIFECYCLE_RANK_PID"], "w", encoding="ascii") as handle:
        handle.write(f"{os.getpid()}\n")

    def stop_rank(signum, _frame):
        name = signal.Signals(signum).name.removeprefix("SIG")
        with open(os.environ["LIFECYCLE_SIGNALS"], "a", encoding="ascii") as handle:
            handle.write(f"rank {name}\n")
        if signum != signal.SIGINT:
            raise SystemExit(0)

    signal.signal(signal.SIGINT, stop_rank)
    signal.signal(signal.SIGTERM, stop_rank)
while True:
    time.sleep(1)
EOF
cat > "$lifecycle_cxl_build/cxlmemsim_server" <<'EOF'
#!/usr/bin/env python3
import os
import signal
import struct
import sys
import time

shm_name = next(arg.split("=", 1)[1] for arg in sys.argv[1:] if arg.startswith("--pgas-shm-name="))
shm_path = os.path.join(os.environ["QEMULESS_SHM_DIR"], shm_name.lstrip("/"))
with open(os.environ["LIFECYCLE_SERVER_PID"], "w", encoding="ascii") as handle:
    handle.write(f"{os.getpid()}\n")

def stop(signum, _frame):
    name = signal.Signals(signum).name.removeprefix("SIG")
    with open(os.environ["LIFECYCLE_SIGNALS"], "a", encoding="ascii") as handle:
        handle.write(f"server {name}\n")
    if os.environ.get("LIFECYCLE_SERVER_STUBBORN") != "1":
        raise SystemExit(0)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
with open(shm_path, "r+b", buffering=0) as handle:
    handle.truncate(64 + 64 * 256)
time.sleep(1)
with open(shm_path, "r+b", buffering=0) as handle:
    handle.write(struct.pack("<QIII", 0x43584C53484D454D, 1, 64, 1))
while True:
    time.sleep(1)
EOF
cat > "$lifecycle_splash_build/qemuless_mig_oversubscribe" <<'EOF'
#!/usr/bin/env bash
exit 99
EOF
chmod +x "$lifecycle_bin/cmake" "$lifecycle_bin/mpirun" \
    "$lifecycle_cxl_build/cxlmemsim_server" "$lifecycle_splash_build/qemuless_mig_oversubscribe"

env PATH="$lifecycle_bin:$fixture_bin:$PATH" \
    FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" \
    QEMULESS_CXL_BUILD="$lifecycle_cxl_build" QEMULESS_SPLASH_BUILD="$lifecycle_splash_build" \
    QEMULESS_SHM_DIR="$lifecycle_shm" LIFECYCLE_MPI_PID="$lifecycle_mpi_pid" \
    LIFECYCLE_SERVER_PID="$lifecycle_server_pid" LIFECYCLE_SIGNALS="$lifecycle_signals" \
    "$launcher" --artifact-dir "$lifecycle_artifact" --splash-dir "$splash_dir" \
    > "$tmp/lifecycle-launcher.log" 2>&1 &
launcher_pid=$!

for _ in $(seq 1 100); do
    [[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]] && break
    kill -0 "$launcher_pid" 2>/dev/null || break
    sleep 0.1
done
[[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]]
kill -TERM "$launcher_pid"
if wait "$launcher_pid"; then
    printf 'signal-cleanup fixture unexpectedly succeeded\n' >&2
    exit 1
else
    launcher_status=$?
fi
[[ "$launcher_status" == 143 ]]
grep -F -- 'mpi INT' "$lifecycle_signals" >/dev/null
grep -F -- 'server INT' "$lifecycle_signals" >/dev/null
! kill -0 "$(cat "$lifecycle_mpi_pid")" 2>/dev/null
! kill -0 "$(cat "$lifecycle_server_pid")" 2>/dev/null
[[ -z "$(find "$lifecycle_shm" -mindepth 1 -maxdepth 1 -print -quit)" ]]

orphan_artifact="$tmp/orphan-artifact"
: > "$lifecycle_signals"
rm -f -- "$lifecycle_mpi_pid" "$lifecycle_rank_pid" "$lifecycle_server_pid"
env PATH="$lifecycle_bin:$fixture_bin:$PATH" \
    FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" \
    QEMULESS_CXL_BUILD="$lifecycle_cxl_build" QEMULESS_SPLASH_BUILD="$lifecycle_splash_build" \
    QEMULESS_SHM_DIR="$lifecycle_shm" QEMULESS_MPI_SIGNAL_TIMEOUT_SECONDS=1 \
    LIFECYCLE_MPI_PID="$lifecycle_mpi_pid" LIFECYCLE_RANK_PID="$lifecycle_rank_pid" \
    LIFECYCLE_SERVER_PID="$lifecycle_server_pid" LIFECYCLE_SIGNALS="$lifecycle_signals" \
    LIFECYCLE_ORPHAN_RANK=1 \
    "$launcher" --artifact-dir "$orphan_artifact" --splash-dir "$splash_dir" \
    > "$tmp/orphan-launcher.log" 2>&1 &
launcher_pid=$!

for _ in $(seq 1 100); do
    [[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_rank_pid" && -s "$lifecycle_server_pid" ]] && break
    kill -0 "$launcher_pid" 2>/dev/null || break
    sleep 0.1
done
[[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_rank_pid" && -s "$lifecycle_server_pid" ]]
kill -TERM "$launcher_pid"
if wait "$launcher_pid"; then
    printf 'orphan-rank cleanup fixture unexpectedly succeeded\n' >&2
    exit 1
else
    launcher_status=$?
fi
[[ "$launcher_status" == 143 ]]
grep -F -- 'mpi INT' "$lifecycle_signals" >/dev/null
grep -F -- 'rank INT' "$lifecycle_signals" >/dev/null
grep -F -- 'rank TERM' "$lifecycle_signals" >/dev/null
! kill -0 "$(cat "$lifecycle_mpi_pid")" 2>/dev/null
! kill -0 "$(cat "$lifecycle_rank_pid")" 2>/dev/null
! kill -0 "$(cat "$lifecycle_server_pid")" 2>/dev/null
[[ -z "$(find "$lifecycle_shm" -mindepth 1 -maxdepth 1 -print -quit)" ]]

server_stubborn_artifact="$tmp/server-stubborn-artifact"
: > "$lifecycle_signals"
rm -f -- "$lifecycle_mpi_pid" "$lifecycle_server_pid"
env PATH="$lifecycle_bin:$fixture_bin:$PATH" \
    FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" \
    QEMULESS_CXL_BUILD="$lifecycle_cxl_build" QEMULESS_SPLASH_BUILD="$lifecycle_splash_build" \
    QEMULESS_SHM_DIR="$lifecycle_shm" QEMULESS_SERVER_INT_TIMEOUT_SECONDS=1 \
    QEMULESS_SERVER_TERM_TIMEOUT_SECONDS=1 LIFECYCLE_MPI_PID="$lifecycle_mpi_pid" \
    LIFECYCLE_SERVER_PID="$lifecycle_server_pid" LIFECYCLE_SIGNALS="$lifecycle_signals" \
    LIFECYCLE_SERVER_STUBBORN=1 \
    "$launcher" --artifact-dir "$server_stubborn_artifact" --splash-dir "$splash_dir" \
    > "$tmp/server-stubborn-launcher.log" 2>&1 &
launcher_pid=$!

for _ in $(seq 1 100); do
    [[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]] && break
    kill -0 "$launcher_pid" 2>/dev/null || break
    sleep 0.1
done
[[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]]
kill -TERM "$launcher_pid"
if wait "$launcher_pid"; then
    printf 'stubborn-server cleanup fixture unexpectedly succeeded\n' >&2
    exit 1
else
    launcher_status=$?
fi
[[ "$launcher_status" == 143 ]]
grep -F -- 'server INT' "$lifecycle_signals" >/dev/null
grep -F -- 'server TERM' "$lifecycle_signals" >/dev/null
grep -E -- 'kill -KILL [0-9]+' "$server_stubborn_artifact/commands.log" >/dev/null
! kill -0 "$(cat "$lifecycle_mpi_pid")" 2>/dev/null
! kill -0 "$(cat "$lifecycle_server_pid")" 2>/dev/null
[[ -z "$(find "$lifecycle_shm" -mindepth 1 -maxdepth 1 -print -quit)" ]]

stubborn_artifact="$tmp/stubborn-artifact"
: > "$lifecycle_signals"
rm -f -- "$lifecycle_mpi_pid" "$lifecycle_server_pid"
env PATH="$lifecycle_bin:$fixture_bin:$PATH" \
    FIXTURE_CALLS="$fixture_calls" FIXTURE_MUTATION_LOG="$mutation_log" \
    QEMULESS_CXL_BUILD="$lifecycle_cxl_build" QEMULESS_SPLASH_BUILD="$lifecycle_splash_build" \
    QEMULESS_SHM_DIR="$lifecycle_shm" QEMULESS_MPI_SIGNAL_TIMEOUT_SECONDS=1 \
    LIFECYCLE_MPI_PID="$lifecycle_mpi_pid" LIFECYCLE_SERVER_PID="$lifecycle_server_pid" \
    LIFECYCLE_SIGNALS="$lifecycle_signals" LIFECYCLE_STUBBORN=1 \
    "$launcher" --artifact-dir "$stubborn_artifact" --splash-dir "$splash_dir" \
    > "$tmp/stubborn-launcher.log" 2>&1 &
launcher_pid=$!

for _ in $(seq 1 100); do
    [[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]] && break
    kill -0 "$launcher_pid" 2>/dev/null || break
    sleep 0.1
done
[[ -s "$lifecycle_mpi_pid" && -s "$lifecycle_server_pid" ]]
kill -TERM "$launcher_pid"
if wait "$launcher_pid"; then
    printf 'stubborn signal-cleanup fixture unexpectedly succeeded\n' >&2
    exit 1
else
    launcher_status=$?
fi
[[ "$launcher_status" == 143 ]]
grep -F -- 'mpi INT' "$lifecycle_signals" >/dev/null
grep -F -- 'mpi TERM' "$lifecycle_signals" >/dev/null
grep -E -- 'kill -KILL -- -[0-9]+' "$stubborn_artifact/commands.log" >/dev/null
! kill -0 "$(cat "$lifecycle_mpi_pid")" 2>/dev/null
! kill -0 "$(cat "$lifecycle_server_pid")" 2>/dev/null
[[ -z "$(find "$lifecycle_shm" -mindepth 1 -maxdepth 1 -print -quit)" ]]
