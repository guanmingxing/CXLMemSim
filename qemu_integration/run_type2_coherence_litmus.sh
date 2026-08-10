#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
PROOF_BOUNDARY="TCG functional modeled coherence"

QEMU_BINARY=${QEMU_BINARY:-/home/victoryang00/CXLMemSim/build/worktrees/qemu-type2-hw-cc-fullsystem/build/qemu-system-x86_64}
CXL_MEMSIM_SERVER_BINARY=${CXL_MEMSIM_SERVER_BINARY:-"$REPO_ROOT/build/cxlmemsim_server"}
QEMU_IMG_BINARY=${QEMU_IMG_BINARY:-qemu-img}
SSH_BINARY=${SSH_BINARY:-ssh}
SCP_BINARY=${SCP_BINARY:-scp}
PYTHON_BINARY=${PYTHON_BINARY:-python3}
KERNEL_IMAGE=${KERNEL_IMAGE:-/home/victoryang00/cxl/arch/x86/boot/bzImage}
BASE_DISK_IMAGE=${BASE_DISK_IMAGE:-/home/victoryang00/CXLMemSim/build/qemu.img}
BASE_DISK_FORMAT=${BASE_DISK_FORMAT:-raw}
SETUP_SCRIPT=${SETUP_SCRIPT:-"$SCRIPT_DIR/setup_cxl_numa.sh"}
TYPE2_LITMUS_RUNNER=${TYPE2_LITMUS_RUNNER:-"$REPO_ROOT/build/type2_device_litmus.static"}

CXL_MEMSIM_HOST=${CXL_MEMSIM_HOST:-127.0.0.1}
CXL_MEMSIM_PORT=${CXL_MEMSIM_PORT:-10099}
SSH_HOST=${SSH_HOST:-127.0.0.1}
SSH_PORT=${SSH_PORT:-10022}
SSH_USER=${SSH_USER:-root}
VM_MEMORY=${VM_MEMORY:-2G}
VM_CPUS=${VM_CPUS:-2}
LITMUS_ITERATIONS=${LITMUS_ITERATIONS:-128}
V2_CACHE_CAPACITY=${V2_CACHE_CAPACITY:-1024}
V2_CACHE_WAYS=${V2_CACHE_WAYS:-2}
V2_TIMEOUT_MS=${V2_TIMEOUT_MS:-5000}
SERVER_START_TIMEOUT=${SERVER_START_TIMEOUT:-20}
SSH_BOOT_TIMEOUT=${SSH_BOOT_TIMEOUT:-300}
PROCESS_STOP_TIMEOUT=${PROCESS_STOP_TIMEOUT:-30}
TYPE2_ALLOW_NONSTATIC_RUNNER=${TYPE2_ALLOW_NONSTATIC_RUNNER:-0}

if [[ -z "${RUN_DIR:-}" ]]; then
    RUN_DIR="$REPO_ROOT/build/type2-coherence-litmus/$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi

OVERLAY_IMAGE="$RUN_DIR/qemu-overlay.qcow2"
SERVER_LOG="$RUN_DIR/server.log"
QEMU_LOG="$RUN_DIR/qemu.log"
SETUP_LOG="$RUN_DIR/setup.log"
GUEST_SETUP_LOG="$RUN_DIR/setup-guest.log"
TOPOLOGY_LOG="$RUN_DIR/topology.log"
GUEST_JSON="$RUN_DIR/guest.json"
GUEST_RUNNER_LOG="$RUN_DIR/guest-runner.log"
VALIDATION_JSON="$RUN_DIR/validation.json"
VALIDATION_LOG="$RUN_DIR/validation.log"
COMMAND_LOG="$RUN_DIR/commands.log"

SERVER_PID=
QEMU_PID=

usage() {
    cat <<EOF
Usage: $(basename "$0") [--dry-run]
       $(basename "$0") --validate-only GUEST_JSON SERVER_LOG
       $(basename "$0") --help

Boot one Linux guest under TCG, expose a 256 MiB CXL Type-2 devdax region,
and run the host-CFMWS/device-BAR2 coherence litmus against CXLMemSim v2.

Proof boundary: $PROOF_BOUNDARY. This is functional emulation evidence; it is
not KVM, physical LLC snoop, CXL link, latency, or hardware conformance proof.

Modes:
  --dry-run       Validate inputs and print every external command without
                  creating an overlay or starting a process.
  --validate-only Validate an existing guest JSON file and server log.

Primary overrides:
  QEMU_BINARY                 QEMU executable
  CXL_MEMSIM_SERVER_BINARY    This worktree's cxlmemsim_server
  KERNEL_IMAGE                Guest bzImage
  BASE_DISK_IMAGE             Read-only base image for the qcow2 overlay
  TYPE2_LITMUS_RUNNER         Static runner (default: build/type2_device_litmus.static)
  RUN_DIR                     Artifact directory
  CXL_MEMSIM_PORT / SSH_PORT  Host TCP ports (defaults: 10099 / 10022)
  V2_CACHE_CAPACITY           Endpoint cache bytes (default: 1024 for eviction stress)
EOF
}

die() {
    echo "ERROR: $*" >&2
    exit 1
}

require_executable() {
    local executable=$1

    if [[ "$executable" == */* ]]; then
        [[ -x "$executable" ]] || die "executable not found: $executable"
    else
        command -v "$executable" >/dev/null 2>&1 || die "command not found: $executable"
    fi
}

require_readable_file() {
    local path=$1

    [[ -f "$path" && -r "$path" ]] || die "readable file not found: $path"
}

require_positive_integer() {
    local name=$1
    local value=$2

    [[ "$value" =~ ^[0-9]+$ && "$value" -gt 0 ]] || die "$name must be a positive integer"
}

validate_static_runner() {
    if [[ "$TYPE2_ALLOW_NONSTATIC_RUNNER" == 1 ]]; then
        return 0
    fi
    if command -v readelf >/dev/null 2>&1; then
        readelf -h "$TYPE2_LITMUS_RUNNER" >/dev/null 2>&1 ||
            die "Type-2 litmus runner is not an ELF executable: $TYPE2_LITMUS_RUNNER"
        if readelf -l "$TYPE2_LITMUS_RUNNER" 2>/dev/null | grep -q 'INTERP'; then
            die "Type-2 litmus runner must be statically linked: $TYPE2_LITMUS_RUNNER"
        fi
        return 0
    fi
    if command -v file >/dev/null 2>&1 &&
        file -Lb "$TYPE2_LITMUS_RUNNER" | grep -q 'statically linked'; then
        return 0
    fi
    die "cannot prove that the Type-2 litmus runner is statically linked"
}

validate_inputs() {
    local check_static=$1
    local cache_lines

    require_executable "$QEMU_BINARY"
    require_executable "$CXL_MEMSIM_SERVER_BINARY"
    require_executable "$QEMU_IMG_BINARY"
    require_executable "$SSH_BINARY"
    require_executable "$SCP_BINARY"
    require_executable "$PYTHON_BINARY"
    require_readable_file "$KERNEL_IMAGE"
    require_readable_file "$BASE_DISK_IMAGE"
    require_readable_file "$SETUP_SCRIPT"
    require_executable "$TYPE2_LITMUS_RUNNER"
    require_positive_integer CXL_MEMSIM_PORT "$CXL_MEMSIM_PORT"
    require_positive_integer SSH_PORT "$SSH_PORT"
    require_positive_integer VM_CPUS "$VM_CPUS"
    require_positive_integer LITMUS_ITERATIONS "$LITMUS_ITERATIONS"
    require_positive_integer V2_CACHE_CAPACITY "$V2_CACHE_CAPACITY"
    require_positive_integer V2_CACHE_WAYS "$V2_CACHE_WAYS"
    require_positive_integer V2_TIMEOUT_MS "$V2_TIMEOUT_MS"
    require_positive_integer SERVER_START_TIMEOUT "$SERVER_START_TIMEOUT"
    require_positive_integer SSH_BOOT_TIMEOUT "$SSH_BOOT_TIMEOUT"
    require_positive_integer PROCESS_STOP_TIMEOUT "$PROCESS_STOP_TIMEOUT"
    [[ "$CXL_MEMSIM_PORT" -le 65535 ]] || die "CXL_MEMSIM_PORT must be at most 65535"
    [[ "$SSH_PORT" -le 65535 ]] || die "SSH_PORT must be at most 65535"
    [[ "$V2_CACHE_CAPACITY" -ge 64 && $((V2_CACHE_CAPACITY % 64)) -eq 0 ]] ||
        die "V2_CACHE_CAPACITY must be a positive multiple of 64 bytes"
    cache_lines=$((V2_CACHE_CAPACITY / 64))
    [[ $((cache_lines % V2_CACHE_WAYS)) -eq 0 ]] ||
        die "V2_CACHE_WAYS must divide the bounded cache line count"
    if [[ "$check_static" == 1 ]]; then
        validate_static_runner
    fi
}

print_command() {
    local argument
    local quoted

    for argument in "$@"; do
        printf -v quoted '%q' "$argument"
        quoted=${quoted//\\,/,}
        printf '%s ' "$quoted"
    done
    printf '\n'
}

validate_results() {
    local guest_json=$1
    local server_log=$2

    require_executable "$PYTHON_BINARY"
    require_readable_file "$guest_json"
    require_readable_file "$server_log"
    "$PYTHON_BINARY" - "$guest_json" "$server_log" "$PROOF_BOUNDARY" <<'PY'
import json
import re
import sys

guest_path, server_path, expected_boundary = sys.argv[1:]

try:
    with open(guest_path, "r", encoding="utf-8") as guest_file:
        guest = json.load(guest_file)
except (OSError, json.JSONDecodeError) as error:
    print(f"validation failed: invalid guest JSON: {error}", file=sys.stderr)
    raise SystemExit(1)

errors = []
if guest.get("status") != "pass":
    errors.append("guest status must be pass")
if guest.get("proof_boundary") != expected_boundary:
    errors.append(f"proof_boundary must be {expected_boundary}")

negative = guest.get("negative_control", {}).get("forbidden")
if isinstance(negative, bool) or not isinstance(negative, int) or negative <= 0:
    errors.append("negative forbidden must be greater than zero")
forbidden_total = guest.get("forbidden_total")
if isinstance(forbidden_total, bool) or not isinstance(forbidden_total, int) or forbidden_total != 0:
    errors.append("positive forbidden_total must be zero")

topology = guest.get("topology", {})
if topology.get("host_endpoint") != 0:
    errors.append("host_endpoint must be 0")
if topology.get("device_endpoint") != 1:
    errors.append("device_endpoint must be 1")
device_session = topology.get("device_session")
if isinstance(device_session, bool) or not isinstance(device_session, int) or device_session <= 0:
    errors.append("device_session must be greater than zero")

try:
    with open(server_path, "r", encoding="utf-8", errors="replace") as server_file:
        server_text = server_file.read()
except OSError as error:
    print(f"validation failed: cannot read server log: {error}", file=sys.stderr)
    raise SystemExit(1)

counters = {}
statistics_heading = "Coherence v2 MESI Statistics:"
if statistics_heading not in server_text:
    errors.append("Coherence v2 MESI Statistics section is missing")
    statistics_text = ""
else:
    statistics_text = server_text.rsplit(statistics_heading, 1)[1]
    statistics_text = statistics_text.split("Legacy controller counters", 1)[0]
for label in ("GETS", "GETM", "UPGRADE", "PUTM", "Atomic"):
    matches = re.findall(rf"\b{re.escape(label)}:\s*([0-9]+)", statistics_text)
    if not matches:
        errors.append(f"server counter {label} is missing")
        continue
    counters[label] = int(matches[-1])
    if counters[label] <= 0:
        errors.append(f"server counter {label} must be nonzero")

if errors:
    for error in errors:
        print(f"validation failed: {error}", file=sys.stderr)
    raise SystemExit(1)

result = {
    "schema": "cxlmemsim.type2-litmus-harness.v1",
    "status": "pass",
    "proof_boundary": expected_boundary,
    "guest": {
        "negative_forbidden": negative,
        "forbidden_total": forbidden_total,
        "host_endpoint": topology["host_endpoint"],
        "device_endpoint": topology["device_endpoint"],
        "device_session": device_session,
    },
    "server_counters": counters,
}
print(json.dumps(result, separators=(",", ":"), sort_keys=True))
PY
}

tcp_is_open() {
    local host=$1
    local port=$2

    (exec 3<>"/dev/tcp/$host/$port") >/dev/null 2>&1
}

wait_for_server() {
    local attempts=$((SERVER_START_TIMEOUT * 10))

    for ((attempt = 0; attempt < attempts; ++attempt)); do
        if tcp_is_open "$CXL_MEMSIM_HOST" "$CXL_MEMSIM_PORT"; then
            return 0
        fi
        if [[ -n "$SERVER_PID" ]] && ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
            die "CXLMemSim server exited during startup; see $SERVER_LOG"
        fi
        sleep 0.1
    done
    die "timed out waiting for CXLMemSim server; see $SERVER_LOG"
}

wait_for_ssh() {
    local ssh_target=$1
    shift

    for ((attempt = 0; attempt < SSH_BOOT_TIMEOUT; ++attempt)); do
        if "$SSH_BINARY" "$@" "$ssh_target" true >/dev/null 2>&1; then
            return 0
        fi
        if [[ -n "$QEMU_PID" ]] && ! kill -0 "$QEMU_PID" >/dev/null 2>&1; then
            die "QEMU exited before SSH became ready; see $QEMU_LOG"
        fi
        sleep 1
    done
    die "timed out waiting for guest SSH; see $QEMU_LOG"
}

wait_for_process_exit() {
    local pid=$1
    local timeout_seconds=$2
    local attempts=$((timeout_seconds * 10))

    for ((attempt = 0; attempt < attempts; ++attempt)); do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            wait "$pid" >/dev/null 2>&1 || true
            return 0
        fi
        sleep 0.1
    done
    return 1
}

stop_process() {
    local pid=$1
    local label=$2

    [[ -n "$pid" ]] || return 0
    if ! kill -0 "$pid" >/dev/null 2>&1; then
        wait "$pid" >/dev/null 2>&1 || true
        return 0
    fi
    kill -TERM "$pid" >/dev/null 2>&1 || true
    if ! wait_for_process_exit "$pid" "$PROCESS_STOP_TIMEOUT"; then
        echo "WARNING: forcing $label process $pid to stop" >&2
        kill -KILL "$pid" >/dev/null 2>&1 || true
        wait "$pid" >/dev/null 2>&1 || true
    fi
}

cleanup() {
    local saved_status=$?

    stop_process "$QEMU_PID" QEMU
    stop_process "$SERVER_PID" CXLMemSim
    exit "$saved_status"
}

MODE=run
VALIDATE_GUEST=
VALIDATE_SERVER=
case "${1:-}" in
    --help|-h)
        usage
        exit 0
        ;;
    --dry-run)
        MODE=dry-run
        shift
        ;;
    --validate-only)
        [[ $# -eq 3 ]] || die "--validate-only requires GUEST_JSON and SERVER_LOG"
        MODE=validate-only
        VALIDATE_GUEST=$2
        VALIDATE_SERVER=$3
        shift 3
        ;;
    "")
        ;;
    *)
        usage >&2
        die "unknown option: $1"
        ;;
esac
[[ $# -eq 0 ]] || die "unexpected positional arguments"

if [[ "$MODE" == validate-only ]]; then
    validate_results "$VALIDATE_GUEST" "$VALIDATE_SERVER"
    exit 0
fi

validate_inputs "$([[ "$MODE" == run ]] && echo 1 || echo 0)"

QEMU_DEVICE="cxl-type2,bus=type2_rp,id=cxl_type2_0,sn=2,gpu-mode=0,coherency-enabled=true,cache-size=128M,mem-size=256M,cxlmemsim-addr=$CXL_MEMSIM_HOST,cxlmemsim-port=$CXL_MEMSIM_PORT,coherence-v2=on,coherence-v2-host-endpoint=0,coherence-v2-device-endpoint=1,coherence-v2-cache-capacity=$V2_CACHE_CAPACITY,coherence-v2-cache-ways=$V2_CACHE_WAYS,coherence-v2-timeout-ms=$V2_TIMEOUT_MS,coherence-v2-write-through=off"
KERNEL_COMMAND_LINE="root=/dev/vda rw console=ttyS0,115200 nokaslr systemd.mask=cxl-numa-setup.service systemd.unit=multi-user.target"

OVERLAY_COMMAND=(
    "$QEMU_IMG_BINARY" create -f qcow2 -F "$BASE_DISK_FORMAT"
    -b "$BASE_DISK_IMAGE" "$OVERLAY_IMAGE"
)
SERVER_COMMAND=(
    "$CXL_MEMSIM_SERVER_BINARY"
    --comm-mode=tcp
    --port="$CXL_MEMSIM_PORT"
    --capacity=256
    --coherence-v2
    --coherence-v2-snoop-timeout-ms="$V2_TIMEOUT_MS"
)
QEMU_COMMAND=(
    "$QEMU_BINARY"
    -accel "tcg,thread=multi"
    -machine "q35,cxl=on,cxl-fmw.0.targets.0=cxl.1,cxl-fmw.0.size=256M"
    -cpu max
    -m "$VM_MEMORY"
    -smp "$VM_CPUS"
    -kernel "$KERNEL_IMAGE"
    -append "$KERNEL_COMMAND_LINE"
    -drive "file=$OVERLAY_IMAGE,if=none,id=osdisk,format=qcow2"
    -device "virtio-blk-pci,drive=osdisk,bus=pcie.0"
    -netdev "user,id=net0,hostfwd=tcp:$SSH_HOST:$SSH_PORT-:22"
    -device "virtio-net-pci,netdev=net0,bus=pcie.0"
    -device "pxb-cxl,bus_nr=12,bus=pcie.0,id=cxl.1"
    -device "cxl-rp,port=0,bus=cxl.1,id=type2_rp,chassis=0,slot=0"
    -device "$QEMU_DEVICE"
    -nographic
    -no-reboot
)
SSH_OPTIONS=(
    -p "$SSH_PORT"
    -o BatchMode=yes
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o ConnectTimeout=3
    -o LogLevel=ERROR
)
SCP_OPTIONS=(
    -P "$SSH_PORT"
    -o BatchMode=yes
    -o StrictHostKeyChecking=no
    -o UserKnownHostsFile=/dev/null
    -o ConnectTimeout=3
    -o LogLevel=ERROR
)
SSH_TARGET="$SSH_USER@$SSH_HOST"
GUEST_SETUP_COMMAND="LOG_FILE=/root/type2-coherence-setup.log REGION_SIZE=256M CXL_REGION_TYPE=ram CXL_CREATE_DAX=1 CXL_DAX_MODE=devdax CXL_TOUCH_DAX=0 CXL_CONFIGURE_NET=0 /root/setup_cxl_numa.sh"

# Guest paths and command substitutions must expand in the remote shell.
# shellcheck disable=SC2016
DISCOVER_COMMAND='set -eu
dax_path=
for candidate in /dev/dax*; do
    if [ -c "$candidate" ]; then
        dax_path=$candidate
        break
    fi
done
bar_path=
for device_dir in /sys/bus/pci/devices/*; do
    if [ -r "$device_dir/vendor" ] && [ -r "$device_dir/device" ] &&
       [ "$(cat "$device_dir/vendor")" = "0x8086" ] &&
       [ "$(cat "$device_dir/device")" = "0x0d92" ] &&
       [ -e "$device_dir/resource2" ]; then
        bar_path=$device_dir/resource2
        break
    fi
done
[ -n "$dax_path" ]
[ -n "$bar_path" ]
printf "DAX_PATH=%s\nBAR2_PATH=%s\n" "$dax_path" "$bar_path"'

if [[ "$MODE" == dry-run ]]; then
    echo "proof_boundary=$PROOF_BOUNDARY"
    echo "static_runner=required_for_normal_run"
    print_command "${OVERLAY_COMMAND[@]}"
    print_command env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
    print_command env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}"
    print_command "$SCP_BINARY" "${SCP_OPTIONS[@]}" "$SETUP_SCRIPT" "$SSH_TARGET:/root/setup_cxl_numa.sh"
    print_command "$SCP_BINARY" "${SCP_OPTIONS[@]}" "$TYPE2_LITMUS_RUNNER" "$SSH_TARGET:/root/type2_device_litmus.static"
    print_command "$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "$GUEST_SETUP_COMMAND"
    print_command "$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "$DISCOVER_COMMAND"
    print_command "$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "/root/type2_device_litmus.static --dax /dev/daxX.Y --bar /sys/bus/pci/devices/BDF/resource2 --iterations $LITMUS_ITERATIONS"
    echo "artifacts=$RUN_DIR"
    exit 0
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$RUN_DIR"
[[ ! -e "$OVERLAY_IMAGE" ]] || die "overlay already exists: $OVERLAY_IMAGE"

{
    echo "proof_boundary=$PROOF_BOUNDARY"
    print_command "${OVERLAY_COMMAND[@]}"
    print_command env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
    print_command env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}"
} >"$COMMAND_LOG"

"${OVERLAY_COMMAND[@]}" >>"$COMMAND_LOG" 2>&1

if tcp_is_open "$CXL_MEMSIM_HOST" "$CXL_MEMSIM_PORT"; then
    die "CXLMemSim port is already in use: $CXL_MEMSIM_HOST:$CXL_MEMSIM_PORT"
fi
if tcp_is_open "$SSH_HOST" "$SSH_PORT"; then
    die "SSH forwarding port is already in use: $SSH_HOST:$SSH_PORT"
fi

(
    cd "$REPO_ROOT"
    exec env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
) >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_for_server

env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}" >"$QEMU_LOG" 2>&1 &
QEMU_PID=$!
wait_for_ssh "$SSH_TARGET" "${SSH_OPTIONS[@]}"

"$SCP_BINARY" "${SCP_OPTIONS[@]}" "$SETUP_SCRIPT" "$SSH_TARGET:/root/setup_cxl_numa.sh"
"$SCP_BINARY" "${SCP_OPTIONS[@]}" "$TYPE2_LITMUS_RUNNER" "$SSH_TARGET:/root/type2_device_litmus.static"
"$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" \
    "chmod 0755 /root/setup_cxl_numa.sh /root/type2_device_litmus.static"

"$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "$GUEST_SETUP_COMMAND" >"$SETUP_LOG" 2>&1
"$SCP_BINARY" "${SCP_OPTIONS[@]}" "$SSH_TARGET:/root/type2-coherence-setup.log" "$GUEST_SETUP_LOG" \
    >/dev/null 2>&1 || true

"$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "$DISCOVER_COMMAND" >"$TOPOLOGY_LOG"
DAX_PATH=$(sed -n 's/^DAX_PATH=//p' "$TOPOLOGY_LOG" | head -1)
BAR2_PATH=$(sed -n 's/^BAR2_PATH=//p' "$TOPOLOGY_LOG" | head -1)
[[ "$DAX_PATH" =~ ^/dev/dax[[:alnum:]_.-]+$ ]] || die "invalid or missing DAX path in $TOPOLOGY_LOG"
[[ "$BAR2_PATH" =~ ^/sys/bus/pci/devices/[^/]+/resource2$ ]] ||
    die "invalid or missing BAR2 path in $TOPOLOGY_LOG"

printf -v GUEST_RUNNER_COMMAND '%q ' \
    /root/type2_device_litmus.static \
    --dax "$DAX_PATH" \
    --bar "$BAR2_PATH" \
    --iterations "$LITMUS_ITERATIONS" \
    --map-size 2097152 \
    --base-offset 65536

set +e
"$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" "$GUEST_RUNNER_COMMAND" \
    >"$GUEST_JSON" 2>"$GUEST_RUNNER_LOG"
RUNNER_STATUS=$?
set -e

"$SSH_BINARY" "${SSH_OPTIONS[@]}" "$SSH_TARGET" \
    "sync; systemctl poweroff --no-block || poweroff" >/dev/null 2>&1 || true
if ! wait_for_process_exit "$QEMU_PID" "$PROCESS_STOP_TIMEOUT"; then
    stop_process "$QEMU_PID" QEMU
fi
QEMU_PID=
stop_process "$SERVER_PID" CXLMemSim
SERVER_PID=

set +e
validate_results "$GUEST_JSON" "$SERVER_LOG" >"$VALIDATION_JSON" 2>"$VALIDATION_LOG"
VALIDATION_STATUS=$?
set -e

if [[ -s "$VALIDATION_JSON" ]]; then
    cat "$VALIDATION_JSON"
fi
if [[ "$RUNNER_STATUS" -ne 0 ]]; then
    echo "ERROR: guest litmus runner exited with status $RUNNER_STATUS; see $GUEST_RUNNER_LOG" >&2
fi
if [[ "$VALIDATION_STATUS" -ne 0 ]]; then
    cat "$VALIDATION_LOG" >&2
fi
if [[ "$RUNNER_STATUS" -ne 0 || "$VALIDATION_STATUS" -ne 0 ]]; then
    exit 1
fi

echo "Type-2 coherence litmus passed"
echo "Proof boundary: $PROOF_BOUNDARY"
echo "Artifacts: $RUN_DIR"
