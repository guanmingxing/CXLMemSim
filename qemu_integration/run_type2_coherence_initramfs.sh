#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
PROOF_BOUNDARY="TCG functional modeled coherence"

QEMU_BINARY=${QEMU_BINARY:-/home/victoryang00/CXLMemSim/build/worktrees/qemu-type2-hw-cc-fullsystem/build/qemu-system-x86_64}
CXL_MEMSIM_SERVER_BINARY=${CXL_MEMSIM_SERVER_BINARY:-"$REPO_ROOT/build/cxlmemsim_server"}
BUSYBOX_BINARY=${BUSYBOX_BINARY:-/usr/bin/busybox}
CXL_BINARY=${CXL_BINARY:-/usr/bin/cxl}
DAXCTL_BINARY=${DAXCTL_BINARY:-/usr/bin/daxctl}
CPIO_BINARY=${CPIO_BINARY:-/usr/bin/cpio}
KERNEL_IMAGE=${KERNEL_IMAGE:-/home/victoryang00/cxl/arch/x86/boot/bzImage}
KERNEL_CONFIG=${KERNEL_CONFIG:-}
TYPE2_LITMUS_RUNNER=${TYPE2_LITMUS_RUNNER:-"$REPO_ROOT/build/type2_device_litmus.static"}
GUEST_INIT=${GUEST_INIT:-"$SCRIPT_DIR/type2_litmus_initramfs_init.sh"}
VALIDATOR=${VALIDATOR:-"$SCRIPT_DIR/run_type2_coherence_litmus.sh"}

CXL_MEMSIM_HOST=${CXL_MEMSIM_HOST:-127.0.0.1}
CXL_MEMSIM_PORT=${CXL_MEMSIM_PORT:-10099}
VM_MEMORY=${VM_MEMORY:-2G}
VM_CPUS=${VM_CPUS:-2}
LITMUS_ITERATIONS=${LITMUS_ITERATIONS:-128}
V2_CACHE_CAPACITY=${V2_CACHE_CAPACITY:-1024}
V2_CACHE_WAYS=${V2_CACHE_WAYS:-2}
V2_TIMEOUT_MS=${V2_TIMEOUT_MS:-5000}
SERVER_START_TIMEOUT=${SERVER_START_TIMEOUT:-20}
QEMU_RUN_TIMEOUT=${QEMU_RUN_TIMEOUT:-600}
PROCESS_STOP_TIMEOUT=${PROCESS_STOP_TIMEOUT:-30}

if [[ -z "$KERNEL_CONFIG" && "$KERNEL_IMAGE" == */arch/x86/boot/bzImage ]]; then
    KERNEL_CONFIG="${KERNEL_IMAGE%/arch/x86/boot/bzImage}/.config"
fi

if [[ -z "${RUN_DIR:-}" ]]; then
    RUN_DIR="$REPO_ROOT/build/type2-coherence-initramfs/$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi

INITRAMFS_ROOT="$RUN_DIR/initramfs-root"
INITRAMFS_IMAGE="$RUN_DIR/type2-litmus-initramfs.cpio"
SERVER_LOG="$RUN_DIR/server.log"
QEMU_LOG="$RUN_DIR/qemu.log"
SERIAL_LOG="$RUN_DIR/serial.log"
GUEST_JSON="$RUN_DIR/guest.json"
VALIDATION_JSON="$RUN_DIR/validation.json"
VALIDATION_LOG="$RUN_DIR/validation.log"
COMMAND_LOG="$RUN_DIR/commands.log"

SERVER_PID=
QEMU_PID=

usage() {
    cat <<EOF
Usage: $(basename "$0") [--dry-run]
       $(basename "$0") --help

Build a minimal initramfs, boot one Linux guest under TCG without a disk,
create a 256 MiB Type-2 devdax region, and run the coherent-domain litmus.
The CXL core, PCI, region, cache, Type-2 accelerator, and devdax drivers must be built into the kernel.

Proof boundary: $PROOF_BOUNDARY. This is functional emulation evidence; it is
not KVM, physical LLC snoop, CXL link, latency, or hardware conformance proof.

Modes:
  --dry-run  Validate inputs and print the server/QEMU commands only.
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
    if command -v readelf >/dev/null 2>&1; then
        readelf -h "$TYPE2_LITMUS_RUNNER" >/dev/null 2>&1 ||
            die "Type-2 litmus runner is not an ELF executable"
        if readelf -l "$TYPE2_LITMUS_RUNNER" 2>/dev/null | grep -q INTERP; then
            die "Type-2 litmus runner must be statically linked"
        fi
        return 0
    fi
    die "readelf is required to prove that the guest runner is static"
}

validate_builtin_kernel_config() {
    local option

    for option in \
        CONFIG_CXL_BUS \
        CONFIG_CXL_PCI \
        CONFIG_CXL_ACPI \
        CONFIG_CXL_MEM \
        CONFIG_CXL_PORT \
        CONFIG_CXL_REGION \
        CONFIG_CXL_CACHE \
        CONFIG_CXL_TYPE2_ACCEL \
        CONFIG_DEV_DAX \
        CONFIG_DEV_DAX_CXL; do
        grep -qx "$option=y" "$KERNEL_CONFIG" ||
            die "$KERNEL_CONFIG must contain $option=y (built into the kernel)"
    done
}

validate_inputs() {
    local check_static=$1
    local cache_lines

    require_executable "$QEMU_BINARY"
    require_executable "$CXL_MEMSIM_SERVER_BINARY"
    require_executable "$BUSYBOX_BINARY"
    require_executable "$CXL_BINARY"
    require_executable "$DAXCTL_BINARY"
    require_executable "$CPIO_BINARY"
    require_executable "$TYPE2_LITMUS_RUNNER"
    require_readable_file "$KERNEL_IMAGE"
    require_readable_file "$KERNEL_CONFIG"
    require_readable_file "$GUEST_INIT"
    require_readable_file "$VALIDATOR"
    require_positive_integer CXL_MEMSIM_PORT "$CXL_MEMSIM_PORT"
    require_positive_integer VM_CPUS "$VM_CPUS"
    require_positive_integer LITMUS_ITERATIONS "$LITMUS_ITERATIONS"
    require_positive_integer V2_CACHE_CAPACITY "$V2_CACHE_CAPACITY"
    require_positive_integer V2_CACHE_WAYS "$V2_CACHE_WAYS"
    require_positive_integer V2_TIMEOUT_MS "$V2_TIMEOUT_MS"
    require_positive_integer SERVER_START_TIMEOUT "$SERVER_START_TIMEOUT"
    require_positive_integer QEMU_RUN_TIMEOUT "$QEMU_RUN_TIMEOUT"
    require_positive_integer PROCESS_STOP_TIMEOUT "$PROCESS_STOP_TIMEOUT"
    [[ "$CXL_MEMSIM_PORT" -le 65535 ]] || die "CXL_MEMSIM_PORT must be at most 65535"
    [[ "$V2_CACHE_CAPACITY" -ge 64 && $((V2_CACHE_CAPACITY % 64)) -eq 0 ]] ||
        die "V2_CACHE_CAPACITY must be a positive multiple of 64 bytes"
    cache_lines=$((V2_CACHE_CAPACITY / 64))
    [[ $((cache_lines % V2_CACHE_WAYS)) -eq 0 ]] ||
        die "V2_CACHE_WAYS must divide the bounded cache line count"
    validate_builtin_kernel_config
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

copy_into_initramfs() {
    local source=$1
    local destination="$INITRAMFS_ROOT$source"

    mkdir -p "$(dirname "$destination")"
    cp -L "$source" "$destination"
}

copy_dynamic_dependencies() {
    local binary=$1
    local dependency
    local interpreter

    interpreter=$(readelf -l "$binary" |
        sed -n 's/.*Requesting program interpreter: \([^]]*\)].*/\1/p')
    if [[ -n "$interpreter" ]]; then
        copy_into_initramfs "$interpreter"
    fi

    while IFS= read -r dependency; do
        [[ -n "$dependency" ]] || continue
        copy_into_initramfs "$dependency"
    done < <(ldd "$binary" |
        awk '/=> \// { print $3 } /^[[:space:]]*\// { print $1 }')
}

build_initramfs() {
    local applet

    mkdir -p "$INITRAMFS_ROOT"/{bin,dev,etc,lib,lib64,proc,run,sbin,sys,tmp,usr/bin,usr/lib}
    cp "$BUSYBOX_BINARY" "$INITRAMFS_ROOT/bin/busybox"
    chmod 0755 "$INITRAMFS_ROOT/bin/busybox"
    for applet in sh mount mkdir sleep cat dmesg poweroff sync; do
        ln -s busybox "$INITRAMFS_ROOT/bin/$applet"
    done
    cp "$CXL_BINARY" "$INITRAMFS_ROOT/usr/bin/cxl"
    cp "$DAXCTL_BINARY" "$INITRAMFS_ROOT/usr/bin/daxctl"
    copy_dynamic_dependencies "$CXL_BINARY"
    copy_dynamic_dependencies "$DAXCTL_BINARY"
    cp "$TYPE2_LITMUS_RUNNER" "$INITRAMFS_ROOT/bin/type2_device_litmus.static"
    cp "$GUEST_INIT" "$INITRAMFS_ROOT/init"
    chmod 0755 "$INITRAMFS_ROOT/init" \
        "$INITRAMFS_ROOT/usr/bin/cxl" \
        "$INITRAMFS_ROOT/usr/bin/daxctl" \
        "$INITRAMFS_ROOT/bin/type2_device_litmus.static"
    (
        cd "$INITRAMFS_ROOT"
        find . -print0 | "$CPIO_BINARY" --null -o --format=newc
    ) >"$INITRAMFS_IMAGE" 2>"$RUN_DIR/cpio.log"
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

wait_for_process_exit() {
    local pid=$1
    local timeout_seconds=$2
    local attempts=$((timeout_seconds * 10))

    for ((attempt = 0; attempt < attempts; ++attempt)); do
        if ! kill -0 "$pid" >/dev/null 2>&1; then
            wait "$pid"
            return $?
        fi
        sleep 0.1
    done
    return 124
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
case "${1:-}" in
    --help|-h)
        usage
        exit 0
        ;;
    --dry-run)
        MODE=dry-run
        shift
        ;;
    "")
        ;;
    *)
        usage >&2
        die "unknown option: $1"
        ;;
esac
[[ $# -eq 0 ]] || die "unexpected positional arguments"

validate_inputs "$([[ "$MODE" == run ]] && echo 1 || echo 0)"

QEMU_DEVICE="cxl-type2,bus=type2_rp,id=cxl_type2_0,sn=2,gpu-mode=0,coherency-enabled=true,cache-size=128M,mem-size=256M,cxlmemsim-addr=$CXL_MEMSIM_HOST,cxlmemsim-port=$CXL_MEMSIM_PORT,coherence-v2=on,coherence-v2-host-endpoint=0,coherence-v2-device-endpoint=1,coherence-v2-cache-capacity=$V2_CACHE_CAPACITY,coherence-v2-cache-ways=$V2_CACHE_WAYS,coherence-v2-timeout-ms=$V2_TIMEOUT_MS,coherence-v2-write-through=off"
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
    -initrd "$INITRAMFS_IMAGE"
    -append "console=ttyS0,115200 rdinit=/init nokaslr panic=-1 type2_iterations=$LITMUS_ITERATIONS cxl_type2_accel.enable_cache=1 cxl_type2_accel.enable_memdev=1 cxl_type2_accel.use_dvsec_hdm=0"
    -device "pxb-cxl,bus_nr=12,bus=pcie.0,id=cxl.1"
    -device "cxl-rp,port=0,bus=cxl.1,id=type2_rp,chassis=0,slot=0"
    -device "$QEMU_DEVICE"
    -nographic
    -no-reboot
)

if [[ "$MODE" == dry-run ]]; then
    echo "proof_boundary=$PROOF_BOUNDARY"
    echo "build-minimal-initramfs $INITRAMFS_IMAGE"
    print_command env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
    print_command env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}"
    echo "artifacts=$RUN_DIR"
    exit 0
fi

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$RUN_DIR"
build_initramfs

{
    echo "proof_boundary=$PROOF_BOUNDARY"
    echo "initramfs=$INITRAMFS_IMAGE"
    print_command env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
    print_command env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}"
} >"$COMMAND_LOG"

if tcp_is_open "$CXL_MEMSIM_HOST" "$CXL_MEMSIM_PORT"; then
    die "CXLMemSim port is already in use: $CXL_MEMSIM_HOST:$CXL_MEMSIM_PORT"
fi

(
    cd "$REPO_ROOT"
    exec env CXL_BASE_ADDR=0 SPDLOG_LEVEL=info "${SERVER_COMMAND[@]}"
) >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
wait_for_server

env CXL_TRANSPORT_MODE=tcp "${QEMU_COMMAND[@]}" >"$QEMU_LOG" 2>&1 &
QEMU_PID=$!
set +e
wait_for_process_exit "$QEMU_PID" "$QEMU_RUN_TIMEOUT"
QEMU_STATUS=$?
set -e
if [[ "$QEMU_STATUS" -eq 124 ]]; then
    die "QEMU timed out after $QEMU_RUN_TIMEOUT seconds; see $QEMU_LOG"
fi
QEMU_PID=

stop_process "$SERVER_PID" CXLMemSim
SERVER_PID=

tr -d '\r' <"$QEMU_LOG" >"$SERIAL_LOG"
awk '
    /^TYPE2_LITMUS_JSON_BEGIN$/ { capture = 1; next }
    /^TYPE2_LITMUS_JSON_END$/ { capture = 0 }
    capture && /^\{/ { print; exit }
' "$SERIAL_LOG" >"$GUEST_JSON"
[[ -s "$GUEST_JSON" ]] || die "guest JSON is missing; see $SERIAL_LOG"

set +e
bash "$VALIDATOR" --validate-only "$GUEST_JSON" "$SERVER_LOG" \
    >"$VALIDATION_JSON" 2>"$VALIDATION_LOG"
VALIDATION_STATUS=$?
set -e
if [[ -s "$VALIDATION_JSON" ]]; then
    cat "$VALIDATION_JSON"
fi
if [[ "$QEMU_STATUS" -ne 0 ]]; then
    echo "ERROR: QEMU exited with status $QEMU_STATUS; see $QEMU_LOG" >&2
fi
if [[ "$VALIDATION_STATUS" -ne 0 ]]; then
    cat "$VALIDATION_LOG" >&2
fi
if [[ "$QEMU_STATUS" -ne 0 || "$VALIDATION_STATUS" -ne 0 ]]; then
    exit 1
fi

echo "Type-2 coherence litmus passed"
echo "Proof boundary: $PROOF_BOUNDARY"
echo "Artifacts: $RUN_DIR"
