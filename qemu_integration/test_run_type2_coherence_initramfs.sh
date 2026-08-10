#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS="$SCRIPT_DIR/run_type2_coherence_initramfs.sh"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/type2-initramfs-harness-test.XXXXXX")
trap 'rm -rf "$TEST_DIR"' EXIT

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_contains() {
    local haystack=$1
    local needle=$2

    [[ "$haystack" == *"$needle"* ]] || fail "expected output to contain: $needle"
}

make_stub() {
    local path=$1

    printf '#!/usr/bin/env bash\nexit 0\n' >"$path"
    chmod +x "$path"
}

mkdir -p "$TEST_DIR/bin" "$TEST_DIR/input"
for command in qemu-system-x86_64 cxlmemsim_server busybox cxl daxctl cpio; do
    make_stub "$TEST_DIR/bin/$command"
done
printf 'kernel\n' >"$TEST_DIR/input/bzImage"
cat >"$TEST_DIR/input/kernel.config" <<'EOF'
CONFIG_CXL_BUS=y
CONFIG_CXL_PCI=y
CONFIG_CXL_ACPI=y
CONFIG_CXL_MEM=y
CONFIG_CXL_PORT=y
CONFIG_CXL_REGION=y
CONFIG_CXL_CACHE=y
CONFIG_CXL_TYPE2_ACCEL=y
CONFIG_DEV_DAX=y
CONFIG_DEV_DAX_CXL=y
EOF
make_stub "$TEST_DIR/input/type2_device_litmus.static"

help_output=$(bash "$HARNESS" --help)
assert_contains "$help_output" "TCG functional modeled coherence"
assert_contains "$help_output" "minimal initramfs"
assert_contains "$help_output" "built into the kernel"
assert_contains "$help_output" "--dry-run"

dry_output=$(
    QEMU_BINARY="$TEST_DIR/bin/qemu-system-x86_64" \
    CXL_MEMSIM_SERVER_BINARY="$TEST_DIR/bin/cxlmemsim_server" \
    BUSYBOX_BINARY="$TEST_DIR/bin/busybox" \
    CXL_BINARY="$TEST_DIR/bin/cxl" \
    DAXCTL_BINARY="$TEST_DIR/bin/daxctl" \
    CPIO_BINARY="$TEST_DIR/bin/cpio" \
    KERNEL_IMAGE="$TEST_DIR/input/bzImage" \
    KERNEL_CONFIG="$TEST_DIR/input/kernel.config" \
    TYPE2_LITMUS_RUNNER="$TEST_DIR/input/type2_device_litmus.static" \
    RUN_DIR="$TEST_DIR/run" \
    bash "$HARNESS" --dry-run
)
assert_contains "$dry_output" "rdinit=/init"
assert_contains "$dry_output" "-initrd $TEST_DIR/run/type2-litmus-initramfs.cpio"
assert_contains "$dry_output" "coherence-v2-cache-capacity=1024"
assert_contains "$dry_output" "coherence-v2-cache-ways=2"
assert_contains "$dry_output" "coherence-v2-write-through=off"
assert_contains "$dry_output" "cxl_type2_accel.enable_cache=1"
assert_contains "$dry_output" "cxl_type2_accel.enable_memdev=1"
assert_contains "$dry_output" "cxl_type2_accel.use_dvsec_hdm=0"
assert_contains "$dry_output" "--coherence-v2"
if [[ "$dry_output" == *" -drive "* ]]; then
    fail "diskless harness unexpectedly contains -drive"
fi
[[ ! -e "$TEST_DIR/run" ]] || fail "dry-run created its artifact directory"

guest_init=$(<"$SCRIPT_DIR/type2_litmus_initramfs_init.sh")
if [[ "$guest_init" == *"modprobe"* || "$guest_init" == *"insmod"* ]]; then
    fail "built-in-kernel guest init unexpectedly loads modules"
fi
first_dax_probe_line=$(grep -n 'for candidate in /dev/dax\*' \
    "$SCRIPT_DIR/type2_litmus_initramfs_init.sh" | head -n 1 | cut -d: -f1)
create_dax_line=$(grep -n '/usr/bin/daxctl create-device' \
    "$SCRIPT_DIR/type2_litmus_initramfs_init.sh" | head -n 1 | cut -d: -f1)
[[ "$first_dax_probe_line" -lt "$create_dax_line" ]] ||
    fail "guest init does not reuse an already-created devdax device"

sed 's/CONFIG_DEV_DAX_CXL=y/CONFIG_DEV_DAX_CXL=m/' \
    "$TEST_DIR/input/kernel.config" >"$TEST_DIR/input/modular-kernel.config"
set +e
modular_output=$(
    QEMU_BINARY="$TEST_DIR/bin/qemu-system-x86_64" \
    CXL_MEMSIM_SERVER_BINARY="$TEST_DIR/bin/cxlmemsim_server" \
    BUSYBOX_BINARY="$TEST_DIR/bin/busybox" \
    CXL_BINARY="$TEST_DIR/bin/cxl" \
    DAXCTL_BINARY="$TEST_DIR/bin/daxctl" \
    CPIO_BINARY="$TEST_DIR/bin/cpio" \
    KERNEL_IMAGE="$TEST_DIR/input/bzImage" \
    KERNEL_CONFIG="$TEST_DIR/input/modular-kernel.config" \
    TYPE2_LITMUS_RUNNER="$TEST_DIR/input/type2_device_litmus.static" \
    RUN_DIR="$TEST_DIR/run-modular" \
    bash "$HARNESS" --dry-run 2>&1
)
modular_status=$?
set -e
[[ "$modular_status" -ne 0 ]] || fail "modular CXL/DAX kernel config was accepted"
assert_contains "$modular_output" "CONFIG_DEV_DAX_CXL=y"

sed 's/CONFIG_CXL_TYPE2_ACCEL=y/# CONFIG_CXL_TYPE2_ACCEL is not set/' \
    "$TEST_DIR/input/kernel.config" >"$TEST_DIR/input/no-type2-kernel.config"
set +e
no_type2_output=$(
    QEMU_BINARY="$TEST_DIR/bin/qemu-system-x86_64" \
    CXL_MEMSIM_SERVER_BINARY="$TEST_DIR/bin/cxlmemsim_server" \
    BUSYBOX_BINARY="$TEST_DIR/bin/busybox" \
    CXL_BINARY="$TEST_DIR/bin/cxl" \
    DAXCTL_BINARY="$TEST_DIR/bin/daxctl" \
    CPIO_BINARY="$TEST_DIR/bin/cpio" \
    KERNEL_IMAGE="$TEST_DIR/input/bzImage" \
    KERNEL_CONFIG="$TEST_DIR/input/no-type2-kernel.config" \
    TYPE2_LITMUS_RUNNER="$TEST_DIR/input/type2_device_litmus.static" \
    RUN_DIR="$TEST_DIR/run-no-type2" \
    bash "$HARNESS" --dry-run 2>&1
)
no_type2_status=$?
set -e
[[ "$no_type2_status" -ne 0 ]] || fail "kernel without the Type-2 accelerator driver was accepted"
assert_contains "$no_type2_output" "CONFIG_CXL_TYPE2_ACCEL=y"

echo "Type-2 initramfs coherence harness tests passed"
