#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
HARNESS="$SCRIPT_DIR/run_type2_coherence_litmus.sh"
TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/type2-litmus-harness-test.XXXXXX")
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
for command in qemu-system-x86_64 cxlmemsim_server qemu-img ssh scp; do
    make_stub "$TEST_DIR/bin/$command"
done
printf 'kernel\n' >"$TEST_DIR/input/bzImage"
printf 'base image\n' >"$TEST_DIR/input/qemu.img"
printf '#!/usr/bin/env bash\nexit 0\n' >"$TEST_DIR/input/setup_cxl_numa.sh"
printf '#!/usr/bin/env bash\nexit 0\n' >"$TEST_DIR/input/type2_device_litmus.static"
chmod +x "$TEST_DIR/input/setup_cxl_numa.sh" "$TEST_DIR/input/type2_device_litmus.static"

help_output=$(bash "$HARNESS" --help)
assert_contains "$help_output" "TCG functional modeled coherence"
assert_contains "$help_output" "--dry-run"
assert_contains "$help_output" "--validate-only"
assert_contains "$help_output" "build/type2_device_litmus.static"

dry_output=$(
    QEMU_BINARY="$TEST_DIR/bin/qemu-system-x86_64" \
    CXL_MEMSIM_SERVER_BINARY="$TEST_DIR/bin/cxlmemsim_server" \
    QEMU_IMG_BINARY="$TEST_DIR/bin/qemu-img" \
    SSH_BINARY="$TEST_DIR/bin/ssh" \
    SCP_BINARY="$TEST_DIR/bin/scp" \
    KERNEL_IMAGE="$TEST_DIR/input/bzImage" \
    BASE_DISK_IMAGE="$TEST_DIR/input/qemu.img" \
    SETUP_SCRIPT="$TEST_DIR/input/setup_cxl_numa.sh" \
    TYPE2_LITMUS_RUNNER="$TEST_DIR/input/type2_device_litmus.static" \
    RUN_DIR="$TEST_DIR/run" \
    bash "$HARNESS" --dry-run
)
assert_contains "$dry_output" "qemu-img create -f qcow2 -F raw -b $TEST_DIR/input/qemu.img"
assert_contains "$dry_output" "--coherence-v2"
assert_contains "$dry_output" "--comm-mode=tcp"
assert_contains "$dry_output" "-accel tcg,thread=multi"
assert_contains "$dry_output" "mem-size=256M"
assert_contains "$dry_output" "coherence-v2-host-endpoint=0"
assert_contains "$dry_output" "coherence-v2-device-endpoint=1"
assert_contains "$dry_output" "coherence-v2-cache-capacity=1024"
assert_contains "$dry_output" "coherence-v2-cache-ways=2"
assert_contains "$dry_output" "coherence-v2-write-through=off"
assert_contains "$dry_output" "systemd.mask=cxl-numa-setup.service"
assert_contains "$dry_output" "REGION_SIZE=256M"
assert_contains "$dry_output" "TCG functional modeled coherence"

cat >"$TEST_DIR/guest-pass.json" <<'EOF'
{"schema":"cxlmemsim.type2-litmus.v1","status":"pass","proof_boundary":"TCG functional modeled coherence","topology":{"host_endpoint":0,"device_endpoint":1,"device_session":73},"negative_control":{"forbidden":1},"forbidden_total":0}
EOF
cat >"$TEST_DIR/server-pass.log" <<'EOF'
Coherence v2 MESI Statistics:
  GETS: 19
  GETM: 23
  UPGRADE: 7
  PUTM: 11
  Atomic: 5
EOF

validate_output=$(bash "$HARNESS" --validate-only "$TEST_DIR/guest-pass.json" "$TEST_DIR/server-pass.log")
assert_contains "$validate_output" '"status":"pass"'
assert_contains "$validate_output" '"proof_boundary":"TCG functional modeled coherence"'

cat >"$TEST_DIR/guest-bad-endpoint.json" <<'EOF'
{"schema":"cxlmemsim.type2-litmus.v1","status":"pass","proof_boundary":"TCG functional modeled coherence","topology":{"host_endpoint":0,"device_endpoint":2,"device_session":73},"negative_control":{"forbidden":1},"forbidden_total":0}
EOF
if bash "$HARNESS" --validate-only "$TEST_DIR/guest-bad-endpoint.json" "$TEST_DIR/server-pass.log" \
    >"$TEST_DIR/bad-endpoint.out" 2>&1; then
    fail "validation accepted device endpoint 2"
fi
assert_contains "$(<"$TEST_DIR/bad-endpoint.out")" "device_endpoint must be 1"

sed 's/GETM: 23/GETM: 0/' "$TEST_DIR/server-pass.log" >"$TEST_DIR/server-zero-getm.log"
if bash "$HARNESS" --validate-only "$TEST_DIR/guest-pass.json" "$TEST_DIR/server-zero-getm.log" \
    >"$TEST_DIR/zero-getm.out" 2>&1; then
    fail "validation accepted zero GETM count"
fi
assert_contains "$(<"$TEST_DIR/zero-getm.out")" "GETM must be nonzero"

tail -n +2 "$TEST_DIR/server-pass.log" >"$TEST_DIR/server-no-v2-heading.log"
if bash "$HARNESS" --validate-only "$TEST_DIR/guest-pass.json" "$TEST_DIR/server-no-v2-heading.log" \
    >"$TEST_DIR/no-v2-heading.out" 2>&1; then
    fail "validation accepted counters outside the coherence v2 statistics section"
fi
assert_contains "$(<"$TEST_DIR/no-v2-heading.out")" "Coherence v2 MESI Statistics section is missing"

echo "Type-2 coherence litmus harness tests passed"
