#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
launcher="$repo_root/tools/qemuless_mig/run_qemuless_mig.sh"
splash_dir=$(cd "$repo_root/../Splash" && pwd -P)
tmp=$(mktemp -d)
fixture_bin="$tmp/bin"
artifact="$tmp/artifact"
fixture_calls="$tmp/fixture-calls.log"

trap 'rm -rf "$tmp"' EXIT
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
    --query-compute-apps=pid)
        ;;
    *)
        printf 'fixture nvidia-smi snapshot\n'
        ;;
esac
EOF
cat > "$fixture_bin/cmake" <<'EOF'
#!/usr/bin/env bash
printf 'cmake %q\n' "$*" >> "${FIXTURE_CALLS:?}"
exit 97
EOF
cat > "$fixture_bin/mpirun" <<'EOF'
#!/usr/bin/env bash
printf 'mpirun %q\n' "$*" >> "${FIXTURE_CALLS:?}"
exit 97
EOF
chmod +x "$fixture_bin/nvidia-smi" "$fixture_bin/cmake" "$fixture_bin/mpirun"

PATH="$fixture_bin:$PATH" FIXTURE_CALLS="$fixture_calls" "$launcher" \
    --dry-run --artifact-dir "$artifact" --splash-dir "$splash_dir"

test -s "$artifact/commands.log"
test -s "$artifact/nvidia-smi-before.txt"
test -s "$artifact/nvidia-smi-after.txt"
test -s "$artifact/mig-uuids.txt"
test -s "$artifact/mig-map.json"
test ! -e "$fixture_calls"

grep -F -- '--comm-mode=pgas-shm' "$artifact/commands.log" >/dev/null
grep -F -- '--capacity=524288' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-backing-file=' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-cache-mb=64' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-io-uring=false' "$artifact/commands.log" >/dev/null
grep -F -- '--ssd-odirect=false' "$artifact/commands.log" >/dev/null
baseline_command="mpirun --allow-run-as-root --oversubscribe --bind-to none -np 4 -x MIG_UUID_FILE=$artifact/mig-uuids.txt $splash_dir/script/qemuless_mig_rank_wrapper.sh /tmp/splash-qemuless-build/qemuless_mig_oversubscribe --shm-name /cxlmemsim_pgas --result-dir $artifact/baseline --checkpoint-bytes 262144 --iterations 100 "
oversub_command="mpirun --allow-run-as-root --oversubscribe --bind-to none -np 8 -x MIG_UUID_FILE=$artifact/mig-uuids.txt $splash_dir/script/qemuless_mig_rank_wrapper.sh /tmp/splash-qemuless-build/qemuless_mig_oversubscribe --shm-name /cxlmemsim_pgas --result-dir $artifact/oversub --checkpoint-bytes 262144 --iterations 100 "
grep -Fx -- "$baseline_command" "$artifact/commands.log" >/dev/null
grep -Fx -- "$oversub_command" "$artifact/commands.log" >/dev/null
grep -F -- 'rm -f -- '"$artifact"'/cxlmemsim.ssd' "$artifact/commands.log" >/dev/null
grep -F -- 'rm -f -- /dev/shm/cxlmemsim_pgas' "$artifact/commands.log" >/dev/null
! grep -Fqi 'qemu-system' "$artifact/commands.log"

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
    if PATH="$fixture_bin:$PATH" FIXTURE_CALLS="$fixture_calls" "$launcher" "$@" >/dev/null 2>&1; then
        printf 'expected launcher failure: %q\n' "$*" >&2
        exit 1
    fi
}

expect_failure --dry-run --artifact-dir "$tmp/unknown" --unknown
expect_failure --dry-run --artifact-dir "$tmp/missing-checkpoint" --checkpoint-bytes
expect_failure --dry-run --artifact-dir "$tmp/zero-checkpoint" --checkpoint-bytes 0
expect_failure --dry-run --artifact-dir "$tmp/large-checkpoint" --checkpoint-bytes 16777217
expect_failure --dry-run --artifact-dir "$tmp/negative-iterations" --iterations -1
expect_failure --dry-run --artifact-dir "$tmp/missing-splash" --splash-dir
expect_failure --dry-run --artifact-dir "$tmp/empty-artifact" --artifact-dir ''
