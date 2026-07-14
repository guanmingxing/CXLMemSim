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
    mig)
        if [[ "${2:-}" == -i && "${4:-}" == -lgi ]]; then
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
expect_failure env FIXTURE_PROFILE_IDS=13 FIXTURE_FAIL_DCI=1 "$launcher" \
    --configure-mig --artifact-dir "$tmp/dci-failure" --splash-dir "$splash_dir"
grep -F -- 'mig -i 0 -dci' "$mutation_log" >/dev/null
! grep -F -- 'mig -i 0 -dgi' "$mutation_log"
! grep -F -- 'mig -i 0 -cgi 14,14,14,14 -C' "$mutation_log"
