#!/usr/bin/env bash
# Run the host-only QEMULess four-MIG checkpoint experiment.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
capacity_mb=524288
checkpoint_bytes=$((256 * 1024))
iterations=100
shm_name=/cxlmemsim_pgas
cxl_build=/tmp/cxlmemsim-qemuless-build
splash_build=/tmp/splash-qemuless-build
splash_dir=$(realpath "${repo_root}/../Splash")
artifact_dir="${repo_root}/artifact/qemuless_mig/$(date -u +%Y%m%dT%H%M%SZ)"
configure_mig=false
dry_run=false
show_help=false
server_pid=
server_started=false
server_stopped=false
shm_owned=false
backing_owned=false
artifact_ready=false
commands_log=
ssd_backing_file=
shm_path=
summarizer="$repo_root/tools/qemuless_mig/summarize_results.py"

declare -a mig_uuids=()
declare -A seen_options=()

usage() {
    cat <<'EOF'
Usage: run_qemuless_mig.sh [options]

Options:
  --configure-mig              Create four profile-14 MIG instances on GPU 0 when needed.
  --dry-run                    Record the production command plan without configuring, building, or launching.
  --artifact-dir PATH          Empty directory for experiment artifacts.
  --splash-dir PATH            Splash checkout containing the workload and rank wrapper.
  --checkpoint-bytes BYTES     Checkpoint size in bytes (1 through 16777216).
  --iterations COUNT           CUDA kernel iteration count (1 through 2147483647).
  -h, --help                   Show this help text.
EOF
}

die() {
    printf 'run_qemuless_mig: %s\n' "$*" >&2
    exit 2
}

mark_option() {
    local option=$1
    [[ -z "${seen_options[$option]:-}" ]] || die "duplicate option: $option"
    seen_options[$option]=1
}

require_value() {
    local option=$1
    local value=${2-}
    [[ -n "$value" && "$value" != --* ]] || die "missing value for $option"
    printf '%s\n' "$value"
}

validate_positive_decimal() {
    local option=$1
    local value=$2
    local maximum=$3

    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "invalid $option value: $value"
    if ((${#value} > ${#maximum})) ||
        { ((${#value} == ${#maximum})) && [[ "$value" > "$maximum" ]]; }; then
        die "invalid $option value: $value"
    fi
}

parse_args() {
    while (($#)); do
        case "$1" in
            --configure-mig)
                mark_option "$1"
                configure_mig=true
                shift
                ;;
            --dry-run)
                mark_option "$1"
                dry_run=true
                shift
                ;;
            --artifact-dir)
                mark_option "$1"
                (($# >= 2)) || die "missing value for $1"
                artifact_dir=$(require_value "$1" "$2")
                shift 2
                ;;
            --splash-dir)
                mark_option "$1"
                (($# >= 2)) || die "missing value for $1"
                splash_dir=$(require_value "$1" "$2")
                shift 2
                ;;
            --checkpoint-bytes)
                mark_option "$1"
                (($# >= 2)) || die "missing value for $1"
                checkpoint_bytes=$(require_value "$1" "$2")
                validate_positive_decimal "$1" "$checkpoint_bytes" 16777216
                shift 2
                ;;
            --iterations)
                mark_option "$1"
                (($# >= 2)) || die "missing value for $1"
                iterations=$(require_value "$1" "$2")
                validate_positive_decimal "$1" "$iterations" 2147483647
                shift 2
                ;;
            -h|--help)
                mark_option --help
                show_help=true
                shift
                ;;
            *)
                die "unknown option: $1"
                ;;
        esac
    done
    if "$show_help"; then
        usage
        exit 0
    fi
}

prepare_artifact_dir() {
    if [[ -e "$artifact_dir" ]]; then
        [[ -d "$artifact_dir" ]] || die "artifact path is not a directory: $artifact_dir"
        shopt -s dotglob nullglob
        local entries=("$artifact_dir"/*)
        shopt -u dotglob nullglob
        ((${#entries[@]} == 0)) || die "artifact directory is not empty: $artifact_dir"
    else
        mkdir -p -- "$artifact_dir"
    fi

    artifact_dir=$(cd "$artifact_dir" && pwd -P)
    commands_log="$artifact_dir/commands.log"
    : > "$commands_log"
    : > "$artifact_dir/server.log"
    ssd_backing_file="$artifact_dir/cxlmemsim.ssd"
    shm_path="/dev/shm/${shm_name#/}"
    artifact_ready=true
}

record_command() {
    local argument
    for argument in "$@"; do
        printf '%q ' "$argument" >> "$commands_log"
    done
    printf '\n' >> "$commands_log"
}

record_command_with_io() {
    local input=$1
    local output=$2
    shift 2
    local argument
    for argument in "$@"; do
        printf '%q ' "$argument" >> "$commands_log"
    done
    printf '< %q > %q\n' "$input" "$output" >> "$commands_log"
}

run_or_plan() {
    record_command "$@"
    if ! "$dry_run"; then
        "$@"
    fi
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

capture_nvidia_snapshot() {
    local output=$1
    record_command nvidia-smi
    if command -v nvidia-smi >/dev/null 2>&1; then
        nvidia-smi > "$output" 2>&1 || printf 'nvidia-smi snapshot failed\n' >> "$output"
    else
        printf 'nvidia-smi is unavailable\n' > "$output"
    fi
}

capture_mig_listing() {
    local output=$1
    record_command nvidia-smi -L
    nvidia-smi -L > "$output"
}

extract_gpu_zero_migs() {
    local listing=$1
    local line
    local gpu_index=-1
    local device_index
    local uuid

    mig_uuids=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ ^GPU[[:space:]]+([0-9]+): ]]; then
            gpu_index=${BASH_REMATCH[1]}
            continue
        fi
        if [[ "$gpu_index" == 0 && "$line" =~ ^[[:space:]]+MIG[[:space:]]+1g\.24gb[[:space:]]+Device[[:space:]]+([0-9]+):[[:space:]]+\(UUID:[[:space:]]*(MIG-[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12})\)[[:space:]]*$ ]]; then
            device_index=${BASH_REMATCH[1]}
            uuid=${BASH_REMATCH[2]}
            [[ "$device_index" == "${#mig_uuids[@]}" ]] || return 1
            mig_uuids+=("$uuid")
        fi
    done < "$listing"

    ((${#mig_uuids[@]} == 4)) || return 1
    local -A seen=()
    for uuid in "${mig_uuids[@]}"; do
        [[ -z "${seen[$uuid]:-}" ]] || return 1
        seen[$uuid]=1
    done
}

record_compute_pids() {
    local output="$artifact_dir/compute-pids.txt"
    record_command nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits
    nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits > "$output"
    [[ -z "$(tr -d '[:space:]' < "$output")" ]]
}

configure_gpu_zero_mig() {
    if ! record_compute_pids; then
        die "refusing MIG reconfiguration while compute PIDs are active; see $artifact_dir/compute-pids.txt"
    fi

    run_or_plan sudo nvidia-smi -i 0 -mig 1
    if "$dry_run"; then
        record_command sudo nvidia-smi mig -i 0 -dci
        record_command sudo nvidia-smi mig -i 0 -dgi
    else
        run_or_plan sudo nvidia-smi mig -i 0 -dci || true
        run_or_plan sudo nvidia-smi mig -i 0 -dgi || true
    fi
    run_or_plan sudo nvidia-smi mig -i 0 -cgi 14,14,14,14 -C

    local mig_mode="$artifact_dir/mig-mode.txt"
    record_command nvidia-smi -i 0 --query-gpu=mig.mode.current --format=csv,noheader,nounits
    if ! "$dry_run"; then
        nvidia-smi -i 0 --query-gpu=mig.mode.current --format=csv,noheader,nounits > "$mig_mode"
        [[ "$(tr -d '[:space:]' < "$mig_mode")" == Enabled ]] || die "GPU 0 did not remain MIG enabled"
    fi
}

write_mig_artifacts() {
    printf '%s\n' "${mig_uuids[@]}" > "$artifact_dir/mig-uuids.txt"
    local filter='[inputs] as $u | {migs:$u, ranks:[range(0;8) | {rank:., mig_index:(. % 4), mig_uuid:$u[. % 4]}]}'
    record_command_with_io "$artifact_dir/mig-uuids.txt" "$artifact_dir/mig-map.json" jq -Rn "$filter"
    jq -Rn "$filter" < "$artifact_dir/mig-uuids.txt" > "$artifact_dir/mig-map.json"
}

validate_prerequisites() {
    local command
    for command in nvidia-smi cmake mpirun jq python3; do
        require_command "$command"
    done
    [[ -x /usr/local/cuda-12.8/bin/nvcc ]] || die "CUDA 12.8 compiler is required at /usr/local/cuda-12.8/bin/nvcc"
    [[ -f "$repo_root/CMakeLists.txt" ]] || die "missing CXLMemSim CMakeLists.txt"
    [[ -f "$summarizer" ]] || die "missing evidence validator: $summarizer"
    [[ -d "$splash_dir" ]] || die "Splash checkout is not a directory: $splash_dir"
    splash_dir=$(cd "$splash_dir" && pwd -P)
    [[ -f "$splash_dir/CMakeLists.txt" ]] || die "missing Splash CMakeLists.txt"
    [[ -x "$splash_dir/script/qemuless_mig_rank_wrapper.sh" ]] ||
        die "missing executable Splash MIG rank wrapper"
    [[ ! -e "$shm_path" ]] || die "refusing to reuse pre-existing PGAS SHM object: $shm_path"
    [[ ! -e "$ssd_backing_file" ]] || die "refusing to reuse pre-existing SSD backing file: $ssd_backing_file"
}

configure_builds() {
    run_or_plan env CCACHE_DISABLE=1 cmake -S "$repo_root" -B "$cxl_build" -DCMAKE_BUILD_TYPE=Release
    run_or_plan cmake --build "$cxl_build" --target cxlmemsim_server -j
    run_or_plan env CCACHE_DISABLE=1 cmake -S "$splash_dir" -B "$splash_build" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CUDA_COMPILER=/usr/local/cuda-12.8/bin/nvcc
    run_or_plan cmake --build "$splash_build" --target qemuless_mig_oversubscribe -j

    if ! "$dry_run"; then
        [[ -x "$cxl_build/cxlmemsim_server" ]] || die "CXLMemSim server build did not produce cxlmemsim_server"
        [[ -x "$splash_build/qemuless_mig_oversubscribe" ]] || die "Splash build did not produce qemuless_mig_oversubscribe"
    fi
}

start_server() {
    local server="$cxl_build/cxlmemsim_server"
    record_command "$server" --comm-mode=pgas-shm --pgas-shm-name="$shm_name" --capacity="$capacity_mb" \
        --default_latency=100 --backing-mode=ssd-stream --ssd-backing-file="$ssd_backing_file" --ssd-cache-mb=64 \
        --ssd-io-uring=false --ssd-odirect=false
    if "$dry_run"; then
        return
    fi

    "$server" --comm-mode=pgas-shm --pgas-shm-name="$shm_name" --capacity="$capacity_mb" --default_latency=100 \
        --backing-mode=ssd-stream --ssd-backing-file="$ssd_backing_file" --ssd-cache-mb=64 --ssd-io-uring=false \
        --ssd-odirect=false > "$artifact_dir/server.log" 2>&1 &
    server_pid=$!
    server_started=true
    backing_owned=true

    local deadline=$((SECONDS + 30))
    while ((SECONDS < deadline)); do
        if [[ -e "$shm_path" ]]; then
            shm_owned=true
            return
        fi
        if ! kill -0 "$server_pid" 2>/dev/null; then
            wait "$server_pid" || true
            die "CXLMemSim server exited before PGAS SHM became ready; see $artifact_dir/server.log"
        fi
        sleep 1
    done
    die "timed out waiting for PGAS SHM object: $shm_path"
}

run_mpi_phase() {
    local ranks=$1
    local label=$2
    local result_dir="$artifact_dir/$label"
    local mpi_log="$artifact_dir/${label}-mpirun.log"
    local workload="$splash_build/qemuless_mig_oversubscribe"
    local wrapper="$splash_dir/script/qemuless_mig_rank_wrapper.sh"
    local -a command=(
        mpirun --allow-run-as-root --oversubscribe --bind-to none -np "$ranks"
        -x "MIG_UUID_FILE=$artifact_dir/mig-uuids.txt"
        "$wrapper" "$workload" --shm-name "$shm_name" --result-dir "$result_dir"
        --checkpoint-bytes "$checkpoint_bytes" --iterations "$iterations"
    )

    record_command "${command[@]}"
    if ! "$dry_run"; then
        "${command[@]}" > "$mpi_log" 2>&1
    fi
}

stop_server() {
    if [[ -z "$server_pid" || "$server_stopped" == true ]]; then
        return
    fi
    server_stopped=true

    if ! kill -0 "$server_pid" 2>/dev/null; then
        wait "$server_pid" || true
        return
    fi

    record_command kill -INT "$server_pid"
    kill -INT "$server_pid"
    local deadline=$((SECONDS + 30))
    while kill -0 "$server_pid" 2>/dev/null; do
        if ((SECONDS >= deadline)); then
            printf 'run_qemuless_mig: timed out waiting for server PID %s after SIGINT\n' "$server_pid" >&2
            return 1
        fi
        sleep 1
    done
    wait "$server_pid" || true
}

write_jsonl() {
    local label=$1
    local ranks=$2
    local result_dir="$artifact_dir/$label"
    local output="$artifact_dir/ranks-${label}.jsonl"
    local rank
    local result

    : > "$output"
    for ((rank = 0; rank < ranks; rank++)); do
        result="$result_dir/rank-$rank.json"
        if "$dry_run"; then
            record_command_with_io "$result" "$output" jq -c .
            continue
        fi
        [[ -f "$result" ]] || die "missing rank result: $result"
        record_command_with_io "$result" "$output" jq -c .
        jq -c . "$result" >> "$output"
    done
}

run_validator() {
    local output="$artifact_dir/summary.json"
    local -a command=(
        python3 "$summarizer" --baseline-dir "$artifact_dir/baseline" --oversub-dir "$artifact_dir/oversub"
        --server-log "$artifact_dir/server.log" --mig-uuid-file "$artifact_dir/mig-uuids.txt" --output "$output"
    )
    run_or_plan "${command[@]}"
}

cleanup_owned_resources() {
    if ! "$artifact_ready"; then
        return
    fi
    if "$dry_run"; then
        record_command rm -f -- "$ssd_backing_file"
        record_command rm -f -- "$shm_path"
        return
    fi
    if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
        printf 'run_qemuless_mig: preserving owned resources because server PID %s is still running\n' "$server_pid" >&2
        return
    fi
    if "$backing_owned"; then
        record_command rm -f -- "$ssd_backing_file"
        rm -f -- "$ssd_backing_file" || true
    fi
    if "$shm_owned"; then
        record_command rm -f -- "$shm_path"
        rm -f -- "$shm_path" || true
    fi
}

on_exit() {
    local status=$?
    trap - EXIT
    if [[ -n "$server_pid" && "$server_stopped" == false ]]; then
        if ! stop_server; then
            status=1
        fi
    fi
    cleanup_owned_resources
    capture_nvidia_snapshot "$artifact_dir/nvidia-smi-after.txt"
    exit "$status"
}

main() {
    parse_args "$@"
    prepare_artifact_dir
    trap on_exit EXIT
    validate_prerequisites
    capture_nvidia_snapshot "$artifact_dir/nvidia-smi-before.txt"

    local mig_listing="$artifact_dir/nvidia-smi-mig-before.txt"
    capture_mig_listing "$mig_listing"
    if ! extract_gpu_zero_migs "$mig_listing"; then
        if ! "$configure_mig"; then
            die "GPU 0 must expose exactly four canonical MIG UUIDs; rerun with --configure-mig after stopping compute jobs"
        fi
        configure_gpu_zero_mig
        mig_listing="$artifact_dir/nvidia-smi-mig-after-configure.txt"
        capture_mig_listing "$mig_listing"
        extract_gpu_zero_migs "$mig_listing" || die "GPU 0 did not expose exactly four canonical MIG UUIDs after configuration"
    fi
    write_mig_artifacts

    configure_builds
    start_server
    run_mpi_phase 4 baseline
    run_mpi_phase 8 oversub
    stop_server
    write_jsonl baseline 4
    write_jsonl oversub 8
    run_validator
}

main "$@"
