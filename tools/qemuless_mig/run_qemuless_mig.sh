#!/usr/bin/env bash
# Run the host-only QEMULess four-MIG checkpoint experiment.
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
capacity_mb=524288
checkpoint_bytes=$((256 * 1024))
iterations=100
shm_name=
cxl_build=${QEMULESS_CXL_BUILD:-/tmp/cxlmemsim-qemuless-build}
splash_build=${QEMULESS_SPLASH_BUILD:-/tmp/splash-qemuless-build}
shm_dir=${QEMULESS_SHM_DIR:-/dev/shm}
mpi_signal_timeout=${QEMULESS_MPI_SIGNAL_TIMEOUT_SECONDS:-5}
server_int_timeout=${QEMULESS_SERVER_INT_TIMEOUT_SECONDS:-30}
server_term_timeout=${QEMULESS_SERVER_TERM_TIMEOUT_SECONDS:-5}
splash_dir=$(realpath "${repo_root}/../Splash")
artifact_dir="${repo_root}/artifact/qemuless_mig/$(date -u +%Y%m%dT%H%M%SZ)"
configure_mig=false
dry_run=false
show_help=false
server_pid=
server_started=false
server_stopped=false
server_reaped=false
mpi_pid=
mpi_pgid=
mpi_reaped=true
shm_owned=false
backing_owned=false
artifact_ready=false
commands_log=
ssd_backing_file=
shm_path=
summarizer="$repo_root/tools/qemuless_mig/summarize_results.py"

declare -a mig_uuids=()
declare -a gpu_instance_profiles=()
declare -A seen_options=()

usage() {
    cat <<'EOF'
Usage: run_qemuless_mig.sh [options]

Options:
  --configure-mig              Create four profile-14 MIG instances on GPU 0 when needed.
  --dry-run                    Record the production command plan without configuring, building, or launching.
                               Read-only NVIDIA probes and local jq map generation still run.
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
    shm_name="/cxlmemsim_pgas_$(date -u +%Y%m%dT%H%M%S)_${BASHPID}_${RANDOM}${RANDOM}"
    shm_path="${shm_dir%/}/${shm_name#/}"
    artifact_ready=true
}

record_command() {
    local argument
    for argument in "$@"; do
        printf '%q ' "$argument" >> "$commands_log"
    done
    printf '\n' >> "$commands_log"
}

record_command_with_redirection() {
    local input=$1
    local output=$2
    local operator=$3
    local suffix=$4
    shift 4
    local argument
    for argument in "$@"; do
        printf '%q ' "$argument" >> "$commands_log"
    done
    if [[ -n "$input" ]]; then
        printf '< %q ' "$input" >> "$commands_log"
    fi
    printf '%s %q' "$operator" "$output" >> "$commands_log"
    if [[ -n "$suffix" ]]; then
        printf ' %s' "$suffix" >> "$commands_log"
    fi
    printf '\n' >> "$commands_log"
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

pid_is_running() {
    local pid=$1
    local stat
    local state

    kill -0 "$pid" 2>/dev/null || return 1
    [[ -r "/proc/$pid/stat" ]] || return 0
    stat=$(< "/proc/$pid/stat")
    state=${stat#*) }
    state=${state%% *}
    [[ "$state" != Z && "$state" != X ]]
}

process_group_is_running() {
    local target_pgid=$1
    local member_pgid
    local member_state

    while read -r member_pgid member_state; do
        [[ "$member_pgid" == "$target_pgid" ]] || continue
        [[ "$member_state" != Z* && "$member_state" != X* ]] && return 0
    done < <(ps -eo pgid=,stat=)
    return 1
}

capture_nvidia_snapshot() {
    local output=$1
    record_command_with_redirection "" "$output" ">" "2>&1" nvidia-smi
    if command -v nvidia-smi >/dev/null 2>&1; then
        nvidia-smi > "$output" 2>&1 || printf 'nvidia-smi snapshot failed\n' >> "$output"
    else
        printf 'nvidia-smi is unavailable\n' > "$output"
    fi
}

capture_mig_listing() {
    local output=$1
    record_command_with_redirection "" "$output" ">" "" nvidia-smi -L
    nvidia-smi -L > "$output"
}

capture_gpu_instance_listing() {
    local output=$1
    local listing
    local status
    record_command_with_redirection "" "$output" ">" "2>&1" nvidia-smi mig -i 0 -lgi
    if nvidia-smi mig -i 0 -lgi > "$output" 2>&1; then
        return
    else
        status=$?
    fi
    listing=$(< "$output")
    case "$listing" in
        'No GPU instances found: Not Found'|'No MIG-enabled devices found.') return ;;
        *) return "$status" ;;
    esac
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
        if [[ "$gpu_index" == 0 && "$line" =~ ^[[:space:]]+MIG[[:space:]].*Device[[:space:]]+([0-9]+):[[:space:]]+\(UUID:[[:space:]]*(MIG-[[:xdigit:]]{8}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{4}-[[:xdigit:]]{12})\)[[:space:]]*$ ]]; then
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

extract_gpu_zero_profile_ids() {
    local listing=$1
    local line
    local -a fields=()
    local rows=0

    gpu_instance_profiles=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ "$line" =~ ^\|[[:space:]]*0[[:space:]]+MIG[[:space:]] ]]; then
            ((rows += 1))
            line=${line#|}
            line=${line%|}
            read -r -a fields <<< "$line"
            ((${#fields[@]} >= 6)) || return 1
            [[ "${fields[0]}" == 0 && "${fields[1]}" == MIG ]] || return 1
            [[ "${fields[3]}" =~ ^[0-9]+$ && "${fields[4]}" =~ ^[0-9]+$ ]] || return 1
            [[ "${fields[5]}" =~ ^[0-9]+:[0-9]+$ ]] || return 1
            gpu_instance_profiles+=("${fields[3]}")
        fi
    done < "$listing"
    ((rows == ${#gpu_instance_profiles[@]}))
}

validate_gpu_zero_mig_layout() {
    local uuid_listing=$1
    local gi_listing=$2
    local profile_id

    extract_gpu_zero_profile_ids "$gi_listing" || return 1
    extract_gpu_zero_migs "$uuid_listing" || return 1
    ((${#gpu_instance_profiles[@]} == 4)) || return 1
    for profile_id in "${gpu_instance_profiles[@]}"; do
        [[ "$profile_id" == 14 ]] || return 1
    done
}

record_compute_pids() {
    local output="$artifact_dir/compute-pids.txt"
    record_command_with_redirection "" "$output" ">" "" nvidia-smi --query-compute-apps=pid \
        --format=csv,noheader,nounits
    nvidia-smi --query-compute-apps=pid --format=csv,noheader,nounits > "$output"
    [[ -z "$(tr -d '[:space:]' < "$output")" ]]
}

configure_gpu_zero_mig() {
    if ! record_compute_pids; then
        die "refusing MIG reconfiguration while compute PIDs are active; see $artifact_dir/compute-pids.txt"
    fi

    run_or_plan sudo nvidia-smi -i 0 -mig 1
    if ((${#gpu_instance_profiles[@]} > 0)); then
        run_or_plan sudo nvidia-smi mig -i 0 -dci
        run_or_plan sudo nvidia-smi mig -i 0 -dgi
    fi
    run_or_plan sudo nvidia-smi mig -i 0 -cgi 14,14,14,14 -C

    local mig_mode="$artifact_dir/mig-mode.txt"
    record_command_with_redirection "" "$mig_mode" ">" "" nvidia-smi -i 0 --query-gpu=mig.mode.current \
        --format=csv,noheader,nounits
    if ! "$dry_run"; then
        nvidia-smi -i 0 --query-gpu=mig.mode.current --format=csv,noheader,nounits > "$mig_mode"
        [[ "$(tr -d '[:space:]' < "$mig_mode")" == Enabled ]] || die "GPU 0 did not remain MIG enabled"
    fi
}

write_mig_artifacts() {
    printf '%s\n' "${mig_uuids[@]}" > "$artifact_dir/mig-uuids.txt"
    local filter='[inputs] as $u | {migs:$u, ranks:[range(0;8) | {rank:., mig_index:(. % 4), mig_uuid:$u[. % 4]}]}'
    record_command_with_redirection "$artifact_dir/mig-uuids.txt" "$artifact_dir/mig-map.json" ">" "" jq -Rn "$filter"
    jq -Rn "$filter" < "$artifact_dir/mig-uuids.txt" > "$artifact_dir/mig-map.json"
}

validate_prerequisites() {
    local command
    validate_positive_decimal QEMULESS_MPI_SIGNAL_TIMEOUT_SECONDS "$mpi_signal_timeout" 60
    validate_positive_decimal QEMULESS_SERVER_INT_TIMEOUT_SECONDS "$server_int_timeout" 60
    validate_positive_decimal QEMULESS_SERVER_TERM_TIMEOUT_SECONDS "$server_term_timeout" 60
    for command in nvidia-smi cmake mpirun jq python3 ps setsid; do
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
    [[ -d "$shm_dir" ]] || die "PGAS SHM directory is not a directory: $shm_dir"
    [[ ! -e "$shm_path" ]] || die "refusing to reuse pre-existing PGAS SHM object: $shm_path"
    [[ ! -e "$ssd_backing_file" ]] || die "refusing to reuse pre-existing SSD backing file: $ssd_backing_file"
}

configure_builds() {
    run_or_plan env CCACHE_DISABLE=1 cmake -S "$repo_root" -B "$cxl_build" -DCMAKE_BUILD_TYPE=Release
    run_or_plan env CCACHE_DISABLE=1 cmake --build "$cxl_build" --target cxlmemsim_server -j
    run_or_plan env CCACHE_DISABLE=1 cmake -S "$splash_dir" -B "$splash_build" -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_CUDA_COMPILER=/usr/local/cuda-12.8/bin/nvcc
    run_or_plan env CCACHE_DISABLE=1 cmake --build "$splash_build" --target qemuless_mig_oversubscribe -j

    if ! "$dry_run"; then
        [[ -x "$cxl_build/cxlmemsim_server" ]] || die "CXLMemSim server build did not produce cxlmemsim_server"
        [[ -x "$splash_build/qemuless_mig_oversubscribe" ]] || die "Splash build did not produce qemuless_mig_oversubscribe"
    fi
}

claim_shm_name() {
    local claim_program='import os, sys; fd = os.open(sys.argv[1], os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600); os.close(fd)'

    [[ ! -e "$shm_path" ]] || die "refusing to reuse pre-existing PGAS SHM object: $shm_path"
    run_or_plan python3 -c "$claim_program" "$shm_path"
    if ! "$dry_run"; then
        shm_owned=true
    fi
}

pgas_header_ready() {
    local probe_program='import os, struct, sys
path = sys.argv[1]
try:
    if os.stat(path).st_size < 64 + 64 * 256:
        raise ValueError("short PGAS SHM object")
    with open(path, "rb", buffering=0) as handle:
        magic, version, num_slots, server_ready = struct.unpack("<QIII", handle.read(20))
    if (magic, version, num_slots, server_ready) != (0x43584C53484D454D, 1, 64, 1):
        raise ValueError("invalid PGAS SHM header")
except (OSError, ValueError, struct.error):
    sys.exit(1)'

    record_command python3 -c "$probe_program" "$shm_path"
    python3 -c "$probe_program" "$shm_path"
}

start_server() {
    local server="$cxl_build/cxlmemsim_server"
    claim_shm_name
    record_command_with_redirection "" "$artifact_dir/server.log" ">" "2>&1 &" "$server" --comm-mode=pgas-shm \
        --pgas-shm-name="$shm_name" --capacity="$capacity_mb" \
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
        if pgas_header_ready; then
            return
        fi
        if ! pid_is_running "$server_pid"; then
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

    record_command_with_redirection "" "$mpi_log" ">" "2>&1 &" setsid --wait "${command[@]}"
    if ! "$dry_run"; then
        setsid --wait "${command[@]}" > "$mpi_log" 2>&1 &
        mpi_pid=$!
        mpi_pgid=$mpi_pid
        mpi_reaped=false
        local mpi_status
        if wait "$mpi_pid"; then
            mpi_status=0
        else
            mpi_status=$?
        fi
        mpi_reaped=true
        if process_group_is_running "$mpi_pgid"; then
            terminate_mpi || true
            ((mpi_status != 0)) || mpi_status=1
        else
            mpi_pid=
            mpi_pgid=
        fi
        return "$mpi_status"
    fi
}

terminate_mpi() {
    if [[ -z "$mpi_pgid" ]]; then
        return
    fi
    if ! process_group_is_running "$mpi_pgid"; then
        if [[ "$mpi_reaped" == false ]]; then
            wait "$mpi_pid" 2>/dev/null || true
        fi
        mpi_reaped=true
        mpi_pid=
        mpi_pgid=
        return
    fi

    local signal
    local timeout
    local deadline
    for signal in INT TERM KILL; do
        timeout=$mpi_signal_timeout
        [[ "$signal" != KILL ]] || timeout=1
        record_command kill "-$signal" -- "-$mpi_pgid"
        kill "-$signal" -- "-$mpi_pgid" 2>/dev/null || true
        deadline=$((SECONDS + timeout))
        while process_group_is_running "$mpi_pgid"; do
            ((SECONDS < deadline)) || break
            sleep 1
        done
        if ! process_group_is_running "$mpi_pgid"; then
            break
        fi
    done
    local group_survived=false
    process_group_is_running "$mpi_pgid" && group_survived=true
    if [[ "$mpi_reaped" == false ]]; then
        wait "$mpi_pid" 2>/dev/null || true
    fi
    mpi_reaped=true
    mpi_pid=
    mpi_pgid=
    [[ "$group_survived" == false ]]
}

stop_server() {
    if [[ -z "$server_pid" || "$server_stopped" == true ]]; then
        return
    fi
    if ! pid_is_running "$server_pid"; then
        if ! wait "$server_pid"; then
            server_stopped=true
            server_reaped=true
            return 1
        fi
        server_stopped=true
        server_reaped=true
        return
    fi

    record_command kill -INT "$server_pid"
    kill -INT "$server_pid"
    local deadline=$((SECONDS + server_int_timeout))
    while pid_is_running "$server_pid"; do
        ((SECONDS < deadline)) || break
        sleep 1
    done
    if pid_is_running "$server_pid"; then
        record_command kill -TERM "$server_pid"
        kill -TERM "$server_pid"
        deadline=$((SECONDS + server_term_timeout))
        while pid_is_running "$server_pid"; do
            ((SECONDS < deadline)) || break
            sleep 1
        done
        if pid_is_running "$server_pid"; then
            record_command kill -KILL "$server_pid"
            kill -KILL "$server_pid"
        fi
        if ! wait "$server_pid"; then
            server_stopped=true
            server_reaped=true
            return 1
        fi
        server_stopped=true
        server_reaped=true
        return 1
    fi
    if ! wait "$server_pid"; then
        server_stopped=true
        server_reaped=true
        return 1
    fi
    server_stopped=true
    server_reaped=true
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
            record_command_with_redirection "$result" "$output" ">>" "" jq -c .
            continue
        fi
        [[ -f "$result" ]] || die "missing rank result: $result"
        record_command_with_redirection "$result" "$output" ">>" "" jq -c .
        jq -c . < "$result" >> "$output"
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
    if [[ -n "$server_pid" && "$server_reaped" == false ]] && pid_is_running "$server_pid"; then
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
    trap '' INT TERM
    terminate_mpi
    if [[ -n "$server_pid" && "$server_stopped" == false ]]; then
        if ! stop_server; then
            ((status != 0)) || status=1
        fi
    fi
    cleanup_owned_resources
    capture_nvidia_snapshot "$artifact_dir/nvidia-smi-after.txt"
    exit "$status"
}

on_signal() {
    exit "$1"
}

main() {
    parse_args "$@"
    prepare_artifact_dir
    trap on_exit EXIT
    trap 'on_signal 130' INT
    trap 'on_signal 143' TERM
    validate_prerequisites
    capture_nvidia_snapshot "$artifact_dir/nvidia-smi-before.txt"

    local mig_listing="$artifact_dir/nvidia-smi-mig-before.txt"
    local gi_listing="$artifact_dir/nvidia-smi-gi-before.txt"
    capture_mig_listing "$mig_listing"
    capture_gpu_instance_listing "$gi_listing"
    if ! validate_gpu_zero_mig_layout "$mig_listing" "$gi_listing"; then
        if ! "$configure_mig"; then
            die "GPU 0 must expose exactly four canonical UUIDs and GI profile IDs 14; rerun with --configure-mig after stopping compute jobs"
        fi
        configure_gpu_zero_mig
        mig_listing="$artifact_dir/nvidia-smi-mig-after-configure.txt"
        gi_listing="$artifact_dir/nvidia-smi-gi-after-configure.txt"
        capture_mig_listing "$mig_listing"
        capture_gpu_instance_listing "$gi_listing"
        validate_gpu_zero_mig_layout "$mig_listing" "$gi_listing" ||
            die "GPU 0 did not expose exactly four canonical UUIDs and GI profile IDs 14 after configuration"
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
