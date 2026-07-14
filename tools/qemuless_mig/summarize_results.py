#!/usr/bin/env python3
"""Validate QEMULess MIG rank evidence and emit a machine-readable summary."""

import argparse
import json
import math
import re
import sys
from collections import Counter
from pathlib import Path


RANK_FILE_RE = re.compile(r"rank-(0|[1-9][0-9]*)\.json$")
MIG_UUID_RE = re.compile(r"MIG-[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$")
UINT64_MAX = (1 << 64) - 1
REMOTE_RE = re.compile(r"^\s*Remote:\s+(\d+)\s*$", re.MULTILINE)
LOAD_RE = re.compile(r"^\s*Load:\s+(\d+)\s*$", re.MULTILINE)
STORE_RE = re.compile(r"^\s*Store:\s+(\d+)\s*$", re.MULTILINE)
THREADS_RE = re.compile(r"^\s*Number of Threads created:\s*(\d+)\s*$", re.MULTILINE)


class SummaryBuilder:
    def __init__(self):
        self.summary = {
            "verdict": "fail",
            "checks": {
                "arguments": False,
                "mig_uuid_file": False,
                "baseline_rank_files": False,
                "baseline_records": False,
                "baseline_ranges": False,
                "baseline_mig_occupancy": False,
                "oversubscription_rank_files": False,
                "oversubscription_records": False,
                "oversubscription_ranges": False,
                "oversubscription_mig_occupancy": False,
                "controller_counters": False,
            },
            "baseline": {"record_count": 0, "ranks": [], "mig_occupancy": {}},
            "oversubscription": {"record_count": 0, "ranks": [], "mig_occupancy": {}},
            "controller": {"remote": None, "loads": None, "stores": None, "threads": None},
            "errors": [],
        }

    def fail(self, check, error):
        self.summary["checks"][check] = False
        self.summary["errors"].append({"check": check, "error": error})

    def pass_check(self, check):
        if not any(error["check"] == check for error in self.summary["errors"]):
            self.summary["checks"][check] = True


class ArgumentParseError(Exception):
    pass


class EvidenceArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise ArgumentParseError(message)


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def is_uint64(value):
    return is_integer(value) and 0 <= value <= UINT64_MAX


def is_nonnegative_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def load_mig_uuids(path, builder):
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        builder.fail("mig_uuid_file", "cannot read MIG UUID file: {}".format(error))
        return []

    if len(lines) != 4:
        builder.fail("mig_uuid_file", "expected exactly four MIG UUID lines")
        return []
    if any(not MIG_UUID_RE.fullmatch(uuid) for uuid in lines):
        builder.fail("mig_uuid_file", "MIG UUID lines must use the Task 2 MIG UUID grammar")
        return []
    if len(set(lines)) != 4:
        builder.fail("mig_uuid_file", "MIG UUID lines must be unique")
        return []

    builder.pass_check("mig_uuid_file")
    return lines


def load_rank_records(directory, expected_world_size, label, builder):
    file_check = "{}_rank_files".format(label)
    group = builder.summary[label]
    expected_ranks = list(range(expected_world_size))
    directory = Path(directory)

    try:
        candidates = list(directory.glob("rank-*.json"))
    except OSError as error:
        builder.fail(file_check, "cannot read rank directory: {}".format(error))
        return []

    invalid_names = []
    rank_paths = {}
    for path in candidates:
        match = RANK_FILE_RE.fullmatch(path.name)
        if match is None:
            invalid_names.append(path.name)
            continue
        rank = int(match.group(1))
        if rank in rank_paths:
            invalid_names.append(path.name)
            continue
        rank_paths[rank] = path

    if invalid_names:
        builder.fail(file_check, "invalid or duplicate rank filenames: {}".format(", ".join(sorted(invalid_names))))
    if sorted(rank_paths) != expected_ranks:
        builder.fail(
            file_check,
            "expected rank files {} but found {}".format(expected_ranks, sorted(rank_paths)),
        )
    builder.pass_check(file_check)

    records = []
    for rank in sorted(rank_paths):
        path = rank_paths[rank]
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            builder.fail(file_check, "cannot parse {}: {}".format(path.name, error))
            continue
        if not isinstance(record, dict):
            builder.fail(file_check, "{} must contain a JSON object".format(path.name))
            continue
        records.append((rank, record))

    group["record_count"] = len(records)
    group["ranks"] = [rank for rank, _ in records]
    return records


def validate_records(records, expected_world_size, label, mig_uuids, builder):
    record_check = "{}_records".format(label)
    range_check = "{}_ranges".format(label)
    mig_check = "{}_mig_occupancy".format(label)
    group = builder.summary[label]
    expected_ranks = list(range(expected_world_size))
    records_valid = len(records) == expected_world_size
    ranges = []
    observed_uuids = []

    for filename_rank, record in records:
        prefix = "rank-{}".format(filename_rank)
        rank = record.get("rank")
        if not is_integer(rank) or rank != filename_rank:
            builder.fail(record_check, "{} has an invalid rank".format(prefix))
            records_valid = False
            rank = filename_rank
        if record.get("world_size") != expected_world_size or not is_integer(record.get("world_size")):
            builder.fail(record_check, "{} has an invalid world_size".format(prefix))
            records_valid = False
        if record.get("slot") != rank or not is_integer(record.get("slot")):
            builder.fail(record_check, "{} slot must equal rank".format(prefix))
            records_valid = False
        if not isinstance(record.get("device_name"), str) or not record["device_name"]:
            builder.fail(record_check, "{} device_name must be a nonempty string".format(prefix))
            records_valid = False

        for field in ("cuda_valid", "checkpoint_valid"):
            if type(record.get(field)) is not bool or not record[field]:
                builder.fail(record_check, "{} {} must be true".format(prefix, field))
                records_valid = False

        checkpoint_bytes = record.get("checkpoint_bytes")
        checkpoint_valid = is_uint64(checkpoint_bytes) and checkpoint_bytes > 0
        if not checkpoint_valid:
            builder.fail(record_check, "{} checkpoint_bytes must be a positive uint64".format(prefix))
            records_valid = False
        for field in ("cxl_bytes_read", "cxl_bytes_written"):
            value = record.get(field)
            if not is_uint64(value) or value <= 0 or value != checkpoint_bytes:
                builder.fail(
                    record_check,
                    "{} {} must be a positive uint64 equal to checkpoint_bytes".format(prefix, field),
                )
                records_valid = False

        cxl_addr = record.get("cxl_addr")
        if not is_uint64(cxl_addr):
            builder.fail(range_check, "{} cxl_addr must be a uint64".format(prefix))
        elif checkpoint_valid:
            range_end = cxl_addr + checkpoint_bytes
            if range_end > UINT64_MAX:
                builder.fail(range_check, "{} CXL range exceeds the uint64 address space".format(prefix))
            else:
                ranges.append((cxl_addr, range_end, filename_rank))

        for field in ("kernel_ms", "checkpoint_write_ms", "checkpoint_read_ms"):
            if not is_nonnegative_number(record.get(field)):
                builder.fail(record_check, "{} {} must be a nonnegative finite number".format(prefix, field))
                records_valid = False

        mig_uuid = record.get("mig_uuid")
        if not isinstance(mig_uuid, str):
            builder.fail(mig_check, "{} MIG UUID must be a string".format(prefix))
        else:
            observed_uuids.append(mig_uuid)
            if mig_uuids and mig_uuid != mig_uuids[filename_rank % 4]:
                builder.fail(mig_check, "{} MIG UUID does not match rank modulo four".format(prefix))

    if [rank for rank, _ in records] != expected_ranks:
        builder.fail(record_check, "records do not contain the complete expected rank set")
        records_valid = False
    if records_valid:
        builder.pass_check(record_check)

    ranges.sort()
    ranges_valid = len(ranges) == expected_world_size
    previous_end = None
    for start, end, rank in ranges:
        if previous_end is not None and start < previous_end:
            builder.fail(range_check, "rank-{} CXL range overlaps a previous range".format(rank))
            ranges_valid = False
        previous_end = max(previous_end or end, end)
    if ranges_valid:
        builder.pass_check(range_check)

    group["mig_occupancy"] = dict(sorted(Counter(observed_uuids).items()))
    expected_occupancy = Counter(mig_uuids)
    if expected_world_size == 8:
        expected_occupancy = Counter({uuid: 2 for uuid in mig_uuids})
    if mig_uuids and Counter(observed_uuids) == expected_occupancy:
        builder.pass_check(mig_check)
    else:
        builder.fail(mig_check, "MIG UUID occupancy does not match the canonical mapping")


def parse_controller(server_log, builder):
    try:
        log = Path(server_log).read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        builder.fail("controller_counters", "cannot read server log: {}".format(error))
        return

    remote_matches = REMOTE_RE.findall(log)
    load_matches = LOAD_RE.findall(log)
    store_matches = STORE_RE.findall(log)
    thread_matches = THREADS_RE.findall(log)
    if len(remote_matches) != 1 or not load_matches or not store_matches or len(thread_matches) != 1:
        builder.fail(
            "controller_counters",
            "server log must contain one Remote and thread count plus Load and Store counters",
        )
        return

    remote = int(remote_matches[0])
    loads = [int(value) for value in load_matches]
    stores = [int(value) for value in store_matches]
    threads = int(thread_matches[0])
    builder.summary["controller"] = {
        "remote": remote,
        "loads": sum(loads),
        "stores": sum(stores),
        "threads": threads,
    }
    if remote <= 0 or sum(loads) <= 0 or sum(stores) <= 0 or threads < 8:
        builder.fail(
            "controller_counters",
            "Remote, Load, and Store counters must be positive and threads must be at least eight",
        )
        return
    builder.pass_check("controller_counters")


def validate(args):
    builder = SummaryBuilder()
    builder.pass_check("arguments")
    mig_uuids = load_mig_uuids(args.mig_uuid_file, builder)
    baseline = load_rank_records(args.baseline_dir, 4, "baseline", builder)
    oversubscription = load_rank_records(args.oversub_dir, 8, "oversubscription", builder)
    validate_records(baseline, 4, "baseline", mig_uuids, builder)
    validate_records(oversubscription, 8, "oversubscription", mig_uuids, builder)
    parse_controller(args.server_log, builder)
    if all(builder.summary["checks"].values()) and not builder.summary["errors"]:
        builder.summary["verdict"] = "pass"
    return builder.summary


def write_summary(path, summary):
    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def build_parser():
    parser = EvidenceArgumentParser(description=__doc__)
    parser.add_argument("--baseline-dir", required=True)
    parser.add_argument("--oversub-dir", required=True)
    parser.add_argument("--server-log", required=True)
    parser.add_argument("--mig-uuid-file", required=True)
    parser.add_argument("--output", required=True)
    return parser


def output_path_from_argv(argv):
    output = None
    index = 0
    while index < len(argv):
        argument = argv[index]
        if argument == "--output" and index + 1 < len(argv) and not argv[index + 1].startswith("-"):
            output = argv[index + 1]
            index += 2
            continue
        if argument.startswith("--output=") and argument != "--output=":
            output = argument.split("=", 1)[1]
        index += 1
    return output


def write_argument_failure(output, error):
    builder = SummaryBuilder()
    builder.fail("arguments", "argument parsing failed: {}".format(error))
    try:
        write_summary(output, builder.summary)
    except OSError as write_error:
        print("cannot write summary: {}".format(write_error), file=sys.stderr)
    return 2


def main(argv=None):
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    try:
        args = parser.parse_args(raw_argv)
    except ArgumentParseError as error:
        output = output_path_from_argv(raw_argv)
        if output is not None:
            return write_argument_failure(output, error)
        parser.print_usage(sys.stderr)
        print("{}: error: {}".format(parser.prog, error), file=sys.stderr)
        return 2
    try:
        summary = validate(args)
    except Exception as error:  # Keep evidence output available after malformed input.
        builder = SummaryBuilder()
        builder.fail("controller_counters", "unexpected validation error: {}".format(error))
        summary = builder.summary

    try:
        write_summary(args.output, summary)
    except OSError as error:
        print("cannot write summary: {}".format(error), file=sys.stderr)
        return 1
    return 0 if summary["verdict"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
