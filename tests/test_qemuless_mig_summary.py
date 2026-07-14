#!/usr/bin/env python3
"""Contract tests for QEMULess MIG evidence summaries."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SUMMARIZER = REPO_ROOT / "tools" / "qemuless_mig" / "summarize_results.py"
MIG_UUIDS = [
    "MIG-00000000-0000-0000-0000-000000000001",
    "MIG-00000000-0000-0000-0000-000000000002",
    "MIG-00000000-0000-0000-0000-000000000003",
    "MIG-00000000-0000-0000-0000-000000000004",
]
SERVER_LOG = """Global Counter:
  Local: 0
  Remote: 4096
Switch id=0:
  Events:
    Load: 2048
    Store: 2048
Statistics:
  Number of Threads created: 8
"""


class QemuLessMigSummaryTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        self.baseline_dir = self.root / "baseline"
        self.oversub_dir = self.root / "oversub"
        self.baseline_dir.mkdir()
        self.oversub_dir.mkdir()
        self.server_log = self.root / "server.log"
        self.mig_uuid_file = self.root / "mig-uuids.txt"
        self.output = self.root / "summary.json"
        self.server_log.write_text(SERVER_LOG, encoding="utf-8")
        self.mig_uuid_file.write_text("\n".join(MIG_UUIDS) + "\n", encoding="utf-8")
        self.write_records(self.baseline_dir, 4)
        self.write_records(self.oversub_dir, 8)

    def tearDown(self):
        self.tempdir.cleanup()

    def write_records(self, directory, world_size):
        for rank in range(world_size):
            self.write_record(directory, rank, world_size)

    def write_record(self, directory, rank, expected_world_size, **overrides):
        checkpoint_bytes = 1024
        record = {
            "rank": rank,
            "world_size": expected_world_size,
            "slot": rank,
            "mig_uuid": MIG_UUIDS[rank % 4],
            "device_name": "fixture-mig",
            "cxl_addr": 0x10000000 + rank * 0x1000,
            "checkpoint_bytes": checkpoint_bytes,
            "cxl_bytes_written": checkpoint_bytes,
            "cxl_bytes_read": checkpoint_bytes,
            "kernel_ms": 1.0,
            "checkpoint_write_ms": 0.5,
            "checkpoint_read_ms": 0.5,
            "cuda_valid": True,
            "checkpoint_valid": True,
        }
        record.update(overrides)
        (directory / f"rank-{rank}.json").write_text(json.dumps(record), encoding="utf-8")

    def set_mig_uuid_fixture(self, mig_uuids):
        self.mig_uuid_file.write_text("\n".join(mig_uuids) + "\n", encoding="utf-8")
        for directory, world_size in ((self.baseline_dir, 4), (self.oversub_dir, 8)):
            for rank in range(world_size):
                self.write_record(directory, rank, world_size, mig_uuid=mig_uuids[rank % 4])

    def run_summary(self):
        return self.run_command(self.summary_args())

    def summary_args(self):
        return [
            "--baseline-dir",
            str(self.baseline_dir),
            "--oversub-dir",
            str(self.oversub_dir),
            "--server-log",
            str(self.server_log),
            "--mig-uuid-file",
            str(self.mig_uuid_file),
            "--output",
            str(self.output),
        ]

    def run_command(self, args):
        return subprocess.run(
            [sys.executable, str(SUMMARIZER), *args],
            capture_output=True,
            text=True,
            check=False,
        )

    def read_summary(self):
        return json.loads(self.output.read_text(encoding="utf-8"))

    def assert_fail_summary(self, completed):
        self.assertNotEqual(completed.returncode, 0, completed.stderr)
        summary = self.read_summary()
        self.assertEqual("fail", summary["verdict"])
        self.assertEqual(
            {"verdict", "checks", "baseline", "oversubscription", "controller", "errors"},
            set(summary),
        )
        self.assertIsInstance(summary["checks"], dict)
        self.assertIsInstance(summary["errors"], list)
        self.assertTrue(summary["errors"])
        for error in summary["errors"]:
            self.assertEqual({"check", "error"}, set(error))

    def test_valid_evidence_passes_with_stable_summary_structure(self):
        completed = self.run_summary()

        self.assertEqual(0, completed.returncode, completed.stderr)
        summary = self.read_summary()
        self.assertEqual("pass", summary["verdict"])
        self.assertEqual(
            {"verdict", "checks", "baseline", "oversubscription", "controller", "errors"},
            set(summary),
        )
        self.assertTrue(all(summary["checks"].values()))
        self.assertEqual([], summary["errors"])
        self.assertEqual(4, summary["baseline"]["record_count"])
        self.assertEqual(8, summary["oversubscription"]["record_count"])
        self.assertEqual(4096, summary["controller"]["remote"])
        self.assertEqual(2048, summary["controller"]["loads"])
        self.assertEqual(2048, summary["controller"]["stores"])
        self.assertEqual(8, summary["controller"]["threads"])

    def test_checksum_failure_returns_fail_summary(self):
        self.write_record(self.oversub_dir, 7, 8, checkpoint_valid=False)

        self.assert_fail_summary(self.run_summary())

    def test_extra_or_gapped_rank_files_return_fail_summary(self):
        (self.oversub_dir / "rank-2.json").unlink()
        self.write_record(self.oversub_dir, 8, 8)

        self.assert_fail_summary(self.run_summary())

    def test_overlapping_ranges_and_noncanonical_mig_mapping_fail(self):
        self.write_record(self.oversub_dir, 5, 8, cxl_addr=0x10000000, mig_uuid=MIG_UUIDS[0])

        self.assert_fail_summary(self.run_summary())

    def test_record_contract_rejects_invalid_world_slot_bytes_and_timing(self):
        invalid_records = [
            {"world_size": 4},
            {"slot": 0},
            {"cuda_valid": 1},
            {"checkpoint_bytes": 0},
            {"cxl_bytes_written": 1},
            {"cxl_bytes_read": 0},
            {"kernel_ms": -0.1},
        ]

        for overrides in invalid_records:
            with self.subTest(overrides=overrides):
                self.write_record(self.oversub_dir, 7, 8, **overrides)
                self.assert_fail_summary(self.run_summary())
                self.write_record(self.oversub_dir, 7, 8)

    def test_duplicate_mig_uuid_file_entries_fail(self):
        self.mig_uuid_file.write_text(
            "\n".join([MIG_UUIDS[0], MIG_UUIDS[1], MIG_UUIDS[2], MIG_UUIDS[2]]) + "\n",
            encoding="utf-8",
        )

        self.assert_fail_summary(self.run_summary())

    def test_mig_uuid_file_rejects_non_task2_grammar(self):
        malformed_uuids = [
            "MIG-A",
            "mig-00000000-0000-0000-0000-000000000001",
            "MIG-00000000-0000-0000-0000-00000000000G",
            "MIG-00000000-0000-0000-0000-00000000001",
        ]

        for malformed_uuid in malformed_uuids:
            with self.subTest(malformed_uuid=malformed_uuid):
                self.set_mig_uuid_fixture([malformed_uuid, *MIG_UUIDS[1:]])
                self.assert_fail_summary(self.run_summary())

    def test_mig_uuid_file_accepts_mixed_case_hex(self):
        mixed_case_uuids = [
            "MIG-aBcD0123-4567-89aB-cDeF-0123456789aB",
            "MIG-00000000-0000-0000-0000-000000000002",
            "MIG-00000000-0000-0000-0000-000000000003",
            "MIG-00000000-0000-0000-0000-000000000004",
        ]
        self.set_mig_uuid_fixture(mixed_case_uuids)

        completed = self.run_summary()

        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual("pass", self.read_summary()["verdict"])

    def test_missing_required_argument_writes_fail_summary(self):
        args = self.summary_args()
        server_log_index = args.index("--server-log")
        del args[server_log_index : server_log_index + 2]

        self.assert_fail_summary(self.run_command(args))

    def test_unknown_argument_with_equals_output_writes_fail_summary(self):
        args = self.summary_args()
        output_index = args.index("--output")
        output = args[output_index + 1]
        args[output_index : output_index + 2] = ["--output={}".format(output)]
        args.extend(["--unexpected-option", "value"])

        self.assert_fail_summary(self.run_command(args))

    def test_controller_allows_zero_parent_counters_with_positive_aggregate(self):
        self.server_log.write_text(
            "Global Counter:\n"
            "  Remote: 4096\n"
            "Switch id=0:\n"
            "  Events:\n"
            "    Load: 0\n"
            "      Load: 2048\n"
            "    Store: 0\n"
            "      Store: 2048\n"
            "Statistics:\n"
            "  Number of Threads created: 8\n",
            encoding="utf-8",
        )

        completed = self.run_summary()

        self.assertEqual(0, completed.returncode, completed.stderr)
        self.assertEqual(2048, self.read_summary()["controller"]["loads"])
        self.assertEqual(2048, self.read_summary()["controller"]["stores"])

    def test_record_contract_requires_nonempty_string_device_name(self):
        invalid_records = [{}, {"device_name": ""}, {"device_name": 7}]

        for overrides in invalid_records:
            with self.subTest(overrides=overrides):
                self.write_record(self.oversub_dir, 7, 8, **overrides)
                if not overrides:
                    record_path = self.oversub_dir / "rank-7.json"
                    record = json.loads(record_path.read_text(encoding="utf-8"))
                    del record["device_name"]
                    record_path.write_text(json.dumps(record), encoding="utf-8")
                self.assert_fail_summary(self.run_summary())
                self.write_record(self.oversub_dir, 7, 8)

    def test_accidental_server_counter_matches_fail(self):
        self.server_log.write_text(
            "Remote: 4096 ignored\nLoad: 2048 extra\nStore: 2048 trailing\n"
            "Number of Threads created: 8 trailing\n",
            encoding="utf-8",
        )

        self.assert_fail_summary(self.run_summary())


if __name__ == "__main__":
    unittest.main()
