import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import textwrap
import unittest


MODULE = Path(__file__).parents[1] / "run.py"
SPEC = importlib.util.spec_from_file_location("write_latency_runner", MODULE)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


class RunnerTests(unittest.TestCase):
    def test_nearest_rank_percentiles_and_failure_separation(self):
        self.assertEqual(runner.nearest_rank([1, 2, 3, 4], 0.50), 2)
        self.assertEqual(runner.nearest_rank([1, 2, 3, 4], 0.95), 4)
        summary = runner.summarize([
            {"success": True, "result": {"acknowledged_latency_ns": 3}},
            {"success": False, "error": "bad"},
            {"success": True, "result": {"acknowledged_latency_ns": 1}},
        ])
        self.assertEqual(summary["count"], 2)
        self.assertEqual(summary["failures"], 1)
        self.assertEqual(summary["min_ns"], 1)
        self.assertEqual(summary["max_ns"], 3)

    def test_scaling_retains_all_failure_cases(self):
        summaries = []
        for size in runner.PROPERTY_BYTES:
            for mode in runner.MODES:
                summaries.append(
                    {
                        "property_bytes": size,
                        "mode": mode,
                        "phases": {
                            name: {"p50_ns": None} for name in runner.TOP_LEVEL_PHASES
                        },
                    }
                )
        self.assertEqual(
            runner.scaling(summaries)["classification"],
            {"kind": "distributed", "phase": None},
        )

    def test_process_capture_separates_malformed_nonzero_and_timeout(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            malformed = self._script(root, "malformed.py", "print('not json')")
            record = runner.run_process([sys.executable, str(malformed)], 2)
            self.assertFalse(record["success"])
            self.assertEqual(record["failure_kind"], "malformed_json")

            nonzero = self._script(
                root, "nonzero.py", "print('{}'); raise SystemExit(7)"
            )
            record = runner.run_process([sys.executable, str(nonzero)], 2)
            self.assertFalse(record["success"])
            self.assertEqual(record["exit_status"], 7)

            timeout = self._script(root, "timeout.py", "import time; time.sleep(30)")
            record = runner.run_process([sys.executable, str(timeout)], 0.1)
            self.assertTrue(record["timed_out"])
            self.assertFalse(record["success"])

    def test_output_refusal_source_immutability_and_copy_retention(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            output = root / "output"
            output.mkdir()
            with self.assertRaises(FileExistsError):
                runner.create_output_root(output)

            source = root / "fixture"
            source.mkdir()
            (source / "data").write_bytes(b"immutable")
            before = runner.tree_hash(source)
            success_copy = root / "success-copy"
            record = runner.execute_sample(
                [sys.executable, "-c", "import json; print(json.dumps({'ok': True}))"],
                source,
                success_copy,
                2,
            )
            self.assertTrue(record["success"])
            self.assertFalse(success_copy.exists())
            self.assertEqual(runner.tree_hash(source), before)

            mismatched_copy = root / "mismatched-copy"
            record = runner.execute_sample(
                [sys.executable, "-c", "import json; print(json.dumps({'ok': True}))"],
                source,
                mismatched_copy,
                2,
                expected_source_hash="0" * 64,
            )
            self.assertFalse(record["success"])
            self.assertEqual(record["failure_kind"], "source_fixture_identity_mismatch")
            self.assertTrue(mismatched_copy.exists())

            failed_copy = root / "failed-copy"
            record = runner.execute_sample(
                [sys.executable, "-c", "raise SystemExit(9)"],
                source,
                failed_copy,
                2,
            )
            self.assertFalse(record["success"])
            self.assertTrue(failed_copy.exists())
            self.assertEqual(runner.tree_hash(source), before)

    @staticmethod
    def _script(root, name, body):
        path = root / name
        path.write_text(textwrap.dedent(body), encoding="utf-8")
        return path


if __name__ == "__main__":
    unittest.main()
