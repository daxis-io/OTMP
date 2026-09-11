import hashlib
import json
import pathlib
import stat
import tempfile
import time
import unittest
from unittest import mock

import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import cache_matrix


class ScheduleTests(unittest.TestCase):
    def test_rotation_blocks_reverse_orientation(self):
        base = ["a", "b", "c"]

        self.assertCountEqual(
            cache_matrix.rotation_block(base, reverse=False),
            [["a", "b", "c"], ["b", "c", "a"], ["c", "a", "b"]],
        )
        self.assertCountEqual(
            cache_matrix.rotation_block(base, reverse=True),
            [["a", "c", "b"], ["b", "a", "c"], ["c", "b", "a"]],
        )

    def test_balanced_order_limits_each_case_to_six_or_seven_of_each_position(self):
        names = ["engine-4m", "engine-16m", "engine-32m"]

        first = cache_matrix.balanced_order(names, samples=20, seed=7)
        second = cache_matrix.balanced_order(names, samples=20, seed=7)

        self.assertEqual(first, second)
        self.assertEqual(len(first), 60)
        for round_number in range(1, 21):
            row = [item["case"] for item in first if item["round"] == round_number]
            self.assertCountEqual(row, names)
        for name in names:
            self.assertEqual(sum(item["case"] == name for item in first), 20)
            positions = [
                sum(
                    item["case"] == name and item["position"] == position
                    for item in first
                )
                for position in (1, 2, 3)
            ]
            self.assertTrue(all(count in (6, 7) for count in positions), positions)


class DriverTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = pathlib.Path(self.temp.name)
        self.root = self.base / "fixture"
        (self.root / "_otmp").mkdir(parents=True)
        self._write_fixture(b"original-head")
        self.binary = self.base / "reader_scale"
        self.binary.write_text(
            "#!/bin/sh\n"
            "if [ \"$1\" = verify ]; then echo '{}'; exit 0; fi\n"
            "exit 9\n"
        )
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        self.old_cwd = pathlib.Path.cwd()

    def tearDown(self):
        import os

        os.chdir(self.old_cwd)
        self.temp.cleanup()

    def _write_fixture(self, head):
        (self.root / "_otmp" / "HEAD").write_bytes(head)
        digest = hashlib.sha256(head).hexdigest()
        (self.root / "qualification.json").write_text(
            json.dumps(
                {
                    "files": 16384,
                    "head_sha256": f"sha256:{digest}",
                    "verified": True,
                }
            )
        )

    def arguments(self, out, timeout="2"):
        return [
            "--binary",
            str(self.binary),
            "--root",
            str(self.root),
            "--out",
            str(out),
            "--samples",
            "1",
            "--timeout",
            timeout,
        ]

    def test_between_round_fixture_change_fails_and_preserves_provenance(self):
        out = self.base / "evidence"
        calls = 0

        def mutate_after_sample(binary, root, config, destination, samples, timeout):
            nonlocal calls
            calls += 1
            destination.mkdir(parents=True)
            self._write_fixture(b"different-head")
            return {"samples": {"total": 1, "successful": 1, "failed": 0}}

        with mock.patch.object(cache_matrix.runner, "run_samples", mutate_after_sample):
            with self.assertRaisesRegex(RuntimeError, "fixture identity changed"):
                cache_matrix.main(self.arguments(out))

        self.assertEqual(calls, 1)
        verification = json.loads((out / "verify.process.json").read_text())
        self.assertEqual(verification["exit_code"], 0)
        self.assertFalse(verification["timed_out"])
        manifest = json.loads((out / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["binary_before"]["sha256"], cache_matrix.runner.sha256_file(self.binary))
        self.assertEqual(
            pathlib.Path(manifest["source"]["root"]),
            pathlib.Path(cache_matrix.__file__).resolve().parents[2],
        )
        self.assertNotEqual(
            manifest["fixture_before"]["head_sha256"],
            manifest["fixture_after"]["head_sha256"],
        )
        self.assertIn("after_sample", manifest["failure"]["stage"])

    def test_verification_timeout_is_bounded_and_recorded(self):
        self.binary.write_text("#!/bin/sh\nsleep 10\n")
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)
        out = self.base / "timeout-evidence"
        started = time.monotonic()

        with self.assertRaisesRegex(RuntimeError, "verification timed out"):
            cache_matrix.main(self.arguments(out, timeout="0.05"))

        self.assertLess(time.monotonic() - started, 2)
        verification = json.loads((out / "verify.process.json").read_text())
        self.assertTrue(verification["timed_out"])
        self.assertIsNotNone(verification["exit_code"])
        manifest = json.loads((out / "manifest.json").read_text())
        self.assertEqual(manifest["status"], "failed")
        self.assertEqual(manifest["failure"]["stage"], "verification")


if __name__ == "__main__":
    unittest.main()
