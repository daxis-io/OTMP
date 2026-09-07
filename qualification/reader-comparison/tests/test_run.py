import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import run


FAKE = pathlib.Path(__file__).with_name("fake_worker.py")


class StatisticsTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        self.assertEqual(run.distribution([9, 1, 5, 3]), {
            "count": 4,
            "min": 1,
            "p50": 3,
            "p95": 9,
            "max": 9,
        })
        self.assertEqual(run.distribution([]), {
            "count": 0,
            "min": None,
            "p50": None,
            "p95": None,
            "max": None,
        })

    def test_paired_order_is_balanced_and_reproducible(self):
        first = run.balanced_pairs(20, 41)
        second = run.balanced_pairs(20, 41)
        self.assertEqual(first, second)
        self.assertEqual(sum(pair[0] == "otmp" for pair in first), 10)
        self.assertEqual(sum(pair[0] == "iceberg" for pair in first), 10)
        self.assertTrue(all(set(pair) == {"otmp", "iceberg"} for pair in first))


class CaptureTests(unittest.TestCase):
    def test_failed_process_is_preserved_and_excluded_from_percentiles(self):
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            good = run.capture_process(
                [sys.executable, str(FAKE), "success"], root / "good", 5
            )
            bad = run.capture_process(
                [sys.executable, str(FAKE), "malformed"], root / "bad", 5
            )
            summary = run.summarize_provider_samples([good, bad])

            self.assertEqual(summary["samples"], {
                "total": 2,
                "successful": 1,
                "failed": 1,
            })
            self.assertEqual(summary["latency_ms"]["planning:0"]["count"], 1)
            self.assertEqual(summary["failures"][0]["kind"], "invalid_output")
            self.assertEqual((root / "bad" / "stdout.txt").read_text(), "not json\n")
            self.assertTrue((root / "bad" / "process.json").is_file())


if __name__ == "__main__":
    unittest.main()
