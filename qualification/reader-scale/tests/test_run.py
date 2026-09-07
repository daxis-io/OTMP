import json
import hashlib
import pathlib
import stat
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import run


HERE = pathlib.Path(__file__).resolve().parent
FAKE = HERE / "fake_worker.py"


class PercentileTests(unittest.TestCase):
    def test_nearest_rank_boundaries(self):
        self.assertEqual(run.nearest_rank([9, 1, 5, 3], 0.50), 3)
        self.assertEqual(run.nearest_rank([9, 1, 5, 3], 0.95), 9)
        self.assertEqual(run.distribution([9, 1, 5, 3]), {
            "count": 4, "min": 1, "p50": 3, "p95": 9, "max": 9
        })
        self.assertEqual(run.distribution([]), {
            "count": 0, "min": None, "p50": None, "p95": None, "max": None
        })


class RunnerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = pathlib.Path(self.temp.name)
        self.root = self.base / "fixture"
        self.root.mkdir()
        (self.root / "_otmp").mkdir()
        (self.root / "_otmp" / "HEAD").write_bytes(b"original-head")
        head_sha256 = hashlib.sha256(b"original-head").hexdigest()
        (self.root / "qualification.json").write_text(
            json.dumps({"head_sha256": f"sha256:{head_sha256}"})
        )
        self.binary = self.base / "reader_scale"
        self.binary.write_text("#!/bin/sh\nexec python3 \"$0.py\" \"$@\"\n")
        self.binary.with_suffix(".py").write_bytes(FAKE.read_bytes())
        self.binary.chmod(self.binary.stat().st_mode | stat.S_IXUSR)

    def tearDown(self):
        self.temp.cleanup()

    def execute(self, modes, timeout=2):
        config = self.base / "config.json"
        config.write_text(json.dumps({"test_mode": modes[0]}))
        out = self.base / "evidence"
        if len(modes) == 1:
            run.run_samples(self.binary, self.root, config, out, 1, timeout)
        else:
            out.mkdir()
            for index, mode in enumerate(modes, 1):
                config.write_text(json.dumps({"test_mode": mode}))
                run.capture_sample(self.binary, self.root, config, out / f"sample-{index:04d}", timeout, index)
            run.write_summary(out)
        return json.loads((out / "summary.json").read_text()), out

    def test_failures_do_not_contaminate_success_percentiles(self):
        summary, _ = self.execute(["success", "error"])
        self.assertEqual(summary["samples"], {"total": 2, "successful": 1, "failed": 1})
        self.assertEqual(summary["latency_ms"]["phase"]["planning:1"]["count"], 1)
        self.assertEqual(summary["failures"][0]["observed_phases"], [])

    def test_secondary_violation_is_not_an_expected_resource_failure(self):
        _, out = self.execute(["partial"])
        path = out / "sample-0001" / "result.json"
        value = json.loads(path.read_text())
        value["violations"] = ["I/O remains active"]
        path.write_text(json.dumps(value))
        summary = run.write_summary(out)
        self.assertEqual(summary["failures"][0]["kind"], "qualification_invariant_violation")
        self.assertEqual(summary["failures"][0]["violations"], ["I/O remains active"])

    def test_partial_failure_phases_are_retained_separately(self):
        summary, out = self.execute(["success", "partial"])
        self.assertEqual(summary["latency_ms"]["phase"]["registration:1"]["count"], 1)
        self.assertEqual(
            summary["failures"][0]["observed_phases"],
            ["registration:1", "planning:1"],
        )
        self.assertEqual(summary["failure_phase_latency_ms"]["planning:1"]["count"], 1)
        self.assertEqual(
            summary["failure_phase_io_by_class"]["planning:1"]["metadata"]["bytes"]["max"],
            100,
        )
        self.assertEqual(
            summary["failure_phase_memory"]["planning:1"]["provider_footer_cache_bytes"]["max"],
            64,
        )
        process_path = out / "sample-0002" / "process.json"
        process = json.loads(process_path.read_text())
        process["rss"] = {"bytes": 1234, "method": "test", "missing": False}
        process_path.write_text(json.dumps(process))
        run.write_summary(out)
        summary = json.loads((out / "summary.json").read_text())
        self.assertEqual(summary["failure_rss_bytes"]["max"], 1234)

    def test_malformed_child_output_is_preserved(self):
        summary, out = self.execute(["malformed"])
        self.assertEqual(summary["samples"]["failed"], 1)
        self.assertEqual(summary["failures"][0]["kind"], "invalid_output")
        self.assertEqual((out / "sample-0001" / "stdout.txt").read_text(), "not json\n")
        process = json.loads((out / "sample-0001" / "process.json").read_text())
        self.assertIsNone(process["result_arrival_ms"])
        self.assertIn("rss", process)

    def test_timeout_kills_group_and_preserves_evidence(self):
        summary, out = self.execute(["sleep"], timeout=0.1)
        self.assertEqual(summary["failures"][0]["kind"], "timeout")
        process = json.loads((out / "sample-0001" / "process.json").read_text())
        self.assertTrue(process["timed_out"])
        self.assertTrue((out / "sample-0001" / "stderr.txt").exists())

    def test_head_bytes_are_checked_even_when_manifest_is_unchanged(self):
        summary, out = self.execute(["mutate_head"])
        process = json.loads((out / "sample-0001" / "process.json").read_text())
        self.assertNotEqual(
            process["fixture_before"]["head_sha256"],
            process["fixture_after"]["head_sha256"],
        )
        self.assertTrue(process["fixture_before"]["manifest_matches_head"])
        self.assertFalse(process["fixture_after"]["manifest_matches_head"])
        manifest = json.loads((out / "manifest.json").read_text())
        self.assertFalse(manifest["fixture_head_unchanged"])
        self.assertEqual(summary["failures"][0]["kind"], "fixture_mutated")

    def test_deadline_covers_descendant_holding_output_pipes(self):
        summary, out = self.execute(["descendant_pipe"], timeout=0.2)
        self.assertEqual(summary["failures"][0]["kind"], "timeout")
        process = json.loads((out / "sample-0001" / "process.json").read_text())
        self.assertTrue(process["timed_out"])
        self.assertLess(process["wall_ms"], 2_000)

    def test_result_timestamp_distinguishes_child_teardown(self):
        summary, out = self.execute(["teardown"])
        process = json.loads((out / "sample-0001" / "process.json").read_text())
        self.assertGreater(process["teardown_gap_ms"], 150)
        self.assertLess(process["result_arrival_ms"], process["wall_ms"])
        self.assertEqual(summary["latency_ms"]["teardown_gap"]["count"], 1)
        if process["rss"]["method"] == "unavailable":
            self.assertTrue(process["rss"]["missing"])

    def test_existing_output_directory_is_rejected(self):
        config = self.base / "config.json"
        config.write_text("{}")
        out = self.base / "evidence"
        out.mkdir()
        with self.assertRaises(FileExistsError):
            run.run_samples(self.binary, self.root, config, out, 1, 1)


class TimeParsingTests(unittest.TestCase):
    def test_darwin_and_linux_rss(self):
        self.assertEqual(run.parse_rss(" 12345  maximum resident set size\n", "darwin"), 12345)
        self.assertEqual(
            run.parse_rss("Maximum resident set size (kbytes): 12345\n", "linux"),
            12345 * 1024,
        )
        self.assertIsNone(run.parse_rss("no measurement", "linux"))

    def test_native_cache_memory_fields_are_summarized(self):
        phase = {
            "name": "planning",
            "elapsed_ms": 1,
            "io": {},
            "reader": {"cache_bytes": 11, "peak_cache_bytes": 22},
            "provider": {"footer_cache_bytes": 33, "peak_footer_cache_bytes": 44},
            "pool_reserved_bytes": 55,
        }
        metrics = run._phase_metrics([{"phases": [phase]}])["memory"]["planning:0"]
        self.assertEqual(metrics["reader_cache_bytes"]["max"], 11)
        self.assertEqual(metrics["reader_peak_cache_bytes"]["max"], 22)
        self.assertEqual(metrics["provider_footer_cache_bytes"]["max"], 33)
        self.assertEqual(metrics["provider_peak_footer_cache_bytes"]["max"], 44)

    def test_passes_are_summarized_independently_with_native_counters(self):
        phases = []
        for pass_number, requests in [(0, 9), (1, 1)]:
            phases.append({
                "name": "planning",
                "pass": pass_number,
                "elapsed_ms": requests,
                "io": {},
                "reader": {
                    "bytes": requests * 10,
                    "requests": requests,
                    "pages": requests,
                    "cache_hits": 10 - requests,
                },
                "provider": {
                    "files_considered": 16,
                    "files_pruned": 14,
                    "files_opened": 2,
                    "planning_micros": requests * 1000,
                    "parquet_bytes": requests * 100,
                    "parquet_requests": requests,
                    "footer_cache_hits": 10 - requests,
                },
                "pool_reserved_bytes": 0,
            })
        metrics = run._phase_metrics([{"phases": phases}])
        self.assertEqual(metrics["latency"]["planning:0"]["max"], 9)
        self.assertEqual(metrics["latency"]["planning:1"]["max"], 1)
        self.assertEqual(metrics["reader"]["planning:0"]["requests"]["max"], 9)
        self.assertEqual(metrics["reader"]["planning:1"]["cache_hits"]["max"], 9)
        self.assertEqual(metrics["provider"]["planning:0"]["planning_micros"]["max"], 9000)
        self.assertEqual(metrics["provider"]["planning:0"]["parquet_bytes"]["max"], 900)
        self.assertEqual(metrics["provider"]["planning:1"]["footer_cache_hits"]["max"], 9)


if __name__ == "__main__":
    unittest.main()
