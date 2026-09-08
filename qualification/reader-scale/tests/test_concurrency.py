import copy
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import concurrency


class GateTests(unittest.TestCase):
    def complete_summaries(self):
        summaries = {}
        for name, _, config, samples in concurrency.cases():
            for label in ("baseline", "candidate"):
                value = 2 if label == "baseline" else 1
                metric = {"count": samples, "p50": value, "p95": value}
                summaries[name + "/" + label] = {
                    "samples": {"failed": 0, "successful": samples, "total": samples},
                    "overlapping": {
                        "query_latency_ms": {
                            f"{p}:{q}": {field: metric.copy() for field in (
                                "planning_ms", "planning_to_first_result_ms", "execution_ms", "complete_ms")}
                            for p in range(config.get("passes", 1))
                            for q in range(len(config["overlapping_survivors"]))
                        },
                        "throughput_queries_per_second": {
                            str(p): {"count": samples, "p50": 1, "p95": 1}
                            for p in range(config.get("passes", 1))
                        },
                    },
                }
        return summaries

    def test_missing_positions_and_sample_counts_fail_closed(self):
        complete = self.complete_summaries()
        expected = concurrency.gates(complete)
        self.assertEqual(len(expected), 45)
        self.assertTrue(all(expected.values()))
        for name, _, _, _ in concurrency.cases():
            for label in ("baseline", "candidate"):
                key = name + "/" + label
                for missing in ("case", "positions", "sample", "metric_count", "throughput_count"):
                    with self.subTest(case=key, missing=missing):
                        summaries = copy.deepcopy(complete)
                        item = summaries[key]
                        if missing == "case":
                            del summaries[key]
                        elif missing == "positions":
                            item["overlapping"]["query_latency_ms"].popitem()
                        elif missing == "sample":
                            item["samples"] = {"failed": 0, "successful": 1, "total": 1}
                        elif missing == "metric_count":
                            next(iter(item["overlapping"]["query_latency_ms"].values()))["planning_ms"]["count"] -= 1
                        else:
                            item["overlapping"]["throughput_queries_per_second"]["0"]["count"] -= 1
                        measured = concurrency.gates(summaries)
                        self.assertEqual(measured.keys(), expected.keys())
                        self.assertFalse(measured[key + "/completion"])
                        self.assertFalse(all(measured.values()))

    def test_failed_rounds_fail_gates_without_crashing_the_report(self):
        summaries = {
            name + "/" + label: {"samples": {"failed": samples}, "overlapping": {
                "query_latency_ms": {}, "throughput_queries_per_second": {}}}
            for name, _, _, samples in concurrency.cases()
            for label in ("baseline", "candidate")
        }
        self.assertFalse(any(concurrency.gates(summaries).values()))

    def test_each_percentile_must_pass_and_missing_evidence_fails(self):
        self.assertTrue(concurrency.latency_gate({"p50": 10, "p95": 20}, {"p50": 5, "p95": 10}, .5))
        self.assertFalse(concurrency.latency_gate({"p50": 10, "p95": 20}, {"p50": 5, "p95": 11}, .5))
        self.assertFalse(concurrency.latency_gate({"p50": 10, "p95": 20}, {"p50": 5}, .5))
        self.assertTrue(concurrency.latency_gate({"p50": .2, "p95": 1}, {"p50": 1.2, "p95": 2}, 1.05, 1))
        self.assertTrue(concurrency.latency_gate({"p50": 1, "p95": 20}, {"p50": 10, "p95": 21}, 1.1, percentiles=("p95",)))

    def test_matrix_keeps_twenty_local_and_mixed_and_ten_delayed_samples(self):
        cases = concurrency.cases()
        self.assertTrue(all(case[3] == (10 if case[0].startswith("delayed") else 20) for case in cases))
        mixed = [case[2]["overlapping_survivors"] for case in cases if case[0].startswith("mixed")]
        self.assertIn([None, 2, 2, 2], mixed)
        for count in [1, 2, 4, 8]:
            self.assertIn([2] * count, mixed)
