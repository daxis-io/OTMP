#!/usr/bin/env python3
"""Interleaved concurrency qualification using the frozen instrumentation baseline."""
import argparse
import pathlib
import random
import subprocess

import run


def latency_gate(baseline, candidate, factor, allowance=None, percentiles=("p50", "p95")):
    return all(
        isinstance(baseline.get(p), (int, float))
        and isinstance(candidate.get(p), (int, float))
        and candidate[p] <= max(baseline[p] * factor,
                                baseline[p] + allowance if allowance is not None else 0)
        for p in percentiles
    )


def cases():
    result = [
        ("local-broad", "growth-256", {"overlapping_survivors": [None], "passes": 2}, 20),
        ("local-small", "growth-4096", {"overlapping_survivors": [2], "passes": 2}, 20),
        ("local-heterogeneous", "heterogeneous-256", {"overlapping_survivors": [None], "passes": 2}, 20),
        ("delayed-broad", "growth-256", {"overlapping_survivors": [None], "passes": 2,
                                         "data_delays_ms": {"stat": 10, "trailer": 10, "footer": 10}}, 10),
        ("mixed-broad-small", "growth-256", {"overlapping_survivors": [None, 2, 2, 2]}, 20),
    ]
    result.extend((f"mixed-homogeneous-{n}", "growth-256", {"overlapping_survivors": [2] * n}, 20)
                  for n in (1, 2, 4, 8))
    return result


def verify(args, phase):
    for fixture in sorted({case[1] for case in cases()}):
        with (args.out / f"verify-{phase}-{fixture}.json").open("wb") as output:
            with (args.out / f"verify-{phase}-{fixture}.stderr").open("wb") as errors:
                subprocess.run([str(args.candidate), "verify", str(args.fixtures / fixture)],
                               stdout=output, stderr=errors, check=True, timeout=300)


def gates(summaries):
    result = {}
    for name, _, config, samples in cases():
        positions = [f"{p}:{q}" for p in range(config.get("passes", 1))
                     for q in range(len(config["overlapping_survivors"]))]
        passes = {str(p) for p in range(config.get("passes", 1))}
        baseline = summaries.get(name + "/baseline", {}).get("overlapping", {})
        candidate = summaries.get(name + "/candidate", {}).get("overlapping", {})
        for label in ("baseline", "candidate"):
            summary = summaries.get(name + "/" + label, {})
            overlap = summary.get("overlapping", {})
            queries = overlap.get("query_latency_ms", {})
            throughput = overlap.get("throughput_queries_per_second", {})
            result[f"{name}/{label}/completion"] = (
                summary.get("samples") == {"failed": 0, "successful": samples, "total": samples}
                and set(queries) == set(positions) and set(throughput) == passes
                and all(queries[key].get(field, {}).get("count") == samples
                        for key in positions for field in (
                            "planning_ms", "planning_to_first_result_ms", "execution_ms", "complete_ms"))
                and all(throughput[p].get("count") == samples for p in passes)
            )
        for key in positions:
            before = baseline.get("query_latency_ms", {}).get(key, {})
            after = candidate.get("query_latency_ms", {}).get(key, {})
            if name.startswith("delayed") and key == "0:0":
                result[name + "/planning-2x"] = latency_gate(before.get("planning_ms", {}), after.get("planning_ms", {}), .5)
            if name.startswith("local") and key.startswith("1:"):
                result[name + "/warm"] = latency_gate(before.get("planning_ms", {}), after.get("planning_ms", {}), 1.05, 1)
            if name.startswith("mixed") and config["overlapping_survivors"][int(key.split(":")[1])] is not None:
                result[name + "/" + key + "/small"] = latency_gate(
                    before.get("complete_ms", {}), after.get("complete_ms", {}), 1.1, percentiles=("p95",))
        if name.startswith("mixed"):
            before = baseline.get("throughput_queries_per_second", {}).get("0", {}).get("p50")
            after = candidate.get("throughput_queries_per_second", {}).get("0", {}).get("p50")
            result[name + "/throughput"] = (
                isinstance(before, (int, float)) and isinstance(after, (int, float))
                and after >= before * .9)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=pathlib.Path, required=True)
    parser.add_argument("--candidate", type=pathlib.Path, required=True)
    parser.add_argument("--eight-query-baseline", type=pathlib.Path,
                        help="Optional separately archived reservation-only control for the baseline's eight-query budget failure")
    parser.add_argument("--fixtures", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    verify(args, "before")
    summaries = {}
    for name, fixture, config, samples in cases():
        orders = [("baseline", "candidate"), ("candidate", "baseline")] * (samples // 2)
        random.Random(74415).shuffle(orders)
        run.write_json(args.out / f"{name}-order.json", orders)
        for number, order in enumerate(orders, 1):
            for label in order:
                path = args.out / f"{name}-{label}.json"
                run.write_json(path, dict(config, preflight_concurrency=1 if label == "baseline" else 8))
                out = args.out / name / label / f"round-{number:04d}"
                binary = getattr(args, label)
                if name == "mixed-homogeneous-8" and label == "baseline" and args.eight_query_baseline:
                    binary = args.eight_query_baseline
                summary = run.run_samples(binary, args.fixtures / fixture, path, out, 1, 300)
                print(name, number, label, summary["samples"], flush=True)
        for label in ("baseline", "candidate"):
            summaries[name + "/" + label] = run.build_summary((args.out / name / label).glob("round-*/sample-*"))
        run.write_json(args.out / "results.json", summaries)
    measured = gates(summaries)
    run.write_json(args.out / "gates.json", measured)
    verify(args, "after")
    if not all(measured.values()):
        raise SystemExit("qualification gates failed; see retained gates.json and raw results")


if __name__ == "__main__":
    main()
