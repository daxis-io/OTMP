#!/usr/bin/env python3
import argparse
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import time


EXACT_BASE = "78c3311a5d883fdda3542ff6e5a918bb24fee3ca"
PROPERTY_BYTES = (0, 131072, 2097152, 16777216)
MODES = ("fresh", "pre_pinned")
TOP_LEVEL_PHASES = (
    "parent_pin",
    "idempotency",
    "candidate_build",
    "immutable_publication",
    "head_cas",
)


def nearest_rank(values, percentile):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(percentile * len(ordered)) - 1)]


def distribution(values):
    if not values:
        return {"count": 0, "min_ns": None, "p50_ns": None, "p95_ns": None, "max_ns": None}
    return {
        "count": len(values),
        "min_ns": min(values),
        "p50_ns": nearest_rank(values, 0.50),
        "p95_ns": nearest_rank(values, 0.95),
        "max_ns": max(values),
    }


def summarize(records):
    successes = [
        record["result"]["acknowledged_latency_ns"]
        for record in records
        if record.get("success") and record.get("result")
    ]
    result = distribution(successes)
    result["failures"] = len(records) - len(successes)
    return result


def tree_hash(root):
    digest = hashlib.sha256()
    root = Path(root)
    for path in sorted(path for path in root.rglob("*") if path.is_file()):
        digest.update(path.relative_to(root).as_posix().encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_output_root(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=False)
    return path


_TIME_MODE = ...


def _time_mode():
    global _TIME_MODE
    if _TIME_MODE is not ...:
        return _TIME_MODE
    timer = Path("/usr/bin/time")
    if not timer.is_file():
        _TIME_MODE = None
        return _TIME_MODE
    flag, unit = ("-l", "bytes") if sys.platform == "darwin" else ("-v", "kib")
    probe = subprocess.run(
        [str(timer), flag, "/usr/bin/true"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    _TIME_MODE = (flag, unit) if probe.returncode == 0 else None
    return _TIME_MODE


def _time_command(command):
    mode = _time_mode()
    if mode is None:
        return list(command), None
    flag, unit = mode
    return ["/usr/bin/time", flag, *command], unit


def _rss(stderr, unit):
    if unit == "bytes":
        for line in stderr.splitlines():
            if "maximum resident set size" in line.lower():
                fields = line.split()
                if fields and fields[0].isdigit():
                    return int(fields[0]), "bytes"
    if unit == "kib":
        for line in stderr.splitlines():
            if "maximum resident set size" in line.lower():
                value = line.rsplit(":", 1)[-1].strip()
                if value.isdigit():
                    return int(value), "kib"
    return None, None


def run_process(command, timeout):
    timed_command, rss_hint = _time_command(command)
    started = time.monotonic_ns()
    process = subprocess.Popen(
        timed_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    timed_out = False
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
    wall_ns = time.monotonic_ns() - started
    lines = [line for line in stdout.splitlines() if line.strip()]
    parsed = None
    malformed = False
    if len(lines) == 1:
        try:
            parsed = json.loads(lines[0])
            malformed = not isinstance(parsed, dict)
        except json.JSONDecodeError:
            malformed = True
    else:
        malformed = True
    rss, rss_unit = _rss(stderr, rss_hint)
    success = (
        not timed_out
        and process.returncode == 0
        and not malformed
        and parsed.get("ok", True) is not False
    )
    if timed_out:
        failure_kind = "timeout"
    elif process.returncode != 0:
        failure_kind = "nonzero_exit"
    elif malformed:
        failure_kind = "malformed_json"
    elif parsed.get("ok", True) is False:
        failure_kind = "product_error"
    else:
        failure_kind = None
    return {
        "command": list(command),
        "success": success,
        "failure_kind": failure_kind,
        "stdout": stdout,
        "stderr": stderr,
        "exit_status": process.returncode,
        "timed_out": timed_out,
        "process_wall_ns": wall_ns,
        "maximum_rss": rss,
        "maximum_rss_unit": rss_unit,
        "result": parsed,
    }


def execute_sample(
    command, source, destination, timeout, verify_command=None, expected_source_hash=None
):
    source = Path(source)
    destination = Path(destination)
    source_before = tree_hash(source)
    shutil.copytree(source, destination)
    record = run_process(command, timeout)
    if record["success"] and verify_command is not None:
        verification = run_process(verify_command, timeout)
        record["verification"] = verification
        if not verification["success"]:
            record["success"] = False
            record["failure_kind"] = "verification_failed"
    record["source_tree_sha256_before"] = source_before
    record["source_tree_sha256_after"] = tree_hash(source)
    record["copy_tree_sha256"] = tree_hash(destination)
    head = destination / "_otmp" / "HEAD"
    record["copy_head_sha256"] = sha256_file(head) if head.is_file() else None
    if record["source_tree_sha256_after"] != source_before:
        record["success"] = False
        record["failure_kind"] = "source_fixture_changed"
    if expected_source_hash is not None and source_before != expected_source_hash:
        record["success"] = False
        record["failure_kind"] = "source_fixture_identity_mismatch"
    if record["success"]:
        shutil.rmtree(destination)
    return record


def git(command):
    return subprocess.run(
        ["git", *command], check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    ).stdout.strip()


def provenance(binary, allow_dirty):
    head = git(["rev-parse", "HEAD"])
    subprocess.run(
        ["git", "merge-base", "--is-ancestor", EXACT_BASE, head],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    status = git(["status", "--porcelain"])
    if status and not allow_dirty:
        raise RuntimeError("final evidence requires a clean committed source tree")
    rustc = subprocess.run(
        ["rustc", "--version", "--verbose"],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    ).stdout
    return {
        "exact_base": EXACT_BASE,
        "source_sha": head,
        "source_status": status,
        "source_clean": not bool(status),
        "binary": str(Path(binary).resolve()),
        "binary_sha256": sha256_file(binary),
        "host": platform.platform(),
        "python": sys.version,
        "rustc": rustc,
        "os_cache_state": "uncontrolled",
    }


def matrix_order(samples):
    cases = [(size, mode) for size in PROPERTY_BYTES for mode in MODES]
    order = []
    for sample in range(samples):
        rotated = cases[sample % len(cases) :] + cases[: sample % len(cases)]
        if sample % 2:
            rotated.reverse()
        order.extend(
            {"sample": sample + 1, "property_bytes": size, "mode": mode}
            for size, mode in rotated
        )
    return order


def case_summary(records):
    grouped = {}
    for record in records:
        key = (record["property_bytes"], record["mode"])
        grouped.setdefault(key, []).append(record)
    result = []
    for (size, mode), samples in sorted(grouped.items()):
        successful = [sample for sample in samples if sample["success"]]
        phases = {}
        for name in TOP_LEVEL_PHASES:
            values = []
            for sample in successful:
                matches = [
                    phase["duration_ns"]
                    for phase in sample["result"]["probe"]["phases"]
                    if phase["name"] == name
                ]
                if len(matches) == 1:
                    values.append(matches[0])
            phases[name] = distribution(values)
        summary = summarize(samples)
        summary.update(
            {
                "property_bytes": size,
                "mode": mode,
                "logical_image_bytes": successful[0]["result"]["fixture"]["logical_image_bytes"]
                if successful
                else None,
                "phases": phases,
            }
        )
        result.append(summary)
    return result


def scaling(summaries):
    modes = {}
    for mode in MODES:
        small = next(item for item in summaries if item["property_bytes"] == 0 and item["mode"] == mode)
        large = next(
            item
            for item in summaries
            if item["property_bytes"] == max(PROPERTY_BYTES) and item["mode"] == mode
        )
        deltas = {
            name: large["phases"][name]["p50_ns"] - small["phases"][name]["p50_ns"]
            for name in TOP_LEVEL_PHASES
            if large["phases"][name]["p50_ns"] is not None
            and small["phases"][name]["p50_ns"] is not None
        }
        ranked = sorted(deltas.items(), key=lambda item: item[1], reverse=True)
        positive = sum(max(0, value) for value in deltas.values())
        modes[mode] = {
            "large_minus_small_p50_ns": deltas,
            "ranked": [{"phase": name, "delta_ns": value} for name, value in ranked],
            "positive_phase_delta_ns": positive,
        }
    leaders = [
        modes[mode]["ranked"][0]["phase"] if modes[mode]["ranked"] else None
        for mode in MODES
    ]
    leader = leaders[0] if leaders[0] == leaders[1] else None
    dominant = leader is not None and all(
        modes[mode]["large_minus_small_p50_ns"][leader]
        >= modes[mode]["positive_phase_delta_ns"] / 2
        for mode in MODES
    )
    return {
        "modes": modes,
        "classification": {"kind": "dominant", "phase": leader}
        if dominant
        else {"kind": "distributed", "phase": None},
    }


def recommendation(scaling_result):
    phase = scaling_result["classification"]["phase"]
    if phase == "candidate_build":
        return "Profile the nested candidate-build phases and design the smallest validation or materialization reduction at the leading shared seam."
    if phase == "parent_pin":
        return "Evaluate explicit parent-pin reuse before adding a generation cache."
    if phase == "immutable_publication":
        return "Evaluate reducing immutable image-artifact publication work without changing publication ordering."
    if phase == "head_cas":
        return "Recheck storage and filesystem synchronization cost before changing transaction construction."
    return "Treat write scaling as distributed; do not optimize one phase until a follow-up profile identifies a stable shared leader."


def markdown(manifest, results):
    def milliseconds(value):
        return "n/a" if value is None else f"{value / 1_000_000:.3f}"

    lines = [
        "# OTMP metadata write-latency qualification",
        "",
        f"Source: `{manifest['provenance']['source_sha']}` from exact base `{EXACT_BASE}`.",
        "This is local qualification with uncontrolled OS cache state, not provider or production qualification.",
        "",
        "| mode | logical image bytes | count | min ms | p50 ms | p95 ms | max ms |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for item in results["cases"]:
        lines.append(
            "| {mode} | {logical} | {count} | {minimum} | {p50} | {p95} | {maximum} |".format(
                mode=item["mode"],
                logical=item["logical_image_bytes"],
                count=item["count"],
                minimum=milliseconds(item["min_ns"]),
                p50=milliseconds(item["p50_ns"]),
                p95=milliseconds(item["p95_ns"]),
                maximum=milliseconds(item["max_ns"]),
            )
        )
    lines.extend(
        [
            "",
            f"Scaling classification: **{results['scaling']['classification']['kind']}**.",
            "",
            "| mode | ranked large-minus-small p50 phase deltas |",
            "| --- | --- |",
        ]
    )
    for mode in MODES:
        ranked = results["scaling"]["modes"][mode]["ranked"]
        description = ", ".join(
            f"{item['phase']}={item['delta_ns'] / 1_000_000:.3f} ms" for item in ranked
        )
        lines.append(f"| {mode} | {description} |")
    lines.extend(
        [
            "",
            f"Recommendation: {results['recommendation']}",
            "",
            "Excluded: append staging, conflicts/rebases, live S3/R2, Turso Cloud, production throughput, named branches, generation caching, and incremental validation.",
        ]
    )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--samples", type=int, choices=(1, 20), default=20)
    parser.add_argument("--timeout", type=float, default=300)
    parser.add_argument("--report", type=Path, default=Path("docs/qualification/2026-09-10-write-latency.md"))
    parser.add_argument("--allow-dirty", action="store_true")
    arguments = parser.parse_args()
    if arguments.allow_dirty and arguments.samples != 1:
        parser.error("--allow-dirty is limited to one-sample smoke runs")
    if arguments.report.exists():
        raise FileExistsError(arguments.report)
    metadata = provenance(arguments.binary, arguments.allow_dirty)
    output = create_output_root(arguments.output)
    fixtures = output / "fixtures"
    configs = output / "configs"
    copies = output / "copies"
    fixtures.mkdir()
    configs.mkdir()
    copies.mkdir()
    validation = []
    validation_path = output / "validation.jsonl"
    identities = {}
    for size in PROPERTY_BYTES:
        config = configs / f"prepare-{size}.json"
        config.write_text(json.dumps({"property_bytes": size}), encoding="utf-8")
        fixture = fixtures / str(size)
        record = run_process(
            [str(arguments.binary), "prepare", str(fixture), str(config)], arguments.timeout
        )
        validation.append({"step": "prepare", "property_bytes": size, **record})
        with validation_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(validation[-1], sort_keys=True) + "\n")
        if not record["success"]:
            raise RuntimeError(f"fixture preparation failed for {size}")
        identities[str(size)] = record["result"]
        verify = run_process([str(arguments.binary), "verify", str(fixture)], arguments.timeout)
        validation.append({"step": "verify_fixture", "property_bytes": size, **verify})
        with validation_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(validation[-1], sort_keys=True) + "\n")
        if not verify["success"]:
            raise RuntimeError(f"fixture verification failed for {size}")
    fixture_hashes = {str(size): tree_hash(fixtures / str(size)) for size in PROPERTY_BYTES}
    order = matrix_order(arguments.samples)
    records = []
    for index, case in enumerate(order, 1):
        size = case["property_bytes"]
        mode = case["mode"]
        config = configs / f"run-{mode}.json"
        if not config.exists():
            config.write_text(json.dumps({"mode": mode}), encoding="utf-8")
        destination = copies / f"{index:03d}-{size}-{mode}"
        command = [str(arguments.binary), "run", str(destination), str(config)]
        verify_command = [str(arguments.binary), "verify", str(destination)]
        record = execute_sample(
            command,
            fixtures / str(size),
            destination,
            arguments.timeout,
            verify_command,
            fixture_hashes[str(size)],
        )
        record.update({"ordinal": index, **case})
        records.append(record)
    cases = case_summary(records)
    scaling_result = scaling(cases)
    results = {
        "expected_samples": arguments.samples * len(PROPERTY_BYTES) * len(MODES),
        "successful_samples": sum(record["success"] for record in records),
        "failed_samples": sum(not record["success"] for record in records),
        "cases": cases,
        "scaling": scaling_result,
        "recommendation": recommendation(scaling_result),
    }
    manifest = {
        "provenance": metadata,
        "samples_per_case": arguments.samples,
        "fixtures": identities,
        "fixture_tree_sha256": fixture_hashes,
        "matrix_order": order,
        "rss_unavailable": all(record["maximum_rss"] is None for record in records),
    }
    raw = output / "raw-samples.jsonl.gz"
    with gzip.open(raw, "wt", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, sort_keys=True) + "\n")
    (output / "raw-hashes.json").write_text(
        json.dumps({raw.name: sha256_file(raw)}, indent=2) + "\n", encoding="utf-8"
    )
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    arguments.report.parent.mkdir(parents=True, exist_ok=True)
    arguments.report.write_text(markdown(manifest, results), encoding="utf-8")
    if results["failed_samples"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
