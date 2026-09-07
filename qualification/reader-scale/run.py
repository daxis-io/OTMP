#!/usr/bin/env python3
"""Run reader_scale in fresh subprocesses and summarize preserved evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import re
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Iterable


IO_FIELDS = (
    "stat_requests",
    "range_requests",
    "full_reads",
    "bytes",
    "errors",
    "cancelled",
    "elapsed_us",
    "injected_us",
    "active",
    "peak_inflight",
)
READER_COUNTER_FIELDS = ("bytes", "requests", "pages", "cache_hits")
PROVIDER_COUNTER_FIELDS = (
    "planning_micros",
    "files_considered",
    "files_pruned",
    "files_opened",
    "parquet_bytes",
    "parquet_requests",
    "footer_cache_hits",
)
READER_MEMORY_FIELDS = ("cache_bytes", "peak_cache_bytes")
PROVIDER_MEMORY_FIELDS = ("footer_cache_bytes", "peak_footer_cache_bytes")


def nearest_rank(values: Iterable[float | int], quantile: float) -> float | int | None:
    ordered = sorted(values)
    if not ordered:
        return None
    if not 0 < quantile <= 1:
        raise ValueError("quantile must be in (0, 1]")
    return ordered[math.ceil(quantile * len(ordered)) - 1]


def distribution(values: Iterable[float | int]) -> dict[str, float | int | None]:
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0] if ordered else None,
        "p50": nearest_rank(ordered, 0.50),
        "p95": nearest_rank(ordered, 0.95),
        "max": ordered[-1] if ordered else None,
    }


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: pathlib.Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def fixture_identity(root: pathlib.Path) -> dict[str, Any]:
    manifest = root / "qualification.json"
    head_path = root / "_otmp" / "HEAD"
    manifest_raw = manifest.read_bytes() if manifest.is_file() else None
    head_raw = head_path.read_bytes() if head_path.is_file() else None
    claimed_head = None
    if manifest_raw is not None:
        try:
            value = json.loads(manifest_raw)
            claimed_head = value.get("head_sha256") if isinstance(value, dict) else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
    if isinstance(claimed_head, str):
        digest = claimed_head.removeprefix("sha256:")
        claimed_head = f"sha256:{digest.lower()}" if re.fullmatch(r"[0-9a-fA-F]{64}", digest) else None
    else:
        claimed_head = None
    actual_head = (
        f"sha256:{hashlib.sha256(head_raw).hexdigest()}" if head_raw is not None else None
    )
    return {
        "manifest_present": manifest_raw is not None,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest() if manifest_raw is not None else None,
        "manifest_head_sha256": claimed_head,
        "head_present": head_raw is not None,
        "head_sha256": actual_head,
        "manifest_matches_head": claimed_head is not None and claimed_head == actual_head,
    }


def git_identity(path: pathlib.Path) -> dict[str, Any]:
    def git(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", "-C", str(path), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    top = git("rev-parse", "--show-toplevel")
    if top.returncode != 0:
        return {"available": False, "sha": None, "dirty": None, "root": None}
    root = pathlib.Path(top.stdout.strip())
    sha = git("rev-parse", "HEAD")
    status = git("status", "--porcelain", "--untracked-files=normal")
    return {
        "available": sha.returncode == 0 and status.returncode == 0,
        "sha": sha.stdout.strip() if sha.returncode == 0 else None,
        "dirty": bool(status.stdout) if status.returncode == 0 else None,
        "root": str(root),
    }


def time_wrapper(output: pathlib.Path) -> tuple[list[str], str]:
    timer = pathlib.Path("/usr/bin/time")
    if not timer.is_file():
        return [], "unavailable"
    if sys.platform == "darwin":
        options, method = ["-l"], "darwin"
    elif sys.platform.startswith("linux"):
        options, method = ["-v"], "linux"
    else:
        return [], "unavailable"
    probe = subprocess.run(
        [str(timer), *options, "-o", os.devnull, "true"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    wrapper = [str(timer), *options, "-o", str(output)]
    return (wrapper, method) if probe.returncode == 0 else ([], "unavailable")


def parse_rss(stderr: str, method: str) -> int | None:
    if method == "darwin":
        match = re.search(r"(?m)^\s*(\d+)\s+maximum resident set size\s*$", stderr)
        return int(match.group(1)) if match else None
    if method == "linux":
        match = re.search(r"(?m)^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$", stderr)
        return int(match.group(1)) * 1024 if match else None
    return None


def _read_pipe(pipe: Any, chunks: list[bytes], arrival: list[float], start: float) -> None:
    pending = bytearray()
    while True:
        chunk = pipe.read1(8192)
        if not chunk:
            break
        chunks.append(chunk)
        pending.extend(chunk)
        while b"\n" in pending:
            line, _, pending = pending.partition(b"\n")
            try:
                parsed = json.loads(line)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(parsed, dict) and not arrival:
                arrival.append((time.monotonic() - start) * 1000)


def _valid_result(value: Any) -> bool:
    return (
        isinstance(value, dict)
        and value.get("outcome") in ("success", "error")
        and isinstance(value.get("phases"), list)
        and all(isinstance(phase, dict) and isinstance(phase.get("name"), str) for phase in value["phases"])
    )


def _start_process_waiter(process: Any, start: float) -> tuple[threading.Thread, dict[str, Any]]:
    state: dict[str, Any] = {}

    def wait() -> None:
        state["exit_code"] = process.wait()
        state["process_exit_ms"] = (time.monotonic() - start) * 1000

    waiter = threading.Thread(target=wait, daemon=True)
    waiter.start()
    return waiter, state


def capture_sample(
    binary: pathlib.Path,
    root: pathlib.Path,
    config: pathlib.Path,
    sample_dir: pathlib.Path,
    timeout_seconds: float,
    sample_number: int,
) -> dict[str, Any]:
    sample_dir.mkdir(parents=True, exist_ok=False)
    before = fixture_identity(root)
    timing_path = sample_dir / "process-time.txt"
    wrapper, rss_method = time_wrapper(timing_path)
    command = wrapper + [str(binary), "run", str(root), str(config)]
    start = time.monotonic()
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    arrival: list[float] = []
    stdout_thread = threading.Thread(
        target=_read_pipe, args=(process.stdout, stdout_chunks, arrival, start), daemon=True
    )
    stderr_thread = threading.Thread(
        target=_read_pipe, args=(process.stderr, stderr_chunks, [], start), daemon=True
    )
    stdout_thread.start()
    stderr_thread.start()
    waiter_thread, waiter_state = _start_process_waiter(process, start)
    timed_out = False
    deadline = start + timeout_seconds

    def kill_process_group() -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    waiter_thread.join(timeout=max(0, deadline - time.monotonic()))
    cleanup_deadline = deadline
    if waiter_thread.is_alive():
        timed_out = True
        kill_process_group()
        cleanup_deadline = time.monotonic() + 1.0
        waiter_thread.join(timeout=max(0, cleanup_deadline - time.monotonic()))
    for thread in (stdout_thread, stderr_thread):
        thread.join(timeout=max(0, cleanup_deadline - time.monotonic()))
    if stdout_thread.is_alive() or stderr_thread.is_alive():
        timed_out = True
        kill_process_group()
        cleanup_deadline = time.monotonic() + 1.0
        waiter_thread.join(timeout=max(0, cleanup_deadline - time.monotonic()))
        for thread in (stdout_thread, stderr_thread):
            thread.join(timeout=max(0, cleanup_deadline - time.monotonic()))
    if not stdout_thread.is_alive():
        process.stdout.close()
    if not stderr_thread.is_alive():
        process.stderr.close()
    end = time.monotonic()
    exit_code = waiter_state.get("exit_code")
    process_exit_ms = waiter_state.get("process_exit_ms")
    capture_complete_ms = (end - start) * 1000
    stdout = b"".join(stdout_chunks).decode("utf-8", errors="replace")
    stderr = b"".join(stderr_chunks).decode("utf-8", errors="replace")
    (sample_dir / "stdout.txt").write_text(stdout)
    (sample_dir / "stderr.txt").write_text(stderr)

    parsed: Any = None
    parse_error: str | None = None
    try:
        parsed = json.loads(stdout)
        if not _valid_result(parsed):
            parse_error = "stdout JSON does not match the reader_scale result envelope"
            parsed = None
        elif not arrival:
            arrival.append((end - start) * 1000)
    except json.JSONDecodeError as error:
        parse_error = f"invalid stdout JSON: {error}"

    if parsed is not None:
        write_json(sample_dir / "result.json", parsed)
    timing_output = timing_path.read_text(errors="replace") if timing_path.is_file() else ""
    rss_bytes = parse_rss(timing_output, rss_method)
    after = fixture_identity(root)
    head_unchanged = before["head_sha256"] is not None and before["head_sha256"] == after["head_sha256"]
    process_record = {
        "sample": sample_number,
        "command": command,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "wall_ms": capture_complete_ms,
        "process_exit_ms": process_exit_ms,
        "capture_complete_ms": capture_complete_ms,
        "capture_after_exit_ms": (
            capture_complete_ms - process_exit_ms if process_exit_ms is not None else None
        ),
        "result_arrival_ms": arrival[0] if arrival else None,
        "result_to_process_exit_ms": (
            process_exit_ms - arrival[0] if arrival and process_exit_ms is not None else None
        ),
        "teardown_gap_ms": (capture_complete_ms - arrival[0]) if arrival else None,
        "rss": {
            "bytes": rss_bytes,
            "method": rss_method,
            "missing": rss_bytes is None,
        },
        "fixture_before": before,
        "fixture_after": after,
        "fixture_head_unchanged": head_unchanged,
        "parse_error": parse_error,
    }
    write_json(sample_dir / "process.json", process_record)
    return process_record


def _sample_kind(process: dict[str, Any], result: dict[str, Any] | None) -> str:
    if process["timed_out"]:
        return "timeout"
    if process["exit_code"] != 0:
        return "process_exit"
    if result is None:
        return "invalid_output"
    if not process["fixture_head_unchanged"]:
        return "fixture_mutated"
    if not process["fixture_before"]["manifest_matches_head"]:
        return "fixture_identity_mismatch"
    if not process["fixture_after"]["manifest_matches_head"]:
        return "fixture_identity_mismatch"
    if result.get("violations"):
        return "qualification_invariant_violation"
    if result["outcome"] == "error":
        return "observed_reader_error"
    return "success"


def _append_metric(target: dict[str, list[float | int]], key: str, value: Any) -> None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        target.setdefault(key, []).append(value)


def _phase_key(phase: dict[str, Any]) -> str:
    return f"{phase['name']}:{phase.get('pass', 0)}"


def _phase_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    latency: dict[str, list[float | int]] = {}
    io: dict[str, dict[str, list[float | int]]] = {}
    readers: dict[str, dict[str, list[float | int]]] = {}
    providers: dict[str, dict[str, list[float | int]]] = {}
    memory: dict[str, dict[str, list[float | int]]] = {}
    by_class: dict[str, dict[str, dict[str, list[float | int]]]] = {}
    for result in records:
        for phase in result.get("phases", []):
            name = _phase_key(phase)
            _append_metric(latency, name, phase.get("elapsed_ms"))
            phase_io = io.setdefault(name, {})
            for field in IO_FIELDS:
                _append_metric(phase_io, field, phase.get("io", {}).get(field))
            for class_name, class_io in phase.get("io", {}).get("by_class", {}).items():
                class_metrics = by_class.setdefault(name, {}).setdefault(class_name, {})
                for field in IO_FIELDS:
                    _append_metric(class_metrics, field, class_io.get(field))
            reader_metrics = readers.setdefault(name, {})
            for field in READER_COUNTER_FIELDS:
                _append_metric(reader_metrics, field, (phase.get("reader") or {}).get(field))
            provider_metrics = providers.setdefault(name, {})
            for field in PROVIDER_COUNTER_FIELDS:
                _append_metric(provider_metrics, field, (phase.get("provider") or {}).get(field))
            memory_metrics = memory.setdefault(name, {})
            _append_metric(memory_metrics, "pool_reserved_bytes", phase.get("pool_reserved_bytes"))
            for field in READER_MEMORY_FIELDS:
                _append_metric(memory_metrics, f"reader_{field}", (phase.get("reader") or {}).get(field))
            for field in PROVIDER_MEMORY_FIELDS:
                _append_metric(
                    memory_metrics,
                    f"provider_{field}",
                    (phase.get("provider") or {}).get(field),
                )
    summarize = lambda groups: {
        name: {metric: distribution(values) for metric, values in metrics.items()}
        for name, metrics in groups.items()
    }
    return {
        "latency": {name: distribution(values) for name, values in latency.items()},
        "io": summarize(io),
        "reader": summarize(readers),
        "provider": summarize(providers),
        "memory": summarize(memory),
        "by_class": {
            phase: {
                class_name: {metric: distribution(values) for metric, values in metrics.items()}
                for class_name, metrics in classes.items()
            }
            for phase, classes in by_class.items()
        },
    }


def build_summary(sample_dirs: Iterable[pathlib.Path]) -> dict[str, Any]:
    successes: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    process_successes: list[dict[str, Any]] = []
    process_failures: list[dict[str, Any]] = []
    failure_results: list[dict[str, Any]] = []
    total = 0
    for sample_dir in sorted(sample_dirs):
        total += 1
        process = json.loads((sample_dir / "process.json").read_text())
        result_path = sample_dir / "result.json"
        result = json.loads(result_path.read_text()) if result_path.is_file() else None
        kind = _sample_kind(process, result)
        if kind == "success":
            successes.append(result)
            process_successes.append(process)
            continue
        if result is not None:
            failure_results.append(result)
        process_failures.append(process)
        failures.append({
            "sample_dir": str(sample_dir),
            "kind": kind,
            "exit_code": process["exit_code"],
            "timed_out": process["timed_out"],
            "parse_error": process.get("parse_error"),
            "error": result.get("error") if result else None,
            "violations": result.get("violations", []) if result else [],
            "observed_phases": [_phase_key(phase) for phase in result.get("phases", [])]
            if result
            else [],
        })
    success_phase = _phase_metrics(successes)
    failure_phase = _phase_metrics(failure_results)
    wall = [record["wall_ms"] for record in process_successes]
    arrival = [record["result_arrival_ms"] for record in process_successes if record["result_arrival_ms"] is not None]
    gap = [record["teardown_gap_ms"] for record in process_successes if record["teardown_gap_ms"] is not None]
    process_exit = [
        record["process_exit_ms"]
        for record in process_successes
        if record["process_exit_ms"] is not None
    ]
    capture_complete = [record["capture_complete_ms"] for record in process_successes]
    capture_after_exit = [
        record["capture_after_exit_ms"]
        for record in process_successes
        if record["capture_after_exit_ms"] is not None
    ]
    result_to_process_exit = [
        record["result_to_process_exit_ms"]
        for record in process_successes
        if record["result_to_process_exit_ms"] is not None
    ]
    rss = [record["rss"]["bytes"] for record in process_successes if record["rss"]["bytes"] is not None]
    failure_rss = [
        record["rss"]["bytes"]
        for record in process_failures
        if record["rss"]["bytes"] is not None
    ]
    failure_wall = [record["wall_ms"] for record in process_failures]
    return {
        "samples": {"total": total, "successful": len(successes), "failed": len(failures)},
        "latency_ms": {
            "wall": distribution(wall),
            "process_exit": distribution(process_exit),
            "capture_complete": distribution(capture_complete),
            "capture_after_exit": distribution(capture_after_exit),
            "result_arrival": distribution(arrival),
            "result_to_process_exit": distribution(result_to_process_exit),
            "teardown_gap": distribution(gap),
            "phase": success_phase["latency"],
        },
        "rss_bytes": distribution(rss),
        "failure_rss_bytes": distribution(failure_rss),
        "failure_wall_ms": distribution(failure_wall),
        "phase_io": success_phase["io"],
        "phase_io_by_class": success_phase["by_class"],
        "phase_reader": success_phase["reader"],
        "phase_provider": success_phase["provider"],
        "phase_memory": success_phase["memory"],
        "failure_phase_latency_ms": failure_phase["latency"],
        "failure_phase_io": failure_phase["io"],
        "failure_phase_io_by_class": failure_phase["by_class"],
        "failure_phase_reader": failure_phase["reader"],
        "failure_phase_provider": failure_phase["provider"],
        "failure_phase_memory": failure_phase["memory"],
        "failures": failures,
        "notes": {
            "percentiles": "nearest-rank over successful samples only",
            "os_cache": "uncontrolled; each sample uses a fresh process, not a flushed operating-system cache",
            "peak_inflight": "reported values are the child's cumulative maximum, summarized across samples",
        },
    }


def write_summary(evidence_dir: pathlib.Path) -> dict[str, Any]:
    summary = build_summary(evidence_dir.glob("sample-*"))
    write_json(evidence_dir / "summary.json", summary)
    return summary


def run_samples(
    binary: pathlib.Path,
    root: pathlib.Path,
    config: pathlib.Path,
    out: pathlib.Path,
    samples: int,
    timeout_seconds: float,
) -> dict[str, Any]:
    if out.exists():
        raise FileExistsError(f"output path already exists: {out}")
    if samples < 1:
        raise ValueError("samples must be at least one")
    if timeout_seconds <= 0:
        raise ValueError("timeout must be positive")
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"binary is not an executable file: {binary}")
    if not root.is_dir():
        raise ValueError(f"fixture root is not a directory: {root}")
    config_value = json.loads(config.read_text())
    out.mkdir(parents=True, exist_ok=False)
    before = fixture_identity(root)
    write_json(out / "manifest.json", {
        "schema_version": 1,
        "binary": {"path": str(binary.resolve()), "sha256": sha256_file(binary)},
        "runner_git": git_identity(pathlib.Path.cwd()),
        "fixture_root": str(root.resolve()),
        "fixture_before": before,
        "config_path": str(config.resolve()),
        "config": config_value,
        "samples": samples,
        "timeout_seconds": timeout_seconds,
        "host": {"platform": platform.platform(), "python": platform.python_version()},
        "os_cache": "uncontrolled",
    })
    for number in range(1, samples + 1):
        capture_sample(binary, root, config, out / f"sample-{number:04d}", timeout_seconds, number)
    manifest = json.loads((out / "manifest.json").read_text())
    manifest["fixture_after"] = fixture_identity(root)
    before_head = manifest["fixture_before"]["head_sha256"]
    after_head = manifest["fixture_after"]["head_sha256"]
    manifest["fixture_head_unchanged"] = before_head is not None and before_head == after_head
    manifest["fixture_manifest_unchanged"] = (
        manifest["fixture_before"]["manifest_sha256"]
        == manifest["fixture_after"]["manifest_sha256"]
    )
    write_json(out / "manifest.json", manifest)
    return write_summary(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="run fresh reader_scale subprocess samples")
    run_parser.add_argument("--binary", required=True, type=pathlib.Path)
    run_parser.add_argument("--root", required=True, type=pathlib.Path)
    run_parser.add_argument("--config", required=True, type=pathlib.Path)
    run_parser.add_argument("--out", required=True, type=pathlib.Path)
    run_parser.add_argument("--samples", type=int, default=10)
    run_parser.add_argument("--timeout", type=float, default=300.0)
    summary_parser = commands.add_parser("summarize", help="aggregate existing evidence directories")
    summary_parser.add_argument("evidence", nargs="+", type=pathlib.Path)
    summary_parser.add_argument("--out", required=True, type=pathlib.Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "run":
            summary = run_samples(
                args.binary, args.root, args.config, args.out, args.samples, args.timeout
            )
            print(json.dumps(summary, sort_keys=True))
        else:
            if args.out.exists():
                raise FileExistsError(f"output path already exists: {args.out}")
            sample_dirs = [sample for evidence in args.evidence for sample in evidence.glob("sample-*")]
            summary = build_summary(sample_dirs)
            write_json(args.out, summary)
            print(json.dumps(summary, sort_keys=True))
    except (FileExistsError, OSError, ValueError, json.JSONDecodeError) as error:
        parser.error(str(error))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
