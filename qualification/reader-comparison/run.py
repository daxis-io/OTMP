#!/usr/bin/env python3
"""Run the frozen OTMP/Iceberg comparison in fresh, paired subprocesses."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import pathlib
import platform
import random
import re
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Iterable


FORMATS = ("otmp", "iceberg")


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


def balanced_pairs(samples: int, seed: int) -> list[tuple[str, str]]:
    if samples < 2 or samples % 2:
        raise ValueError("paired samples must be a positive even number")
    pairs = [("otmp", "iceberg")] * (samples // 2)
    pairs += [("iceberg", "otmp")] * (samples // 2)
    random.Random(seed).shuffle(pairs)
    return pairs


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path: pathlib.Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def git_identity(root: pathlib.Path) -> dict[str, Any]:
    def git(*args: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            ["git", "-C", str(root), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

    head = git("rev-parse", "HEAD")
    status = git("status", "--porcelain=v1", "--untracked-files=normal")
    diff = git("diff", "HEAD", "--binary")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z")
    if any(result.returncode != 0 for result in (head, status, diff, untracked)):
        raise ValueError(f"cannot record Git provenance for {root}")
    untracked_digest = hashlib.sha256()
    for relative_raw in sorted(filter(None, untracked.stdout.split(b"\0"))):
        relative = relative_raw.decode(errors="surrogateescape")
        path = root / relative
        untracked_digest.update(relative_raw)
        untracked_digest.update(b"\0")
        if path.is_file():
            untracked_digest.update(sha256_file(path).encode())
        elif path.is_symlink():
            untracked_digest.update(os.readlink(path).encode(errors="surrogateescape"))
        untracked_digest.update(b"\0")
    return {
        "root": str(root.resolve()),
        "head": head.stdout.decode().strip(),
        "dirty": bool(status.stdout),
        "status": status.stdout.decode(errors="replace").splitlines(),
        "status_sha256": hashlib.sha256(status.stdout).hexdigest(),
        "diff_from_head_sha256": hashlib.sha256(diff.stdout).hexdigest(),
        "untracked_sha256": untracked_digest.hexdigest(),
    }


def fixture_identity(source: pathlib.Path, derived: pathlib.Path) -> dict[str, Any]:
    source_manifest = source / "qualification.json"
    source_head = source / "_otmp" / "HEAD"
    derived_manifest = derived / "qualification.json"
    return {
        "source": str(source.resolve()),
        "derived": str(derived.resolve()),
        "source_qualification_sha256": sha256_file(source_manifest),
        "source_head_sha256": sha256_file(source_head),
        "derived_qualification_sha256": sha256_file(derived_manifest),
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
    if probe.returncode != 0:
        return [], "unavailable"
    return [str(timer), *options, "-o", str(output)], method


def parse_rss(raw: str, method: str) -> int | None:
    if method == "darwin":
        match = re.search(r"(?m)^\s*(\d+)\s+maximum resident set size\s*$", raw)
        return int(match.group(1)) if match else None
    if method == "linux":
        match = re.search(
            r"(?m)^\s*Maximum resident set size \(kbytes\):\s*(\d+)\s*$", raw
        )
        return int(match.group(1)) * 1024 if match else None
    return None


def _read_pipe(pipe: Any, chunks: list[bytes]) -> None:
    while True:
        chunk = pipe.read1(8192)
        if not chunk:
            return
        chunks.append(chunk)


def capture_process(
    command: list[str], sample_dir: pathlib.Path, timeout_seconds: float
) -> dict[str, Any]:
    sample_dir.mkdir(parents=True, exist_ok=False)
    timing_path = sample_dir / "process-time.txt"
    wrapper, rss_method = time_wrapper(timing_path)
    wrapped_command = wrapper + command
    started = time.monotonic()
    process = subprocess.Popen(
        wrapped_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    assert process.stdout is not None and process.stderr is not None
    stdout_chunks: list[bytes] = []
    stderr_chunks: list[bytes] = []
    stdout_thread = threading.Thread(
        target=_read_pipe, args=(process.stdout, stdout_chunks), daemon=True
    )
    stderr_thread = threading.Thread(
        target=_read_pipe, args=(process.stderr, stderr_chunks), daemon=True
    )
    exit_state: dict[str, Any] = {}

    def wait_for_exit() -> None:
        exit_state["exit_code"] = process.wait()
        exit_state["elapsed_ms"] = (time.monotonic() - started) * 1000

    waiter = threading.Thread(target=wait_for_exit, daemon=True)
    stdout_thread.start()
    stderr_thread.start()
    waiter.start()
    deadline = started + timeout_seconds
    waiter.join(timeout=max(0.0, deadline - time.monotonic()))
    timed_out = waiter.is_alive()

    def kill_group() -> None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    if timed_out:
        kill_group()
    cleanup_deadline = time.monotonic() + 1.0 if timed_out else deadline
    waiter.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
    for thread in (stdout_thread, stderr_thread):
        thread.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
    if stdout_thread.is_alive() or stderr_thread.is_alive():
        timed_out = True
        kill_group()
        cleanup_deadline = time.monotonic() + 1.0
        for thread in (waiter, stdout_thread, stderr_thread):
            thread.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
    if not stdout_thread.is_alive():
        process.stdout.close()
    if not stderr_thread.is_alive():
        process.stderr.close()
    ended = time.monotonic()
    stdout = b"".join(stdout_chunks).decode(errors="replace")
    stderr = b"".join(stderr_chunks).decode(errors="replace")
    (sample_dir / "stdout.txt").write_text(stdout)
    (sample_dir / "stderr.txt").write_text(stderr)

    result = None
    parse_error = None
    try:
        candidate = json.loads(stdout)
        if not isinstance(candidate, dict) or candidate.get("outcome") != "success":
            parse_error = "stdout JSON is not a successful comparator result"
        else:
            result = candidate
            write_json(sample_dir / "result.json", result)
    except json.JSONDecodeError as error:
        parse_error = f"invalid stdout JSON: {error}"
    timing = timing_path.read_text(errors="replace") if timing_path.is_file() else ""
    rss = parse_rss(timing, rss_method)
    process_record = {
        "command": wrapped_command,
        "exit_code": exit_state.get("exit_code"),
        "timed_out": timed_out,
        "timeout_seconds": timeout_seconds,
        "process_exit_ms": exit_state.get("elapsed_ms"),
        "wall_ms": (ended - started) * 1000,
        "rss_bytes": rss,
        "rss_method": rss_method,
        "rss_missing": rss is None,
        "parse_error": parse_error,
    }
    write_json(sample_dir / "process.json", process_record)
    return {"directory": str(sample_dir), "process": process_record, "result": result}


def sample_kind(sample: dict[str, Any]) -> str:
    process = sample["process"]
    if process["timed_out"]:
        return "timeout"
    if process["exit_code"] != 0:
        return "process_exit"
    if sample["result"] is None:
        return "invalid_output"
    return "success"


def _append(metrics: dict[str, list[float | int]], key: str, value: Any) -> None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        metrics.setdefault(key, []).append(value)


def summarize_provider_samples(
    samples: list[dict[str, Any]], expected_format: str | None = None
) -> dict[str, Any]:
    metrics: dict[str, list[float | int]] = {}
    provider: dict[str, list[float | int]] = {}
    failures = []
    successful = 0
    for sample in samples:
        kind = sample_kind(sample)
        if kind == "success" and expected_format is not None:
            if sample["result"].get("format") != expected_format:
                kind = "unexpected_format"
        if kind != "success":
            failures.append({
                "directory": sample["directory"],
                "kind": kind,
                "exit_code": sample["process"]["exit_code"],
                "timed_out": sample["process"]["timed_out"],
                "parse_error": sample["process"]["parse_error"],
            })
            continue
        successful += 1
        result = sample["result"]
        _append(metrics, "wall", sample["process"].get("wall_ms"))
        _append(metrics, "rss_bytes", sample["process"].get("rss_bytes"))
        phases = {
            (phase["name"], phase.get("pass", 0)): phase
            for phase in result.get("phases", [])
        }
        for (name, pass_number), phase in phases.items():
            elapsed = phase.get("elapsed_ms")
            if name == "initialization":
                _append(metrics, "initialization", elapsed)
            elif name == "planning":
                _append(metrics, f"planning:{pass_number}", elapsed)
                values = (phase.get("details") or {}).get("provider") or {}
                for field in (
                    "planning_micros",
                    "files_considered",
                    "files_pruned",
                    "files_opened",
                ):
                    _append(provider, f"{field}:{pass_number}", values.get(field))
            elif name == "execution_to_first_batch":
                _append(metrics, f"first_result:{pass_number}", elapsed)
            elif name == "execution_rest":
                _append(metrics, f"drain:{pass_number}", elapsed)
        passes = sorted(pass_number for name, pass_number in phases if name == "planning")
        for pass_number in passes:
            planning = phases[("planning", pass_number)].get("elapsed_ms")
            first = phases.get(("execution_to_first_batch", pass_number), {}).get("elapsed_ms")
            if isinstance(planning, (int, float)) and isinstance(first, (int, float)):
                _append(metrics, f"combined_ready_first_result:{pass_number}", planning + first)
    return {
        "samples": {
            "total": len(samples),
            "successful": successful,
            "failed": len(failures),
        },
        "latency_ms": {
            key: distribution(values) for key, values in metrics.items() if key != "rss_bytes"
        },
        "rss_bytes": distribution(metrics.get("rss_bytes", [])),
        "provider": {key: distribution(values) for key, values in provider.items()},
        "failures": failures,
        "percentile_contract": "nearest-rank over successful samples only; failures retained separately",
    }


def summarize_plan_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    metrics: dict[str, list[float | int]] = {}
    tasks: dict[str, list[float | int]] = {}
    failures = []
    successful = 0
    for sample in samples:
        kind = sample_kind(sample)
        if kind == "success" and sample["result"].get("format") != "iceberg":
            kind = "unexpected_format"
        if kind != "success":
            failures.append({
                "directory": sample["directory"],
                "kind": kind,
                "exit_code": sample["process"]["exit_code"],
                "timed_out": sample["process"]["timed_out"],
                "parse_error": sample["process"]["parse_error"],
            })
            continue
        successful += 1
        result = sample["result"]
        _append(metrics, "wall", sample["process"].get("wall_ms"))
        _append(metrics, "rss_bytes", sample["process"].get("rss_bytes"))
        _append(metrics, "initialization", result.get("initialization_ms"))
        for phase in result.get("phases", []):
            pass_number = phase.get("pass", 0)
            _append(metrics, f"metadata_file_selection:{pass_number}", phase.get("elapsed_ms"))
            _append(tasks, f"file_tasks:{pass_number}", (phase.get("details") or {}).get("file_tasks"))
    return {
        "samples": {"total": len(samples), "successful": successful, "failed": len(failures)},
        "latency_ms": {
            key: distribution(values) for key, values in metrics.items() if key != "rss_bytes"
        },
        "rss_bytes": distribution(metrics.get("rss_bytes", [])),
        "tasks": {key: distribution(values) for key, values in tasks.items()},
        "failures": failures,
        "percentile_contract": "nearest-rank over successful samples only; failures retained separately",
    }


def verify_fixture(
    binary: pathlib.Path,
    source: pathlib.Path,
    derived: pathlib.Path,
    output: pathlib.Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    return capture_process(
        [str(binary), "verify", str(source), str(derived)], output, timeout_seconds
    )


def case_seed(base_seed: int, name: str) -> int:
    digest = hashlib.sha256(f"{base_seed}:{name}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def run_suite(args: argparse.Namespace) -> dict[str, Any]:
    paths = [
        args.binary,
        args.lock,
        args.source_repo,
        args.source_4096,
        args.derived_4096,
        args.source_16384,
        args.derived_16384,
        args.out,
    ]
    (
        binary,
        lock,
        source_repo,
        source_4096,
        derived_4096,
        source_16384,
        derived_16384,
        out,
    ) = [path.resolve() for path in paths]
    if out.exists():
        raise FileExistsError(f"output path already exists: {out}")
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f"binary is not executable: {binary}")
    if not lock.is_file():
        raise ValueError(f"lockfile does not exist: {lock}")
    out.mkdir(parents=True)
    fixtures = {
        4096: (source_4096, derived_4096),
        16384: (source_16384, derived_16384),
    }
    initial = {
        "binary_sha256": sha256_file(binary),
        "lock_sha256": sha256_file(lock),
        "source": git_identity(source_repo),
        "fixtures": {
            str(size): fixture_identity(source, derived)
            for size, (source, derived) in fixtures.items()
        },
    }
    manifest = {
        "schema_version": 1,
        "created_unix_ns": time.time_ns(),
        "host": {"platform": platform.platform(), "python": platform.python_version()},
        "samples_per_format_case": args.samples,
        "plan_files_samples_per_case": args.plan_files_samples,
        "passes": 2,
        "seed": args.seed,
        "timeout_seconds": args.timeout,
        "verify_timeout_seconds": args.verify_timeout,
        "os_cache": "uncontrolled",
        "provenance_before": initial,
        "verification": {},
    }
    write_json(out / "manifest.json", manifest)

    for size, (source, derived) in fixtures.items():
        verification = verify_fixture(
            binary, source, derived, out / "verification" / f"before-{size}", args.verify_timeout
        )
        manifest["verification"][f"before-{size}"] = {
            "kind": sample_kind(verification),
            "process": verification["process"],
        }
        write_json(out / "manifest.json", manifest)
        if sample_kind(verification) != "success":
            raise RuntimeError(
                f"fixture verification failed; evidence retained in {verification['directory']}"
            )

    provider_summaries: dict[str, Any] = {}
    for size, (source, derived) in fixtures.items():
        for selection, survivors in (("selective-2", "2"), ("broad-all", "all")):
            name = f"growth-{size}-{selection}"
            samples_by_format = {item: [] for item in FORMATS}
            schedule = balanced_pairs(args.samples, case_seed(args.seed, name))
            case_root = out / "provider" / name
            case_root.mkdir(parents=True)
            write_json(case_root.parent / f"{name}-schedule.json", schedule)
            for round_number, pair in enumerate(schedule, 1):
                for format_name in pair:
                    sample = capture_process(
                        [
                            str(binary),
                            "run",
                            format_name,
                            str(source),
                            str(derived),
                            "--survivors",
                            survivors,
                            "--passes",
                            "2",
                        ],
                        case_root / f"round-{round_number:04d}" / format_name,
                        args.timeout,
                    )
                    samples_by_format[format_name].append(sample)
            provider_summaries[name] = {
                format_name: summarize_provider_samples(
                    samples_by_format[format_name], expected_format=format_name
                )
                for format_name in FORMATS
            }
            write_json(case_root / "summary.json", provider_summaries[name])

    plan_summaries: dict[str, Any] = {}
    for size, (source, derived) in fixtures.items():
        for selection, survivors in (("selective-2", "2"), ("broad-all", "all")):
            name = f"growth-{size}-{selection}"
            samples = []
            case_root = out / "plan-files" / name
            for sample_number in range(1, args.plan_files_samples + 1):
                samples.append(capture_process(
                    [
                        str(binary),
                        "plan-files",
                        str(source),
                        str(derived),
                        "--survivors",
                        survivors,
                        "--passes",
                        "2",
                    ],
                    case_root / f"sample-{sample_number:04d}",
                    args.timeout,
                ))
            plan_summaries[name] = summarize_plan_samples(samples)
            write_json(case_root / "summary.json", plan_summaries[name])

    for size, (source, derived) in fixtures.items():
        verification = verify_fixture(
            binary, source, derived, out / "verification" / f"after-{size}", args.verify_timeout
        )
        manifest["verification"][f"after-{size}"] = {
            "kind": sample_kind(verification),
            "process": verification["process"],
        }
        write_json(out / "manifest.json", manifest)
        if sample_kind(verification) != "success":
            raise RuntimeError(
                f"fixture verification failed; evidence retained in {verification['directory']}"
            )
    final = {
        "binary_sha256": sha256_file(binary),
        "lock_sha256": sha256_file(lock),
        "source": git_identity(source_repo),
        "fixtures": {
            str(size): fixture_identity(source, derived)
            for size, (source, derived) in fixtures.items()
        },
    }
    manifest["provenance_after"] = final
    manifest["provenance_unchanged"] = initial == final
    write_json(out / "manifest.json", manifest)
    summary = {
        "provider": provider_summaries,
        "plan_files": plan_summaries,
        "provenance_unchanged": manifest["provenance_unchanged"],
        "notes": {
            "provider_order": "paired and interleaved; each format occupies each position equally",
            "primary_measure": "planning plus execution_to_first_batch",
            "iceberg_deferral": "Iceberg file enumeration occurs during execution; direct plan-files is separate",
            "os_cache": "uncontrolled",
        },
    }
    write_json(out / "summary.json", summary)
    require_valid_summary(summary)
    return summary


def require_valid_summary(summary: dict[str, Any]) -> None:
    """Fail automation after all raw evidence and summaries have been saved."""
    results = [result for case in summary["provider"].values() for result in case.values()]
    results.extend(summary["plan_files"].values())
    if not summary["provenance_unchanged"] or any(result["samples"]["failed"] for result in results):
        raise RuntimeError("qualification failed; inspect retained summary and raw evidence")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--binary", required=True, type=pathlib.Path)
    result.add_argument("--lock", required=True, type=pathlib.Path)
    result.add_argument("--source-repo", required=True, type=pathlib.Path)
    result.add_argument("--source-4096", required=True, type=pathlib.Path)
    result.add_argument("--derived-4096", required=True, type=pathlib.Path)
    result.add_argument("--source-16384", required=True, type=pathlib.Path)
    result.add_argument("--derived-16384", required=True, type=pathlib.Path)
    result.add_argument("--out", required=True, type=pathlib.Path)
    result.add_argument("--samples", type=int, default=20)
    result.add_argument("--plan-files-samples", type=int, default=10)
    result.add_argument("--seed", type=int, default=7301)
    result.add_argument("--timeout", type=float, default=300.0)
    result.add_argument("--verify-timeout", type=float, default=900.0)
    return result


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    if args.samples < 2 or args.samples % 2:
        raise SystemExit("--samples must be a positive even number")
    if args.plan_files_samples < 1:
        raise SystemExit("--plan-files-samples must be positive")
    if args.timeout <= 0 or args.verify_timeout <= 0:
        raise SystemExit("timeouts must be positive")
    summary = run_suite(args)
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
