#!/usr/bin/env python3
"""Interleave engine page-cache capacities while holding the fixture and query fixed."""
import argparse
import json
import os
import pathlib
import random
import signal
import subprocess
import time

import run as runner


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]


def rotation_block(base, reverse):
    direction = -1 if reverse else 1
    return [
        [base[(rotation + direction * position) % 3] for position in range(3)]
        for rotation in range(3)
    ]


def balanced_order(names, samples, seed=7):
    """Return balanced, reproducible three-case rounds."""
    if len(names) != 3:
        raise ValueError("cache experiment requires exactly three cases")
    randomizer = random.Random(seed)
    order = []
    round_number = 1
    block = 0
    while round_number <= samples:
        base = list(names)
        randomizer.shuffle(base)
        rows = rotation_block(base, reverse=block % 2 == 1)
        randomizer.shuffle(rows)
        for row in rows:
            if round_number > samples:
                break
            order.extend(
                dict(round=round_number, case=name, position=position)
                for position, name in enumerate(row, 1)
            )
            round_number += 1
        block += 1
    return order


def _binary_identity(binary):
    return {"path": str(binary), "sha256": runner.sha256_file(binary)}


def _require_identity(binary, root, binary_before, fixture_before, stage):
    try:
        binary_after = _binary_identity(binary)
    except OSError as error:
        raise RuntimeError(f"{stage}: binary identity changed: {error}") from error
    fixture_after = runner.fixture_identity(root)
    if binary_after != binary_before:
        raise RuntimeError(f"{stage}: binary identity changed")
    if (
        not fixture_after["manifest_matches_head"]
        or fixture_after["head_sha256"] != fixture_before["head_sha256"]
        or fixture_after["manifest_sha256"] != fixture_before["manifest_sha256"]
    ):
        raise RuntimeError(f"{stage}: fixture identity changed")
    return binary_after, fixture_after


def _verify_fixture(binary, root, out, timeout):
    command = [str(binary), "verify", str(root)]
    record = {
        "command": command,
        "exit_code": None,
        "timed_out": False,
        "timeout_seconds": timeout,
        "elapsed_seconds": None,
        "launch_error": None,
    }
    started = time.monotonic()
    try:
        with (out / "verify.stdout").open("x") as stdout, (out / "verify.stderr").open(
            "x"
        ) as stderr:
            try:
                child = subprocess.Popen(
                    command,
                    stdout=stdout,
                    stderr=stderr,
                    start_new_session=True,
                )
                try:
                    record["exit_code"] = child.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    record["timed_out"] = True
                    try:
                        os.killpg(child.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    record["exit_code"] = child.wait(timeout=1)
            except OSError as error:
                record["launch_error"] = str(error)
    finally:
        record["elapsed_seconds"] = time.monotonic() - started
        record["fixture"] = runner.fixture_identity(root)
        runner.write_json(out / "verify.process.json", record)
    if record["timed_out"]:
        raise RuntimeError("fixture verification timed out; evidence retained")
    if record["launch_error"] is not None:
        raise RuntimeError("fixture verification could not start; evidence retained")
    if record["exit_code"]:
        raise RuntimeError("fixture verification failed; evidence retained")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=pathlib.Path, required=True)
    parser.add_argument("--root", type=pathlib.Path, required=True)
    parser.add_argument("--out", type=pathlib.Path, required=True)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args(argv)
    if args.samples < 1 or args.timeout <= 0:
        parser.error("samples and timeout must be positive")
    binary, root, out = args.binary.resolve(), args.root.resolve(), args.out.resolve()
    os.chdir(REPO_ROOT)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error(f"binary is not an executable file: {binary}")
    if not root.is_dir():
        parser.error(f"fixture root is not a directory: {root}")
    out.mkdir(parents=True, exist_ok=False)

    configs = {}
    for mib in (4, 16, 32):
        name = f"engine-{mib}m"
        configs[name] = dict(
            survivors=2, passes=2, engine_page_cache_bytes=mib * 1024**2
        )
        runner.write_json(out / f"{name}.json", configs[name])
    order = balanced_order(list(configs), args.samples, seed=7)
    runner.write_json(
        out / "order.json",
        dict(seed=7, samples_per_case=args.samples, order=order),
    )

    binary_before = _binary_identity(binary)
    fixture_before = runner.fixture_identity(root)
    source = runner.git_identity(REPO_ROOT)
    manifest = {
        "schema_version": 1,
        "status": "running",
        "source": source,
        "binary_before": binary_before,
        "fixture_root": str(root),
        "fixture_before": fixture_before,
        "samples_per_case": args.samples,
        "timeout_seconds": args.timeout,
        "configs": configs,
        "order_file": "order.json",
        "seed": 7,
        "failure": None,
    }
    runner.write_json(out / "manifest.json", manifest)
    stage = "preflight"
    try:
        fixture_manifest = json.loads((root / "qualification.json").read_text())
        if fixture_manifest.get("files") != 16384:
            raise RuntimeError("cache experiment requires a 16384-file fixture")
        _require_identity(binary, root, binary_before, fixture_before, stage)

        stage = "verification"
        _verify_fixture(binary, root, out, args.timeout)
        _require_identity(binary, root, binary_before, fixture_before, "after_verification")

        for item in order:
            name, number = item["case"], item["round"]
            stage = f"before_sample:{name}:round-{number:04d}"
            _require_identity(binary, root, binary_before, fixture_before, stage)
            summary = runner.run_samples(
                binary,
                root,
                out / f"{name}.json",
                out / name / f"round-{number:04d}",
                1,
                args.timeout,
            )
            stage = f"after_sample:{name}:round-{number:04d}"
            _require_identity(binary, root, binary_before, fixture_before, stage)
            print(
                json.dumps(dict(round=number, case=name, samples=summary["samples"])),
                flush=True,
            )

        stage = "final"
        binary_after, fixture_after = _require_identity(
            binary, root, binary_before, fixture_before, stage
        )
        manifest.update(
            status="complete",
            binary_after=binary_after,
            fixture_after=fixture_after,
            binary_unchanged=True,
            fixture_unchanged=True,
        )
        runner.write_json(out / "manifest.json", manifest)

        summaries = {}
        for name in configs:
            summaries[name] = runner.build_summary(
                (out / name).glob("round-*/sample-*")
            )
        runner.write_json(
            out / "summary.json",
            dict(
                configs=configs,
                cases=summaries,
                fixture=fixture_manifest,
                fixture_identity=fixture_after,
                binary_sha256=binary_before["sha256"],
                source=source,
                manifest_file="manifest.json",
                order_file="order.json",
                seed=7,
            ),
        )
    except BaseException as error:
        binary_after = (
            _binary_identity(binary)
            if binary.is_file()
            else {"path": str(binary), "sha256": None}
        )
        fixture_after = runner.fixture_identity(root)
        manifest.update(
            status="failed",
            binary_after=binary_after,
            fixture_after=fixture_after,
            binary_unchanged=binary_after == binary_before,
            fixture_unchanged=fixture_after == fixture_before,
            failure={"stage": stage, "type": type(error).__name__, "message": str(error)},
        )
        runner.write_json(out / "manifest.json", manifest)
        raise


if __name__ == "__main__":
    main()
