#!/usr/bin/env python3
"""Reproduce the bounded qualification matrix; all evidence paths are retained."""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import time
import uuid

import run as runner


def recipes():
    fixtures = {}
    cases = []

    def case(name, fixture, config, samples=20):
        cases.append(dict(name=name, fixture=fixture, config=config, samples=samples))

    for n in (16, 256, 1024, 4096, 16384):
        fixture = f"growth-{n}"
        fixtures[fixture] = dict(files=n, rows_per_file=128, batch_size=128, small_tail=True)
        case(f"{fixture}-selective", fixture, dict(survivors=2))
        case(f"{fixture}-all", fixture, {})
        case(f"{fixture}-unpruned", fixture, dict(survivors=2, pruning=False))
        case(f"{fixture}-retained", fixture, dict(survivors=2, passes=2))
    for n in (4096, 16384):
        case(f"growth-{n}-planning-only", f"growth-{n}", dict(execute=False))
    for n in (16, 256, 1024):
        for delay in (1, 10):
            for inflight in (1, 8):
                case(f"remote-{n}-{delay}ms-cap{inflight}", f"growth-{n}",
                     dict(survivors=2, passes=2, delay_ms=delay, metadata_inflight=inflight), 10)
    case("remote-256-all-10ms", "growth-256", dict(delay_ms=10), 10)
    case("remote-256-unpruned-10ms", "growth-256",
         dict(survivors=2, pruning=False, delay_ms=10), 10)
    for n in (128, 512, 1024, 2048):
        name = f"append-{n}"
        fixtures[name] = dict(files=n, rows_per_file=128, batch_size=n, small_tail=False)
        fixtures[f"{name}-tail"] = dict(copy_from=name)
        for suffix in ("", "-tail"):
            case(f"{name}{suffix}", f"{name}{suffix}", dict(survivors=2))
    for size in (16384, 131072, 524288, 1048576, 2097152):
        name = f"property-{size}"
        fixtures[name] = dict(files=0, property_bytes=size, small_tail=False)
        fixtures[f"{name}-tail"] = dict(copy_from=name)
        for suffix in ("", "-tail"):
            case(f"{name}{suffix}", f"{name}{suffix}", {})
    for name in ("append-2048", "property-1048576", "property-2097152"):
        case(f"{name}-raised", name, dict(record_budget=4*1024**2, metadata_budget=256*1024**2), 10)
    for budget in (32768, 262144, 1048576, 8388608):
        case(f"planning-budget-{budget}", "growth-4096", dict(planning_budget=budget))
    case("pool-budget-1048576", "growth-4096", dict(df_pool_bytes=1048576))
    return fixtures, cases


def prepare(binary, out, fixtures):
    for name, config in fixtures.items():
        root = out / "fixtures" / name
        if root.exists():
            # A completed, unchanged fixture may be reused. Never repair partial roots.
            manifest = json.loads((root / "qualification.json").read_text())
            expected = config if "copy_from" not in config else {"small_tail": True}
            if not manifest.get("verified") or any(manifest.get(k) != v for k, v in expected.items()):
                raise ValueError(f"fixture configuration mismatch: {root}")
            if not runner.fixture_identity(root)["manifest_matches_head"]:
                raise ValueError(f"fixture identity mismatch: {root}")
            continue
        config_path = out / "configs" / f"prepare-{name}.json"
        runner.write_json(config_path, config)
        if "copy_from" in config:
            shutil.copytree(out / "fixtures" / config["copy_from"], root)
            command = [str(binary), "tail", str(root)]
        else:
            command = [str(binary), "prepare", str(root), str(config_path)]
        print(f"prepare {name}", flush=True)
        started = time.monotonic()
        with (out / "preparation" / f"{name}.stdout").open("x") as stdout, \
             (out / "preparation" / f"{name}.stderr").open("x") as stderr:
            child = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
        record = dict(case=name, elapsed_seconds=time.monotonic()-started, exit_code=child.returncode)
        runner.write_json(out / "preparation" / f"{name}.process.json", record)
        if child.returncode:
            raise RuntimeError(f"fixture preparation failed: {name}; evidence retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "measure", "all", "list"))
    parser.add_argument("--binary", type=pathlib.Path)
    parser.add_argument("--out", type=pathlib.Path)
    parser.add_argument("--case", action="append", help="exact case name; repeat to select a subset")
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    fixtures, cases = recipes()
    if args.case:
        known = {case["name"] for case in cases}
        if set(args.case) - known:
            parser.error("unknown case name")
        cases = [case for case in cases if case["name"] in args.case]
        needed = {case["fixture"] for case in cases}
        needed |= {fixtures[name]["copy_from"] for name in list(needed) if "copy_from" in fixtures[name]}
        fixtures = {name: config for name, config in fixtures.items() if name in needed}
    if args.action == "list":
        print(json.dumps(dict(fixtures=fixtures, cases=cases), indent=2))
        return
    if args.binary is None or args.out is None:
        parser.error("--binary and --out are required")
    binary, out = args.binary.resolve(), args.out.resolve()
    for directory in ("fixtures", "configs", "preparation", "results"):
        (out / directory).mkdir(parents=True, exist_ok=True)
    if args.action in ("all", "prepare"):
        prepare(binary, out, fixtures)
    if args.action in ("all", "measure"):
        # Recheck content, including pruned files, once per fixture before any timers.
        for name in sorted({case["fixture"] for case in cases}):
            stem = f"verify-{name}-{uuid.uuid4().hex}"
            started = time.monotonic()
            with (out / "preparation" / f"{stem}.stdout").open("x") as stdout, \
                 (out / "preparation" / f"{stem}.stderr").open("x") as stderr:
                child = subprocess.run([str(binary), "verify", str(out / "fixtures" / name)],
                                       stdout=stdout, stderr=stderr, check=False)
            runner.write_json(out / "preparation" / f"{stem}.process.json",
                              dict(fixture=name, exit_code=child.returncode,
                                   elapsed_seconds=time.monotonic()-started))
            if child.returncode:
                raise RuntimeError(f"fixture verification failed: {name}; evidence retained")
        # Case results are never overwritten or implicitly skipped.
        for case in cases:
            destination = out / "results" / case["name"]
            if destination.exists():
                raise FileExistsError(destination)
        for case in cases:
            config = out / "configs" / f"run-{case['name']}.json"
            runner.write_json(config, case["config"])
            print(f"measure {case['name']} ({case['samples']} samples)", flush=True)
            summary = runner.run_samples(binary, out / "fixtures" / case["fixture"], config,
                                         out / "results" / case["name"], case["samples"], args.timeout)
            print(json.dumps(dict(case=case["name"], samples=summary["samples"],
                                  wall_ms=summary["latency_ms"]["wall"])), flush=True)


if __name__ == "__main__":
    main()
