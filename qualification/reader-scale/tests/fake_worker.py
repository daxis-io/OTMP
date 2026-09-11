#!/usr/bin/env python3
import json
import pathlib
import subprocess
import sys
import time


def phase(name, elapsed_ms, pass_number=1):
    return {
        "name": name,
        "pass": pass_number,
        "elapsed_ms": elapsed_ms,
        "io": {
            "stat_requests": 1,
            "range_requests": 2,
            "full_reads": 0,
            "bytes": 100,
            "errors": 0,
            "cancelled": 0,
            "elapsed_us": 1_000,
            "injected_us": 0,
            "active": 0,
            "peak_inflight": 2,
            "by_class": {"metadata": {"stat_requests": 1, "range_requests": 2, "bytes": 100}},
        },
        "reader": {
            "bytes": 100,
            "requests": 3,
            "pages": 2,
            "cache_hits": 1,
            "cache_bytes": 128,
            "peak_cache_bytes": 256,
        },
        "provider": {
            "files_considered": 16,
            "files_pruned": 14,
            "files_opened": 2,
            "parquet_bytes": 90,
            "parquet_requests": 2,
            "footer_cache_hits": 1,
            "footer_cache_bytes": 64,
            "peak_footer_cache_bytes": 96,
        },
        "pool_reserved_bytes": 512,
    }


def main():
    if len(sys.argv) != 4 or sys.argv[1] != "run":
        return 9
    root = pathlib.Path(sys.argv[2])
    config = json.loads(pathlib.Path(sys.argv[3]).read_text())
    mode = config.get("test_mode", "success")
    if mode == "sleep":
        time.sleep(10)
        return 0
    if mode == "malformed":
        print("not json", flush=True)
        return 0
    if mode == "mutate_head":
        (root / "_otmp" / "HEAD").write_bytes(b"changed-head")
    if mode == "descendant_pipe":
        subprocess.Popen([sys.executable, "-c", "import time; time.sleep(10)"])
    phases = [phase("registration", 10.0), phase("planning", 20.0)]
    if mode == "partial":
        payload = {
            "outcome": "error",
            "fixture": {"head_sha256": "abc", "files": 16},
            "config": config,
            "phases": phases,
            "error": {"stage": "planning", "message": "budget", "code": "RESOURCE_EXHAUSTED"},
            "result": None,
        }
    elif mode == "error":
        payload = {
            "outcome": "error",
            "fixture": {"head_sha256": "abc", "files": 16},
            "config": config,
            "phases": [],
            "error": {"stage": "registration", "message": "bad", "code": "INTEGRITY"},
            "result": None,
        }
    else:
        payload = {
            "outcome": "success",
            "fixture": {"head_sha256": "abc", "files": 16},
            "config": config,
            "phases": phases + [phase("execution", 30.0)],
            "error": None,
            "result": {"count": 2, "sum": 29},
        }
    print(json.dumps(payload), flush=True)
    if mode == "teardown":
        time.sleep(0.25)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
