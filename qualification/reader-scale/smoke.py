#!/usr/bin/env python3
"""Exercise the actual native CLI and subprocess report together in CI."""
import argparse
import json
import pathlib
import subprocess
import tempfile

import run as runner

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--binary", type=pathlib.Path, required=True)
args = parser.parse_args()
binary = args.binary.resolve()
out = pathlib.Path(tempfile.mkdtemp(prefix="otmp-reader-scale-smoke-"))
print(f"Evidence: {out}", flush=True)
prepare = out / "prepare.json"
runner.write_json(prepare, dict(files=16, rows_per_file=128))
subprocess.run([str(binary), "prepare", str(out / "table"), str(prepare)], check=True,
               stdout=subprocess.DEVNULL)
for pruning in (True, False):
    config = out / f"run-{pruning}.json"
    runner.write_json(config, dict(survivors=2, passes=2, pruning=pruning))
    summary = runner.run_samples(binary, out / "table", config, out / f"results-{pruning}", 3, 60)
    if summary["samples"] != dict(total=3, successful=3, failed=0):
        raise RuntimeError(json.dumps(summary["failures"]))
    if summary["latency_ms"]["phase"]["planning:0"]["count"] != 3 or \
       summary["latency_ms"]["phase"]["planning:1"]["count"] != 3:
        raise RuntimeError("cold and retained passes were not separated")
print("Native CLI and subprocess reporting: 6/6 samples passed")
