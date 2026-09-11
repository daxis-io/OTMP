import json
import sys


if sys.argv[1] == "malformed":
    print("not json")
    raise SystemExit(0)

print(json.dumps({
    "outcome": "success",
    "format": "otmp",
    "phases": [
        {"name": "initialization", "pass": 0, "elapsed_ms": 1.0},
        {"name": "planning", "pass": 0, "elapsed_ms": 2.0},
        {"name": "execution_to_first_batch", "pass": 0, "elapsed_ms": 3.0},
        {"name": "execution_rest", "pass": 0, "elapsed_ms": 4.0},
    ],
    "result": {"count": 256, "sum": 1},
}))
