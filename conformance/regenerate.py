#!/usr/bin/env python3
"""Regenerate canonical package JSON from parsed values and check exact bytes."""
import argparse
import json
import pathlib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check', action='store_true')
parser.parse_args()
root = pathlib.Path(__file__).resolve().parent
count = 0
for package in ('genesis', 'append'):
    files = [root / 'tables' / package / '_otmp/HEAD']
    files.extend((root / 'tables' / package / '_otmp').rglob('*.json'))
    for path in files:
        original = path.read_bytes()
        value = json.loads(original)
        regenerated = json.dumps(value, ensure_ascii=False, separators=(',', ':'), sort_keys=True).encode()
        assert regenerated == original, f'noncanonical package object: {path}'
        count += 1
print(f'canonical package JSON regeneration passed: {count} objects')
