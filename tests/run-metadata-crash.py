#!/usr/bin/env python3
"""Process-crash qualification for zero-snapshot property/ref publication."""
import json
import os
import pathlib
import sqlite3
import subprocess
import tempfile

root = pathlib.Path(__file__).resolve().parent.parent
binary = root / 'target/debug/otmp'
for kind in ('properties', 'refs'):
    for point in ('during_temporary_head_creation', 'after_immutable_uploads',
                  'after_final_head_rename'):
        with tempfile.TemporaryDirectory(prefix='otmp-metadata-crash-') as scratch:
            scratch = pathlib.Path(scratch)
            table = scratch / 'table'
            subprocess.run([binary, 'init', table, '--schema', root / 'conformance/sources/schema.json'],
                           check=True, stdout=subprocess.DEVNULL)
            if kind == 'properties':
                requirements = [{'type': 'property_is', 'key': 'owner', 'value': None}]
                operations = [{'type': 'set_properties', 'operation_id': 'set',
                               'updates': {'owner': 'test'}, 'removals': []}]
            else:
                requirements = [{'type': 'ref_absent', 'ref': 'audit'}]
                operations = [{'type': 'create_ref', 'operation_id': 'create',
                               'ref': 'audit', 'ref_type': 'branch', 'snapshot_id': None}]
            manifest = scratch / 'transaction.json'
            manifest.write_text(json.dumps({'idempotency_key': point,
                                            'requirements': requirements, 'operations': operations}))
            outcome = subprocess.run([binary, 'transact', table, '--manifest', manifest],
                                     env={**os.environ, 'OTMP_FAILPOINT': point}, capture_output=True)
            assert outcome.returncode == 86, (point, outcome.stderr)
            subprocess.run([binary, 'verify', table, '--history'], check=True, stdout=subprocess.DEVNULL)
            head = json.loads((table / '_otmp/HEAD').read_bytes())
            expected = 1 if point == 'after_final_head_rename' else 0
            assert int(head['table_version']) == expected
            generation = json.loads((table / head['metadata_generation']['uri']).read_bytes())
            resolved = scratch / 'resolved.sqlite3'
            subprocess.run(['python3', root / 'conformance/cow.py', '--resolve', table, '--output', resolved], check=True)
            with sqlite3.connect(resolved) as db:
                assert db.execute('SELECT table_version FROM otmp_meta').fetchone()[0] == expected
                assert db.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
                assert db.execute('SELECT last_sequence_number FROM otmp_meta').fetchone()[0] == 0
                assert db.execute('SELECT count(*) FROM otmp_snapshots').fetchone()[0] == 0
print('metadata process-crash evidence passed for 6 scenarios')
