#!/usr/bin/env python3
"""Paired regression matrix using the existing reader-scale capture contract."""
import argparse
import importlib.util
import json
import pathlib
import random
import subprocess

spec = importlib.util.spec_from_file_location(
    'reader_scale', pathlib.Path(__file__).resolve().parents[1] / 'reader-scale' / 'run.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=pathlib.Path, required=True)
    parser.add_argument('--candidate', type=pathlib.Path, required=True)
    parser.add_argument('--fixtures', type=pathlib.Path, required=True)
    parser.add_argument('--out', type=pathlib.Path, required=True)
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    cases = []
    for files in (4096, 16384):
        for survivors in (2, None):
            cases.append((f'local-{files}-{survivors or "all"}', f'growth-{files}',
                          dict(survivors=survivors, passes=2), 20))
    for files, survivors in ((16, 2), (1024, 2), (256, None)):
        cases.append((f'delay-{files}-{survivors or "all"}', f'growth-{files}',
                      dict(survivors=survivors, passes=2, delay_ms=10), 10))
    for fixture in sorted({case[1] for case in cases}):
        with (args.out / f'verify-before-{fixture}.json').open('wb') as output:
            subprocess.run([str(args.candidate), 'verify', str(args.fixtures / fixture)],
                           stdout=output, check=True)
    summaries = {}
    for name, fixture, config, samples in cases:
        config_path = args.out / f'{name}.json'
        runner.write_json(config_path, config)
        orders = [('baseline', 'candidate')] * (samples // 2)
        orders += [('candidate', 'baseline')] * (samples // 2)
        random.Random(74415).shuffle(orders)
        runner.write_json(args.out / f'{name}-order.json', orders)
        for number, order in enumerate(orders, 1):
            for label in order:
                destination = args.out / name / label / f'round-{number:04d}'
                result = runner.run_samples(getattr(args, label), args.fixtures / fixture,
                                           config_path, destination, 1, 300)
                print(name, number, label, result['samples'], flush=True)
        for label in ('baseline', 'candidate'):
            summary = runner.build_summary((args.out / name / label).glob('round-*/sample-*'))
            runner.write_json(args.out / name / label / 'summary.json', summary)
            summaries[f'{name}/{label}'] = summary
        runner.write_json(args.out / 'results.json', summaries)
    for fixture in sorted({case[1] for case in cases}):
        with (args.out / f'verify-after-{fixture}.json').open('wb') as output:
            subprocess.run([str(args.candidate), 'verify', str(args.fixtures / fixture)],
                           stdout=output, check=True)
    failures = sum(result['samples']['failed'] for result in summaries.values())
    if failures:
        raise SystemExit(f'{failures} failed samples retained; inspect results.json')


if __name__ == '__main__':
    main()
