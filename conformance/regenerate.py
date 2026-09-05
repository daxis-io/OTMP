#!/usr/bin/env python3
"""Rebuild full SQLite images from retained canonical commits and check identity.

UUIDs and commit times are fixture inputs. Regeneration introduces no new IDs,
clock reads, local source paths, cloud access, or migration behavior.
"""
import argparse
import pathlib
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--check', action='store_true', help='verify canonical regeneration')
parser.parse_args()
root = pathlib.Path(__file__).resolve().parent.parent
subprocess.run(['cargo', 'test', '-p', 'otmp', '--lib',
                'canonical_packages_regenerate_from_retained_commits'], cwd=root, check=True)
subprocess.run(['cargo', 'test', '-p', 'otmp-protocol', '--test',
                'conformance_fixtures'], cwd=root, check=True)
