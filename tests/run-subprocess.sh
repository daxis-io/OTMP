#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cargo build --manifest-path "$repo_root/Cargo.toml" -p otmp-cli
otmp_bin="$repo_root/target/debug/otmp"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/otmp-crash.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT

for failpoint in \
  after_staging_flush \
  during_temporary_head_creation \
  after_immutable_uploads \
  after_final_head_rename
do
  case_root="$scratch/$failpoint"
  mkdir -p "$case_root"
  schema="$case_root/schema.json"
  source="$case_root/source.parquet"
  table="$case_root/table"
  manifest="$case_root/manifest.json"

  printf '%s' '{"schema_id":1,"fields":[{"field_id":1,"name":"id","required":true,"type":{"type":"int64"}}],"identifier_field_ids":[1]}' >"$schema"
  printf '%s' 'PAR1crash-fixturePAR1' >"$source"
  "$otmp_bin" init "$table" --schema "$schema" >/dev/null
  fingerprint="$($otmp_bin inspect-file "$source")"
  sha256="$(printf '%s' "$fingerprint" | sed -E 's/.*"sha256":"([^"]+)".*/\1/')"
  length="$(printf '%s' "$fingerprint" | sed -E 's/.*"length":([0-9]+).*/\1/')"
  printf '{"idempotency_key":"crash-%s","files":[{"source":"%s","sha256":"%s","length":%s,"record_count":1,"schema_id":1}]}' \
    "$failpoint" "$source" "$sha256" "$length" >"$manifest"

  set +e
  OTMP_FAILPOINT="$failpoint" "$otmp_bin" append "$table" --manifest "$manifest" >/dev/null 2>&1
  append_status=$?
  set -e
  if [[ $append_status -ne 86 ]]; then
    echo "failpoint $failpoint exited $append_status, expected 86" >&2
    exit 1
  fi

  "$otmp_bin" verify "$table" >/dev/null
  status="$($otmp_bin status "$table")"
  if [[ "$failpoint" == after_final_head_rename ]]; then
    expected_version=1
  else
    expected_version=0
  fi
  actual_version="$(printf '%s' "$status" | sed -E 's/.*"table_version":([0-9]+).*/\1/')"
  if [[ "$actual_version" != "$expected_version" ]]; then
    echo "failpoint $failpoint reopened at version $actual_version, expected $expected_version" >&2
    exit 1
  fi

  checkpoint_path="$(find "$table/_otmp/checkpoints/$expected_version" -type f -name '*.sqlite3' -print -quit)"
  if [[ -z "$checkpoint_path" ]]; then
    echo "failpoint $failpoint has no published checkpoint for version $expected_version" >&2
    exit 1
  fi
  integrity="$(sqlite3 "$checkpoint_path" 'PRAGMA integrity_check;')"
  if [[ "$integrity" != "ok" ]]; then
    echo "failpoint $failpoint checkpoint failed upstream sqlite3 integrity_check: $integrity" >&2
    exit 1
  fi
  if find "$(dirname "$checkpoint_path")" -type f \( -name '*-wal' -o -name '*-shm' -o -name '*-journal' \) -print -quit | grep -q .; then
    echo "failpoint $failpoint checkpoint has a SQLite sidecar" >&2
    exit 1
  fi
done

echo "process-crash reopen and upstream sqlite3 evidence passed for 4 failpoints"
