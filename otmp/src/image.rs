use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use otmp_protocol::{
    COMMIT_MEDIA_TYPE, CanonicalValue, Field, Id, JsonU64, LogicalType, Schema, SemanticCommit,
    Sha256, TypedScalar, canonical_json, decode_partition_tuple, decode_typed_scalar,
    encode_partition_tuple, encode_typed_scalar, partition_hash,
};
use rusqlite::config::DbConfig;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};

use crate::{FileFormat, RuntimeError};

const SCHEMA_SQL: &str = include_str!("../../spec/OTMP-0.0.2-alpha-table-schema.sql");
pub(crate) const PAGE_SIZE: u32 = 4096;
pub(crate) const APPLICATION_ID: i64 = 0x4f54_4d50;
pub(crate) const USER_VERSION: i64 = 2;

pub(crate) struct CheckpointImage {
    _directory: tempfile::TempDir,
    pub path: PathBuf,
    pub bytes: Vec<u8>,
    pub page_count: u64,
}

pub(crate) struct MaterializedImage {
    _directory: tempfile::TempDir,
    pub path: PathBuf,
}

pub(crate) fn open_readonly(path: &Path) -> Result<Connection, RuntimeError> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX;
    let connection = Connection::open_with_flags(path, flags)?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_DEFENSIVE, true)?;
    connection.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF;")?;
    Ok(connection)
}

pub(crate) struct GenesisImage<'a> {
    pub table_id: Id,
    pub schema: &'a Schema,
    pub created_at_ms: i64,
    pub semantic_state: Sha256,
    pub commit_id: Id,
    pub commit_hash: Sha256,
    pub commit_uri: &'a str,
    pub operation_json: &'a str,
    pub result_json: &'a str,
    pub intent_hash: Sha256,
    pub metadata_json: &'a str,
    pub reader_features_json: &'a str,
    pub writer_features_json: &'a str,
}

#[derive(Clone)]
pub(crate) struct ImageMetric {
    pub field_id: u32,
    pub column_size_bytes: Option<u64>,
    pub value_count: Option<u64>,
    pub null_count: Option<u64>,
    pub nan_count: Option<u64>,
    pub distinct_count: Option<u64>,
    pub lower_bound_cbor: Option<Vec<u8>>,
    pub upper_bound_cbor: Option<Vec<u8>>,
    pub metadata_json: String,
}

#[derive(Clone)]
pub(crate) struct ImageFile {
    pub file_id: Id,
    pub uri: String,
    pub format: FileFormat,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub schema_id: u32,
    pub partition_spec_id: u32,
    pub sort_order_id: u32,
    pub partition_values_cbor: Vec<u8>,
    pub partition_hash: Sha256,
    pub content_sha256: Sha256,
    pub metrics: Vec<ImageMetric>,
    pub metadata_json: String,
}

pub(crate) struct AppendImage<'a> {
    pub table_version: u64,
    pub created_at_ms: i64,
    pub semantic_state: Sha256,
    pub commit_id: Id,
    pub commit_hash: Sha256,
    pub commit_uri: &'a str,
    pub operation_json: &'a str,
    pub result_json: &'a str,
    pub commit_metadata_json: &'a str,
    pub idempotency_key: &'a str,
    pub intent_hash: Sha256,
    pub snapshot_id: Id,
    pub parent_snapshot_id: Option<Id>,
    pub target_ref: &'a str,
    pub sequence_number: u64,
    pub summary: &'a BTreeMap<String, CanonicalValue>,
    pub snapshot_metadata_json: &'a str,
    pub files: &'a [ImageFile],
}

pub(crate) fn create_genesis(input: &GenesisImage<'_>) -> Result<CheckpointImage, RuntimeError> {
    input.schema.validate()?;
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("metadata.sqlite3");
    let mut connection = Connection::open(&path)?;
    connection.execute_batch(&format!(
        "PRAGMA page_size={PAGE_SIZE}; PRAGMA application_id={APPLICATION_ID}; PRAGMA user_version={USER_VERSION}; PRAGMA foreign_keys=ON; PRAGMA journal_mode=DELETE;"
    ))?;
    connection.execute_batch(SCHEMA_SQL)?;
    let transaction = connection.transaction()?;
    insert_schema(&transaction, input.schema, 0)?;
    transaction.execute(
        "INSERT INTO otmp_partition_specs(partition_spec_id, created_version) VALUES(0, 0)",
        [],
    )?;
    transaction.execute(
        "INSERT INTO otmp_sort_orders(sort_order_id, created_version) VALUES(0, 0)",
        [],
    )?;
    transaction.execute(
        "INSERT INTO otmp_refs(ref_name, ref_type, snapshot_id, created_version, updated_version) VALUES('main', 'branch', NULL, 0, 0)",
        [],
    )?;
    for feature in [
        "otmp.core.v2",
        "otmp.data.parquet.v1",
        "otmp.metadata.sqlite3-cow.v1",
    ] {
        transaction.execute(
            "INSERT INTO otmp_features(feature_name, requirement, enabled_version) VALUES(?1, 'both', 0)",
            [feature],
        )?;
    }
    transaction.execute(
        "INSERT INTO otmp_commits(table_version, commit_id, parent_table_version, created_at_ms, intent_count, semantic_state_sha256, commit_object_uri, commit_object_sha256, operation_summary_json, result_json, metadata_json) VALUES(0, ?1, NULL, ?2, 1, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            input.commit_id.as_bytes().as_slice(),
            input.created_at_ms,
            input.semantic_state.as_bytes().as_slice(),
            input.commit_uri,
            input.commit_hash.as_bytes().as_slice(),
            input.operation_json,
            input.result_json,
            input.metadata_json,
        ],
    )?;
    transaction.execute(
        "INSERT INTO otmp_idempotency(idempotency_key, intent_sha256, commit_id, table_version, result_json) VALUES('otmp.genesis', ?1, ?2, 0, ?3)",
        params![
            input.intent_hash.as_bytes().as_slice(),
            input.commit_id.as_bytes().as_slice(),
            input.result_json,
        ],
    )?;
    transaction.execute(
        "INSERT INTO otmp_meta(singleton, protocol, protocol_version, table_id, table_version, semantic_state_sha256, last_commit_id, last_commit_sha256, last_sequence_number, current_schema_id, default_partition_spec_id, default_sort_order_id, created_at_ms, required_reader_features_json, required_writer_features_json, metadata_json) VALUES(1, 'otmp', '0.0.2-alpha', ?1, 0, ?2, ?3, ?4, 0, ?5, 0, 0, ?6, ?7, ?8, ?9)",
        params![
            input.table_id.as_bytes().as_slice(),
            input.semantic_state.as_bytes().as_slice(),
            input.commit_id.as_bytes().as_slice(),
            input.commit_hash.as_bytes().as_slice(),
            i64::from(input.schema.schema_id),
            input.created_at_ms,
            input.reader_features_json,
            input.writer_features_json,
            input.metadata_json,
        ],
    )?;
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(connection);
    finish_checkpoint(directory, path)
}

#[allow(clippy::too_many_lines)]
pub(crate) fn apply_append(
    parent: &[u8],
    input: &AppendImage<'_>,
) -> Result<CheckpointImage, RuntimeError> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("metadata.sqlite3");
    fs::write(&path, parent)?;
    let mut connection = Connection::open(&path)?;
    connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=DELETE;")?;
    let transaction = connection.transaction()?;
    let parent_version: i64 = transaction.query_row(
        "SELECT table_version FROM otmp_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )?;
    if parent_version
        .checked_add(1)
        .and_then(|value| u64::try_from(value).ok())
        != Some(input.table_version)
    {
        return Err(RuntimeError::Corrupt(
            "candidate parent version mismatch".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO otmp_snapshots(snapshot_id, parent_snapshot_id, sequence_number, schema_id, partition_spec_id, sort_order_id, operation, committed_table_version, committed_at_ms, summary_json, metadata_json) VALUES(?1, ?2, ?3, ?4, 0, 0, 'append', ?5, ?6, ?7, ?8)",
        params![
            input.snapshot_id.as_bytes().as_slice(),
            input.parent_snapshot_id.map(|id| id.as_bytes().to_vec()),
            sqlite_i64(input.sequence_number, "sequence number")?,
            current_schema(&transaction)?,
            sqlite_i64(input.table_version, "table version")?,
            input.created_at_ms,
            canonical_string(input.summary)?,
            input.snapshot_metadata_json,
        ],
    )?;
    for (key, value) in input.summary {
        transaction.execute(
            "INSERT INTO otmp_snapshot_summary(snapshot_id, summary_key, value_json) VALUES(?1, ?2, ?3)",
            params![
                input.snapshot_id.as_bytes().as_slice(),
                key,
                canonical_string(value)?,
            ],
        )?;
    }
    for file in input.files {
        transaction.execute(
            "INSERT INTO otmp_files(file_id, file_kind, uri, object_identity, file_format, file_size_bytes, record_count, schema_id, partition_spec_id, sort_order_id, partition_values_cbor, partition_hash, content_sha256, data_sequence_number, file_sequence_number, created_snapshot_id, created_version, metadata_json) VALUES(?1, 'data', ?2, NULL, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13, ?14, ?15)",
            params![
                file.file_id.as_bytes().as_slice(),
                file.uri,
                file.format.as_str(),
                sqlite_i64(file.file_size_bytes, "file size")?,
                sqlite_i64(file.record_count, "record count")?,
                i64::from(file.schema_id),
                i64::from(file.partition_spec_id),
                i64::from(file.sort_order_id),
                file.partition_values_cbor,
                file.partition_hash.as_bytes().as_slice(),
                file.content_sha256.as_bytes().as_slice(),
                sqlite_i64(input.sequence_number, "sequence number")?,
                input.snapshot_id.as_bytes().as_slice(),
                sqlite_i64(input.table_version, "table version")?,
                file.metadata_json,
            ],
        )?;
        for metric in &file.metrics {
            transaction.execute(
                "INSERT INTO otmp_file_metrics(file_id, field_id, column_size_bytes, value_count, null_count, nan_count, distinct_count, lower_bound_cbor, upper_bound_cbor, metadata_json) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    file.file_id.as_bytes().as_slice(),
                    i64::from(metric.field_id),
                    optional_sqlite_i64(metric.column_size_bytes, "column size")?,
                    optional_sqlite_i64(metric.value_count, "value count")?,
                    optional_sqlite_i64(metric.null_count, "null count")?,
                    optional_sqlite_i64(metric.nan_count, "nan count")?,
                    optional_sqlite_i64(metric.distinct_count, "distinct count")?,
                    metric.lower_bound_cbor,
                    metric.upper_bound_cbor,
                    metric.metadata_json,
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO otmp_snapshot_file_changes(snapshot_id, file_id, change_kind) VALUES(?1, ?2, 'add')",
            params![
                input.snapshot_id.as_bytes().as_slice(),
                file.file_id.as_bytes().as_slice(),
            ],
        )?;
        transaction.execute(
            "INSERT INTO otmp_ref_live_files(ref_name, file_id, added_snapshot_id, data_sequence_number, file_sequence_number) VALUES(?4, ?1, ?2, ?3, ?3)",
            params![
                file.file_id.as_bytes().as_slice(),
                input.snapshot_id.as_bytes().as_slice(),
                sqlite_i64(input.sequence_number, "sequence number")?,
                input.target_ref,
            ],
        )?;
    }
    let updated_refs = transaction.execute(
        "UPDATE otmp_refs SET snapshot_id=?1, updated_version=?2 WHERE ref_name=?3 AND ref_type='branch'",
        params![
            input.snapshot_id.as_bytes().as_slice(),
            sqlite_i64(input.table_version, "table version")?,
            input.target_ref,
        ],
    )?;
    if updated_refs != 1 {
        return Err(RuntimeError::Corrupt(
            "candidate parent has no unique main branch".into(),
        ));
    }
    transaction.execute(
        "INSERT INTO otmp_commits(table_version, commit_id, parent_table_version, created_at_ms, intent_count, semantic_state_sha256, commit_object_uri, commit_object_sha256, operation_summary_json, result_json, metadata_json) VALUES(?1, ?2, ?3, ?4, 1, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            sqlite_i64(input.table_version, "table version")?,
            input.commit_id.as_bytes().as_slice(),
            sqlite_i64(input.table_version - 1, "parent table version")?,
            input.created_at_ms,
            input.semantic_state.as_bytes().as_slice(),
            input.commit_uri,
            input.commit_hash.as_bytes().as_slice(),
            input.operation_json,
            input.result_json,
            input.commit_metadata_json,
        ],
    )?;
    transaction.execute(
        "INSERT INTO otmp_idempotency(idempotency_key, intent_sha256, commit_id, table_version, result_json) VALUES(?1, ?2, ?3, ?4, ?5)",
        params![
            input.idempotency_key,
            input.intent_hash.as_bytes().as_slice(),
            input.commit_id.as_bytes().as_slice(),
            sqlite_i64(input.table_version, "table version")?,
            input.result_json,
        ],
    )?;
    transaction.execute(
        "UPDATE otmp_meta SET table_version=?1, semantic_state_sha256=?2, last_commit_id=?3, last_commit_sha256=?4, last_sequence_number=?5 WHERE singleton=1",
        params![
            sqlite_i64(input.table_version, "table version")?,
            input.semantic_state.as_bytes().as_slice(),
            input.commit_id.as_bytes().as_slice(),
            input.commit_hash.as_bytes().as_slice(),
            sqlite_i64(input.sequence_number, "sequence number")?,
        ],
    )?;
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(connection);
    finish_checkpoint(directory, path)
}

pub(crate) fn materialize(bytes: &[u8]) -> Result<MaterializedImage, RuntimeError> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("metadata.sqlite3");
    fs::write(&path, bytes)?;
    Ok(MaterializedImage {
        _directory: directory,
        path,
    })
}

pub(crate) struct ExpectedImage<'a> {
    pub table_id: Id,
    pub table_version: u64,
    pub semantic_state: Sha256,
    pub commit_id: Id,
    pub commit_hash: Sha256,
    pub commit_uri: &'a str,
    pub reader_features_json: &'a str,
    pub writer_features_json: &'a str,
    pub previous_semantic_state: Option<Sha256>,
}

#[allow(clippy::too_many_lines)]
pub(crate) fn validate(path: &Path, expected: &ExpectedImage<'_>) -> Result<(), RuntimeError> {
    let connection = open_readonly(path)?;
    let integrity: String = connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(RuntimeError::Corrupt(format!(
            "integrity_check: {integrity}"
        )));
    }
    let foreign_key_errors: i64 =
        connection.query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if foreign_key_errors != 0 {
        return Err(RuntimeError::Corrupt("foreign_key_check failed".into()));
    }
    for (pragma, expected_value) in [
        ("application_id", APPLICATION_ID),
        ("user_version", USER_VERSION),
        ("page_size", i64::from(PAGE_SIZE)),
    ] {
        let value: i64 = connection.query_row(&format!("PRAGMA {pragma}"), [], |row| row.get(0))?;
        if value != expected_value {
            return Err(RuntimeError::Corrupt(format!("invalid SQLite {pragma}")));
        }
    }
    let meta = connection.query_row(
        "SELECT protocol, protocol_version, table_id, table_version, semantic_state_sha256, last_commit_id, last_commit_sha256, last_sequence_number, current_schema_id, default_partition_spec_id, default_sort_order_id, required_reader_features_json, required_writer_features_json FROM otmp_meta WHERE singleton=1",
        [],
        |row| {
            Ok((
                row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, Vec<u8>>(2)?,
                row.get::<_, i64>(3)?, row.get::<_, Vec<u8>>(4)?, row.get::<_, Vec<u8>>(5)?,
                row.get::<_, Vec<u8>>(6)?, row.get::<_, i64>(7)?, row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?, row.get::<_, i64>(10)?, row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
            ))
        },
    )?;
    if meta.0 != "otmp"
        || meta.1 != "0.0.2-alpha"
        || meta.2 != expected.table_id.as_bytes()
        || u64::try_from(meta.3).ok() != Some(expected.table_version)
        || meta.4 != expected.semantic_state.as_bytes()
        || meta.5 != expected.commit_id.as_bytes()
        || meta.6 != expected.commit_hash.as_bytes()
        || meta.11 != expected.reader_features_json
        || meta.12 != expected.writer_features_json
    {
        return Err(RuntimeError::Corrupt(
            "otmp_meta does not match HEAD".into(),
        ));
    }
    let defaults_exist: i64 = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM otmp_schemas WHERE schema_id=?1) AND EXISTS(SELECT 1 FROM otmp_partition_specs WHERE partition_spec_id=?2) AND EXISTS(SELECT 1 FROM otmp_sort_orders WHERE sort_order_id=?3)",
        params![meta.8, meta.9, meta.10],
        |row| row.get(0),
    )?;
    if defaults_exist != 1 {
        return Err(RuntimeError::Corrupt(
            "metadata defaults do not exist".into(),
        ));
    }
    let commit_matches: i64 = connection.query_row(
        "SELECT count(*) FROM otmp_commits WHERE table_version=?1 AND commit_id=?2 AND semantic_state_sha256=?3 AND commit_object_uri=?4 AND commit_object_sha256=?5",
        params![
            sqlite_i64(expected.table_version, "table version")?,
            expected.commit_id.as_bytes().as_slice(),
            expected.semantic_state.as_bytes().as_slice(),
            expected.commit_uri,
            expected.commit_hash.as_bytes().as_slice(),
        ],
        |row| row.get(0),
    )?;
    if commit_matches != 1 {
        return Err(RuntimeError::Corrupt(
            "last commit row does not match commit object".into(),
        ));
    }
    let pairing_errors: i64 = connection.query_row(
        "SELECT count(*) FROM otmp_commits c WHERE c.intent_count != (SELECT count(*) FROM otmp_idempotency i WHERE i.commit_id=c.commit_id AND i.table_version=c.table_version)",
        [],
        |row| row.get(0),
    )?;
    if pairing_errors != 0 {
        return Err(RuntimeError::Corrupt(
            "commit/idempotency pairing mismatch".into(),
        ));
    }
    let (commit_count, minimum_version, maximum_version): (i64, Option<i64>, Option<i64>) =
        connection.query_row(
            "SELECT count(*), min(table_version), max(table_version) FROM otmp_commits",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
    let expected_commit_count = expected
        .table_version
        .checked_add(1)
        .and_then(|value| i64::try_from(value).ok());
    if Some(commit_count) != expected_commit_count
        || minimum_version != Some(0)
        || maximum_version.and_then(|value| u64::try_from(value).ok())
            != Some(expected.table_version)
    {
        return Err(RuntimeError::Corrupt(
            "relational commit history is not contiguous through HEAD".into(),
        ));
    }
    let previous_state = if expected.table_version == 0 {
        None
    } else {
        connection
            .query_row(
                "SELECT semantic_state_sha256 FROM otmp_commits WHERE table_version=?1",
                [sqlite_i64(
                    expected.table_version - 1,
                    "parent table version",
                )?],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()?
            .map(hash_from_blob)
            .transpose()?
    };
    if previous_state != expected.previous_semantic_state {
        return Err(RuntimeError::Corrupt(
            "previous semantic state does not match relational history".into(),
        ));
    }
    validate_relational_history(&connection, expected.table_version, meta.7)?;
    let missing_ref_snapshot: i64 = connection.query_row(
        "SELECT count(*) FROM otmp_refs r WHERE r.snapshot_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM otmp_snapshots s WHERE s.snapshot_id=r.snapshot_id)",
        [],
        |row| row.get(0),
    )?;
    if missing_ref_snapshot != 0 {
        return Err(RuntimeError::Corrupt(
            "ref references missing snapshot".into(),
        ));
    }
    let live_errors: i64 = connection.query_row(
        "SELECT count(*) FROM otmp_ref_live_files rf WHERE NOT EXISTS(SELECT 1 FROM otmp_files f WHERE f.file_id=rf.file_id) OR NOT EXISTS(SELECT 1 FROM otmp_snapshot_file_changes c WHERE c.snapshot_id=rf.added_snapshot_id AND c.file_id=rf.file_id AND c.change_kind='add')",
        [],
        |row| row.get(0),
    )?;
    if live_errors != 0 {
        return Err(RuntimeError::Corrupt(
            "live membership is inconsistent".into(),
        ));
    }
    let unexpected_profile_rows: i64 = connection.query_row(
        "SELECT (SELECT count(*) FROM otmp_snapshot_file_changes WHERE change_kind != 'add') + (SELECT count(*) FROM otmp_snapshots WHERE operation != 'append') + (SELECT count(*) FROM otmp_files WHERE file_kind != 'data' OR file_format != 'parquet' OR object_identity IS NOT NULL OR partition_spec_id != 0 OR sort_order_id != 0 OR content_sha256 IS NULL)",
        [],
        |row| row.get(0),
    )?;
    if unexpected_profile_rows != 0 {
        return Err(RuntimeError::Corrupt(
            "relational state uses features outside the local/full-image profile append profile"
                .into(),
        ));
    }
    let mut refs = connection.prepare("SELECT ref_name,ref_type,snapshot_id FROM otmp_refs")?;
    for row in refs.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<Vec<u8>>>(2)?,
        ))
    })? {
        let (name, kind, snapshot) = row?;
        let mut expected = BTreeSet::new();
        if kind == "branch" {
            for id in crate::runtime::history::ancestry(
                &connection,
                snapshot.map(id_from_blob).transpose()?,
            )? {
                let mut changes = connection.prepare("SELECT c.file_id,c.snapshot_id,s.sequence_number FROM otmp_snapshot_file_changes c JOIN otmp_snapshots s USING(snapshot_id) WHERE c.snapshot_id=?1 AND c.change_kind='add'")?;
                for row in changes.query_map([id.as_bytes().as_slice()], |r| {
                    Ok((
                        r.get::<_, Vec<u8>>(0)?,
                        r.get::<_, Vec<u8>>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(2)?,
                    ))
                })? {
                    expected.insert(row?);
                }
            }
        }
        let mut rows = connection.prepare("SELECT file_id,added_snapshot_id,data_sequence_number,file_sequence_number FROM otmp_ref_live_files WHERE ref_name=?1")?;
        let actual = rows
            .query_map([name], |r| {
                Ok((
                    r.get::<_, Vec<u8>>(0)?,
                    r.get::<_, Vec<u8>>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                ))
            })?
            .collect::<Result<BTreeSet<_>, _>>()?;
        if actual != expected {
            return Err(RuntimeError::Corrupt(
                "materialized membership differs from snapshot ancestry".into(),
            ));
        }
    }
    validate_schemas(&connection)?;
    validate_file_descriptors(&connection)?;
    let reader_feature_rows: Vec<String> = {
        let mut statement = connection.prepare(
            "SELECT feature_name FROM otmp_features WHERE requirement IN ('reader','both') ORDER BY feature_name",
        )?;
        statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    let writer_feature_rows: Vec<String> = {
        let mut statement = connection.prepare(
            "SELECT feature_name FROM otmp_features WHERE requirement IN ('writer','both') ORDER BY feature_name",
        )?;
        statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?
    };
    if canonical_string(&reader_feature_rows)? != expected.reader_features_json
        || canonical_string(&writer_feature_rows)? != expected.writer_features_json
    {
        return Err(RuntimeError::Corrupt(
            "feature rows do not match feature set".into(),
        ));
    }
    Ok(())
}

fn validate_relational_history(
    connection: &Connection,
    table_version: u64,
    last_sequence_number: i64,
) -> Result<(), RuntimeError> {
    let version = sqlite_i64(table_version, "table version")?;
    let invalid: i64 = connection.query_row("SELECT count(*) FROM otmp_snapshots WHERE committed_table_version < 1 OR committed_table_version > ?1 OR sequence_number < 1", [version], |r| r.get(0))?;
    let (count, distinct, maximum): (i64,i64,i64) = connection.query_row("SELECT count(*),count(DISTINCT sequence_number),coalesce(max(sequence_number),0) FROM otmp_snapshots", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
    let duplicates: i64 = connection.query_row("SELECT count(*) FROM (SELECT committed_table_version FROM otmp_snapshots GROUP BY committed_table_version HAVING count(*)>1)", [], |r| r.get(0))?;
    let ancestry: i64 = connection.query_row("SELECT count(*) FROM otmp_snapshots s LEFT JOIN otmp_snapshots p ON p.snapshot_id=s.parent_snapshot_id WHERE s.parent_snapshot_id IS NOT NULL AND (p.snapshot_id IS NULL OR p.sequence_number>=s.sequence_number OR p.committed_table_version>=s.committed_table_version)", [], |r| r.get(0))?;
    let refs: i64 = connection.query_row("SELECT count(*) FROM otmp_refs WHERE created_version>updated_version OR updated_version>?1 OR (ref_type='tag' AND snapshot_id IS NULL)", [version], |r| r.get(0))?;
    let main: bool = connection.query_row("SELECT EXISTS(SELECT 1 FROM otmp_refs WHERE ref_name='main' AND ref_type='branch' AND created_version=0)", [], |r| r.get(0))?;
    if invalid != 0
        || duplicates != 0
        || count != distinct
        || count != maximum
        || maximum != last_sequence_number
        || ancestry != 0
        || refs != 0
        || !main
    {
        return Err(RuntimeError::Corrupt(
            "snapshot, sequence, ancestry, or ref invariant violated".into(),
        ));
    }
    Ok(())
}

fn validate_file_descriptors(connection: &Connection) -> Result<(), RuntimeError> {
    let files = {
        let mut statement = connection.prepare(
            "SELECT uri, partition_spec_id, partition_values_cbor, partition_hash FROM otmp_files ORDER BY file_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                    row.get::<_, Vec<u8>>(3)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (uri, spec_id, tuple_cbor, stored_hash) in files {
        let _: otmp_protocol::RelativeUri = uri.parse()?;
        let tuple = decode_partition_tuple(&tuple_cbor)?;
        let spec_id = u32::try_from(spec_id)
            .map_err(|_| RuntimeError::Corrupt("invalid partition spec ID".into()))?;
        if spec_id != 0 || !tuple.is_empty() {
            return Err(RuntimeError::Corrupt(
                "local/full-image profile requires empty partition tuples for spec 0".into(),
            ));
        }
        if hash_from_blob(stored_hash)? != partition_hash(spec_id, &tuple_cbor) {
            return Err(RuntimeError::Corrupt("partition hash mismatch".into()));
        }
    }

    let metrics = {
        let mut statement = connection.prepare(
            "SELECT m.value_count, m.null_count, m.nan_count, m.lower_bound_cbor, m.upper_bound_cbor, fld.type_json FROM otmp_file_metrics m JOIN otmp_files f ON f.file_id=m.file_id LEFT JOIN otmp_fields fld ON fld.schema_id=f.schema_id AND fld.field_id=m.field_id ORDER BY m.file_id, m.field_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, Option<i64>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, Option<Vec<u8>>>(3)?,
                    row.get::<_, Option<Vec<u8>>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (value_count, null_count, nan_count, lower, upper, field_type) in metrics {
        let Some(field_type) = field_type else {
            return Err(RuntimeError::Corrupt(
                "metric field does not belong to the file schema".into(),
            ));
        };
        let field_type: LogicalType = canonical_json::from_slice_canonical(field_type.as_bytes())?;
        if null_count
            .zip(value_count)
            .is_some_and(|(nulls, values)| nulls > values)
            || nan_count.is_some() && !field_type.is_float()
        {
            return Err(RuntimeError::Corrupt(
                "invalid relational metric counts".into(),
            ));
        }
        let lower = lower.map(|bytes| decode_typed_scalar(&bytes)).transpose()?;
        let upper = upper.map(|bytes| decode_typed_scalar(&bytes)).transpose()?;
        for bound in [&lower, &upper].into_iter().flatten() {
            bound.validate()?;
            if !field_type.accepts(bound)
                || matches!(bound, TypedScalar::Null)
                || matches!(bound, TypedScalar::Float32(value) if value.is_nan())
                || matches!(bound, TypedScalar::Float64(value) if value.is_nan())
            {
                return Err(RuntimeError::Corrupt(
                    "invalid relational metric bound".into(),
                ));
            }
        }
        if let (Some(lower), Some(upper)) = (&lower, &upper)
            && lower
                .partial_cmp_same_type(upper)
                .is_some_and(std::cmp::Ordering::is_gt)
        {
            return Err(RuntimeError::Corrupt(
                "relational metric bounds are reversed".into(),
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
struct NormalizedField {
    parent: Option<u32>,
    ordinal: u32,
    field: Field,
}

fn validate_schemas(connection: &Connection) -> Result<(), RuntimeError> {
    let schemas = {
        let mut statement = connection.prepare(
            "SELECT schema_id, parent_schema_id, doc FROM otmp_schemas ORDER BY schema_id",
        )?;
        statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (schema_id, parent_schema_id, doc) in schemas {
        let schema_id = u32::try_from(schema_id)
            .map_err(|_| RuntimeError::Corrupt("invalid schema ID".into()))?;
        let rows = normalized_fields(connection, schema_id)?;
        let roots = rows
            .values()
            .filter(|row| row.parent.is_none())
            .cloned()
            .collect::<Vec<_>>();
        assert_contiguous(&roots)?;
        let mut root_fields = roots;
        root_fields.sort_by_key(|row| row.ordinal);
        for root in &root_fields {
            validate_normalized_children(&rows, &root.field)?;
        }
        let mut reachable = BTreeSet::new();
        for root in &root_fields {
            collect_field_ids(&root.field, &mut reachable);
        }
        if reachable.len() != rows.len()
            || rows.keys().any(|field_id| !reachable.contains(field_id))
        {
            return Err(RuntimeError::Corrupt(
                "normalized schema contains unreachable field rows".into(),
            ));
        }
        let identifiers = {
            let mut statement = connection.prepare(
                "SELECT field_id FROM otmp_identifier_fields WHERE schema_id=?1 ORDER BY ordinal",
            )?;
            statement
                .query_map([i64::from(schema_id)], |row| row.get::<_, i64>(0))?
                .map(|result| {
                    result.and_then(|value| {
                        u32::try_from(value).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                0,
                                rusqlite::types::Type::Integer,
                                Box::new(error),
                            )
                        })
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        Schema {
            schema_id,
            parent_schema_id: parent_schema_id
                .map(u32::try_from)
                .transpose()
                .map_err(|_| RuntimeError::Corrupt("invalid parent schema ID".into()))?,
            fields: root_fields.into_iter().map(|row| row.field).collect(),
            identifier_field_ids: identifiers,
            doc,
        }
        .validate()?;
    }
    Ok(())
}

fn collect_field_ids(field: &Field, output: &mut BTreeSet<u32>) {
    output.insert(field.field_id);
    match &field.field_type {
        LogicalType::Struct { fields } => {
            for child in fields {
                collect_field_ids(child, output);
            }
        }
        LogicalType::List { element } => collect_field_ids(element, output),
        LogicalType::Map { key, value } => {
            collect_field_ids(key, output);
            collect_field_ids(value, output);
        }
        _ => {}
    }
}

fn normalized_fields(
    connection: &Connection,
    schema_id: u32,
) -> Result<BTreeMap<u32, NormalizedField>, RuntimeError> {
    let mut statement = connection.prepare(
        "SELECT field_id, parent_field_id, name, ordinal, required, type_json, doc, initial_default_json, write_default_json FROM otmp_fields WHERE schema_id=?1 ORDER BY field_id",
    )?;
    let raw = statement
        .query_map([i64::from(schema_id)], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    raw.into_iter()
        .map(|row| {
            let field_id = u32::try_from(row.0)
                .map_err(|_| RuntimeError::Corrupt("invalid field ID".into()))?;
            Ok((
                field_id,
                NormalizedField {
                    parent: row
                        .1
                        .map(u32::try_from)
                        .transpose()
                        .map_err(|_| RuntimeError::Corrupt("invalid parent field ID".into()))?,
                    ordinal: u32::try_from(row.3)
                        .map_err(|_| RuntimeError::Corrupt("invalid field ordinal".into()))?,
                    field: Field {
                        field_id,
                        name: row.2,
                        required: row.4 == 1,
                        field_type: canonical_json::from_slice_canonical(row.5.as_bytes())?,
                        doc: row.6,
                        initial_default: parse_optional_scalar(row.7)?,
                        write_default: parse_optional_scalar(row.8)?,
                    },
                },
            ))
        })
        .collect()
}

fn validate_normalized_children(
    rows: &BTreeMap<u32, NormalizedField>,
    field: &Field,
) -> Result<(), RuntimeError> {
    let expected: Vec<&Field> = match &field.field_type {
        LogicalType::Struct { fields } => fields.iter().collect(),
        LogicalType::List { element } => vec![element],
        LogicalType::Map { key, value } => vec![key, value],
        _ => Vec::new(),
    };
    let mut actual = rows
        .values()
        .filter(|row| row.parent == Some(field.field_id))
        .collect::<Vec<_>>();
    assert_contiguous(&actual.iter().map(|row| (*row).clone()).collect::<Vec<_>>())?;
    actual.sort_by_key(|row| row.ordinal);
    if actual.len() != expected.len()
        || actual
            .iter()
            .zip(&expected)
            .any(|(actual, expected)| actual.field != **expected)
    {
        return Err(RuntimeError::Corrupt(
            "recursive type JSON disagrees with normalized field rows".into(),
        ));
    }
    for child in expected {
        validate_normalized_children(rows, child)?;
    }
    Ok(())
}

fn assert_contiguous(rows: &[NormalizedField]) -> Result<(), RuntimeError> {
    let ordinals = rows.iter().map(|row| row.ordinal).collect::<BTreeSet<_>>();
    if ordinals.len() != rows.len()
        || ordinals
            .iter()
            .copied()
            .ne(0..u32::try_from(rows.len()).unwrap_or(u32::MAX))
    {
        return Err(RuntimeError::Corrupt(
            "field ordinals are not unique and contiguous".into(),
        ));
    }
    Ok(())
}

fn parse_optional_scalar(value: Option<String>) -> Result<Option<TypedScalar>, RuntimeError> {
    value
        .map(|value| canonical_json::from_slice_canonical(value.as_bytes()).map_err(Into::into))
        .transpose()
}

pub(crate) fn idempotency(
    path: &Path,
    key: &str,
) -> Result<Option<(Sha256, String)>, RuntimeError> {
    let connection = open_readonly(path)?;
    connection
        .query_row(
            "SELECT intent_sha256, result_json FROM otmp_idempotency WHERE idempotency_key=?1",
            [key],
            |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?
        .map(|(hash, result)| {
            let hash: [u8; 32] = hash
                .try_into()
                .map_err(|_| RuntimeError::Corrupt("invalid idempotency hash".into()))?;
            Ok((Sha256::from_bytes(hash), result))
        })
        .transpose()
}

pub(crate) fn validate_commit_projection(
    path: &Path,
    commit: &SemanticCommit,
) -> Result<(), RuntimeError> {
    let connection = open_readonly(path)?;
    let row: (i64, String, String, String) = connection.query_row(
        "SELECT intent_count, operation_summary_json, result_json, metadata_json FROM otmp_commits WHERE table_version=?1 AND commit_id=?2",
        params![
            sqlite_i64(commit.table_version.0, "table version")?,
            commit.commit_id.as_bytes().as_slice(),
        ],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;
    if usize::try_from(row.0).ok() != Some(commit.intents.len())
        || row.1 != canonical_string(&commit.operations)?
        || row.3 != canonical_string(&commit.metadata)?
        || commit.intents.len() == 1 && row.2 != canonical_string(&commit.intents[0].result)?
    {
        return Err(RuntimeError::Corrupt(
            "semantic commit projection differs from relational commit row".into(),
        ));
    }
    validate_snapshot_projection(&connection, commit)?;
    validate_metadata_projection(&connection, commit)?;
    for intent in &commit.intents {
        let projected: Option<(Vec<u8>, Vec<u8>, i64, String)> = connection
            .query_row(
                "SELECT intent_sha256, commit_id, table_version, result_json FROM otmp_idempotency WHERE idempotency_key=?1",
                [&intent.key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()?;
        let Some(projected) = projected else {
            return Err(RuntimeError::Corrupt(
                "semantic intent has no idempotency row".into(),
            ));
        };
        if projected.0 != intent.intent_sha256.as_bytes()
            || projected.1 != commit.commit_id.as_bytes()
            || u64::try_from(projected.2).ok() != Some(commit.table_version.0)
            || projected.3 != canonical_string(&intent.result)?
        {
            return Err(RuntimeError::Corrupt(
                "semantic intent differs from idempotency row".into(),
            ));
        }
    }
    if let (Some(parent_version), Some(parent_reference)) =
        (commit.parent_table_version, &commit.parent_commit)
    {
        let (uri, hash): (String, Vec<u8>) = connection.query_row(
            "SELECT commit_object_uri, commit_object_sha256 FROM otmp_commits WHERE table_version=?1",
            [sqlite_i64(parent_version.0, "parent table version")?],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if uri != parent_reference.uri.as_str()
            || hash_from_blob(hash)? != parent_reference.sha256
            || parent_reference.media_type.as_deref() != Some(COMMIT_MEDIA_TYPE)
        {
            return Err(RuntimeError::Corrupt(
                "semantic parent commit differs from relational history".into(),
            ));
        }
    }
    Ok(())
}

fn validate_snapshot_projection(
    connection: &Connection,
    commit: &SemanticCommit,
) -> Result<(), RuntimeError> {
    let expected_snapshot_rows: i64 = connection.query_row(
        "SELECT count(*) FROM otmp_snapshots WHERE committed_table_version=?1",
        [sqlite_i64(commit.table_version.0, "table version")?],
        |row| row.get(0),
    )?;
    let mut operation_snapshot_ids = BTreeSet::new();
    for operation in &commit.operations {
        let CanonicalValue::Object(operation) = operation else {
            continue;
        };
        if operation.get("type") != Some(&CanonicalValue::String("commit_snapshot".into())) {
            continue;
        }
        let operation: ProjectedCommitSnapshot = canonical_json::from_slice_canonical(
            &canonical_json::to_vec(&CanonicalValue::Object(operation.clone()))?,
        )?;
        let snapshot_id = operation.snapshot.snapshot_id;
        if !operation_snapshot_ids.insert(snapshot_id) {
            return Err(RuntimeError::Corrupt(
                "semantic commit contains a duplicate snapshot identity".into(),
            ));
        }
        let projected: Option<ProjectedSnapshotRow> = connection
            .query_row(
                "SELECT parent_snapshot_id, sequence_number, schema_id, partition_spec_id, sort_order_id, operation, committed_at_ms, scan_root_uri, scan_root_sha256, summary_json, metadata_json FROM otmp_snapshots WHERE snapshot_id=?1 AND committed_table_version=?2",
                params![
                    snapshot_id.as_bytes().as_slice(),
                    sqlite_i64(commit.table_version.0, "table version")?,
                ],
                |row| {
                    Ok(ProjectedSnapshotRow {
                        parent_snapshot_id: row.get(0)?,
                        sequence_number: row.get(1)?,
                        schema_id: row.get(2)?,
                        partition_spec_id: row.get(3)?,
                        sort_order_id: row.get(4)?,
                        operation: row.get(5)?,
                        committed_at_ms: row.get(6)?,
                        scan_root_uri: row.get(7)?,
                        scan_root_sha256: row.get(8)?,
                        summary_json: row.get(9)?,
                        metadata_json: row.get(10)?,
                    })
                },
            )
            .optional()?;
        let Some(projected) = projected else {
            return Err(RuntimeError::Corrupt(
                "semantic snapshot has no relational snapshot row".into(),
            ));
        };
        validate_projected_snapshot_row(commit, &operation, projected)?;
        validate_projected_snapshot_summary(connection, &operation.snapshot)?;
        validate_snapshot_changes(connection, commit, &operation)?;
        validate_projected_ref(connection, commit, &operation)?;
    }
    if usize::try_from(expected_snapshot_rows).ok() != Some(operation_snapshot_ids.len()) {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot operations differ from relational snapshot rows".into(),
        ));
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedCommitSnapshot {
    #[serde(rename = "operation_id")]
    _operation_id: String,
    #[serde(rename = "type")]
    _operation_type: String,
    target_ref: String,
    snapshot: ProjectedSemanticSnapshot,
    added_files: Vec<ProjectedSemanticFile>,
    removed_file_ids: Vec<Id>,
    scan_projection: CanonicalValue,
    #[serde(rename = "rebase_mode")]
    _rebase_mode: String,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedSemanticSnapshot {
    snapshot_id: Id,
    parent_snapshot_id: Option<Id>,
    sequence_number: JsonU64,
    schema_id: JsonU64,
    partition_spec_id: JsonU64,
    sort_order_id: JsonU64,
    operation: String,
    summary: CanonicalValue,
    metadata: CanonicalValue,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedSemanticFile {
    file_id: Id,
    uri: String,
    object_identity: Option<String>,
    file_format: String,
    file_size_bytes: JsonU64,
    record_count: JsonU64,
    schema_id: JsonU64,
    partition_spec_id: JsonU64,
    sort_order_id: JsonU64,
    content_sha256: Sha256,
    partition_values: BTreeMap<u32, TypedScalar>,
    metrics: Vec<ProjectedSemanticMetric>,
    metadata: CanonicalValue,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectedSemanticMetric {
    field_id: JsonU64,
    column_size_bytes: Option<JsonU64>,
    value_count: Option<JsonU64>,
    null_count: Option<JsonU64>,
    nan_count: Option<JsonU64>,
    distinct_count: Option<JsonU64>,
    lower_bound: Option<TypedScalar>,
    upper_bound: Option<TypedScalar>,
    metadata: CanonicalValue,
}

struct ProjectedSnapshotRow {
    parent_snapshot_id: Option<Vec<u8>>,
    sequence_number: i64,
    schema_id: i64,
    partition_spec_id: i64,
    sort_order_id: i64,
    operation: String,
    committed_at_ms: i64,
    scan_root_uri: Option<String>,
    scan_root_sha256: Option<Vec<u8>>,
    summary_json: String,
    metadata_json: String,
}

fn validate_projected_snapshot_row(
    commit: &SemanticCommit,
    operation: &ProjectedCommitSnapshot,
    projected: ProjectedSnapshotRow,
) -> Result<(), RuntimeError> {
    if projected.parent_snapshot_id.map(id_from_blob).transpose()?
        != operation.snapshot.parent_snapshot_id
        || u64::try_from(projected.sequence_number).ok()
            != Some(operation.snapshot.sequence_number.0)
        || u64::try_from(projected.schema_id).ok() != Some(operation.snapshot.schema_id.0)
        || u64::try_from(projected.partition_spec_id).ok()
            != Some(operation.snapshot.partition_spec_id.0)
        || u64::try_from(projected.sort_order_id).ok() != Some(operation.snapshot.sort_order_id.0)
        || projected.operation != operation.snapshot.operation
        || projected.committed_at_ms != commit.created_at_ms.0
        || projected.scan_root_uri.is_some()
        || projected.scan_root_sha256.is_some()
        || !matches!(operation.scan_projection, CanonicalValue::Null)
        || projected.summary_json != canonical_string(&operation.snapshot.summary)?
    {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot differs from relational snapshot row".into(),
        ));
    }
    if projected.metadata_json != canonical_string(&operation.snapshot.metadata)? {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot metadata differs from relational snapshot metadata".into(),
        ));
    }
    Ok(())
}

fn validate_projected_ref(
    connection: &Connection,
    commit: &SemanticCommit,
    operation: &ProjectedCommitSnapshot,
) -> Result<(), RuntimeError> {
    let projected: Option<(Option<Vec<u8>>, i64)> = connection
        .query_row(
            "SELECT snapshot_id, updated_version FROM otmp_refs WHERE ref_name=?1",
            [&operation.target_ref],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let projected = projected
        .map(|(id, version)| Ok::<_, RuntimeError>((id.map(id_from_blob).transpose()?, version)))
        .transpose()?;
    let expected = Some((
        Some(operation.snapshot.snapshot_id),
        sqlite_i64(commit.table_version.0, "table version")?,
    ));
    if projected != expected {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot target differs from relational ref".into(),
        ));
    }
    Ok(())
}

fn validate_projected_snapshot_summary(
    connection: &Connection,
    snapshot: &ProjectedSemanticSnapshot,
) -> Result<(), RuntimeError> {
    let projected = {
        let mut statement = connection.prepare(
            "SELECT summary_key, value_json FROM otmp_snapshot_summary WHERE snapshot_id=?1 ORDER BY summary_key",
        )?;
        statement
            .query_map([snapshot.snapshot_id.as_bytes().as_slice()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?
    };
    let CanonicalValue::Object(summary) = &snapshot.summary else {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot summary is not an object".into(),
        ));
    };
    let expected = summary
        .iter()
        .map(|(key, value)| Ok((key.clone(), canonical_string(value)?)))
        .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?;
    if projected != expected {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot summary differs from relational summary rows".into(),
        ));
    }
    Ok(())
}

fn validate_snapshot_changes(
    connection: &Connection,
    commit: &SemanticCommit,
    operation: &ProjectedCommitSnapshot,
) -> Result<(), RuntimeError> {
    let changes = {
        let mut statement = connection.prepare(
            "SELECT file_id, change_kind FROM otmp_snapshot_file_changes WHERE snapshot_id=?1 ORDER BY file_id, change_kind",
        )?;
        statement
            .query_map(
                [operation.snapshot.snapshot_id.as_bytes().as_slice()],
                |row| Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<Result<Vec<_>, _>>()?
    };
    let projected_changes = changes
        .into_iter()
        .map(|(id, kind)| Ok((id_from_blob(id)?, kind)))
        .collect::<Result<BTreeSet<_>, RuntimeError>>()?;
    let semantic_changes = operation
        .added_files
        .iter()
        .map(|file| (file.file_id, "add".to_owned()))
        .chain(
            operation
                .removed_file_ids
                .iter()
                .copied()
                .map(|file_id| (file_id, "remove".to_owned())),
        )
        .collect::<BTreeSet<_>>();
    if projected_changes != semantic_changes {
        return Err(RuntimeError::Corrupt(
            "semantic snapshot file changes differ from relational changes".into(),
        ));
    }
    let projected_created_files = {
        let mut statement = connection.prepare(
            "SELECT file_id FROM otmp_files WHERE created_snapshot_id=?1 OR created_version=?2 ORDER BY file_id",
        )?;
        statement
            .query_map(
                params![
                    operation.snapshot.snapshot_id.as_bytes().as_slice(),
                    sqlite_i64(commit.table_version.0, "table version")?,
                ],
                |row| row.get::<_, Vec<u8>>(0),
            )?
            .map(|row| id_from_blob(row?))
            .collect::<Result<BTreeSet<_>, RuntimeError>>()?
    };
    let semantic_added_files = operation
        .added_files
        .iter()
        .map(|file| file.file_id)
        .collect::<BTreeSet<_>>();
    if projected_created_files != semantic_added_files {
        return Err(RuntimeError::Corrupt(
            "semantic added files differ from relational files created by the commit".into(),
        ));
    }
    for file in &operation.added_files {
        validate_projected_file(
            connection,
            commit,
            &operation.snapshot,
            file,
            &operation.target_ref,
        )?;
    }
    Ok(())
}

fn validate_projected_file(
    connection: &Connection,
    commit: &SemanticCommit,
    snapshot: &ProjectedSemanticSnapshot,
    file: &ProjectedSemanticFile,
    target_ref: &str,
) -> Result<(), RuntimeError> {
    let projected: Option<ProjectedFileRow> = connection
        .query_row(
            "SELECT file_kind, uri, object_identity, file_format, file_size_bytes, record_count, schema_id, partition_spec_id, sort_order_id, partition_values_cbor, partition_hash, content_sha256, encryption_metadata, data_sequence_number, file_sequence_number, created_snapshot_id, created_version, metadata_json FROM otmp_files WHERE file_id=?1",
            [file.file_id.as_bytes().as_slice()],
            |row| {
                Ok(ProjectedFileRow {
                    file_kind: row.get(0)?,
                    uri: row.get(1)?,
                    object_identity: row.get(2)?,
                    file_format: row.get(3)?,
                    file_size_bytes: row.get(4)?,
                    record_count: row.get(5)?,
                    schema_id: row.get(6)?,
                    partition_spec_id: row.get(7)?,
                    sort_order_id: row.get(8)?,
                    partition_values_cbor: row.get(9)?,
                    partition_hash: row.get(10)?,
                    content_sha256: row.get(11)?,
                    encryption_metadata: row.get(12)?,
                    data_sequence_number: row.get(13)?,
                    file_sequence_number: row.get(14)?,
                    created_snapshot_id: row.get(15)?,
                    created_version: row.get(16)?,
                    metadata_json: row.get(17)?,
                })
            },
        )
        .optional()?;
    let Some(projected) = projected else {
        return Err(RuntimeError::Corrupt(
            "semantic added file has no relational file row".into(),
        ));
    };
    let partition_values_cbor = encode_partition_tuple(&file.partition_values);
    let partition_spec_id = u32::try_from(file.partition_spec_id.0).map_err(|_| {
        RuntimeError::Corrupt("semantic partition spec ID exceeds the u32 range".into())
    })?;
    if projected.file_kind != "data"
        || projected.uri != file.uri
        || projected.object_identity != file.object_identity
        || projected.file_format != file.file_format
        || u64::try_from(projected.file_size_bytes).ok() != Some(file.file_size_bytes.0)
        || u64::try_from(projected.record_count).ok() != Some(file.record_count.0)
        || u64::try_from(projected.schema_id).ok() != Some(file.schema_id.0)
        || u64::try_from(projected.partition_spec_id).ok() != Some(file.partition_spec_id.0)
        || projected
            .sort_order_id
            .and_then(|value| u64::try_from(value).ok())
            != Some(file.sort_order_id.0)
        || projected.partition_values_cbor != partition_values_cbor
        || hash_from_blob(projected.partition_hash)?
            != partition_hash(partition_spec_id, &partition_values_cbor)
        || projected.content_sha256.map(hash_from_blob).transpose()? != Some(file.content_sha256)
        || projected.encryption_metadata.is_some()
        || u64::try_from(projected.data_sequence_number).ok() != Some(snapshot.sequence_number.0)
        || u64::try_from(projected.file_sequence_number).ok() != Some(snapshot.sequence_number.0)
        || id_from_blob(projected.created_snapshot_id)? != snapshot.snapshot_id
        || u64::try_from(projected.created_version).ok() != Some(commit.table_version.0)
        || projected.metadata_json != canonical_string(&file.metadata)?
    {
        return Err(RuntimeError::Corrupt(
            "semantic added file differs from relational file descriptor".into(),
        ));
    }
    let live_projection: Option<(Vec<u8>, i64, i64)> = connection
        .query_row(
            "SELECT added_snapshot_id, data_sequence_number, file_sequence_number FROM otmp_ref_live_files WHERE ref_name=?2 AND file_id=?1",
            params![file.file_id.as_bytes().as_slice(),target_ref],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    if live_projection
        .map(|(snapshot_id, data_sequence, file_sequence)| {
            Ok::<_, RuntimeError>((id_from_blob(snapshot_id)?, data_sequence, file_sequence))
        })
        .transpose()?
        != Some((
            snapshot.snapshot_id,
            projected.data_sequence_number,
            projected.file_sequence_number,
        ))
    {
        return Err(RuntimeError::Corrupt(
            "semantic added file differs from relational live-file projection".into(),
        ));
    }
    validate_projected_metrics(connection, file)
}

struct ProjectedFileRow {
    file_kind: String,
    uri: String,
    object_identity: Option<String>,
    file_format: String,
    file_size_bytes: i64,
    record_count: i64,
    schema_id: i64,
    partition_spec_id: i64,
    sort_order_id: Option<i64>,
    partition_values_cbor: Vec<u8>,
    partition_hash: Vec<u8>,
    content_sha256: Option<Vec<u8>>,
    encryption_metadata: Option<Vec<u8>>,
    data_sequence_number: i64,
    file_sequence_number: i64,
    created_snapshot_id: Vec<u8>,
    created_version: i64,
    metadata_json: String,
}

fn validate_projected_metrics(
    connection: &Connection,
    file: &ProjectedSemanticFile,
) -> Result<(), RuntimeError> {
    let projected = {
        let mut statement = connection.prepare(
            "SELECT field_id, column_size_bytes, value_count, null_count, nan_count, distinct_count, lower_bound_cbor, upper_bound_cbor, bloom_filter_uri, bloom_filter_sha256, metadata_json FROM otmp_file_metrics WHERE file_id=?1 ORDER BY field_id",
        )?;
        statement
            .query_map([file.file_id.as_bytes().as_slice()], |row| {
                Ok(ProjectedMetricRow {
                    field_id: row.get(0)?,
                    column_size_bytes: row.get(1)?,
                    value_count: row.get(2)?,
                    null_count: row.get(3)?,
                    nan_count: row.get(4)?,
                    distinct_count: row.get(5)?,
                    lower_bound_cbor: row.get(6)?,
                    upper_bound_cbor: row.get(7)?,
                    bloom_filter_uri: row.get(8)?,
                    bloom_filter_sha256: row.get(9)?,
                    metadata_json: row.get(10)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    let mut semantic_metrics = file.metrics.iter().collect::<Vec<_>>();
    semantic_metrics.sort_by_key(|metric| metric.field_id.0);
    if projected.len() != semantic_metrics.len() {
        return Err(RuntimeError::Corrupt(
            "semantic file metrics differ from relational metrics".into(),
        ));
    }
    for (projected, metric) in projected.iter().zip(semantic_metrics) {
        if u64::try_from(projected.field_id).ok() != Some(metric.field_id.0)
            || projected_u64(projected.column_size_bytes) != metric.column_size_bytes
            || projected_u64(projected.value_count) != metric.value_count
            || projected_u64(projected.null_count) != metric.null_count
            || projected_u64(projected.nan_count) != metric.nan_count
            || projected_u64(projected.distinct_count) != metric.distinct_count
            || projected.lower_bound_cbor != metric.lower_bound.as_ref().map(encode_typed_scalar)
            || projected.upper_bound_cbor != metric.upper_bound.as_ref().map(encode_typed_scalar)
            || projected.bloom_filter_uri.is_some()
            || projected.bloom_filter_sha256.is_some()
            || projected.metadata_json != canonical_string(&metric.metadata)?
        {
            return Err(RuntimeError::Corrupt(
                "semantic file metrics differ from relational metrics".into(),
            ));
        }
    }
    Ok(())
}

fn projected_u64(value: Option<i64>) -> Option<JsonU64> {
    value
        .and_then(|value| u64::try_from(value).ok())
        .map(JsonU64)
}

struct ProjectedMetricRow {
    field_id: i64,
    column_size_bytes: Option<i64>,
    value_count: Option<i64>,
    null_count: Option<i64>,
    nan_count: Option<i64>,
    distinct_count: Option<i64>,
    lower_bound_cbor: Option<Vec<u8>>,
    upper_bound_cbor: Option<Vec<u8>>,
    bloom_filter_uri: Option<String>,
    bloom_filter_sha256: Option<Vec<u8>>,
    metadata_json: String,
}

pub(crate) fn current_schema_and_snapshot(
    path: &Path,
) -> Result<(u32, Option<Id>, u64), RuntimeError> {
    let connection = open_readonly(path)?;
    let (schema, snapshot, sequence): (i64, Option<Vec<u8>>, i64) = connection.query_row(
        "SELECT m.current_schema_id, r.snapshot_id, m.last_sequence_number FROM otmp_meta m JOIN otmp_refs r ON r.ref_name='main' WHERE m.singleton=1",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )?;
    Ok((
        u32::try_from(schema).map_err(|_| RuntimeError::Corrupt("invalid schema ID".into()))?,
        snapshot.map(id_from_blob).transpose()?,
        u64::try_from(sequence).map_err(|_| RuntimeError::Corrupt("invalid sequence".into()))?,
    ))
}

pub(crate) fn field_types(
    path: &Path,
    schema_id: u32,
) -> Result<BTreeMap<u32, LogicalType>, RuntimeError> {
    let connection = open_readonly(path)?;
    let mut statement = connection.prepare(
        "SELECT field_id, type_json FROM otmp_fields WHERE schema_id=?1 ORDER BY field_id",
    )?;
    let rows = statement.query_map([i64::from(schema_id)], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut output = BTreeMap::new();
    for row in rows {
        let (id, json) = row?;
        let field_type = canonical_json::from_slice_canonical(json.as_bytes())?;
        output.insert(
            u32::try_from(id).map_err(|_| RuntimeError::Corrupt("invalid field ID".into()))?,
            field_type,
        );
    }
    Ok(output)
}

fn finish_checkpoint(
    directory: tempfile::TempDir,
    path: PathBuf,
) -> Result<CheckpointImage, RuntimeError> {
    let connection = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let page_count: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    drop(connection);
    let bytes = fs::read(&path)?;
    if bytes.len() % PAGE_SIZE as usize != 0 {
        return Err(RuntimeError::Corrupt(
            "checkpoint is not page aligned".into(),
        ));
    }
    Ok(CheckpointImage {
        _directory: directory,
        path,
        bytes,
        page_count: u64::try_from(page_count)
            .map_err(|_| RuntimeError::Corrupt("invalid page count".into()))?,
    })
}

pub(crate) fn insert_schema(
    transaction: &Transaction<'_>,
    schema: &Schema,
    version: u64,
) -> Result<(), RuntimeError> {
    transaction.execute(
        "INSERT INTO otmp_schemas(schema_id, parent_schema_id, created_version, doc) VALUES(?1, ?2, ?3, ?4)",
        params![
            i64::from(schema.schema_id),
            schema.parent_schema_id.map(i64::from),
            sqlite_i64(version, "schema version")?,
            schema.doc,
        ],
    )?;
    insert_fields(transaction, schema.schema_id, None, &schema.fields, version)?;
    for (ordinal, field_id) in schema.identifier_field_ids.iter().enumerate() {
        transaction.execute(
            "INSERT INTO otmp_identifier_fields(schema_id, ordinal, field_id) VALUES(?1, ?2, ?3)",
            params![
                i64::from(schema.schema_id),
                i64::try_from(ordinal).map_err(|_| {
                    RuntimeError::InvalidAppend("identifier ordinal overflow".into())
                })?,
                i64::from(*field_id)
            ],
        )?;
    }
    Ok(())
}

fn insert_fields(
    transaction: &Transaction<'_>,
    schema_id: u32,
    parent: Option<u32>,
    fields: &[Field],
    version: u64,
) -> Result<(), RuntimeError> {
    for (ordinal, field) in fields.iter().enumerate() {
        transaction.execute(
            "INSERT OR IGNORE INTO otmp_field_ids(field_id, first_schema_id, created_version) VALUES(?1, ?2, ?3)",
            params![i64::from(field.field_id), i64::from(schema_id), sqlite_i64(version, "field version")?],
        )?;
        transaction.execute(
            "INSERT INTO otmp_fields(schema_id, field_id, parent_field_id, name, ordinal, required, type_json, doc, initial_default_json, write_default_json) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                i64::from(schema_id), i64::from(field.field_id), parent.map(i64::from), field.name,
                i64::try_from(ordinal)
                    .map_err(|_| RuntimeError::InvalidAppend("field ordinal overflow".into()))?,
                i64::from(field.required), canonical_string(&field.field_type)?, field.doc,
                optional_canonical(field.initial_default.as_ref())?, optional_canonical(field.write_default.as_ref())?,
            ],
        )?;
        match &field.field_type {
            LogicalType::Struct { fields } => {
                insert_fields(
                    transaction,
                    schema_id,
                    Some(field.field_id),
                    fields,
                    version,
                )?;
            }
            LogicalType::List { element } => insert_fields(
                transaction,
                schema_id,
                Some(field.field_id),
                std::slice::from_ref(element.as_ref()),
                version,
            )?,
            LogicalType::Map { key, value } => {
                insert_fields(
                    transaction,
                    schema_id,
                    Some(field.field_id),
                    std::slice::from_ref(key.as_ref()),
                    version,
                )?;
                insert_fields(
                    transaction,
                    schema_id,
                    Some(field.field_id),
                    std::slice::from_ref(value.as_ref()),
                    version,
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn current_schema(transaction: &Transaction<'_>) -> Result<i64, rusqlite::Error> {
    transaction.query_row(
        "SELECT current_schema_id FROM otmp_meta WHERE singleton=1",
        [],
        |row| row.get(0),
    )
}

fn canonical_string<T: serde::Serialize>(value: &T) -> Result<String, RuntimeError> {
    String::from_utf8(canonical_json::to_vec(value)?)
        .map_err(|error| RuntimeError::Corrupt(error.to_string()))
}

fn optional_canonical(value: Option<&TypedScalar>) -> Result<Option<String>, RuntimeError> {
    value.map(canonical_string).transpose()
}

fn sqlite_i64(value: u64, name: &str) -> Result<i64, RuntimeError> {
    i64::try_from(value)
        .map_err(|_| RuntimeError::InvalidAppend(format!("{name} exceeds SQLite INTEGER")))
}

fn optional_sqlite_i64(value: Option<u64>, name: &str) -> Result<Option<i64>, RuntimeError> {
    value.map(|value| sqlite_i64(value, name)).transpose()
}

fn id_from_blob(blob: Vec<u8>) -> Result<Id, RuntimeError> {
    let bytes: [u8; 16] = blob
        .try_into()
        .map_err(|_| RuntimeError::Corrupt("invalid ID blob".into()))?;
    Id::try_from_bytes(bytes).map_err(Into::into)
}

fn hash_from_blob(blob: Vec<u8>) -> Result<Sha256, RuntimeError> {
    let bytes: [u8; 32] = blob
        .try_into()
        .map_err(|_| RuntimeError::Corrupt("invalid SHA-256 blob".into()))?;
    Ok(Sha256::from_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use otmp_protocol::{Generation, Head};

    use super::*;

    fn schema() -> Schema {
        Schema {
            schema_id: 1,
            parent_schema_id: None,
            fields: vec![Field {
                field_id: 1,
                name: "id".into(),
                required: true,
                field_type: LogicalType::Int64,
                doc: None,
                initial_default: None,
                write_default: None,
            }],
            identifier_field_ids: vec![1],
            doc: None,
        }
    }

    fn genesis() -> (CheckpointImage, Id, Id, Sha256, Sha256) {
        let table_id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0a").unwrap();
        let commit_id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0b").unwrap();
        let semantic_state = Sha256::digest(b"state");
        let commit_hash = Sha256::digest(b"commit");
        let checkpoint = create_genesis(&GenesisImage {
            table_id,
            schema: &schema(),
            created_at_ms: 1,
            semantic_state,
            commit_id,
            commit_hash,
            commit_uri: "_otmp/commits/0/018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0b.json",
            operation_json: "[]",
            result_json: "{}",
            intent_hash: Sha256::digest(b"intent"),
            metadata_json: "{}",
            reader_features_json: "[\"otmp.core.v2\",\"otmp.data.parquet.v1\",\"otmp.metadata.sqlite3-cow.v1\"]",
            writer_features_json: "[\"otmp.core.v2\",\"otmp.data.parquet.v1\",\"otmp.metadata.sqlite3-cow.v1\"]",
        })
        .unwrap();
        (checkpoint, table_id, commit_id, semantic_state, commit_hash)
    }

    fn expected(
        table_id: Id,
        commit_id: Id,
        semantic_state: Sha256,
        commit_hash: Sha256,
    ) -> ExpectedImage<'static> {
        ExpectedImage {
            table_id,
            table_version: 0,
            semantic_state,
            commit_id,
            commit_hash,
            commit_uri: "_otmp/commits/0/018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0b.json",
            reader_features_json: "[\"otmp.core.v2\",\"otmp.data.parquet.v1\",\"otmp.metadata.sqlite3-cow.v1\"]",
            writer_features_json: "[\"otmp.core.v2\",\"otmp.data.parquet.v1\",\"otmp.metadata.sqlite3-cow.v1\"]",
            previous_semantic_state: None,
        }
    }

    fn static_append_projection() -> (MaterializedImage, SemanticCommit) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        (image, commit)
    }

    #[test]
    fn validation_rejects_foreign_key_damage() {
        let (checkpoint, table_id, commit_id, state, commit_hash) = genesis();
        let connection = Connection::open(&checkpoint.path).unwrap();
        connection
            .execute_batch("PRAGMA foreign_keys=OFF;")
            .unwrap();
        connection
            .execute(
                "INSERT INTO otmp_identifier_fields(schema_id, ordinal, field_id) VALUES(1, 1, 99)",
                [],
            )
            .unwrap();
        drop(connection);

        let error = validate(
            &checkpoint.path,
            &expected(table_id, commit_id, state, commit_hash),
        )
        .unwrap_err();
        assert!(error.to_string().contains("foreign_key_check"));
    }

    #[test]
    fn validation_rejects_normalized_child_rows_under_a_primitive() {
        let (checkpoint, table_id, commit_id, state, commit_hash) = genesis();
        let connection = Connection::open(&checkpoint.path).unwrap();
        connection
            .execute(
                "INSERT INTO otmp_field_ids(field_id, first_schema_id, created_version) VALUES(2, 1, 0)",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO otmp_fields(schema_id, field_id, parent_field_id, name, ordinal, required, type_json) VALUES(1, 2, 1, 'orphan', 0, 1, '{\"type\":\"int64\"}')",
                [],
            )
            .unwrap();
        drop(connection);

        let error = validate(
            &checkpoint.path,
            &expected(table_id, commit_id, state, commit_hash),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("normalized field rows"),
            "{error}"
        );
    }

    #[test]
    fn validation_rejects_a_future_snapshot_in_genesis() {
        let (checkpoint, table_id, commit_id, state, commit_hash) = genesis();
        let snapshot_id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0c").unwrap();
        let connection = Connection::open(&checkpoint.path).unwrap();
        connection
            .execute(
                "INSERT INTO otmp_snapshots(snapshot_id, parent_snapshot_id, sequence_number, schema_id, partition_spec_id, sort_order_id, operation, committed_table_version, committed_at_ms) VALUES(?1, NULL, 1, 1, 0, 0, 'append', 1, 2)",
                [snapshot_id.as_bytes().as_slice()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE otmp_refs SET snapshot_id=?1 WHERE ref_name='main'",
                [snapshot_id.as_bytes().as_slice()],
            )
            .unwrap();
        drop(connection);

        let error = validate(
            &checkpoint.path,
            &expected(table_id, commit_id, state, commit_hash),
        )
        .unwrap_err();
        assert!(error.to_string().contains("snapshot, sequence"), "{error}");
    }

    #[test]
    fn profile_history_rejects_ref_shape_and_sequence_allocator_divergence() {
        for mutation in [
            "UPDATE otmp_refs SET ref_type='tag' WHERE ref_name='main'",
            "UPDATE otmp_refs SET updated_version=2 WHERE ref_name='main'",
        ] {
            let (image, _) = static_append_projection();
            let connection = Connection::open(&image.path).unwrap();
            connection.execute(mutation, []).unwrap();
            let error = validate_relational_history(&connection, 1, 1).unwrap_err();
            assert!(error.to_string().contains("ref invariant"), "{error}");
        }

        let (image, _) = static_append_projection();
        let connection = Connection::open(&image.path).unwrap();
        for last_sequence in [0, 2] {
            let error = validate_relational_history(&connection, 1, last_sequence).unwrap_err();
            assert!(error.to_string().contains("sequence"), "{error}");
        }
    }

    #[test]
    fn profile_history_rejects_truncated_snapshot_ancestry() {
        let (image, _) = static_append_projection();
        let snapshot_id = Id::from_str("018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0e").unwrap();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute(
                "INSERT INTO otmp_snapshots(snapshot_id, parent_snapshot_id, sequence_number, schema_id, partition_spec_id, sort_order_id, operation, committed_table_version, committed_at_ms) VALUES(?1, ?1, 2, 1, 0, 0, 'append', 2, 2)",
                [snapshot_id.as_bytes().as_slice()],
            )
            .unwrap();
        connection
            .execute(
                "UPDATE otmp_refs SET snapshot_id=?1, updated_version=2 WHERE ref_name='main'",
                [snapshot_id.as_bytes().as_slice()],
            )
            .unwrap();

        let error = validate_relational_history(&connection, 2, 2).unwrap_err();
        assert!(error.to_string().contains("ancestry"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_a_parent_reference_that_disagrees_with_history() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        validate_commit_projection(&image.path, &commit).unwrap();

        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute(
                "UPDATE otmp_commits SET commit_object_sha256=zeroblob(32) WHERE table_version=0",
                [],
            )
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("parent commit"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_snapshot_metadata_divergence() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        let connection = Connection::open(&image.path).unwrap();
        validate_commit_projection(&image.path, &commit).unwrap();
        connection
            .execute("UPDATE otmp_snapshots SET metadata_json='{}'", [])
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("snapshot metadata"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_snapshot_from_a_different_table_version() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        let connection = Connection::open(&image.path).unwrap();
        let snapshot_id = id_from_blob(
            connection
                .query_row("SELECT snapshot_id FROM otmp_snapshots", [], |row| {
                    row.get(0)
                })
                .unwrap(),
        )
        .unwrap();
        connection
            .execute(
                "UPDATE otmp_snapshots SET committed_table_version=2 WHERE snapshot_id=?1",
                [snapshot_id.as_bytes().as_slice()],
            )
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("snapshot"), "{error}");
    }

    #[test]
    fn commit_projection_requires_one_operation_per_relational_snapshot() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        let connection = Connection::open(&image.path).unwrap();
        let extra_snapshot: Id = "018f31f4-2bbd-7e47-a8bd-e5c9b36d8b0d".parse().unwrap();
        connection
            .execute(
                "INSERT INTO otmp_snapshots(snapshot_id, parent_snapshot_id, sequence_number, schema_id, partition_spec_id, sort_order_id, operation, committed_table_version, committed_at_ms, summary_json, metadata_json) SELECT ?1, snapshot_id, 2, schema_id, partition_spec_id, sort_order_id, operation, committed_table_version, committed_at_ms, summary_json, metadata_json FROM otmp_snapshots WHERE committed_table_version=1",
                [extra_snapshot.as_bytes().as_slice()],
            )
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("snapshot operations"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_snapshot_field_divergence() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute(
                "UPDATE otmp_snapshots SET sequence_number=2 WHERE committed_table_version=1",
                [],
            )
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("snapshot"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_file_descriptor_divergence() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables/append");
        let head: Head =
            canonical_json::from_slice_canonical(&fs::read(root.join("_otmp/HEAD")).unwrap())
                .unwrap();
        let commit: SemanticCommit = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.semantic_commit.uri.as_str())).unwrap(),
        )
        .unwrap();
        let generation: Generation = canonical_json::from_slice_canonical(
            &fs::read(root.join(head.metadata_generation.uri.as_str())).unwrap(),
        )
        .unwrap();
        let image = materialize(
            &fs::read(root.join(generation.metadata_image.checkpoint.uri.as_str())).unwrap(),
        )
        .unwrap();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute("UPDATE otmp_files SET uri='data/other.parquet'", [])
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("file"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_normalized_summary_divergence() {
        let (image, commit) = static_append_projection();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute(
                "UPDATE otmp_snapshot_summary SET value_json='\"wrong\"' WHERE summary_key='added-data-files'",
                [],
            )
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("summary"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_snapshot_change_set_divergence() {
        let (image, commit) = static_append_projection();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute("DELETE FROM otmp_snapshot_file_changes", [])
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("file changes"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_content_hash_divergence() {
        let (image, commit) = static_append_projection();
        let connection = Connection::open(&image.path).unwrap();
        connection
            .execute("UPDATE otmp_files SET content_sha256=zeroblob(32)", [])
            .unwrap();
        drop(connection);

        let error = validate_commit_projection(&image.path, &commit).unwrap_err();
        assert!(error.to_string().contains("file descriptor"), "{error}");
    }

    #[test]
    fn commit_projection_rejects_ref_and_metric_divergence() {
        for mutation in [
            "UPDATE otmp_refs SET updated_version=2 WHERE ref_name='main'",
            "INSERT INTO otmp_file_metrics(file_id, field_id, metadata_json) SELECT file_id, 1, '{}' FROM otmp_files",
        ] {
            let (image, commit) = static_append_projection();
            let connection = Connection::open(&image.path).unwrap();
            connection.execute(mutation, []).unwrap();
            drop(connection);

            assert!(validate_commit_projection(&image.path, &commit).is_err());
        }
    }
}

pub(crate) fn apply_metadata(
    parent: &[u8],
    commit: &SemanticCommit,
    uri: &otmp_protocol::RelativeUri,
    operations: &[crate::OperationRequest],
) -> Result<CheckpointImage, RuntimeError> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("metadata.sqlite3");
    fs::write(&path, parent)?;
    let mut connection = Connection::open(&path)?;
    connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=DELETE;")?;
    let tx = connection.transaction()?;
    crate::runtime::transactions::apply_operations(&tx, operations, commit.table_version.0)?;
    let hash = otmp_protocol::object_hash(&canonical_json::to_vec(commit)?);
    let version = sqlite_i64(commit.table_version.0, "version")?;
    let intent = &commit.intents[0];
    let result = canonical_string(&intent.result)?;
    tx.execute("INSERT INTO otmp_commits(table_version,commit_id,parent_table_version,created_at_ms,intent_count,semantic_state_sha256,commit_object_uri,commit_object_sha256,operation_summary_json,result_json,metadata_json) VALUES(?1,?2,?3,?4,1,?5,?6,?7,?8,?9,?10)", params![version,commit.commit_id.as_bytes().as_slice(),version-1,commit.created_at_ms.0,commit.semantic_state_sha256.as_bytes().as_slice(),uri.as_str(),hash.as_bytes().as_slice(),canonical_string(&commit.operations)?,result,canonical_string(&commit.metadata)?])?;
    tx.execute(
        "INSERT INTO otmp_idempotency VALUES(?1,?2,?3,?4,?5)",
        params![
            intent.key,
            intent.intent_sha256.as_bytes().as_slice(),
            commit.commit_id.as_bytes().as_slice(),
            version,
            result
        ],
    )?;
    tx.execute("UPDATE otmp_meta SET table_version=?1,semantic_state_sha256=?2,last_commit_id=?3,last_commit_sha256=?4", params![version,commit.semantic_state_sha256.as_bytes().as_slice(),commit.commit_id.as_bytes().as_slice(),hash.as_bytes().as_slice()])?;
    tx.commit()?;
    drop(connection);
    finish_checkpoint(directory, path)
}

pub(crate) fn validate_transition(
    parent: &Path,
    selected: &Path,
    commit: &SemanticCommit,
) -> Result<(), RuntimeError> {
    let previous = open_readonly(parent)?;
    let version: i64 =
        previous.query_row("SELECT table_version FROM otmp_meta", [], |r| r.get(0))?;
    if u64::try_from(version).ok() == Some(commit.table_version.0) {
        return compare_logical_images(parent, selected);
    }
    let selected_connection = open_readonly(selected)?;
    let uri: String = selected_connection.query_row(
        "SELECT commit_object_uri FROM otmp_commits WHERE commit_id=?1",
        [commit.commit_id.as_bytes().as_slice()],
        |r| r.get(0),
    )?;
    let expected = replay_semantic_commit(parent, commit, &uri.parse()?)?;
    compare_logical_images(&expected.path, selected)
}

pub(crate) fn replay_semantic_commit(
    parent: &Path,
    commit: &SemanticCommit,
    uri: &otmp_protocol::RelativeUri,
) -> Result<CheckpointImage, RuntimeError> {
    let previous = open_readonly(parent)?;
    let version: i64 =
        previous.query_row("SELECT table_version FROM otmp_meta", [], |r| r.get(0))?;
    if version.checked_add(1).and_then(|n| u64::try_from(n).ok()) != Some(commit.table_version.0) {
        return Err(RuntimeError::Corrupt(
            "retained semantic version gap".into(),
        ));
    }
    let requirements = commit
        .requirements
        .iter()
        .map(|v| canonical_json::from_slice_canonical(&canonical_json::to_vec(v)?))
        .collect::<Result<Vec<crate::Requirement>, otmp_protocol::ProtocolError>>()?;
    crate::runtime::transactions::evaluate(&previous, &requirements)?;
    let is_append = matches!(&commit.operations[0],CanonicalValue::Object(o) if o.get("type") == Some(&CanonicalValue::String("commit_snapshot".into())));
    if is_append {
        replay_append(&fs::read(parent)?, commit, uri)
    } else {
        let operations = commit
            .operations
            .iter()
            .map(|v| canonical_json::from_slice_canonical(&canonical_json::to_vec(v)?))
            .collect::<Result<Vec<crate::OperationRequest>, otmp_protocol::ProtocolError>>()?;
        let request = crate::TransactionRequest {
            idempotency_key: commit.intents[0].key.clone(),
            requirements,
            operations: operations.clone(),
            commit_metadata: canonical_json::from_slice_canonical(&canonical_json::to_vec(
                &commit.metadata,
            )?)?,
        };
        let results = crate::runtime::transactions::prepare_operations(&previous, &request)?;
        let durable = crate::runtime::transactions::DurableResult {
            table_version: commit.table_version.0,
            commit_id: commit.commit_id,
            operation_results: results,
        };
        if canonical_json::to_value(&durable)? != commit.intents[0].result {
            return Err(RuntimeError::Corrupt(
                "durable metadata result differs from operations".into(),
            ));
        }
        apply_metadata(&fs::read(parent)?, commit, uri, &operations)
    }
}

pub(crate) fn compare_logical_images(a: &Path, b: &Path) -> Result<(), RuntimeError> {
    fn rows(path: &Path) -> Result<BTreeMap<String, Vec<String>>, RuntimeError> {
        let connection = open_readonly(path)?;
        let mut tables=connection.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'otmp_%' ORDER BY name")?;
        let names = tables
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        let mut result = BTreeMap::new();
        for name in names {
            let mut stmt =
                connection.prepare(&format!("SELECT * FROM \"{}\"", name.replace('"', "\"\"")))?;
            let count = stmt.column_count();
            let mut values = stmt
                .query_map([], |r| {
                    (0..count)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<Result<Vec<_>, _>>()
                })?
                .map(|r| r.map(|v| format!("{v:?}")))
                .collect::<Result<Vec<_>, _>>()?;
            values.sort();
            result.insert(name, values);
        }
        Ok(result)
    }
    if rows(a)? != rows(b)? {
        return Err(RuntimeError::Corrupt(
            "retained commit does not explain relational transition".into(),
        ));
    }
    Ok(())
}
fn replay_append(
    parent: &[u8],
    commit: &SemanticCommit,
    uri: &otmp_protocol::RelativeUri,
) -> Result<CheckpointImage, RuntimeError> {
    let operation: ProjectedCommitSnapshot =
        canonical_json::from_slice_canonical(&canonical_json::to_vec(&commit.operations[0])?)?;
    let files = operation
        .added_files
        .into_iter()
        .map(|f| {
            let cbor = encode_partition_tuple(&f.partition_values);
            Ok(ImageFile {
                file_id: f.file_id,
                uri: f.uri,
                format: FileFormat::Parquet,
                file_size_bytes: f.file_size_bytes.0,
                record_count: f.record_count.0,
                schema_id: u32::try_from(f.schema_id.0)
                    .map_err(|_| RuntimeError::Corrupt("schema ID overflow".into()))?,
                partition_spec_id: 0,
                sort_order_id: 0,
                partition_hash: partition_hash(0, &cbor),
                partition_values_cbor: cbor,
                content_sha256: f.content_sha256,
                metadata_json: canonical_string(&f.metadata)?,
                metrics: f
                    .metrics
                    .into_iter()
                    .map(|m| {
                        Ok(ImageMetric {
                            field_id: u32::try_from(m.field_id.0)
                                .map_err(|_| RuntimeError::Corrupt("field ID overflow".into()))?,
                            column_size_bytes: m.column_size_bytes.map(|v| v.0),
                            value_count: m.value_count.map(|v| v.0),
                            null_count: m.null_count.map(|v| v.0),
                            nan_count: m.nan_count.map(|v| v.0),
                            distinct_count: m.distinct_count.map(|v| v.0),
                            lower_bound_cbor: m.lower_bound.as_ref().map(encode_typed_scalar),
                            upper_bound_cbor: m.upper_bound.as_ref().map(encode_typed_scalar),
                            metadata_json: canonical_string(&m.metadata)?,
                        })
                    })
                    .collect::<Result<_, RuntimeError>>()?,
            })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    let CanonicalValue::Object(summary) = operation.snapshot.summary else {
        return Err(RuntimeError::Corrupt("invalid summary".into()));
    };
    apply_append(
        parent,
        &AppendImage {
            table_version: commit.table_version.0,
            created_at_ms: commit.created_at_ms.0,
            semantic_state: commit.semantic_state_sha256,
            commit_id: commit.commit_id,
            commit_hash: otmp_protocol::object_hash(&canonical_json::to_vec(commit)?),
            commit_uri: uri.as_str(),
            operation_json: &canonical_string(&commit.operations)?,
            result_json: &canonical_string(&commit.intents[0].result)?,
            commit_metadata_json: &canonical_string(&commit.metadata)?,
            idempotency_key: &commit.intents[0].key,
            intent_hash: commit.intents[0].intent_sha256,
            snapshot_id: operation.snapshot.snapshot_id,
            parent_snapshot_id: operation.snapshot.parent_snapshot_id,
            target_ref: &operation.target_ref,
            sequence_number: operation.snapshot.sequence_number.0,
            summary: &summary,
            snapshot_metadata_json: &canonical_string(&operation.snapshot.metadata)?,
            files: &files,
        },
    )
}

fn validate_metadata_projection(
    connection: &Connection,
    commit: &SemanticCommit,
) -> Result<(), RuntimeError> {
    use crate::OperationRequest;
    let version = sqlite_i64(commit.table_version.0, "version")?;
    for value in &commit.operations {
        let CanonicalValue::Object(fields) = value else {
            continue;
        };
        if matches!(fields.get("type"),Some(CanonicalValue::String(t)) if t=="initialize_table" || t=="commit_snapshot")
        {
            continue;
        }
        let operation: OperationRequest =
            canonical_json::from_slice_canonical(&canonical_json::to_vec(value)?)?;
        let valid = match operation {
            OperationRequest::SetProperties {
                updates, removals, ..
            } => {
                let mut valid = true;
                for (key, value) in updates {
                    let row:Option<(String,i64)>=connection.query_row("SELECT value_json,updated_version FROM otmp_properties WHERE property_key=?1",[key],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
                    valid &= row == Some((canonical_string(&value)?, version));
                }
                for key in removals {
                    valid &= !connection.query_row(
                        "SELECT EXISTS(SELECT 1 FROM otmp_properties WHERE property_key=?1)",
                        [key],
                        |r| r.get::<_, bool>(0),
                    )?;
                }
                valid
            }
        };
        if !valid {
            return Err(RuntimeError::Corrupt(
                "metadata operation differs from relational projection".into(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod regeneration {
    use super::*;
    use otmp_protocol::{Generation, Head, object_hash};

    #[test]
    fn canonical_packages_regenerate_from_retained_commits() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/tables");
        for package in ["genesis", "append", "transactions"] {
            let package = root.join(package);
            let head: Head = canonical_json::from_slice_canonical(
                &fs::read(package.join("_otmp/HEAD")).unwrap(),
            )
            .unwrap();
            let mut reference = Some(head.metadata_generation);
            let mut generations = Vec::new();
            while let Some(r) = reference {
                let bytes = fs::read(package.join(r.uri.as_str())).unwrap();
                let generation: Generation = canonical_json::from_slice_canonical(&bytes).unwrap();
                assert_eq!(canonical_json::to_vec(&generation).unwrap(), bytes);
                reference = generation.physical_parent.clone();
                generations.push(generation);
            }
            generations.reverse();
            let mut previous: Option<Vec<u8>> = None;
            for generation in generations {
                let bytes =
                    fs::read(package.join(generation.semantic_commit.uri.as_str())).unwrap();
                let commit: SemanticCommit = canonical_json::from_slice_canonical(&bytes).unwrap();
                assert_eq!(canonical_json::to_vec(&commit).unwrap(), bytes);
                let checkpoint = if let Some(previous) = previous {
                    let CanonicalValue::Object(operation) = &commit.operations[0] else {
                        panic!("operation")
                    };
                    if operation.get("type")
                        == Some(&CanonicalValue::String("commit_snapshot".into()))
                    {
                        replay_append(&previous, &commit, &generation.semantic_commit.uri).unwrap()
                    } else {
                        let operations = commit
                            .operations
                            .iter()
                            .map(|o| {
                                canonical_json::from_slice_canonical(
                                    &canonical_json::to_vec(o).unwrap(),
                                )
                                .unwrap()
                            })
                            .collect::<Vec<_>>();
                        apply_metadata(
                            &previous,
                            &commit,
                            &generation.semantic_commit.uri,
                            &operations,
                        )
                        .unwrap()
                    }
                } else {
                    let CanonicalValue::Object(operation) = &commit.operations[0] else {
                        panic!("genesis operation")
                    };
                    let schema: Schema = canonical_json::from_slice_canonical(
                        &canonical_json::to_vec(&operation["schema"]).unwrap(),
                    )
                    .unwrap();
                    create_genesis(&GenesisImage {
                        table_id: commit.table_id,
                        schema: &schema,
                        created_at_ms: commit.created_at_ms.0,
                        semantic_state: commit.semantic_state_sha256,
                        commit_id: commit.commit_id,
                        commit_hash: object_hash(&bytes),
                        commit_uri: generation.semantic_commit.uri.as_str(),
                        operation_json: &canonical_string(&commit.operations).unwrap(),
                        result_json: &canonical_string(&commit.intents[0].result).unwrap(),
                        intent_hash: commit.intents[0].intent_sha256,
                        metadata_json: &canonical_string(&commit.metadata).unwrap(),
                        reader_features_json: &canonical_string(
                            &commit.required_reader_features_after_commit,
                        )
                        .unwrap(),
                        writer_features_json: &canonical_string(
                            &commit.required_writer_features_after_commit,
                        )
                        .unwrap(),
                    })
                    .unwrap()
                };
                let stored =
                    fs::read(package.join(generation.metadata_image.checkpoint.uri.as_str()))
                        .unwrap();
                assert_eq!(
                    object_hash(&checkpoint.bytes),
                    object_hash(&stored),
                    "checkpoint regeneration at version {}",
                    commit.table_version.0
                );
                assert_eq!(checkpoint.bytes, stored);
                previous = Some(checkpoint.bytes);
            }
        }
    }
}

pub(crate) fn replay_genesis(
    commit: &SemanticCommit,
    uri: &otmp_protocol::RelativeUri,
) -> Result<CheckpointImage, RuntimeError> {
    if commit.table_version.0 != 0 {
        return Err(RuntimeError::Corrupt(
            "missing retained genesis commit".into(),
        ));
    }
    let CanonicalValue::Object(operation) = &commit.operations[0] else {
        return Err(RuntimeError::Corrupt("invalid genesis".into()));
    };
    let schema: Schema = canonical_json::from_slice_canonical(&canonical_json::to_vec(
        operation
            .get("schema")
            .ok_or_else(|| RuntimeError::Corrupt("missing genesis schema".into()))?,
    )?)?;
    create_genesis(&GenesisImage {
        table_id: commit.table_id,
        schema: &schema,
        created_at_ms: commit.created_at_ms.0,
        semantic_state: commit.semantic_state_sha256,
        commit_id: commit.commit_id,
        commit_hash: otmp_protocol::object_hash(&canonical_json::to_vec(commit)?),
        commit_uri: uri.as_str(),
        operation_json: &canonical_string(&commit.operations)?,
        result_json: &canonical_string(&commit.intents[0].result)?,
        intent_hash: commit.intents[0].intent_sha256,
        metadata_json: &canonical_string(&commit.metadata)?,
        reader_features_json: &canonical_string(&commit.required_reader_features_after_commit)?,
        writer_features_json: &canonical_string(&commit.required_writer_features_after_commit)?,
    })
}
