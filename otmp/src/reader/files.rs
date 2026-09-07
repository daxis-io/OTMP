use crate::reader::metadata::{
    CursorState, FileBatch, FileCursor, MetadataReader, ReaderFile, corrupt, id, integer, text,
    uint,
};
use crate::{LiveFile, ObjectStore, RuntimeError};

// The draft schema fixes this primary-key index. Turso 0.7.2 otherwise chooses
// the sequence index and scans/sorts the branch again for every batch. The file
// ID order gives a stable seek in the pinned generation without changing membership.
const BRANCH_FILES_SQL: &str = "SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,rf.added_snapshot_id,rf.file_sequence_number,f.created_snapshot_id,f.created_version,s.sequence_number,s.committed_table_version FROM otmp_ref_live_files rf INDEXED BY sqlite_autoindex_otmp_ref_live_files_1 LEFT JOIN otmp_files f ON f.file_id=rf.file_id LEFT JOIN otmp_snapshots s ON s.snapshot_id=f.created_snapshot_id WHERE rf.ref_name=?1 AND rf.file_id>?2 ORDER BY rf.file_id LIMIT ?3";

const BYTES: usize = 1024 * 1024;

pub(crate) async fn enumerate<S: ObjectStore>(
    reader: &MetadataReader<S>,
    cursor: Option<FileCursor>,
    fields: &[u32],
    limit: usize,
) -> Result<FileBatch, RuntimeError> {
    if limit == 0 || limit > 256 {
        return Err(RuntimeError::ResourceExhausted(
            "file batch limit must be 1..=256".into(),
        ));
    }
    let unique = fields
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    if unique.len() != fields.len() {
        return Err(corrupt("duplicate requested metric field"));
    }
    if fields.len() > 4096 {
        return Err(RuntimeError::ResourceExhausted(
            "metric field batch limit is 4096".into(),
        ));
    }
    let reservation = reader.context.reserve_bytes(8 * 1024 * 1024)?;
    let cursor = match cursor {
        Some(c) if c.pin == reader.pin_id => Some(c),
        Some(_) => return Err(corrupt("file cursor belongs to another reader")),
        None => None,
    };
    if reader.branch.is_none() {
        return historical(reader, cursor, fields, limit, reservation).await;
    }
    let branch = reader
        .branch
        .as_ref()
        .ok_or_else(|| corrupt("historical file traversal is unavailable"))?;
    let file = match cursor.as_ref().map(|c| &c.state) {
        Some(CursorState::Branch { file }) => *file,
        None => otmp_protocol::Id::from_bytes([0; 16]),
        _ => return Err(corrupt("cursor mode differs from selected ref")),
    };
    let rows = reader
        .engine
        .query(
            BRANCH_FILES_SQL,
            vec![
                turso_core::Value::build_text(branch.clone()),
                turso_core::Value::Blob(file.as_bytes().to_vec()),
                integer(i64::try_from(limit).map_err(|_| corrupt("limit"))?),
            ],
            limit,
            BYTES,
        )
        .await?;
    let mut files = Vec::with_capacity(rows.len());
    let mut reservations = vec![reservation];
    let mut next = None;
    for row in rows {
        if row.len() != 19 || matches!(row[0], turso_core::Value::Null) {
            return Err(corrupt("missing joined file descriptor"));
        }
        let file_id = id(&row[0])?;
        let sequence = uint(&row[6])?;
        if text(&row[8])? != "data"
            || text(&row[2])? != "parquet"
            || !matches!(row[9], turso_core::Value::Null)
            || uint(&row[10])? != 0
            || uint(&row[11])? != 0
            || !matches!(row[12], turso_core::Value::Null)
            || uint(&row[14])? != sequence
            || sequence == 0
            || uint(&row[7])? == 0
            || id(&row[13])? != id(&row[15])?
            || uint(&row[16])? != uint(&row[18])?
            || uint(&row[16])? > reader.coordinates().table_version
            || uint(&row[17])? != sequence
        {
            return Err(corrupt("unsupported or inconsistent live file"));
        }
        next = Some(FileCursor {
            pin: reader.pin_id,
            state: CursorState::Branch { file: file_id },
        });
        let live = LiveFile {
            file_id,
            uri: text(&row[1])?.parse()?,
            file_format: text(&row[2])?,
            file_size_bytes: uint(&row[3])?,
            record_count: uint(&row[4])?,
            content_sha256: if matches!(row[5], turso_core::Value::Null) {
                None
            } else {
                Some(crate::reader::metadata::hash(&row[5])?)
            },
            sequence_number: sequence,
        };
        files.push(ReaderFile {
            file: live,
            schema_id: u32::try_from(uint(&row[7])?).map_err(|_| corrupt("schema ID"))?,
            metrics: Vec::new(),
        });
    }
    metrics(reader, &mut files, fields, &mut reservations).await?;
    Ok(FileBatch {
        files,
        next_cursor: next,
        _reservations: reservations,
    })
}

async fn historical<S: ObjectStore>(
    reader: &MetadataReader<S>,
    cursor: Option<FileCursor>,
    fields: &[u32],
    limit: usize,
    reservation: crate::reader::cache::Reservation,
) -> Result<FileBatch, RuntimeError> {
    let selected = reader
        .snapshot
        .as_ref()
        .ok_or_else(|| corrupt("empty snapshot has no historical cursor"))?;
    let (snapshot, before, file) = match cursor.map(|c| c.state) {
        Some(CursorState::Snapshot {
            snapshot,
            before_sequence,
            file,
        }) => (snapshot, before_sequence, file),
        None => (selected.snapshot_id, selected.sequence_number, None),
        _ => return Err(corrupt("cursor mode differs from historical selection")),
    };
    let descriptor = super::snapshot::descriptor(&reader.engine, snapshot).await?;
    if descriptor.sequence_number != before
        || descriptor.committed_table_version > reader.coordinates().table_version
    {
        return Err(corrupt("historical cursor descriptor mismatch"));
    }
    let rows=reader.engine.query("SELECT c.change_kind,f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,f.created_snapshot_id,f.created_version FROM otmp_snapshot_file_changes c LEFT JOIN otmp_files f ON f.file_id=c.file_id WHERE c.snapshot_id=?1 AND (?2 IS NULL OR c.file_id>?2) ORDER BY c.file_id LIMIT ?3",vec![turso_core::Value::Blob(snapshot.as_bytes().to_vec()),file.map_or(turso_core::Value::Null,|v|turso_core::Value::Blob(v.as_bytes().to_vec())),integer(i64::try_from(limit).map_err(|_|corrupt("limit"))?)],limit,BYTES).await?;
    let mut files = Vec::new();
    let mut reservations = vec![reservation];
    let mut last = file;
    for r in rows {
        if r.len() != 16 || matches!(r[1], turso_core::Value::Null) {
            return Err(corrupt("missing historical file descriptor"));
        }
        if text(&r[0])? != "add" {
            return Err(corrupt("historical remove change is unsupported"));
        }
        let file_id = id(&r[1])?;
        last = Some(file_id);
        if text(&r[9])? != "data"
            || text(&r[3])? != "parquet"
            || !matches!(r[10], turso_core::Value::Null)
            || uint(&r[11])? != 0
            || uint(&r[12])? != 0
            || !matches!(r[13], turso_core::Value::Null)
            || id(&r[14])? != snapshot
            || uint(&r[7])? != descriptor.sequence_number
            || uint(&r[8])? == 0
            || uint(&r[15])? != descriptor.committed_table_version
        {
            return Err(corrupt("invalid historical file membership"));
        }
        let live = LiveFile {
            file_id,
            uri: text(&r[2])?.parse()?,
            file_format: text(&r[3])?,
            file_size_bytes: uint(&r[4])?,
            record_count: uint(&r[5])?,
            content_sha256: if matches!(r[6], turso_core::Value::Null) {
                None
            } else {
                Some(crate::reader::metadata::hash(&r[6])?)
            },
            sequence_number: uint(&r[7])?,
        };
        files.push(ReaderFile {
            file: live,
            schema_id: u32::try_from(uint(&r[8])?).map_err(|_| corrupt("schema ID"))?,
            metrics: Vec::new(),
        });
    }
    let next = if files.len() == limit {
        last.map(|file| FileCursor {
            pin: reader.pin_id,
            state: CursorState::Snapshot {
                snapshot,
                before_sequence: before,
                file: Some(file),
            },
        })
    } else if let Some(parent) = descriptor.parent_snapshot_id {
        let parent = super::snapshot::descriptor(&reader.engine, parent).await?;
        if parent.sequence_number >= before
            || parent.committed_table_version > reader.coordinates().table_version
        {
            return Err(corrupt("nonmonotone historical parent"));
        }
        Some(FileCursor {
            pin: reader.pin_id,
            state: CursorState::Snapshot {
                snapshot: parent.snapshot_id,
                before_sequence: parent.sequence_number,
                file: None,
            },
        })
    } else {
        None
    };
    metrics(reader, &mut files, fields, &mut reservations).await?;
    Ok(FileBatch {
        files,
        next_cursor: next,
        _reservations: reservations,
    })
}
// A metric was previously limited to 64 KiB per query. Keep that row bound,
// including the extra file-ID column, while grouping at most sixteen files.
// This bounds a raw batch near 1 MiB even for unusually large scalar statistics.
const METRIC_QUERY_FILES: usize = 16;
const METRIC_ROW_BYTES: usize = 64 * 1024 + 16 + std::mem::size_of::<turso_core::Value>();

async fn metrics<S: ObjectStore>(
    reader: &MetadataReader<S>,
    files: &mut [ReaderFile],
    fields: &[u32],
    retained: &mut Vec<crate::reader::cache::Reservation>,
) -> Result<(), RuntimeError> {
    if fields.is_empty() || files.is_empty() {
        return Ok(());
    }
    let _transient = reader
        .context
        .reserve_bytes(METRIC_QUERY_FILES * METRIC_ROW_BYTES)?;
    for files in files.chunks_mut(METRIC_QUERY_FILES) {
        let placeholders = (2..=files.len() + 1)
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT file_id,field_id,column_size_bytes,value_count,null_count,nan_count,distinct_count,lower_bound_cbor,upper_bound_cbor,metadata_json FROM otmp_file_metrics INDEXED BY sqlite_autoindex_otmp_file_metrics_1 WHERE field_id=?1 AND file_id IN ({placeholders})"
        );
        for field in fields {
            let mut params = vec![integer(i64::from(*field))];
            params.extend(
                files
                    .iter()
                    .map(|file| turso_core::Value::Blob(file.file.file_id.as_bytes().to_vec())),
            );
            let rows = reader
                .engine
                .query_with_row_limit(
                    &sql,
                    params,
                    files.len(),
                    files.len() * METRIC_ROW_BYTES,
                    METRIC_ROW_BYTES,
                )
                .await?;
            for row in rows {
                if row.len() != 10 {
                    return Err(corrupt("invalid metric row"));
                }
                let file_id = id(&row[0])?;
                let file = files
                    .iter_mut()
                    .find(|file| file.file.file_id == file_id)
                    .ok_or_else(|| corrupt("metric belongs to an unrequested file"))?;
                let metric = decode_metric(&row[1..])?;
                if metric.field_id != *field
                    || file.metrics.iter().any(|value| value.field_id == *field)
                {
                    return Err(corrupt("duplicate or unrequested metric field"));
                }
                let charge = otmp_protocol::canonical_json::to_vec(&metric)?
                    .len()
                    .saturating_mul(4)
                    .saturating_add(256);
                retained.push(reader.context.reserve_bytes(charge)?);
                file.metrics.push(metric);
            }
        }
    }
    Ok(())
}

fn decode_metric(r: &[turso_core::Value]) -> Result<crate::FileMetric, RuntimeError> {
    let metric = crate::FileMetric {
        field_id: u32::try_from(uint(&r[0])?).map_err(|_| corrupt("metric field"))?,
        column_size_bytes: opt(&r[1])?,
        value_count: opt(&r[2])?,
        null_count: opt(&r[3])?,
        nan_count: opt(&r[4])?,
        distinct_count: opt(&r[5])?,
        lower_bound: if matches!(r[6], turso_core::Value::Null) {
            None
        } else {
            Some(otmp_protocol::decode_typed_scalar(
                crate::reader::metadata::blob(&r[6])?,
            )?)
        },
        upper_bound: if matches!(r[7], turso_core::Value::Null) {
            None
        } else {
            Some(otmp_protocol::decode_typed_scalar(
                crate::reader::metadata::blob(&r[7])?,
            )?)
        },
        metadata: otmp_protocol::canonical_json::from_slice_canonical(text(&r[8])?.as_bytes())?,
    };
    Ok(metric)
}

fn opt(v: &turso_core::Value) -> Result<Option<u64>, RuntimeError> {
    if matches!(v, turso_core::Value::Null) {
        Ok(None)
    } else {
        uint(v).map(Some)
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn branch_pagination_seeks_without_rescanning_or_sorting() {
        let table = crate::Table::new(crate::InMemoryObjectStore::default());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(crate::InitializeRequest::new(schema))
            .await
            .unwrap();
        let reader = table
            .open_metadata_reader(
                crate::MetadataSelection::Current,
                crate::SnapshotSelection::Ref("main".into()),
                crate::ReaderOptions::default(),
            )
            .await
            .unwrap();
        let plan = reader
            .engine
            .query(
                &format!("EXPLAIN QUERY PLAN {}", super::BRANCH_FILES_SQL),
                vec![
                    turso_core::Value::build_text("main"),
                    turso_core::Value::Blob(vec![0; 16]),
                    super::integer(256),
                ],
                16,
                4096,
            )
            .await
            .unwrap();
        let details: Vec<_> = plan
            .iter()
            .map(|row| super::text(&row[3]).unwrap())
            .collect();
        assert!(
            details
                .iter()
                .any(|line| line.contains("ref_name=? AND file_id>?")),
            "pagination must seek after the previous file: {details:?}"
        );
        assert!(
            !details.iter().any(|line| line.contains("SORT")),
            "pagination must stream its index order: {details:?}"
        );
    }
    #[tokio::test]
    async fn metric_reads_are_batched_and_preserve_sparse_file_associations() {
        use std::collections::BTreeMap;
        let table = crate::Table::new(crate::InMemoryObjectStore::default());
        let schema =
            serde_json::from_slice(include_bytes!("../../../conformance/sources/schema.json"))
                .unwrap();
        table
            .initialize(crate::InitializeRequest::new(schema))
            .await
            .unwrap();
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"metric fixture").unwrap();
        let metrics: Vec<Vec<crate::FileMetric>> = (0..33)
            .map(|i| {
                if i % 3 == 0 {
                    vec![]
                } else {
                    vec![crate::FileMetric {
                        field_id: 1,
                        column_size_bytes: Some(8),
                        value_count: Some(1),
                        null_count: Some(0),
                        nan_count: None,
                        distinct_count: Some(1),
                        lower_bound: Some(otmp_protocol::TypedScalar::Int64(i)),
                        upper_bound: Some(otmp_protocol::TypedScalar::Int64(i)),
                        metadata: BTreeMap::new(),
                    }]
                }
            })
            .collect();
        let files = metrics
            .iter()
            .enumerate()
            .map(|(i, metrics)| crate::AppendFile {
                source_path: source.path().into(),
                fingerprint: crate::SourceFingerprint {
                    sha256: otmp_protocol::Sha256::digest(b"metric fixture"),
                    length: 14,
                },
                format: crate::FileFormat::Parquet,
                record_count: 1,
                schema_id: 1,
                partition_spec_id: 0,
                sort_order_id: 0,
                partition_values: BTreeMap::new(),
                metrics: metrics.clone(),
                metadata: BTreeMap::from([(
                    "ordinal".into(),
                    otmp_protocol::CanonicalValue::Integer(i as i128),
                )]),
            })
            .collect();
        let result = table
            .append_files(&crate::AppendRequest::new("metrics", files))
            .await
            .unwrap();
        let expected: BTreeMap<_, _> = result
            .files
            .iter()
            .zip(metrics)
            .map(|(file, metrics)| (file.file_id, metrics))
            .collect();
        let reader = table
            .open_metadata_reader(
                crate::MetadataSelection::Current,
                crate::SnapshotSelection::Ref("main".into()),
                crate::ReaderOptions::default(),
            )
            .await
            .unwrap();
        let before = reader.engine.query_count();
        let batch = reader.files(None, &[1], 256).await.unwrap();
        assert_eq!(batch.files.len(), 33);
        for file in &batch.files {
            assert_eq!(file.metrics, expected[&file.file.file_id]);
        }
        let queries = reader.engine.query_count() - before;
        assert!(
            queries <= 4,
            "one membership query plus at most three bounded metric queries, got {queries}"
        );
    }
}
