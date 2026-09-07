use crate::reader::metadata::{
    CursorState, FileBatch, FileCursor, MetadataReader, ReaderFile, corrupt, id, integer, text,
    uint,
};
use crate::{LiveFile, ObjectStore, RuntimeError};

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
    let (sequence, file) = match cursor.as_ref().map(|c| &c.state) {
        Some(CursorState::Branch { sequence, file }) => (*sequence, *file),
        None => (0, otmp_protocol::Id::from_bytes([0; 16])),
        _ => return Err(corrupt("cursor mode differs from selected ref")),
    };
    let rows = reader.engine.query("SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,rf.added_snapshot_id,rf.file_sequence_number,f.created_snapshot_id,f.created_version,s.sequence_number,s.committed_table_version FROM otmp_ref_live_files rf LEFT JOIN otmp_files f ON f.file_id=rf.file_id LEFT JOIN otmp_snapshots s ON s.snapshot_id=f.created_snapshot_id WHERE rf.ref_name=?1 AND (rf.file_sequence_number>?2 OR (rf.file_sequence_number=?2 AND rf.file_id>?3)) ORDER BY rf.file_sequence_number,rf.file_id LIMIT ?4", vec![turso_core::Value::build_text(branch.clone()), integer(i64::try_from(sequence).map_err(|_| corrupt("sequence"))?), turso_core::Value::Blob(file.as_bytes().to_vec()), integer(i64::try_from(limit).map_err(|_| corrupt("limit"))?)], limit, BYTES).await?;
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
            state: CursorState::Branch {
                sequence,
                file: file_id,
            },
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
        let (metrics, metric_reservations) = metrics(reader, file_id, fields).await?;
        reservations.extend(metric_reservations);
        files.push(ReaderFile {
            file: live,
            schema_id: u32::try_from(uint(&row[7])?).map_err(|_| corrupt("schema ID"))?,
            metrics,
        });
    }
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
        let (metrics, metric_reservations) = metrics(reader, file_id, fields).await?;
        reservations.extend(metric_reservations);
        files.push(ReaderFile {
            file: live,
            schema_id: u32::try_from(uint(&r[8])?).map_err(|_| corrupt("schema ID"))?,
            metrics,
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
    Ok(FileBatch {
        files,
        next_cursor: next,
        _reservations: reservations,
    })
}
async fn metrics<S: ObjectStore>(
    reader: &MetadataReader<S>,
    file: otmp_protocol::Id,
    fields: &[u32],
) -> Result<
    (
        Vec<crate::FileMetric>,
        Vec<crate::reader::cache::Reservation>,
    ),
    RuntimeError,
> {
    let transient = reader.context.reserve_bytes(BYTES)?;
    let mut out = Vec::new();
    let mut retained = Vec::new();
    for field in fields {
        let rows=reader.engine.query("SELECT field_id,column_size_bytes,value_count,null_count,nan_count,distinct_count,lower_bound_cbor,upper_bound_cbor,metadata_json FROM otmp_file_metrics WHERE file_id=?1 AND field_id=?2",vec![turso_core::Value::Blob(file.as_bytes().to_vec()),integer(i64::from(*field))],1,64 * 1024).await?;
        if let [r] = rows.as_slice() {
            if r.len() != 9 {
                return Err(corrupt("invalid metric row"));
            }
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
                metadata: otmp_protocol::canonical_json::from_slice_canonical(
                    text(&r[8])?.as_bytes(),
                )?,
            };
            let charge = otmp_protocol::canonical_json::to_vec(&metric)?
                .len()
                .saturating_mul(4)
                .saturating_add(256);
            retained.push(reader.context.reserve_bytes(charge)?);
            out.push(metric);
        }
    }
    drop(transient);
    Ok((out, retained))
}
fn opt(v: &turso_core::Value) -> Result<Option<u64>, RuntimeError> {
    if matches!(v, turso_core::Value::Null) {
        Ok(None)
    } else {
        uint(v).map(Some)
    }
}
