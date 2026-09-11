use crate::reader::metadata::{
    CanonicalRange, CursorState, FileBatch, FileCursor, FileMetricRange, MetadataReader, RangeType,
    ReaderFile, corrupt, id, integer, text, uint,
};
use crate::{LiveFile, ObjectStore, RuntimeError};
use std::collections::BTreeMap;
use std::ops::Bound;

// The draft schema fixes this primary-key index. Turso 0.7.2 otherwise chooses
// the sequence index and scans/sorts the branch again for every batch. The file
// ID order gives a stable seek in the pinned generation without changing membership.
const BRANCH_FILES_SQL: &str = "SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,rf.added_snapshot_id,rf.file_sequence_number,f.created_snapshot_id,f.created_version,s.sequence_number,s.committed_table_version FROM otmp_ref_live_files rf INDEXED BY sqlite_autoindex_otmp_ref_live_files_1 LEFT JOIN otmp_files f ON f.file_id=rf.file_id LEFT JOIN otmp_snapshots s ON s.snapshot_id=f.created_snapshot_id WHERE rf.ref_name=?1 AND rf.file_id>?2 ORDER BY rf.file_id LIMIT ?3";

const BRANCH_FIRST_SQL: &str = "SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,rf.added_snapshot_id,rf.file_sequence_number,f.created_snapshot_id,f.created_version,s.sequence_number,s.committed_table_version FROM otmp_ref_live_files rf INDEXED BY sqlite_autoindex_otmp_ref_live_files_1 LEFT JOIN otmp_files f ON f.file_id=rf.file_id LEFT JOIN otmp_snapshots s ON s.snapshot_id=f.created_snapshot_id WHERE rf.ref_name=?1 ORDER BY rf.file_id LIMIT ?2";

const RANGE_REJECTION_SQL: &str = "NOT EXISTS (SELECT 1 FROM ranges q CROSS JOIN otmp_file_metrics m INDEXED BY sqlite_autoindex_otmp_file_metrics_1 WHERE m.file_id=f.file_id AND m.field_id=q.field_id AND m.ordered_bound_type=q.bound_type AND ((q.lower_value IS NOT NULL AND m.ordered_upper_i64 IS NOT NULL AND (m.ordered_upper_i64<q.lower_value OR (m.ordered_upper_i64=q.lower_value AND q.lower_inclusive=0))) OR (q.upper_value IS NOT NULL AND m.ordered_lower_i64 IS NOT NULL AND (m.ordered_lower_i64>q.upper_value OR (m.ordered_lower_i64=q.upper_value AND q.upper_inclusive=0)))))";

const BYTES: usize = 1024 * 1024;

pub(crate) async fn enumerate<S: ObjectStore>(
    reader: &MetadataReader<S>,
    cursor: Option<FileCursor>,
    fields: &[u32],
    ranges: &[FileMetricRange],
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
    let canonical = canonicalize_ranges(reader, ranges)?;
    let cursor = match cursor {
        Some(c) if c.pin != reader.pin_id => {
            return Err(corrupt("file cursor belongs to another reader"));
        }
        Some(c) if c.ranges != canonical.ranges => {
            return Err(corrupt("file cursor range set changed"));
        }
        Some(c) => Some(c),
        None => None,
    };
    if cursor.as_ref().is_some_and(|cursor| {
        matches!(cursor.state, CursorState::Branch { .. }) != reader.branch.is_some()
    }) {
        return Err(corrupt("cursor mode differs from selected ref"));
    }
    if canonical.impossible {
        return Ok(FileBatch {
            files: Vec::new(),
            next_cursor: None,
            _reservations: Vec::new(),
        });
    }
    if reader.branch.is_none() {
        return historical(reader, cursor, fields, &canonical.ranges, limit).await;
    }
    branch(reader, cursor, fields, &canonical.ranges, limit).await
}

struct CanonicalizedRanges {
    ranges: Vec<CanonicalRange>,
    impossible: bool,
}

fn canonicalize_ranges<S: ObjectStore>(
    reader: &MetadataReader<S>,
    ranges: &[FileMetricRange],
) -> Result<CanonicalizedRanges, RuntimeError> {
    if ranges.len() > 4096 {
        return Err(RuntimeError::ResourceExhausted(
            "file metric range limit is 4096".into(),
        ));
    }
    let mut merged = BTreeMap::new();
    for range in ranges {
        let (field_id, range_type, lower, upper) = match range {
            FileMetricRange::Int32 {
                field_id,
                lower,
                upper,
            } => (*field_id, RangeType::Int32, widen(*lower), widen(*upper)),
            FileMetricRange::Int64 {
                field_id,
                lower,
                upper,
            } => (*field_id, RangeType::Int64, *lower, *upper),
            FileMetricRange::Date {
                field_id,
                lower,
                upper,
            } => (*field_id, RangeType::Date, widen(*lower), widen(*upper)),
        };
        let compatible = reader
            .schema()
            .fields
            .iter()
            .find(|field| field.field_id == field_id)
            .is_some_and(|field| {
                matches!(
                    (&field.field_type, range_type),
                    (otmp_protocol::LogicalType::Int32, RangeType::Int32)
                        | (otmp_protocol::LogicalType::Int64, RangeType::Int64)
                        | (otmp_protocol::LogicalType::Date, RangeType::Date)
                )
            });
        if !compatible {
            continue;
        }
        let entry = merged
            .entry((field_id, range_type))
            .or_insert((Bound::Unbounded, Bound::Unbounded));
        entry.0 = strongest_lower(entry.0, lower);
        entry.1 = strongest_upper(entry.1, upper);
    }
    let ranges = merged
        .into_iter()
        .map(|((field_id, range_type), (lower, upper))| CanonicalRange {
            field_id,
            range_type,
            lower,
            upper,
        })
        .collect::<Vec<_>>();
    let impossible = ranges
        .iter()
        .any(|range| bounds_are_impossible(&range.lower, &range.upper));
    Ok(CanonicalizedRanges { ranges, impossible })
}

fn widen(bound: Bound<i32>) -> Bound<i64> {
    match bound {
        Bound::Included(value) => Bound::Included(i64::from(value)),
        Bound::Excluded(value) => Bound::Excluded(i64::from(value)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn strongest_lower(current: Bound<i64>, candidate: Bound<i64>) -> Bound<i64> {
    match (current, candidate) {
        (Bound::Unbounded, value) | (value, Bound::Unbounded) => value,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.max(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.max(b)),
        (Bound::Included(a), Bound::Excluded(b)) | (Bound::Excluded(b), Bound::Included(a)) => {
            if a > b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
    }
}

fn strongest_upper(current: Bound<i64>, candidate: Bound<i64>) -> Bound<i64> {
    match (current, candidate) {
        (Bound::Unbounded, value) | (value, Bound::Unbounded) => value,
        (Bound::Included(a), Bound::Included(b)) => Bound::Included(a.min(b)),
        (Bound::Excluded(a), Bound::Excluded(b)) => Bound::Excluded(a.min(b)),
        (Bound::Included(a), Bound::Excluded(b)) | (Bound::Excluded(b), Bound::Included(a)) => {
            if a < b {
                Bound::Included(a)
            } else {
                Bound::Excluded(b)
            }
        }
    }
}

fn bounds_are_impossible(lower: &Bound<i64>, upper: &Bound<i64>) -> bool {
    match (lower, upper) {
        (Bound::Included(lower), Bound::Included(upper)) => lower > upper,
        (
            Bound::Included(lower) | Bound::Excluded(lower),
            Bound::Included(upper) | Bound::Excluded(upper),
        ) => lower >= upper,
        _ => false,
    }
}

fn range_cte(count: usize) -> String {
    let values = (0..count)
        .map(|row| {
            let first = row * 6 + 1;
            format!(
                "(?{first},?{},?{},?{},?{},?{})",
                first + 1,
                first + 2,
                first + 3,
                first + 4,
                first + 5
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "WITH ranges(field_id,bound_type,lower_value,lower_inclusive,upper_value,upper_inclusive) AS (VALUES {values})"
    )
}

fn range_params(ranges: &[CanonicalRange]) -> Vec<turso_core::Value> {
    let mut params = Vec::with_capacity(ranges.len() * 6);
    for range in ranges {
        params.push(integer(i64::from(range.field_id)));
        params.push(turso_core::Value::build_text(range.range_type.as_str()));
        let (lower, lower_inclusive) = sql_bound(&range.lower);
        let (upper, upper_inclusive) = sql_bound(&range.upper);
        params.extend([
            lower,
            integer(lower_inclusive),
            upper,
            integer(upper_inclusive),
        ]);
    }
    params
}

fn sql_bound(bound: &Bound<i64>) -> (turso_core::Value, i64) {
    match bound {
        Bound::Included(value) => (integer(*value), 1),
        Bound::Excluded(value) => (integer(*value), 0),
        Bound::Unbounded => (turso_core::Value::Null, 1),
    }
}

fn ranged_branch_sql(range_count: usize, after: bool) -> String {
    let branch = range_count * 6 + 1;
    let (seek, limit) = if after {
        (format!(" AND rf.file_id>?{}", branch + 1), branch + 2)
    } else {
        (String::new(), branch + 1)
    };
    format!(
        "{} SELECT f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,rf.added_snapshot_id,rf.file_sequence_number,f.created_snapshot_id,f.created_version,s.sequence_number,s.committed_table_version FROM otmp_ref_live_files rf INDEXED BY sqlite_autoindex_otmp_ref_live_files_1 LEFT JOIN otmp_files f ON f.file_id=rf.file_id LEFT JOIN otmp_snapshots s ON s.snapshot_id=f.created_snapshot_id WHERE rf.ref_name=?{branch}{seek} AND {RANGE_REJECTION_SQL} ORDER BY rf.file_id LIMIT ?{limit}",
        range_cte(range_count)
    )
}

fn ranged_historical_sql(range_count: usize) -> String {
    let snapshot = range_count * 6 + 1;
    let cursor = snapshot + 1;
    let limit = snapshot + 2;
    format!(
        "{} SELECT c.change_kind,f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,f.created_snapshot_id,f.created_version FROM otmp_snapshot_file_changes c LEFT JOIN otmp_files f ON f.file_id=c.file_id WHERE c.snapshot_id=?{snapshot} AND (?{cursor} IS NULL OR c.file_id>?{cursor}) AND (c.change_kind<>'add' OR {RANGE_REJECTION_SQL}) ORDER BY c.file_id LIMIT ?{limit}",
        range_cte(range_count)
    )
}

async fn descriptor_rows<S: ObjectStore>(
    reader: &MetadataReader<S>,
    sql: &str,
    params: Vec<turso_core::Value>,
    limit: usize,
) -> Result<
    (
        Vec<Vec<turso_core::Value>>,
        crate::reader::cache::Reservation,
    ),
    RuntimeError,
> {
    let context = reader.context.clone();
    let mut result = reader
        .engine
        .query_group(
            vec![crate::reader_engine::QueryRequest {
                sql: sql.to_owned(),
                params,
                max_rows: limit,
                max_bytes: BYTES,
                max_row_bytes: BYTES,
            }],
            move |rows| {
                // Engine admission precedes allocation. Its working reservation
                // covers the raw rows until this retained result charge exists.
                // Preserve the previous eightfold decode allowance, sized from
                // the bounded rows instead of charging every queued request 8 MiB.
                let bytes = rows
                    .iter()
                    .flatten()
                    .map(|value| {
                        crate::reader_engine::value_bytes(value)
                            + std::mem::size_of::<turso_core::Value>()
                    })
                    .sum::<usize>();
                let reservation = context.reserve_bytes(bytes.saturating_mul(8).max(4096))?;
                Ok((rows, reservation))
            },
        )
        .await?;
    Ok(result.pop().expect("one descriptor statement"))
}

async fn branch<S: ObjectStore>(
    reader: &MetadataReader<S>,
    cursor: Option<FileCursor>,
    fields: &[u32],
    ranges: &[CanonicalRange],
    limit: usize,
) -> Result<FileBatch, RuntimeError> {
    let branch = reader
        .branch
        .as_ref()
        .ok_or_else(|| corrupt("historical file traversal is unavailable"))?;
    let file = match cursor.as_ref().map(|c| &c.state) {
        Some(CursorState::Branch { file }) => Some(*file),
        None => None,
        _ => return Err(corrupt("cursor mode differs from selected ref")),
    };
    let mut params = range_params(ranges);
    params.push(turso_core::Value::build_text(branch.clone()));
    let sql = match (ranges.is_empty(), file) {
        (true, Some(file)) => {
            params.push(turso_core::Value::Blob(file.as_bytes().to_vec()));
            BRANCH_FILES_SQL.to_owned()
        }
        // No artificial lower bound: malformed IDs must reach validation too.
        (true, None) => BRANCH_FIRST_SQL.to_owned(),
        (false, Some(file)) => {
            params.push(turso_core::Value::Blob(file.as_bytes().to_vec()));
            ranged_branch_sql(ranges.len(), true)
        }
        (false, None) => ranged_branch_sql(ranges.len(), false),
    };
    params.push(integer(i64::try_from(limit).map_err(|_| corrupt("limit"))?));
    let (rows, reservation) = descriptor_rows(reader, &sql, params, limit).await?;
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
            ranges: ranges.to_vec(),
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

#[allow(clippy::too_many_lines)] // Historical traversal and its cursor transition are one invariant.
async fn historical<S: ObjectStore>(
    reader: &MetadataReader<S>,
    cursor: Option<FileCursor>,
    fields: &[u32],
    ranges: &[CanonicalRange],
    limit: usize,
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
    let mut params = range_params(ranges);
    params.extend([
        turso_core::Value::Blob(snapshot.as_bytes().to_vec()),
        file.map_or(turso_core::Value::Null, |value| {
            turso_core::Value::Blob(value.as_bytes().to_vec())
        }),
        integer(i64::try_from(limit).map_err(|_| corrupt("limit"))?),
    ]);
    let sql = if ranges.is_empty() {
        "SELECT c.change_kind,f.file_id,f.uri,f.file_format,f.file_size_bytes,f.record_count,f.content_sha256,f.file_sequence_number,f.schema_id,f.file_kind,f.object_identity,f.partition_spec_id,f.sort_order_id,f.encryption_metadata,f.created_snapshot_id,f.created_version FROM otmp_snapshot_file_changes c LEFT JOIN otmp_files f ON f.file_id=c.file_id WHERE c.snapshot_id=?1 AND (?2 IS NULL OR c.file_id>?2) ORDER BY c.file_id LIMIT ?3".to_owned()
    } else {
        ranged_historical_sql(ranges.len())
    };
    let (rows, reservation) = descriptor_rows(reader, &sql, params, limit).await?;
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
            ranges: ranges.to_vec(),
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
            ranges: ranges.to_vec(),
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
// Keep useful dispatch amortization while returning the engine lane to peers
// before one caller decodes a complete 256-file metadata batch.
const METRIC_DISPATCH_OPERATIONS: usize = 4;
const METRIC_ROW_BYTES: usize = 64 * 1024 + 16 + std::mem::size_of::<turso_core::Value>();

fn metric_sql(count: usize) -> String {
    let placeholders = (2..=count + 1)
        .map(|index| format!("(?{index})"))
        .collect::<Vec<_>>()
        .join(",");
    // IN lists currently become full index scans in Turso 0.7.2. A bounded
    // VALUES relation on the left of CROSS JOIN preserves indexed point probes.
    format!(
        "WITH requested(file_id) AS (VALUES {placeholders}) SELECT m.file_id,m.field_id,m.column_size_bytes,m.value_count,m.null_count,m.nan_count,m.distinct_count,m.lower_bound_cbor,m.upper_bound_cbor,m.metadata_json FROM requested r CROSS JOIN otmp_file_metrics m INDEXED BY sqlite_autoindex_otmp_file_metrics_1 WHERE m.file_id=r.file_id AND m.field_id=?1"
    )
}

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
    let operations = files
        .len()
        .div_ceil(METRIC_QUERY_FILES)
        .checked_mul(fields.len())
        .ok_or_else(|| corrupt("metric operation count overflow"))?;
    for start in (0..operations).step_by(METRIC_DISPATCH_OPERATIONS) {
        let mut requests = Vec::new();
        let mut expected = Vec::new();
        for operation in start..(start + METRIC_DISPATCH_OPERATIONS).min(operations) {
            let offset = operation / fields.len() * METRIC_QUERY_FILES;
            let requested = &files[offset..(offset + METRIC_QUERY_FILES).min(files.len())];
            let field = fields[operation % fields.len()];
            let mut params = vec![integer(i64::from(field))];
            params.extend(
                requested
                    .iter()
                    .map(|file| turso_core::Value::Blob(file.file.file_id.as_bytes().to_vec())),
            );
            requests.push(crate::reader_engine::QueryRequest {
                sql: metric_sql(requested.len()),
                params,
                max_rows: requested.len(),
                max_bytes: requested.len() * METRIC_ROW_BYTES,
                max_row_bytes: METRIC_ROW_BYTES,
            });
            expected.push((
                offset,
                field,
                requested
                    .iter()
                    .map(|file| file.file.file_id)
                    .collect::<Vec<_>>(),
            ));
        }
        let context = reader.context.clone();
        let mut expected_fields = expected.into_iter();
        let decoded = reader
            .engine
            .query_group(requests, move |rows| {
                let (offset, field, file_ids) =
                    expected_fields.next().expect("one field per statement");
                rows.into_iter()
                    .map(|row| {
                        if row.len() != 10 {
                            return Err(corrupt("invalid metric row"));
                        }
                        let file_id = id(&row[0])?;
                        let position = file_ids
                            .iter()
                            .position(|expected| *expected == file_id)
                            .ok_or_else(|| corrupt("metric belongs to an unrequested file"))?;
                        let metric = decode_metric(&row[1..])?;
                        if metric.field_id != field {
                            return Err(corrupt("unrequested metric field"));
                        }
                        let charge = metric_charge(&metric)?;
                        let reservation = context.reserve_bytes(charge)?;
                        Ok((offset + position, metric, reservation))
                    })
                    .collect::<Result<Vec<_>, RuntimeError>>()
            })
            .await?;
        for (position, metric, reservation) in decoded.into_iter().flatten() {
            let file = &mut files[position];
            if file
                .metrics
                .iter()
                .any(|value| value.field_id == metric.field_id)
            {
                return Err(corrupt("duplicate metric field"));
            }
            retained.push(reservation);
            file.metrics.push(metric);
        }
    }
    Ok(())
}

fn metric_charge(metric: &crate::FileMetric) -> Result<usize, RuntimeError> {
    // Validated scalar/metadata values already have canonical JSON semantics.
    // Sorting object keys cannot change their compact serialized length; avoid
    // reparsing and re-encoding the metric just to retain the same byte charge.
    Ok(serde_json::to_vec(metric)
        .map_err(|error| otmp_protocol::ProtocolError::Encoding(error.to_string()))?
        .len()
        .saturating_mul(4)
        .saturating_add(256))
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
    use std::ops::Bound;

    #[test]
    fn conjunctions_keep_the_strongest_bounds_and_detect_contradictions() {
        assert_eq!(
            super::strongest_lower(Bound::Included(4), Bound::Excluded(4)),
            Bound::Excluded(4)
        );
        assert_eq!(
            super::strongest_upper(Bound::Included(9), Bound::Excluded(9)),
            Bound::Excluded(9)
        );
        assert!(!super::bounds_are_impossible(
            &Bound::Included(i64::MIN),
            &Bound::Included(i64::MIN)
        ));
        assert!(super::bounds_are_impossible(
            &Bound::Included(i64::MAX),
            &Bound::Excluded(i64::MAX)
        ));
    }

    #[test]
    fn metric_accounting_preserves_the_canonical_charge_without_reencoding() {
        use otmp_protocol::{CanonicalValue, TypedScalar};
        for scalar in [
            TypedScalar::Null,
            TypedScalar::Boolean(true),
            TypedScalar::Int32(i32::MIN),
            TypedScalar::Int64(i64::MAX),
            TypedScalar::Float32(f32::NAN),
            TypedScalar::Float64(-0.0),
            TypedScalar::Decimal {
                precision: 38,
                scale: 4,
                unscaled: vec![255; 16],
            },
            TypedScalar::String("\"\\\n\u{0000}雪".repeat(1024)),
            TypedScalar::Binary(vec![0, 1, 255]),
            TypedScalar::Fixed(vec![0; 16]),
        ] {
            let metric = crate::FileMetric {
                field_id: u32::MAX,
                column_size_bytes: Some(i64::MAX.cast_unsigned()),
                value_count: None,
                null_count: Some(0),
                nan_count: None,
                distinct_count: None,
                lower_bound: Some(scalar.clone()),
                upper_bound: Some(scalar),
                metadata: [(
                    "z\n雪".into(),
                    CanonicalValue::Array(vec![
                        CanonicalValue::Integer(i128::from(i64::MIN)),
                        CanonicalValue::String("\"\\\n".into()),
                    ]),
                )]
                .into(),
            };
            let previous = otmp_protocol::canonical_json::to_vec(&metric)
                .unwrap()
                .len()
                * 4
                + 256;
            assert_eq!(super::metric_charge(&metric).unwrap(), previous);
        }
    }

    #[test]
    fn first_branch_batch_exposes_invalid_zero_id_membership() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!(
                "../../../spec/OTMP-0.0.2-alpha-table-schema.sql"
            ))
            .unwrap();
        connection.execute_batch("PRAGMA foreign_keys=OFF;
            INSERT INTO otmp_files(file_id,file_kind,uri,file_format,file_size_bytes,record_count,schema_id,partition_spec_id,partition_values_cbor,partition_hash,data_sequence_number,file_sequence_number,created_snapshot_id,created_version)
            VALUES(zeroblob(16),'data','data/invalid.parquet','parquet',1,1,1,0,X'A0',zeroblob(32),0,1,zeroblob(16),1);
            INSERT INTO otmp_ref_live_files VALUES ('main',zeroblob(16),zeroblob(16),0,1);").unwrap();
        let mut statement = connection.prepare(super::BRANCH_FIRST_SQL).unwrap();
        let mut rows = statement.query(rusqlite::params!["main", 256]).unwrap();
        let row = rows
            .next()
            .unwrap()
            .expect("malformed membership must reach descriptor validation");
        let bytes: Vec<u8> = row.get(0).unwrap();
        assert!(super::id(&turso_core::Value::Blob(bytes)).is_err());
    }

    #[test]
    fn ranged_historical_query_keeps_remove_rows() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(include_str!(
                "../../../spec/OTMP-0.0.2-alpha-table-schema.sql"
            ))
            .unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys=OFF;
                 INSERT INTO otmp_files(file_id,file_kind,uri,file_format,file_size_bytes,record_count,schema_id,partition_spec_id,partition_values_cbor,partition_hash,data_sequence_number,file_sequence_number,created_snapshot_id,created_version)
                 VALUES(X'01010101010101010101010101010101','data','data/remove.parquet','parquet',1,1,1,0,X'A0',zeroblob(32),0,1,X'02020202020202020202020202020202',1);
                 INSERT INTO otmp_snapshot_file_changes(snapshot_id,file_id,change_kind)
                 VALUES(X'02020202020202020202020202020202',X'01010101010101010101010101010101','remove');
                 INSERT INTO otmp_file_metrics(file_id,field_id,ordered_bound_type,ordered_lower_i64,ordered_upper_i64,metadata_json)
                 VALUES(X'01010101010101010101010101010101',1,'int64',0,1,'{}');",
            )
            .unwrap();
        let mut statement = connection
            .prepare(&super::ranged_historical_sql(1))
            .unwrap();
        let mut rows = statement
            .query(rusqlite::params![
                1_i64,
                "int64",
                50_i64,
                1_i64,
                rusqlite::types::Null,
                1_i64,
                vec![2_u8; 16],
                rusqlite::types::Null,
                256_i64,
            ])
            .unwrap();

        assert_eq!(
            rows.next().unwrap().unwrap().get::<_, String>(0).unwrap(),
            "remove"
        );
    }

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
    #[allow(
        clippy::too_many_lines,
        reason = "one sparse multi-batch fixture verifies indexed access, association and bounded query count"
    )]
    async fn metric_reads_are_batched_and_preserve_sparse_file_associations() {
        use std::collections::BTreeMap;
        let table = crate::Table::new(crate::InMemoryObjectStore::default());
        let schema = serde_json::from_value(serde_json::json!({
            "schema_id": 1,
            "fields": [
                {"field_id": 1, "name": "id", "required": true, "type": {"type": "int64"}},
                {"field_id": 2, "name": "other", "required": false, "type": {"type": "int64"}}
            ],
            "identifier_field_ids": [1]
        }))
        .unwrap();
        table
            .initialize(crate::InitializeRequest::new(schema))
            .await
            .unwrap();
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"metric fixture").unwrap();
        let metrics: Vec<Vec<crate::FileMetric>> = (0..33)
            .map(|i| {
                (1..=2)
                    .filter(|field| (i + field) % 3 != 0)
                    .map(|field| crate::FileMetric {
                        field_id: u32::try_from(field).unwrap(),
                        column_size_bytes: Some(8),
                        value_count: Some(1),
                        null_count: Some(0),
                        nan_count: None,
                        distinct_count: Some(1),
                        lower_bound: Some(otmp_protocol::TypedScalar::Int64(i * 10 + field)),
                        upper_bound: Some(otmp_protocol::TypedScalar::Int64(i * 10 + field)),
                        metadata: BTreeMap::new(),
                    })
                    .collect()
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
        let result = Box::pin(table.append_files(&crate::AppendRequest::new("metrics", files)))
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
        let plan = reader
            .engine
            .query(
                &format!("EXPLAIN QUERY PLAN {}", super::metric_sql(2)),
                vec![
                    super::integer(1),
                    turso_core::Value::Blob(vec![1; 16]),
                    turso_core::Value::Blob(vec![2; 16]),
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
                .any(|line| line.contains("(file_id=? AND field_id=?)")),
            "metrics must seek by both keys: {details:?}"
        );
        let before = reader.engine.query_count();
        let dispatches_before = reader.engine.dispatch_count();
        let batch = reader.files(None, &[1, 2], 256).await.unwrap();
        assert_eq!(batch.files.len(), 33);
        for file in &batch.files {
            assert_eq!(file.metrics, expected[&file.file.file_id]);
        }
        let queries = reader.engine.query_count() - before;
        assert!(
            queries <= 7,
            "one membership query plus at most six bounded metric queries, got {queries}"
        );
        assert_eq!(
            reader.engine.dispatch_count() - dispatches_before,
            3,
            "one membership dispatch and two bounded metric groups"
        );
    }
}
