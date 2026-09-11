use crate::reader_engine::Engine;
use crate::{RuntimeError, SnapshotDescriptor, SnapshotSelection};
use otmp_protocol::{CanonicalValue, Id};
const BUDGET: usize = 256 * 1024;
pub(crate) async fn resolve(
    engine: &Engine,
    selection: SnapshotSelection,
    table_version: u64,
) -> Result<(Option<SnapshotDescriptor>, Option<String>), RuntimeError> {
    let result = match selection {
        SnapshotSelection::Ref(name) => {
            let rows = engine
                .query(
                    "SELECT ref_type,snapshot_id FROM otmp_refs WHERE ref_name=?1",
                    vec![text(name.clone())],
                    1,
                    BUDGET,
                )
                .await?;
            let [row] = rows.as_slice() else {
                return Err(RuntimeError::RefNotFound(name));
            };
            if row.len() != 2 {
                return Err(corrupt("invalid ref row"));
            }
            let kind = string(&row[0], "ref type")?;
            if !matches!(kind.as_str(), "branch" | "tag") {
                return Err(corrupt("invalid ref type"));
            }
            let descriptor = match optional_id(&row[1])? {
                Some(id) => Some(descriptor(engine, id).await?),
                None => None,
            };
            if kind == "tag" && descriptor.is_none() {
                return Err(corrupt("tag has no selected snapshot"));
            }
            (descriptor, if kind == "branch" { Some(name) } else { None })
        }
        SnapshotSelection::SnapshotId(id) => (Some(descriptor(engine, id).await?), None),
        SnapshotSelection::SequenceNumber(sequence) => {
            if sequence == 0 {
                return Err(RuntimeError::SnapshotNotFound);
            }
            let rows = engine
                .query(
                    "SELECT snapshot_id FROM otmp_snapshots WHERE sequence_number=?1",
                    vec![integer(sequence)?],
                    2,
                    BUDGET,
                )
                .await?;
            match rows.as_slice() {
                [] => return Err(RuntimeError::SnapshotNotFound),
                [row] if row.len() == 1 => (Some(descriptor(engine, id(&row[0])?).await?), None),
                _ => return Err(corrupt("duplicate snapshot sequence")),
            }
        }
    };
    if result
        .0
        .as_ref()
        .is_some_and(|d| d.committed_table_version > table_version)
    {
        Err(corrupt("snapshot belongs to newer metadata"))
    } else {
        Ok(result)
    }
}
pub(crate) async fn descriptor(
    engine: &Engine,
    snapshot_id: Id,
) -> Result<SnapshotDescriptor, RuntimeError> {
    let rows=engine.query("SELECT parent_snapshot_id,sequence_number,committed_table_version,schema_id,partition_spec_id,sort_order_id,operation,committed_at_ms,summary_json,metadata_json FROM otmp_snapshots WHERE snapshot_id=?1",vec![turso_core::Value::Blob(snapshot_id.as_bytes().to_vec())],1,BUDGET).await?;
    let [r] = rows.as_slice() else {
        return Err(RuntimeError::SnapshotNotFound);
    };
    if r.len() != 10 {
        return Err(corrupt("invalid snapshot row"));
    }
    let result = SnapshotDescriptor {
        snapshot_id,
        parent_snapshot_id: optional_id(&r[0])?,
        sequence_number: positive(&r[1])?,
        committed_table_version: nonnegative(&r[2])?,
        schema_id: u32::try_from(nonnegative(&r[3])?).map_err(|_| corrupt("schema ID"))?,
        partition_spec_id: u32::try_from(nonnegative(&r[4])?)
            .map_err(|_| corrupt("partition ID"))?,
        sort_order_id: u32::try_from(nonnegative(&r[5])?).map_err(|_| corrupt("sort ID"))?,
        operation: string(&r[6], "operation")?,
        committed_at_ms: i64v(&r[7])?,
        summary: json(&r[8], "summary")?,
        metadata: json(&r[9], "metadata")?,
    };
    if result.committed_table_version == 0
        || result.schema_id == 0
        || result.partition_spec_id != 0
        || result.sort_order_id != 0
        || result.operation != "append"
        || !matches!(result.summary, CanonicalValue::Object(_))
        || !matches!(result.metadata, CanonicalValue::Object(_))
    {
        return Err(corrupt(
            "snapshot is outside the append-only unpartitioned profile",
        ));
    }
    Ok(result)
}
fn corrupt(s: &str) -> RuntimeError {
    RuntimeError::Corrupt(s.into())
}
fn integer(v: u64) -> Result<turso_core::Value, RuntimeError> {
    Ok(turso_core::Value::Numeric(turso_core::Numeric::Integer(
        i64::try_from(v).map_err(|_| corrupt("integer"))?,
    )))
}
fn i64v(v: &turso_core::Value) -> Result<i64, RuntimeError> {
    match v {
        turso_core::Value::Numeric(turso_core::Numeric::Integer(n)) => Ok(*n),
        _ => Err(corrupt("integer")),
    }
}
fn nonnegative(v: &turso_core::Value) -> Result<u64, RuntimeError> {
    u64::try_from(i64v(v)?).map_err(|_| corrupt("negative integer"))
}
fn positive(v: &turso_core::Value) -> Result<u64, RuntimeError> {
    let n = nonnegative(v)?;
    if n == 0 {
        Err(corrupt("nonpositive sequence"))
    } else {
        Ok(n)
    }
}
fn text(s: String) -> turso_core::Value {
    turso_core::Value::build_text(s)
}
fn string(v: &turso_core::Value, n: &str) -> Result<String, RuntimeError> {
    match v {
        turso_core::Value::Text(s) => Ok(s.to_string()),
        _ => Err(corrupt(n)),
    }
}
fn id(v: &turso_core::Value) -> Result<Id, RuntimeError> {
    match v {
        turso_core::Value::Blob(b) => {
            Id::try_from_bytes(b.clone().try_into().map_err(|_| corrupt("snapshot ID"))?)
                .map_err(RuntimeError::from)
        }
        _ => Err(corrupt("snapshot ID")),
    }
}
fn optional_id(v: &turso_core::Value) -> Result<Option<Id>, RuntimeError> {
    if matches!(v, turso_core::Value::Null) {
        Ok(None)
    } else {
        id(v).map(Some)
    }
}
fn json(v: &turso_core::Value, n: &str) -> Result<CanonicalValue, RuntimeError> {
    otmp_protocol::canonical_json::from_slice_canonical(string(v, n)?.as_bytes())
        .map_err(RuntimeError::from)
}
