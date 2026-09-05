use super::{
    BTreeSet, CanonicalValue, Id, PinnedTable, RefType, RuntimeError, Serialize, canonical_json,
    id_from_blob, image, nonnegative, string, transactions,
};
use rusqlite::{Connection, OptionalExtension};
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SnapshotDescriptor {
    pub snapshot_id: Id,
    pub parent_snapshot_id: Option<Id>,
    pub sequence_number: u64,
    pub committed_table_version: u64,
    pub schema_id: u32,
    pub partition_spec_id: u32,
    pub sort_order_id: u32,
    pub operation: String,
    pub committed_at_ms: i64,
    pub summary: CanonicalValue,
    pub metadata: CanonicalValue,
}
fn descriptor(connection: &Connection, id: Id) -> Result<SnapshotDescriptor, RuntimeError> {
    let row = connection.query_row("SELECT parent_snapshot_id,sequence_number,committed_table_version,schema_id,partition_spec_id,sort_order_id,operation,committed_at_ms,summary_json,metadata_json FROM otmp_snapshots WHERE snapshot_id=?1",[id.as_bytes().as_slice()], |r| Ok((r.get::<_,Option<Vec<u8>>>(0)?,r.get::<_,i64>(1)?,r.get::<_,i64>(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get::<_,String>(8)?,r.get::<_,String>(9)?))).optional()?.ok_or(RuntimeError::SnapshotNotFound)?;
    Ok(SnapshotDescriptor {
        snapshot_id: id,
        parent_snapshot_id: row.0.map(id_from_blob).transpose()?,
        sequence_number: nonnegative(row.1, "sequence")?,
        committed_table_version: nonnegative(row.2, "version")?,
        schema_id: row.3,
        partition_spec_id: row.4,
        sort_order_id: row.5,
        operation: row.6,
        committed_at_ms: row.7,
        summary: canonical_json::parse_canonical(row.8.as_bytes())?,
        metadata: canonical_json::parse_canonical(row.9.as_bytes())?,
    })
}
pub(crate) fn ancestry(
    connection: &Connection,
    mut tip: Option<Id>,
) -> Result<Vec<Id>, RuntimeError> {
    let mut seen = BTreeSet::new();
    let mut chain = Vec::new();
    let mut last_sequence = u64::MAX;
    while let Some(id) = tip {
        if !seen.insert(id) {
            return Err(RuntimeError::Corrupt("snapshot ancestry cycle".into()));
        }
        let snapshot = descriptor(connection, id).map_err(|e| match e {
            RuntimeError::SnapshotNotFound => {
                RuntimeError::Corrupt("dangling snapshot ancestry".into())
            }
            other => other,
        })?;
        if snapshot.sequence_number >= last_sequence {
            return Err(RuntimeError::Corrupt(
                "nondecreasing snapshot ancestry".into(),
            ));
        }
        last_sequence = snapshot.sequence_number;
        chain.push(id);
        tip = snapshot.parent_snapshot_id;
    }
    Ok(chain)
}
pub(super) fn validate_append_rebase(
    parent: &PinnedTable,
    name: &str,
    original: Option<(RefType, Option<Id>)>,
    version: u64,
) -> Result<(), RuntimeError> {
    let connection = image::open_readonly(&parent.image.path)?;
    let Some((RefType::Branch, old)) = original else {
        return Err(RuntimeError::SemanticConflict(
            "append target was not a branch".into(),
        ));
    };
    let Some((RefType::Branch, current)) = transactions::ref_row(&connection, name)? else {
        return Err(RuntimeError::SemanticConflict(
            "append target removed".into(),
        ));
    };
    let chain = ancestry(&connection, current)?;
    if old.is_some_and(|id| !chain.contains(&id)) {
        return Err(RuntimeError::SemanticConflict(
            "target is not an append descendant".into(),
        ));
    }
    // Even moving away and back to the same tip invalidates the prepared append.
    let mut stmt = connection
        .prepare("SELECT operation_summary_json FROM otmp_commits WHERE table_version>?1")?;
    for row in stmt.query_map(
        [i64::try_from(version).map_err(|_| RuntimeError::Corrupt("version overflow".into()))?],
        |r| r.get::<_, String>(0),
    )? {
        let operations: Vec<CanonicalValue> =
            canonical_json::from_slice_canonical(row?.as_bytes())?;
        for operation in operations {
            if let CanonicalValue::Object(fields) = operation {
                if fields.get("ref") == Some(&string(name))
                    && fields.get("type") != Some(&string("commit_snapshot"))
                {
                    return Err(RuntimeError::SemanticConflict(
                        "target ref changed during append".into(),
                    ));
                }
                if fields.get("type") == Some(&string("set_current_schema")) {
                    return Err(RuntimeError::SemanticConflict(
                        "current schema changed during append".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}
