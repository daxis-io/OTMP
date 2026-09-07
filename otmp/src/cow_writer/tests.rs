//! Release qualification of the published engine, before writer integration.
use std::sync::Arc;
use turso_core::{Database, PlatformIO};

mod overlay {
    pub use super::super::Overlay;
    use std::sync::Arc;
    use turso_core::{CheckpointMode, Database, PlatformIO};
    pub fn execute(parent: Arc<[u8]>, sql: &str) -> turso_core::Result<Vec<u8>> {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("candidate.sqlite3");
        let storage = Arc::new(Overlay::new(parent));
        let database = Database::open(
            Arc::new(PlatformIO::new()?),
            path.to_str().unwrap(),
            storage.clone(),
        )?;
        let connection = database.connect()?;
        connection.execute(sql)?;
        connection.checkpoint(CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })?;
        connection.close()?;
        drop(connection);
        drop(database);
        Ok(storage.export())
    }
}

#[test]
fn checkpoint_failure_is_reported() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("failure.sqlite3");
    let storage = Arc::new(overlay::Overlay::new(Arc::from([])));
    let database = Database::open(
        Arc::new(PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        storage.clone(),
    )
    .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("CREATE TABLE failure(id INTEGER PRIMARY KEY); INSERT INTO failure VALUES(1);")
        .unwrap();
    storage
        .fail_writes
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let result = connection.checkpoint(turso_core::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    });
    assert!(result.is_err(), "checkpoint must propagate write failure");
    storage
        .fail_writes
        .store(false, std::sync::atomic::Ordering::Relaxed);
    connection.close().unwrap();
}

#[test]
fn overlay_export_matches_ordinary_turso_exactly() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordinary.sqlite3");
    let database =
        Database::open_file(Arc::new(PlatformIO::new().unwrap()), path.to_str().unwrap()).unwrap();
    let connection = database.connect().unwrap();
    connection.execute("PRAGMA page_size=4096; CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB); INSERT INTO items VALUES(1, x'01');").unwrap();
    connection.close().unwrap();
    drop(connection);
    drop(database);
    let parent: Arc<[u8]> = std::fs::read(&path).unwrap().into();
    let sql = "BEGIN; UPDATE items SET value=zeroblob(20000) WHERE id=1; INSERT INTO items VALUES(2, x'02'); COMMIT; BEGIN; UPDATE items SET value=x'FF'; ROLLBACK;";
    let captured = overlay::execute(parent, sql).unwrap();
    let database =
        Database::open_file(Arc::new(PlatformIO::new().unwrap()), path.to_str().unwrap()).unwrap();
    let connection = database.connect().unwrap();
    connection.execute(sql).unwrap();
    connection
        .checkpoint(turso_core::CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        })
        .unwrap();
    connection.close().unwrap();
    drop(connection);
    drop(database);
    assert!(
        captured == std::fs::read(path).unwrap(),
        "overlay differs from file-backed export"
    );
}

#[test]
fn truncate_and_regrow_never_reveals_parent_or_discarded_overlay_bytes() {
    use turso_core::{Buffer, Completion, DatabaseStorage, IOContext};
    let parent: Arc<[u8]> = vec![0xAB; 3 * 4096].into();
    let storage = overlay::Overlay::new(parent.clone());
    drop(
        storage
            .write_page(
                3,
                Arc::new(Buffer::new(vec![0xCD; 4096])),
                &IOContext::default(),
                Completion::new_write(|_| {}),
            )
            .unwrap(),
    );
    drop(
        storage
            .truncate(4100, Completion::new_trunc(|_| {}))
            .unwrap(),
    );
    drop(
        storage
            .truncate(3 * 4096, Completion::new_trunc(|_| {}))
            .unwrap(),
    );
    let actual = storage.export();
    assert_eq!(&actual[..4100], &parent[..4100]);
    assert!(actual[4100..].iter().all(|b| *b == 0));
}

#[test]
fn engine_enforces_deferred_constraints_and_partial_unique_indexes() {
    let sql = "PRAGMA foreign_keys=ON;
        CREATE TABLE parent(id INTEGER PRIMARY KEY);
        CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED, active INTEGER);
        CREATE UNIQUE INDEX active_parent ON child(parent_id) WHERE active=1;
        BEGIN; INSERT INTO child VALUES(1, 42, 1); INSERT INTO parent VALUES(42); COMMIT;";
    let bytes = overlay::execute(Arc::from([]), sql).unwrap();
    let bytes: Arc<[u8]> = bytes.into();
    assert!(
        overlay::execute(
            bytes.clone(),
            "PRAGMA foreign_keys=ON; BEGIN; INSERT INTO child VALUES(2, 99, 1); COMMIT;"
        )
        .is_err()
    );
    assert!(
        overlay::execute(
            bytes.clone(),
            "BEGIN; INSERT INTO child VALUES(2, 42, 1); COMMIT;"
        )
        .is_err()
    );
    assert!(overlay::execute(bytes, "BEGIN; INSERT INTO child VALUES(2, 42, 0); COMMIT;").is_ok());
}

#[test]
fn existing_otmp_checkpoint_supports_ordered_metadata_mutations() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../conformance/tables/transactions/_otmp/checkpoints/7");
    let path = std::fs::read_dir(root)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let parent: Arc<[u8]> = std::fs::read(path).unwrap().into();
    let sql = r#"PRAGMA foreign_keys=ON; BEGIN;
        INSERT INTO otmp_properties VALUES('cow.test','1',8) ON CONFLICT(property_key) DO UPDATE SET value_json=excluded.value_json, updated_version=excluded.updated_version;
        INSERT INTO otmp_properties VALUES('cow.test','2',8) ON CONFLICT(property_key) DO UPDATE SET value_json=excluded.value_json, updated_version=excluded.updated_version;
        INSERT INTO otmp_refs(ref_name,ref_type,snapshot_id,created_version,updated_version) VALUES('cow.branch','branch',NULL,8,8);
        UPDATE otmp_refs SET snapshot_id=(SELECT snapshot_id FROM otmp_refs WHERE ref_name='main') WHERE ref_name='cow.branch';
        INSERT INTO otmp_ref_live_files SELECT 'cow.branch',file_id,added_snapshot_id,data_sequence_number,file_sequence_number FROM otmp_ref_live_files WHERE ref_name='main';
        DELETE FROM otmp_ref_live_files WHERE ref_name='cow.branch';
        DELETE FROM otmp_refs WHERE ref_name='cow.branch';
        DELETE FROM otmp_properties WHERE property_key='cow.test';
        INSERT INTO otmp_schemas(schema_id,parent_schema_id,created_version,doc) SELECT 3,current_schema_id,8,NULL FROM otmp_meta;
        INSERT INTO otmp_fields SELECT 3,field_id,parent_field_id,name,ordinal,required,type_json,doc,initial_default_json,write_default_json FROM otmp_fields WHERE schema_id=2;
        INSERT INTO otmp_field_ids(field_id,first_schema_id,created_version) VALUES(100,3,8);
        INSERT INTO otmp_fields(schema_id,field_id,name,ordinal,required,type_json) VALUES(3,100,'cow_optional',100,0,'"string"');
        UPDATE otmp_meta SET current_schema_id=3;
        INSERT INTO otmp_snapshots(snapshot_id,parent_snapshot_id,sequence_number,schema_id,partition_spec_id,sort_order_id,operation,committed_table_version,committed_at_ms)
          SELECT x'11111111111171118111111111111111',snapshot_id,99,3,0,0,'append',8,0 FROM otmp_refs WHERE ref_name='main';
        INSERT INTO otmp_files(file_id,file_kind,uri,file_format,file_size_bytes,record_count,schema_id,partition_spec_id,sort_order_id,partition_values_cbor,partition_hash,data_sequence_number,file_sequence_number,created_snapshot_id,created_version)
          VALUES(x'22222222222272228222222222222222','data','data/cow.parquet','parquet',1,1,3,0,0,x'a0',zeroblob(32),99,99,x'11111111111171118111111111111111',8);
        INSERT INTO otmp_snapshot_file_changes VALUES(x'11111111111171118111111111111111',x'22222222222272228222222222222222','add');
        INSERT INTO otmp_ref_live_files VALUES('main',x'22222222222272228222222222222222',x'11111111111171118111111111111111',99,99);
        UPDATE otmp_refs SET snapshot_id=x'11111111111171118111111111111111',updated_version=8 WHERE ref_name='main';
        COMMIT;"#;
    let result = overlay::execute(parent.clone(), sql).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("candidate.sqlite3");
    std::fs::write(&path, &result).unwrap();
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    assert_eq!(
        sqlite
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(
        sqlite
            .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        0
    );
    let oracle_path = directory.path().join("oracle.sqlite3");
    std::fs::write(&oracle_path, &parent).unwrap();
    let oracle = rusqlite::Connection::open(&oracle_path).unwrap();
    oracle.execute_batch(sql).unwrap();
    let tables = sqlite
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    for table in tables {
        let rows = |connection: &rusqlite::Connection| {
            let mut statement = connection
                .prepare(&format!("SELECT * FROM {table}"))
                .unwrap();
            let columns = statement.column_count();
            let mut result = statement
                .query_map([], |r| {
                    (0..columns)
                        .map(|i| r.get::<_, rusqlite::types::Value>(i))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .map(|r| format!("{:?}", r.unwrap()))
                .collect::<Vec<_>>();
            result.sort();
            result
        };
        assert_eq!(rows(&sqlite), rows(&oracle), "semantic rows for {table}");
    }
}

#[test]
fn published_engine_accepts_otmp_schema_and_exports_stock_sqlite() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordinary.sqlite3");
    let database =
        Database::open_file(Arc::new(PlatformIO::new().unwrap()), path.to_str().unwrap()).unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("PRAGMA page_size=4096; PRAGMA journal_mode=WAL;")
        .unwrap();
    connection
        .execute(include_str!(
            "../../../spec/OTMP-0.0.2-alpha-table-schema.sql"
        ))
        .unwrap();
    connection.close().unwrap();
    drop(connection);
    drop(database);
    let captured = overlay::execute(
        Arc::from([]),
        &format!(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; {}",
            include_str!("../../../spec/OTMP-0.0.2-alpha-table-schema.sql")
        ),
    )
    .unwrap();
    assert!(
        captured == std::fs::read(&path).unwrap(),
        "initialization/header capture differs from ordinary Turso"
    );
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    let integrity: String = sqlite
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let count: i64 = sqlite
        .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(count, 0);
    let size: u32 = sqlite
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    assert_eq!(size, 4096);
}

#[test]
fn small_candidate_borrows_parent_and_compares_only_touched_pages() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("large.sqlite3");
    let sqlite = rusqlite::Connection::open(&path).unwrap();
    sqlite.execute_batch("CREATE TABLE items(id INTEGER PRIMARY KEY, value BLOB); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<512) INSERT INTO items SELECT x,zeroblob(4096) FROM n;").unwrap();
    drop(sqlite);
    let parent: Arc<[u8]> = std::fs::read(path).unwrap().into();
    let candidate = super::CandidateWriter::new(parent.clone(), None).unwrap();
    assert!(
        Arc::ptr_eq(&parent, &candidate.storage.parent),
        "candidate borrows the resolved parent allocation"
    );
    candidate
        .sql()
        .execute(
            "UPDATE items SET value=?1 WHERE id=256",
            rusqlite::params![vec![1u8; 4096]],
        )
        .unwrap();
    let frozen = candidate.finish().unwrap();
    assert!(
        frozen.pages_compared < 10,
        "compared {} of {} pages",
        frozen.pages_compared,
        parent.len() / 4096
    );
    assert!(frozen.changed.len() < 10);
    eprintln!(
        "capture evidence: parent={} bytes, parent writable copies=0, compared={} pages, changed={} pages",
        parent.len(),
        frozen.pages_compared,
        frozen.changed.len()
    );
    let bytes = frozen.materialize();
    assert!(bytes.len() <= parent.len() + 4096);
    let export = directory.path().join("export.sqlite3");
    std::fs::write(&export, bytes).unwrap();
    let sqlite = rusqlite::Connection::open(export).unwrap();
    assert_eq!(
        sqlite
            .query_row("SELECT value FROM items WHERE id=256", [], |r| r
                .get::<_, Vec<u8>>(0))
            .unwrap(),
        vec![1; 4096]
    );
}

#[test]
fn repeated_page_writes_replace_private_entries_and_failed_close_cannot_freeze() {
    use turso_core::{Buffer, Completion, DatabaseStorage, IOContext};
    let parent: Arc<[u8]> = vec![0xAB; 8192].into();
    let storage = overlay::Overlay::new(parent.clone());
    for value in [1, 2] {
        drop(
            storage
                .write_page(
                    2,
                    Arc::new(Buffer::new(vec![value; 4096])),
                    &IOContext::default(),
                    Completion::new_write(|_| {}),
                )
                .unwrap(),
        );
    }
    assert!(storage.export()[4096..].iter().all(|b| *b == 2));
    assert!(parent.iter().all(|b| *b == 0xAB));

    let candidate = super::CandidateWriter::new(
        Arc::from([]),
        Some("CREATE TABLE close_failure(id INTEGER PRIMARY KEY);"),
    )
    .unwrap();
    candidate
        .sql()
        .execute("INSERT INTO close_failure VALUES(1)", &[])
        .unwrap();
    candidate.connection.execute("COMMIT").unwrap();
    candidate
        .storage
        .fail_writes
        .store(true, std::sync::atomic::Ordering::Relaxed);
    assert!(candidate.close_and_freeze().is_err());
}
