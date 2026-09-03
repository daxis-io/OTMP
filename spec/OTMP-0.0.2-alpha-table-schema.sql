PRAGMA application_id = 1330924880; -- 0x4F544D50 ("OTMP")
PRAGMA user_version = 2;
PRAGMA foreign_keys = ON;

-- One checkpoint describes exactly one self-contained OTMP table.
CREATE TABLE otmp_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    protocol TEXT NOT NULL CHECK (protocol = 'otmp'),
    protocol_version TEXT NOT NULL,
    table_id BLOB NOT NULL UNIQUE CHECK (length(table_id) = 16),
    table_version INTEGER NOT NULL CHECK (table_version >= 0),
    semantic_state_sha256 BLOB NOT NULL CHECK (length(semantic_state_sha256) = 32),
    last_commit_id BLOB NOT NULL CHECK (length(last_commit_id) = 16),
    last_commit_sha256 BLOB NOT NULL CHECK (length(last_commit_sha256) = 32),
    last_sequence_number INTEGER NOT NULL DEFAULT 0 CHECK (last_sequence_number >= 0),
    current_schema_id INTEGER NOT NULL CHECK (current_schema_id > 0),
    default_partition_spec_id INTEGER NOT NULL CHECK (default_partition_spec_id >= 0),
    default_sort_order_id INTEGER NOT NULL CHECK (default_sort_order_id >= 0),
    created_at_ms INTEGER NOT NULL,
    required_reader_features_json TEXT NOT NULL DEFAULT '[]',
    required_writer_features_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE otmp_commits (
    table_version INTEGER PRIMARY KEY CHECK (table_version >= 0),
    commit_id BLOB NOT NULL UNIQUE CHECK (length(commit_id) = 16),
    parent_table_version INTEGER,
    created_at_ms INTEGER NOT NULL,
    intent_count INTEGER NOT NULL DEFAULT 1 CHECK (intent_count > 0),
    semantic_state_sha256 BLOB NOT NULL UNIQUE CHECK (length(semantic_state_sha256) = 32),
    commit_object_uri TEXT NOT NULL,
    commit_object_sha256 BLOB NOT NULL CHECK (length(commit_object_sha256) = 32),
    operation_summary_json TEXT NOT NULL DEFAULT '[]',
    result_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    CHECK (
        (table_version = 0 AND parent_table_version IS NULL)
        OR
        (table_version > 0 AND parent_table_version = table_version - 1)
    )
) STRICT;

CREATE TABLE otmp_idempotency (
    idempotency_key TEXT PRIMARY KEY,
    intent_sha256 BLOB NOT NULL CHECK (length(intent_sha256) = 32),
    commit_id BLOB NOT NULL CHECK (length(commit_id) = 16),
    table_version INTEGER NOT NULL,
    result_json TEXT NOT NULL,
    FOREIGN KEY (table_version) REFERENCES otmp_commits(table_version),
    FOREIGN KEY (commit_id) REFERENCES otmp_commits(commit_id),
    UNIQUE (commit_id, idempotency_key)
) STRICT;

CREATE TABLE otmp_properties (
    property_key TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_version INTEGER NOT NULL CHECK (updated_version >= 0)
) STRICT;

CREATE TABLE otmp_features (
    feature_name TEXT NOT NULL,
    requirement TEXT NOT NULL CHECK (requirement IN ('reader', 'writer', 'both')),
    enabled_version INTEGER NOT NULL CHECK (enabled_version >= 0),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (feature_name, requirement)
) STRICT;

CREATE TABLE otmp_schemas (
    schema_id INTEGER PRIMARY KEY CHECK (schema_id > 0),
    parent_schema_id INTEGER REFERENCES otmp_schemas(schema_id)
        DEFERRABLE INITIALLY DEFERRED,
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    doc TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

-- Field IDs are table-global identities and are never reused.
CREATE TABLE otmp_field_ids (
    field_id INTEGER PRIMARY KEY CHECK (field_id > 0),
    first_schema_id INTEGER NOT NULL REFERENCES otmp_schemas(schema_id),
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE otmp_fields (
    schema_id INTEGER NOT NULL REFERENCES otmp_schemas(schema_id)
        DEFERRABLE INITIALLY DEFERRED,
    field_id INTEGER NOT NULL REFERENCES otmp_field_ids(field_id),
    parent_field_id INTEGER,
    name TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    required INTEGER NOT NULL CHECK (required IN (0, 1)),
    type_json TEXT NOT NULL,
    doc TEXT,
    initial_default_json TEXT,
    write_default_json TEXT,
    PRIMARY KEY (schema_id, field_id),
    FOREIGN KEY (schema_id, parent_field_id)
        REFERENCES otmp_fields(schema_id, field_id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE UNIQUE INDEX otmp_uq_root_field_name
ON otmp_fields(schema_id, name)
WHERE parent_field_id IS NULL;

CREATE UNIQUE INDEX otmp_uq_nested_field_name
ON otmp_fields(schema_id, parent_field_id, name)
WHERE parent_field_id IS NOT NULL;

CREATE INDEX otmp_idx_fields_parent_ordinal
ON otmp_fields(schema_id, parent_field_id, ordinal);

CREATE TABLE otmp_identifier_fields (
    schema_id INTEGER NOT NULL REFERENCES otmp_schemas(schema_id),
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    field_id INTEGER NOT NULL REFERENCES otmp_field_ids(field_id),
    PRIMARY KEY (schema_id, ordinal),
    UNIQUE (schema_id, field_id)
) STRICT;

CREATE TABLE otmp_partition_specs (
    partition_spec_id INTEGER PRIMARY KEY CHECK (partition_spec_id >= 0),
    parent_partition_spec_id INTEGER REFERENCES otmp_partition_specs(partition_spec_id),
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE otmp_partition_field_ids (
    partition_field_id INTEGER PRIMARY KEY CHECK (partition_field_id > 0),
    first_partition_spec_id INTEGER NOT NULL REFERENCES otmp_partition_specs(partition_spec_id),
    created_version INTEGER NOT NULL CHECK (created_version >= 0)
) STRICT;

CREATE TABLE otmp_partition_fields (
    partition_spec_id INTEGER NOT NULL REFERENCES otmp_partition_specs(partition_spec_id)
        DEFERRABLE INITIALLY DEFERRED,
    partition_field_id INTEGER NOT NULL REFERENCES otmp_partition_field_ids(partition_field_id),
    source_field_id INTEGER NOT NULL REFERENCES otmp_field_ids(field_id),
    name TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    transform_json TEXT NOT NULL,
    result_type_json TEXT NOT NULL,
    PRIMARY KEY (partition_spec_id, partition_field_id),
    UNIQUE (partition_spec_id, name),
    UNIQUE (partition_spec_id, ordinal)
) STRICT;

CREATE TABLE otmp_sort_orders (
    sort_order_id INTEGER PRIMARY KEY CHECK (sort_order_id >= 0),
    parent_sort_order_id INTEGER REFERENCES otmp_sort_orders(sort_order_id),
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE otmp_sort_fields (
    sort_order_id INTEGER NOT NULL REFERENCES otmp_sort_orders(sort_order_id)
        DEFERRABLE INITIALLY DEFERRED,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    source_field_id INTEGER NOT NULL REFERENCES otmp_field_ids(field_id),
    transform_json TEXT NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('asc', 'desc')),
    null_order TEXT NOT NULL CHECK (null_order IN ('nulls_first', 'nulls_last')),
    PRIMARY KEY (sort_order_id, ordinal)
) STRICT;

CREATE TABLE otmp_snapshots (
    snapshot_id BLOB PRIMARY KEY CHECK (length(snapshot_id) = 16),
    parent_snapshot_id BLOB REFERENCES otmp_snapshots(snapshot_id)
        DEFERRABLE INITIALLY DEFERRED,
    sequence_number INTEGER NOT NULL UNIQUE CHECK (sequence_number > 0),
    schema_id INTEGER NOT NULL REFERENCES otmp_schemas(schema_id),
    partition_spec_id INTEGER NOT NULL REFERENCES otmp_partition_specs(partition_spec_id),
    sort_order_id INTEGER NOT NULL REFERENCES otmp_sort_orders(sort_order_id),
    operation TEXT NOT NULL CHECK (
        operation IN ('append', 'overwrite', 'rewrite', 'delete', 'update', 'merge', 'optimize', 'metadata')
    ),
    committed_table_version INTEGER NOT NULL CHECK (committed_table_version > 0),
    committed_at_ms INTEGER NOT NULL,
    scan_root_uri TEXT,
    scan_root_sha256 BLOB CHECK (scan_root_sha256 IS NULL OR length(scan_root_sha256) = 32),
    summary_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    CHECK (
        (scan_root_uri IS NULL AND scan_root_sha256 IS NULL)
        OR
        (scan_root_uri IS NOT NULL AND scan_root_sha256 IS NOT NULL)
    )
) STRICT;

CREATE INDEX otmp_idx_snapshots_sequence
ON otmp_snapshots(sequence_number);

CREATE INDEX otmp_idx_snapshots_committed_version
ON otmp_snapshots(committed_table_version);

CREATE TABLE otmp_snapshot_summary (
    snapshot_id BLOB NOT NULL REFERENCES otmp_snapshots(snapshot_id),
    summary_key TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, summary_key)
) STRICT;

CREATE TABLE otmp_refs (
    ref_name TEXT PRIMARY KEY,
    ref_type TEXT NOT NULL CHECK (ref_type IN ('branch', 'tag')),
    snapshot_id BLOB REFERENCES otmp_snapshots(snapshot_id)
        DEFERRABLE INITIALLY DEFERRED,
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    updated_version INTEGER NOT NULL CHECK (updated_version >= created_version),
    retention_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    CHECK (ref_type = 'branch' OR snapshot_id IS NOT NULL)
) STRICT;

CREATE INDEX otmp_idx_refs_snapshot
ON otmp_refs(snapshot_id);

CREATE TABLE otmp_files (
    file_id BLOB PRIMARY KEY CHECK (length(file_id) = 16),
    file_kind TEXT NOT NULL CHECK (file_kind IN ('data', 'position_delete', 'equality_delete')),
    uri TEXT NOT NULL,
    object_identity TEXT,
    file_format TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0),
    record_count INTEGER NOT NULL CHECK (record_count >= 0),
    schema_id INTEGER NOT NULL REFERENCES otmp_schemas(schema_id),
    partition_spec_id INTEGER NOT NULL REFERENCES otmp_partition_specs(partition_spec_id),
    sort_order_id INTEGER REFERENCES otmp_sort_orders(sort_order_id),
    partition_values_cbor BLOB NOT NULL,
    partition_hash BLOB NOT NULL CHECK (length(partition_hash) = 32),
    content_sha256 BLOB CHECK (content_sha256 IS NULL OR length(content_sha256) = 32),
    encryption_metadata BLOB,
    data_sequence_number INTEGER NOT NULL CHECK (data_sequence_number >= 0),
    file_sequence_number INTEGER NOT NULL CHECK (file_sequence_number > 0),
    created_snapshot_id BLOB NOT NULL REFERENCES otmp_snapshots(snapshot_id),
    created_version INTEGER NOT NULL CHECK (created_version > 0),
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE UNIQUE INDEX otmp_uq_file_uri_no_object_identity
ON otmp_files(uri)
WHERE object_identity IS NULL;

CREATE UNIQUE INDEX otmp_uq_file_uri_object_identity
ON otmp_files(uri, object_identity)
WHERE object_identity IS NOT NULL;

CREATE INDEX otmp_idx_files_partition_hash
ON otmp_files(partition_spec_id, partition_hash);

CREATE INDEX otmp_idx_files_sequence
ON otmp_files(file_sequence_number);

CREATE TABLE otmp_delete_file_details (
    file_id BLOB PRIMARY KEY REFERENCES otmp_files(file_id),
    delete_type TEXT NOT NULL CHECK (delete_type IN ('position', 'equality')),
    referenced_data_file_id BLOB REFERENCES otmp_files(file_id),
    equality_field_ids_json TEXT,
    apply_from_data_sequence INTEGER NOT NULL DEFAULT 0 CHECK (apply_from_data_sequence >= 0),
    apply_through_data_sequence INTEGER CHECK (
        apply_through_data_sequence IS NULL OR apply_through_data_sequence >= apply_from_data_sequence
    ),
    metadata_json TEXT NOT NULL DEFAULT '{}'
) STRICT;

CREATE TABLE otmp_file_metrics (
    file_id BLOB NOT NULL REFERENCES otmp_files(file_id),
    field_id INTEGER NOT NULL REFERENCES otmp_field_ids(field_id),
    column_size_bytes INTEGER CHECK (column_size_bytes IS NULL OR column_size_bytes >= 0),
    value_count INTEGER CHECK (value_count IS NULL OR value_count >= 0),
    null_count INTEGER CHECK (null_count IS NULL OR null_count >= 0),
    nan_count INTEGER CHECK (nan_count IS NULL OR nan_count >= 0),
    distinct_count INTEGER CHECK (distinct_count IS NULL OR distinct_count >= 0),
    lower_bound_cbor BLOB,
    upper_bound_cbor BLOB,
    bloom_filter_uri TEXT,
    bloom_filter_sha256 BLOB CHECK (
        bloom_filter_sha256 IS NULL OR length(bloom_filter_sha256) = 32
    ),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (file_id, field_id),
    CHECK (
        (bloom_filter_uri IS NULL AND bloom_filter_sha256 IS NULL)
        OR
        (bloom_filter_uri IS NOT NULL AND bloom_filter_sha256 IS NOT NULL)
    )
) STRICT;

CREATE TABLE otmp_snapshot_file_changes (
    snapshot_id BLOB NOT NULL REFERENCES otmp_snapshots(snapshot_id),
    file_id BLOB NOT NULL REFERENCES otmp_files(file_id),
    change_kind TEXT NOT NULL CHECK (change_kind IN ('add', 'remove')),
    PRIMARY KEY (snapshot_id, file_id, change_kind)
) STRICT;

-- Complete current membership for each mutable branch. This is a materialized read model,
-- not the historical source of truth. Tags use their snapshot scan root or snapshot changes.
CREATE TABLE otmp_ref_live_files (
    ref_name TEXT NOT NULL REFERENCES otmp_refs(ref_name),
    file_id BLOB NOT NULL REFERENCES otmp_files(file_id),
    added_snapshot_id BLOB NOT NULL REFERENCES otmp_snapshots(snapshot_id),
    data_sequence_number INTEGER NOT NULL CHECK (data_sequence_number >= 0),
    file_sequence_number INTEGER NOT NULL CHECK (file_sequence_number > 0),
    PRIMARY KEY (ref_name, file_id)
) STRICT;

CREATE INDEX otmp_idx_ref_live_file_sequence
ON otmp_ref_live_files(ref_name, file_sequence_number);

CREATE TABLE otmp_artifacts (
    artifact_id BLOB PRIMARY KEY CHECK (length(artifact_id) = 16),
    artifact_kind TEXT NOT NULL,
    snapshot_id BLOB REFERENCES otmp_snapshots(snapshot_id),
    uri TEXT NOT NULL,
    sha256 BLOB NOT NULL CHECK (length(sha256) = 32),
    format TEXT NOT NULL,
    created_version INTEGER NOT NULL CHECK (created_version >= 0),
    metadata_json TEXT NOT NULL DEFAULT '{}',
    UNIQUE (uri, sha256)
) STRICT;

CREATE VIEW otmp_live_files AS
SELECT
    r.ref_name,
    f.file_id,
    f.file_kind,
    f.uri,
    f.object_identity,
    f.file_format,
    f.file_size_bytes,
    f.record_count,
    f.schema_id,
    f.partition_spec_id,
    f.sort_order_id,
    f.partition_values_cbor,
    f.partition_hash,
    f.content_sha256,
    f.data_sequence_number,
    f.file_sequence_number,
    rf.added_snapshot_id
FROM otmp_ref_live_files rf
JOIN otmp_refs r ON r.ref_name = rf.ref_name
JOIN otmp_files f ON f.file_id = rf.file_id
WHERE r.ref_type = 'branch';
