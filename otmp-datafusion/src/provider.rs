/// Controls bounded `DataFusion` scan planning for an OTMP table provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOptions {
    /// Maximum retained file-descriptor bytes while planning one scan.
    pub planning_budget_bytes: usize,
    /// Maximum validated object versions, Parquet footers, schemas, and bindings
    /// retained by this provider.
    pub footer_cache_bytes: usize,
    /// Disable file-metric pruning to obtain an unpruned native scan for qualification.
    pub file_pruning: bool,
    /// Admitted preflights across scans sharing this provider (1..=32).
    pub preflight_concurrency: usize,
}

impl Default for ProviderOptions {
    fn default() -> Self {
        Self {
            planning_budget_bytes: 64 * 1024 * 1024,
            footer_cache_bytes: crate::footer::DEFAULT_FOOTER_CACHE_BYTES,
            file_pruning: true,
            preflight_concurrency: 8,
        }
    }
}

/// A `DataFusion` table provider pinned to one OTMP metadata generation.
///
/// Planning and execution retain the selected generation and immutable file schemas.
pub struct OtmpTableProvider<S> {
    reader: std::sync::Arc<otmp::MetadataReader<S>>,
    schema: datafusion::arrow::datatypes::SchemaRef,
    options: ProviderOptions,
    footer_cache: std::sync::Arc<crate::footer::FooterCache>,
    metrics: ProviderCounters,
    preflight: std::sync::Arc<tokio::sync::Semaphore>,
    active_scans: std::sync::atomic::AtomicUsize,
    io: std::sync::Arc<crate::store::ReadCounters>,
    #[cfg(test)]
    hooks: tests::Hooks,
}

/// Retains the physical-plan charge for selected file descriptors. A clone is
/// attached to every `DataFusion` file descriptor, so plan rewrites and repeated
/// execution cannot release the charge prematurely.
#[derive(Clone, Debug)]
struct PlanningReservation {
    _reservation: std::sync::Arc<datafusion::execution::memory_pool::MemoryReservation>,
}

const DESCRIPTOR_OVERHEAD_BYTES: usize = 512;

fn descriptor_charge(uri: &str) -> usize {
    uri.len()
        .saturating_mul(4)
        .saturating_add(std::mem::size_of::<otmp::ReaderFile>())
        .saturating_add(std::mem::size_of::<
            datafusion::datasource::listing::PartitionedFile,
        >())
        .saturating_add(DESCRIPTOR_OVERHEAD_BYTES)
}

fn elapsed_micros(started: std::time::Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}

impl<S: otmp::ObjectStore> OtmpTableProvider<S> {
    /// Opens and pins one OTMP metadata generation and selected snapshot.
    pub async fn open(
        table: &otmp::Table<S>,
        metadata: otmp::MetadataSelection,
        snapshot: otmp::SnapshotSelection,
        reader_options: otmp::ReaderOptions,
        options: ProviderOptions,
    ) -> datafusion::error::Result<Self> {
        let reader = table
            .open_metadata_reader(metadata, snapshot, reader_options)
            .await
            .map_err(external)?;
        Self::from_reader(reader, options)
    }

    /// Binds the provider to an already selected, authenticated OTMP reader.
    /// Reusing the provider therefore retains the same metadata generation and
    /// snapshot; a caller opens a fresh reader to observe a later generation.
    pub fn from_reader(
        reader: otmp::MetadataReader<S>,
        options: ProviderOptions,
    ) -> datafusion::error::Result<Self> {
        if options.planning_budget_bytes == 0 {
            return Err(datafusion::error::DataFusionError::Plan(
                "OTMP scan planning budget must be positive".into(),
            ));
        }
        if !(1..=32).contains(&options.preflight_concurrency) {
            return Err(datafusion::error::DataFusionError::Plan(
                "OTMP preflight concurrency must be in 1..=32".into(),
            ));
        }
        let preflight =
            std::sync::Arc::new(tokio::sync::Semaphore::new(options.preflight_concurrency));
        let schema = schema_to_arrow(reader.schema())?;
        let footer_cache = crate::footer::FooterCache::new(options.footer_cache_bytes)?;
        Ok(Self {
            reader: std::sync::Arc::new(reader),
            schema,
            options,
            preflight,
            active_scans: std::sync::atomic::AtomicUsize::new(0),
            footer_cache,
            metrics: ProviderCounters::default(),
            io: std::sync::Arc::new(crate::store::ReadCounters::default()),
            #[cfg(test)]
            hooks: tests::Hooks::default(),
        })
    }

    /// The immutable metadata reader retained for this provider's lifetime.
    pub fn reader(&self) -> &otmp::MetadataReader<S> {
        &self.reader
    }

    /// Configured bounded scan-planning policy.
    pub const fn options(&self) -> &ProviderOptions {
        &self.options
    }
}

impl<S: otmp::ObjectStore + std::fmt::Debug> OtmpTableProvider<S> {
    // Planning must retain the descriptor reservation, schema adapters, and footer
    // validation in one scope so every early return releases the same resources.
    #[allow(clippy::too_many_lines)]
    async fn plan_scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
        trace: &mut ScanTrace<'_>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use futures_util::{StreamExt, stream::FuturesUnordered};
        use std::collections::{BTreeMap, BTreeSet};
        use std::sync::Arc;
        let started = std::time::Instant::now();
        let predicate = filters
            .iter()
            .cloned()
            .reduce(datafusion::logical_expr::Expr::and)
            .map(|filter| {
                state.create_physical_expr(
                    filter,
                    &datafusion::common::DFSchema::try_from(self.schema.clone())?,
                )
            })
            .transpose()?;
        let ranges = if self.options.file_pruning {
            crate::pruning::lower_ranges(filters, self.reader.schema())
        } else {
            Vec::new()
        };
        if !ranges.is_empty() {
            self.metrics
                .catalog_pruning_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        let mut metric_fields = std::collections::BTreeSet::new();
        for filter in filters.iter().filter(|_| self.options.file_pruning) {
            for column in filter.column_refs() {
                let field = self
                    .reader
                    .schema()
                    .fields
                    .iter()
                    .find(|f| f.name == column.name)
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Plan(format!(
                            "unknown OTMP query field {}",
                            column.name
                        ))
                    })?;
                metric_fields.insert(field.field_id);
            }
        }
        let metric_fields: Vec<_> = metric_fields.into_iter().collect();
        let reservation = Arc::new(
            datafusion::execution::memory_pool::MemoryConsumer::new("otmp scan descriptors")
                .register(&state.runtime_env().memory_pool),
        );
        let query = Arc::new(self.reader.schema().clone());
        let mut descriptor_bytes = 0usize;
        let mut cursor = None;
        let mut descriptors = BTreeMap::new();
        let mut groups: BTreeMap<u32, Vec<PartitionedFile>> = BTreeMap::new();
        let mut schemas = BTreeMap::new();
        let mut pending = std::collections::VecDeque::new();
        let mut active = FuturesUnordered::new();
        let mut admission = FuturesUnordered::new();
        let mut admission_started = started;
        let mut metadata = FuturesUnordered::new();
        let mut objects = BTreeMap::new();
        let mut validated = BTreeMap::new();
        metadata.push(self.read_batch(None, &metric_fields, &ranges, BTreeSet::new()));
        loop {
            if pending.is_empty() && metadata.is_empty() && cursor.is_some() {
                metadata.push(self.read_batch(
                    cursor.take(),
                    &metric_fields,
                    &ranges,
                    schemas.keys().copied().collect(),
                ));
            }
            // Do not let a broad scan replenish the whole rolling window
            // while peers are still retrieving metadata. Existing permits
            // drain normally; the provider semaphore remains the hard bound.
            let share = (self.options.preflight_concurrency
                / self
                    .active_scans
                    .load(std::sync::atomic::Ordering::Relaxed)
                    .max(1))
            .max(1);
            if !pending.is_empty() && admission.is_empty() && active.len() < share {
                admission_started = std::time::Instant::now();
                admission.push(self.preflight.clone().acquire_owned());
            }
            if metadata.is_empty() && active.is_empty() && pending.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                Some(result) = active.next(), if !active.is_empty() => {
                    let (entry, validation_us): (std::sync::Arc<crate::footer::ValidatedFileEntry>, u64) = result?;
                    trace.validation_us += validation_us;
                    objects.insert(entry.object.uri.to_string(), entry.object.clone());
                    validated.insert(entry.object.uri.to_string(), entry);
                    // A rolling window can stay ready on local/warm storage.
                    // Return to peer scans after one completion even when the
                    // caller drives several query futures from a single task.
                    tokio::task::yield_now().await;
                }
                Some(result) = metadata.next(), if !metadata.is_empty() => {
                    let (batch, new_schemas) = result?;
                    trace.batches += 1;
                    for (id, schema) in new_schemas {
                        reserve_descriptors(&reservation, &mut descriptor_bytes,
                            1024 + schema.fields.len() * 256, self.options.planning_budget_bytes)?;
                        schemas.insert(id, schema);
                    }
                    self.metrics.considered.fetch_add(batch.files.len() as u64, std::sync::atomic::Ordering::Relaxed);
                    let keep = if self.options.file_pruning {
                        if let Some(predicate) = &predicate {
                            crate::pruning::prune(predicate.clone(), self.reader.schema(), self.schema.clone(), &batch.files, &schemas)?
                        } else { vec![true; batch.files.len()] }
                    } else { vec![true; batch.files.len()] };
                    for (file, keep) in batch.files.into_iter().zip(keep) {
                        // Catalog pruning has already rejected unrelated files.
                        // The registry retains the candidate-local identity check.
                        reserve_descriptors(&reservation, &mut descriptor_bytes,
                            descriptor_charge(file.file.uri.as_str()), self.options.planning_budget_bytes)?;
                        let identity = (file.file.content_sha256, file.file.file_size_bytes);
                        let entry = descriptors.entry(file.file.uri.to_string()).or_insert((identity, None));
                        if entry.0 != identity {
                            return Err(external(otmp::RuntimeError::Corrupt("conflicting immutable data descriptors use the same URI".into())));
                        }
                        if !keep {
                            self.metrics.pruned.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            continue;
                        }
                        if entry.1.is_none() {
                            entry.1 = Some(file.schema_id);
                            pending.push_back((file.file.file_id, file.file.uri.clone(), identity, file.schema_id));
                        }
                        let planned = PartitionedFile::new(file.file.uri.as_str(), file.file.file_size_bytes)
                            .with_extension(PlanningReservation { _reservation: reservation.clone() });
                        groups.entry(file.schema_id).or_default().push(planned);
                    }
                    cursor = batch.next_cursor;
                    // The FileBatch metric reservations drop at this branch's
                    // end; only compact, planning-charged candidates remain.
                }
                Some(permit) = admission.next(), if !admission.is_empty() => {
                    trace.admission_us += elapsed_micros(admission_started);
                    let permit = permit.map_err(|_| external(otmp::RuntimeError::Cancelled))?;
                    let (file_id, uri, (hash, length), schema_id) = pending.pop_front().expect("one pending admission");
                    active.push(self.preflight_object(file_id, uri, hash, length, schema_id, schemas[&schema_id].clone(), permit, query.clone()));
                }
            }
        }
        if groups.is_empty() {
            let schema = match projection {
                Some(indices) => Arc::new(self.schema.project(indices)?),
                None => self.schema.clone(),
            };
            self.metrics.planning_micros.fetch_add(
                elapsed_micros(started),
                std::sync::atomic::Ordering::Relaxed,
            );
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                schema,
            )));
        }
        let bridge = Arc::new(crate::store::ReadOnlyStore::from_validated(
            self.reader.store().clone(),
            objects,
            self.io.clone(),
        ));
        let factory = Arc::new(
            crate::footer::FooterReaderFactory::new(bridge, self.footer_cache.clone())
                .with_validated(validated.values().cloned()),
        );
        let mut plans = Vec::new();
        for (schema_id, files) in groups {
            let mut bindings = Vec::new();
            bindings.extend(files.iter().filter_map(|file| {
                validated
                    .get(&file.object_meta.location.to_string())
                    .map(|entry| (entry.physical.clone(), entry.binding.clone()))
            }));
            for file in &files {
                if descriptors[&file.object_meta.location.to_string()].1 != Some(schema_id) {
                    use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
                    use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
                    let waited = std::time::Instant::now();
                    let _permit = self
                        .preflight
                        .acquire()
                        .await
                        .map_err(|_| external(otmp::RuntimeError::Cancelled))?;
                    trace.admission_us += elapsed_micros(waited);
                    let mut reader = factory.as_ref().clone().for_preflight().create_reader(
                        0,
                        file.clone(),
                        None,
                        &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
                    )?;
                    let footer = reader
                        .get_metadata(None)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    let validation_started = std::time::Instant::now();
                    #[cfg(test)]
                    tests::pause(&self.hooks.binding).await;
                    let physical = datafusion::parquet::arrow::parquet_to_arrow_schema(
                        footer.file_metadata().schema_descr(),
                        footer.file_metadata().key_value_metadata(),
                    )?;
                    let physical = Arc::new(physical);
                    let binding = crate::schemaadapter::OtmpAdapterFactory::new(
                        query.clone(),
                        schemas[&schema_id].clone(),
                    )
                    .create(self.schema.clone(), physical.clone())?;
                    bindings.push((physical, binding));
                    trace.validation_us += elapsed_micros(validation_started);
                }
            }
            let adapter = Arc::new(crate::schemaadapter::ValidatedAdapterFactory::new(bindings));
            let mut source = ParquetSource::new(self.schema.clone())
                .with_parquet_file_reader_factory(factory.clone());
            if let Some(predicate) = &predicate {
                source = source.with_predicate(predicate.clone());
            }
            let config =
                FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                    .with_file_groups(
                        files
                            .into_iter()
                            .map(|file| {
                                datafusion::datasource::physical_plan::FileGroup::new(vec![file])
                            })
                            .collect(),
                    )
                    .with_expr_adapter(Some(adapter))
                    .with_projection_indices(projection.cloned())?
                    .with_limit(if filters.is_empty() { limit } else { None })
                    .build();
            plans.push(DataSourceExec::from_data_source(config)
                as Arc<dyn datafusion::physical_plan::ExecutionPlan>);
        }
        self.metrics.planning_micros.fetch_add(
            elapsed_micros(started),
            std::sync::atomic::Ordering::Relaxed,
        );
        if plans.len() == 1 {
            Ok(plans.pop().unwrap())
        } else {
            datafusion::physical_plan::union::UnionExec::try_new(plans)
        }
    }

    async fn read_batch(
        &self,
        cursor: Option<otmp::FileCursor>,
        fields: &[u32],
        ranges: &[otmp::FileMetricRange],
        known: std::collections::BTreeSet<u32>,
    ) -> datafusion::error::Result<(
        otmp::FileBatch,
        std::collections::BTreeMap<u32, std::sync::Arc<otmp_protocol::Schema>>,
    )> {
        let batch = self
            .reader
            .files_matching(cursor, fields, ranges, 256)
            .await
            .map_err(external)?;
        #[cfg(test)]
        let batch = {
            let mut batch = batch;
            if let Some(edit) = &self.hooks.batch {
                edit(&mut batch.files);
            }
            batch
        };
        let mut schemas = std::collections::BTreeMap::new();
        for file in &batch.files {
            if !known.contains(&file.schema_id) && !schemas.contains_key(&file.schema_id) {
                schemas.insert(
                    file.schema_id,
                    self.reader
                        .file_schema(file.schema_id)
                        .await
                        .map_err(external)?,
                );
            }
        }
        Ok((batch, schemas))
    }
    #[allow(clippy::too_many_arguments)] // The cache key components stay explicit at this trust boundary.
    async fn preflight_object(
        &self,
        file_id: otmp_protocol::Id,
        uri: otmp_protocol::RelativeUri,
        hash: Option<otmp_protocol::Sha256>,
        length: u64,
        schema_id: u32,
        schema: std::sync::Arc<otmp_protocol::Schema>,
        _permit: tokio::sync::OwnedSemaphorePermit,
        query: std::sync::Arc<otmp_protocol::Schema>,
    ) -> datafusion::error::Result<(std::sync::Arc<crate::footer::ValidatedFileEntry>, u64)> {
        use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
        use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
        use std::sync::Arc;
        let key = crate::footer::ValidatedFileKey {
            file_id,
            uri: uri.to_string(),
            sha256: hash,
            length,
            schema_id,
        };
        let validation_started = std::time::Instant::now();
        let load_key = key.clone();
        let entry = self
            .footer_cache
            .validated_or_load(key, async {
                let bridge = Arc::new(
                    crate::store::ReadOnlyStore::with_counters(
                        self.reader.store().clone(),
                        [(uri.clone(), hash, length)],
                        self.io.clone(),
                    )
                    .await
                    .map_err(external)?,
                );
                #[cfg(test)]
                tests::pause(&self.hooks.after_pin).await;
                let factory = crate::footer::FooterReaderFactory::new(
                    bridge.clone(),
                    self.footer_cache.clone(),
                )
                .for_preflight();
                let mut reader = factory.create_reader(
                    0,
                    datafusion::datasource::listing::PartitionedFile::new(uri.as_str(), length),
                    None,
                    &datafusion::physical_plan::metrics::ExecutionPlanMetricsSet::new(),
                )?;
                let metadata = reader.get_metadata(None).await.map_err(|error| {
                    datafusion::error::DataFusionError::External(Box::new(error))
                })?;
                #[cfg(test)]
                tests::pause(&self.hooks.validation).await;
                let physical = Arc::new(datafusion::parquet::arrow::parquet_to_arrow_schema(
                    metadata.file_metadata().schema_descr(),
                    metadata.file_metadata().key_value_metadata(),
                )?);
                let binding_charge = crate::schemaadapter::binding_charge(&query)?;
                let binding = crate::schemaadapter::OtmpAdapterFactory::new(query, schema)
                    .create(self.schema.clone(), physical.clone())?;
                let object = bridge
                    .pinned_objects()
                    .next()
                    .expect("one pinned object")
                    .clone();
                let footer_key =
                    bridge.footer_identity(&object_store::path::Path::from(uri.as_str()))?;
                self.footer_cache
                    .insert_validated(
                        load_key,
                        object,
                        &footer_key,
                        physical,
                        binding,
                        binding_charge,
                    )
                    .await
            })
            .await?;
        Ok((entry, elapsed_micros(validation_started)))
    }
}

#[cfg(test)]
#[path = "provider_tests.rs"]
mod tests;

impl<S> std::fmt::Debug for OtmpTableProvider<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OtmpTableProvider")
            .field("schema", &self.schema)
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl<S> datafusion::catalog::TableProvider for OtmpTableProvider<S>
where
    S: otmp::ObjectStore + std::fmt::Debug,
{
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    #[tracing::instrument(name = "otmp.scan", skip_all, fields(scan_id = NEXT_SCAN.fetch_add(1, std::sync::atomic::Ordering::Relaxed)))]
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        self.active_scans
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut trace = ScanTrace {
            active_scans: &self.active_scans,
            started: std::time::Instant::now(),
            admission_us: 0,
            validation_us: 0,
            batches: 0,
            outcome: "cancelled",
        };
        let result = self
            .plan_scan(state, projection, filters, limit, &mut trace)
            .await;
        trace.outcome = if result.is_ok() { "success" } else { "error" };
        result
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&datafusion::logical_expr::Expr],
    ) -> datafusion::error::Result<Vec<datafusion::logical_expr::TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| datafusion::logical_expr::TableProviderFilterPushDown::Inexact)
            .collect())
    }
}

fn external(error: otmp::RuntimeError) -> datafusion::error::DataFusionError {
    datafusion::error::DataFusionError::External(Box::new(error))
}

/// Converts the selected OTMP schema into its Arrow query-schema equivalent.
///
/// The conversion is deliberately performed at provider construction so an
/// unsupported OTMP type fails before `DataFusion` starts planning a query.
pub fn schema_to_arrow(
    schema: &otmp_protocol::Schema,
) -> datafusion::error::Result<datafusion::arrow::datatypes::SchemaRef> {
    use datafusion::arrow::datatypes::Schema;
    Ok(std::sync::Arc::new(Schema::new(
        schema
            .fields
            .iter()
            .map(field_to_arrow)
            .collect::<datafusion::error::Result<Vec<_>>>()?,
    )))
}

fn field_to_arrow(
    field: &otmp_protocol::Field,
) -> datafusion::error::Result<datafusion::arrow::datatypes::Field> {
    use datafusion::arrow::datatypes::{DataType, Field};
    let data_type = match &field.field_type {
        otmp_protocol::LogicalType::Boolean => DataType::Boolean,
        otmp_protocol::LogicalType::Int32 => DataType::Int32,
        otmp_protocol::LogicalType::Int64 => DataType::Int64,
        otmp_protocol::LogicalType::Float32 => DataType::Float32,
        otmp_protocol::LogicalType::Float64 => DataType::Float64,
        otmp_protocol::LogicalType::Decimal { precision, scale } => {
            let precision = u8::try_from(*precision).map_err(|_| {
                datafusion::error::DataFusionError::Plan(
                    "decimal precision exceeds Arrow Decimal256".into(),
                )
            })?;
            if precision > 76 {
                return Err(datafusion::error::DataFusionError::Plan(
                    "decimal precision exceeds Arrow Decimal256".into(),
                ));
            }
            let scale = i8::try_from(*scale).map_err(|_| {
                datafusion::error::DataFusionError::Plan(
                    "decimal scale exceeds Arrow Decimal256".into(),
                )
            })?;
            if precision <= 38 {
                DataType::Decimal128(precision, scale)
            } else {
                DataType::Decimal256(precision, scale)
            }
        }
        otmp_protocol::LogicalType::Date => DataType::Date32,
        otmp_protocol::LogicalType::TimeMicros => {
            DataType::Time64(datafusion::arrow::datatypes::TimeUnit::Microsecond)
        }
        otmp_protocol::LogicalType::TimestampMicros => {
            DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None)
        }
        otmp_protocol::LogicalType::TimestamptzMicros => DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Microsecond,
            Some("UTC".into()),
        ),
        otmp_protocol::LogicalType::String => DataType::Utf8,
        otmp_protocol::LogicalType::Binary => DataType::Binary,
        otmp_protocol::LogicalType::Fixed { length } => {
            DataType::FixedSizeBinary(i32::try_from(*length).map_err(|_| {
                datafusion::error::DataFusionError::Plan("fixed binary width exceeds Arrow".into())
            })?)
        }
        otmp_protocol::LogicalType::Uuid => DataType::FixedSizeBinary(16),
        otmp_protocol::LogicalType::Struct { fields } => DataType::Struct(
            fields
                .iter()
                .map(field_to_arrow)
                .collect::<datafusion::error::Result<Vec<_>>>()?
                .into_iter()
                .map(std::sync::Arc::new)
                .collect::<Vec<_>>()
                .into(),
        ),
        otmp_protocol::LogicalType::List { element } => {
            DataType::List(std::sync::Arc::new(field_to_arrow(element)?))
        }
        otmp_protocol::LogicalType::Map { key, value } => {
            let entries = Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        std::sync::Arc::new(field_to_arrow(key)?),
                        std::sync::Arc::new(field_to_arrow(value)?),
                    ]
                    .into(),
                ),
                false,
            );
            DataType::Map(std::sync::Arc::new(entries), false)
        }
    };
    Ok(Field::new(&field.name, data_type, !field.required)
        .with_metadata([("PARQUET:field_id".into(), field.field_id.to_string())].into()))
}

#[derive(Default)]
struct ProviderCounters {
    considered: std::sync::atomic::AtomicU64,
    pruned: std::sync::atomic::AtomicU64,
    catalog_pruning_scans: std::sync::atomic::AtomicU64,
    planning_micros: std::sync::atomic::AtomicU64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderStatistics {
    /// File descriptors returned across the authenticated metadata-reader boundary.
    pub files_considered: u64,
    /// Additional files rejected by the provider's conservative client-side oracle.
    pub files_pruned: u64,
    /// Scans that supplied at least one supported range to catalog selection.
    pub catalog_pruning_scans: u64,
    pub planning_micros: u64,
    /// Validated data-object bytes, including Parquet footers during planning.
    pub parquet_bytes: u64,
    pub parquet_requests: u64,
    /// Native readers that requested data ranges. Repeated execution counts again.
    pub files_opened: u64,
    pub footer_cache_bytes: usize,
    pub peak_footer_cache_bytes: usize,
    pub footer_cache_hits: u64,
    /// Files whose stat, footer, physical schema, and binding were reused.
    pub validated_file_cache_hits: u64,
}
impl<S> OtmpTableProvider<S> {
    pub fn metrics(&self) -> ProviderStatistics {
        use std::sync::atomic::Ordering::Relaxed;
        let (footer_cache_bytes, peak_footer_cache_bytes, footer_cache_hits) =
            self.footer_cache.statistics();
        ProviderStatistics {
            files_considered: self.metrics.considered.load(Relaxed),
            files_pruned: self.metrics.pruned.load(Relaxed),
            catalog_pruning_scans: self.metrics.catalog_pruning_scans.load(Relaxed),
            planning_micros: self.metrics.planning_micros.load(Relaxed),
            parquet_bytes: self.io.bytes.load(Relaxed),
            parquet_requests: self.io.requests.load(Relaxed),
            files_opened: self.io.files_opened.load(Relaxed),
            footer_cache_bytes,
            peak_footer_cache_bytes,
            footer_cache_hits,
            validated_file_cache_hits: self.footer_cache.validated_hits(),
        }
    }
}
fn reserve_descriptors(
    reservation: &datafusion::execution::memory_pool::MemoryReservation,
    used: &mut usize,
    amount: usize,
    limit: usize,
) -> datafusion::error::Result<()> {
    let next = used
        .checked_add(amount)
        .filter(|n| *n <= limit)
        .ok_or_else(|| {
            datafusion::error::DataFusionError::ResourcesExhausted(
                "OTMP scan planning descriptor budget exhausted".into(),
            )
        })?;
    reservation.try_grow(amount)?;
    *used = next;
    Ok(())
}

static NEXT_SCAN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
struct ScanTrace<'a> {
    active_scans: &'a std::sync::atomic::AtomicUsize,
    started: std::time::Instant,
    admission_us: u64,
    validation_us: u64,
    batches: u64,
    outcome: &'static str,
}
impl Drop for ScanTrace<'_> {
    fn drop(&mut self) {
        self.active_scans
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(target: "otmp.scan", elapsed_us = elapsed_micros(self.started),
            admission_wait_us = self.admission_us, validation_us = self.validation_us,
            metadata_batches = self.batches, outcome = self.outcome, "scan summary");
    }
}
