/// Controls bounded `DataFusion` scan planning for an OTMP table provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderOptions {
    /// Maximum retained file-descriptor bytes while planning one scan.
    pub planning_budget_bytes: usize,
    /// Maximum decoded Parquet footer metadata retained by this provider.
    pub footer_cache_bytes: usize,
    /// Disable file-metric pruning to obtain an unpruned native scan for qualification.
    pub file_pruning: bool,
}

impl Default for ProviderOptions {
    fn default() -> Self {
        Self {
            planning_budget_bytes: 64 * 1024 * 1024,
            footer_cache_bytes: crate::footer::DEFAULT_FOOTER_CACHE_BYTES,
            file_pruning: true,
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
    io: std::sync::Arc<crate::store::ReadCounters>,
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
        let schema = schema_to_arrow(reader.schema())?;
        let footer_cache = crate::footer::FooterCache::new(options.footer_cache_bytes)?;
        Ok(Self {
            reader: std::sync::Arc::new(reader),
            schema,
            options,
            footer_cache,
            metrics: ProviderCounters::default(),
            io: std::sync::Arc::new(crate::store::ReadCounters::default()),
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

    // Planning must retain the descriptor reservation, schema adapters, and footer
    // validation in one scope so every early return releases the same resources.
    #[allow(clippy::too_many_lines)]
    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>>
    {
        use datafusion::datasource::listing::PartitionedFile;
        use datafusion::datasource::physical_plan::parquet::ParquetFileReaderFactory;
        use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
        use datafusion::datasource::source::DataSourceExec;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
        use std::collections::BTreeMap;
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
        let mut metric_fields = std::collections::BTreeSet::new();
        for filter in filters {
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
        let mut descriptor_bytes = 0usize;
        let mut cursor = None;
        let mut descriptors = Vec::new();
        let mut groups: BTreeMap<u32, Vec<PartitionedFile>> = BTreeMap::new();
        let mut schemas = BTreeMap::new();
        loop {
            let batch = self
                .reader
                .files(cursor, &metric_fields, 256)
                .await
                .map_err(external)?;
            for file in &batch.files {
                if let std::collections::btree_map::Entry::Vacant(entry) =
                    schemas.entry(file.schema_id)
                {
                    let schema = self
                        .reader
                        .file_schema(file.schema_id)
                        .await
                        .map_err(external)?;
                    // The metadata cache owns the immutable schema allocation;
                    // this plan retains one bounded schema reference and adapter.
                    let charge = 1024 + schema.fields.len() * 256;
                    reserve_descriptors(
                        &reservation,
                        &mut descriptor_bytes,
                        charge,
                        self.options.planning_budget_bytes,
                    )?;
                    entry.insert(schema);
                }
            }
            self.metrics.considered.fetch_add(
                batch.files.len() as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            let keep = if self.options.file_pruning {
                if let Some(predicate) = &predicate {
                    crate::pruning::prune(
                        predicate.clone(),
                        self.reader.schema(),
                        self.schema.clone(),
                        &batch.files,
                        &schemas,
                    )?
                } else {
                    vec![true; batch.files.len()]
                }
            } else {
                vec![true; batch.files.len()]
            };
            for (file, keep) in batch.files.into_iter().zip(keep) {
                if !keep {
                    self.metrics
                        .pruned
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                reserve_descriptors(
                    &reservation,
                    &mut descriptor_bytes,
                    descriptor_charge(file.file.uri.as_str()),
                    self.options.planning_budget_bytes,
                )?;
                let planned =
                    PartitionedFile::new(file.file.uri.as_str(), file.file.file_size_bytes)
                        .with_extension(PlanningReservation {
                            _reservation: reservation.clone(),
                        });
                groups.entry(file.schema_id).or_default().push(planned);
                descriptors.push((
                    file.file.uri,
                    file.file.content_sha256,
                    file.file.file_size_bytes,
                ));
            }
            cursor = batch.next_cursor;
            if cursor.is_none() {
                break;
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
        let bridge = Arc::new(
            crate::store::ReadOnlyStore::with_counters(
                self.reader.store().clone(),
                descriptors,
                self.io.clone(),
            )
            .await
            .map_err(external)?,
        );
        let factory = Arc::new(crate::footer::FooterReaderFactory::new(
            bridge,
            self.footer_cache.clone(),
        ));
        let query = Arc::new(self.reader.schema().clone());
        let mut plans = Vec::new();
        let metrics = ExecutionPlanMetricsSet::new();
        for (schema_id, files) in groups {
            let adapter = Arc::new(crate::schemaadapter::OtmpAdapterFactory::new(
                query.clone(),
                schemas[&schema_id].clone(),
            ));
            // Validate all query fields, including projected-away required fields,
            // through the same bounded footer cache used during execution.
            for file in &files {
                let mut reader = factory.create_reader(0, file.clone(), None, &metrics)?;
                let metadata = reader
                    .get_metadata(None)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let physical = datafusion::parquet::arrow::parquet_to_arrow_schema(
                    metadata.file_metadata().schema_descr(),
                    metadata.file_metadata().key_value_metadata(),
                )?;
                adapter.create(self.schema.clone(), Arc::new(physical))?;
            }
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
    planning_micros: std::sync::atomic::AtomicU64,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProviderStatistics {
    pub files_considered: u64,
    pub files_pruned: u64,
    pub planning_micros: u64,
    /// Validated data-object bytes, including Parquet footers during planning.
    pub parquet_bytes: u64,
    pub parquet_requests: u64,
    /// Native readers that requested data ranges. Repeated execution counts again.
    pub files_opened: u64,
    pub footer_cache_bytes: usize,
    pub peak_footer_cache_bytes: usize,
    pub footer_cache_hits: u64,
}
impl<S> OtmpTableProvider<S> {
    pub fn metrics(&self) -> ProviderStatistics {
        use std::sync::atomic::Ordering::Relaxed;
        let (footer_cache_bytes, peak_footer_cache_bytes, footer_cache_hits) =
            self.footer_cache.statistics();
        ProviderStatistics {
            files_considered: self.metrics.considered.load(Relaxed),
            files_pruned: self.metrics.pruned.load(Relaxed),
            planning_micros: self.metrics.planning_micros.load(Relaxed),
            parquet_bytes: self.io.bytes.load(Relaxed),
            parquet_requests: self.io.requests.load(Relaxed),
            files_opened: self.io.files_opened.load(Relaxed),
            footer_cache_bytes,
            peak_footer_cache_bytes,
            footer_cache_hits,
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
