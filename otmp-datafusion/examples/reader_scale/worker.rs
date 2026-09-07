use super::fixture::Error;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunConfig {
    pub survivors: Option<usize>,
    pub pruning: bool,
    pub passes: usize,
    pub delay_ms: u64,
    pub metadata_inflight: usize,
    pub engine_page_cache_bytes: usize,
    pub metadata_budget: usize,
    pub planning_budget: usize,
    pub record_budget: usize,
    pub df_pool_bytes: usize,
    pub execute: bool,
}
impl Default for RunConfig {
    fn default() -> Self {
        Self {
            survivors: None,
            pruning: true,
            passes: 1,
            delay_ms: 0,
            metadata_inflight: 8,
            engine_page_cache_bytes: 4 * 1024 * 1024,
            metadata_budget: 64 * 1024 * 1024,
            planning_budget: 64 * 1024 * 1024,
            record_budget: 1024 * 1024,
            df_pool_bytes: 256 * 1024 * 1024,
            execute: true,
        }
    }
}
// Keep timed phases in order so setup and cleanup cannot move across timer boundaries.
#[allow(clippy::too_many_lines)]
pub async fn run(root: &Path, config: RunConfig) -> Result<Value, Error> {
    let worker_started = Instant::now();
    if !(1..=20).contains(&config.passes)
        || !(1..=8).contains(&config.metadata_inflight)
        || config.delay_ms > 10_000
        || config.df_pool_bytes == 0
    {
        return Err("invalid run configuration".into());
    }
    let fixture = fixture::load(root)?;
    let files = usize::try_from(
        fixture["files"]
            .as_u64()
            .ok_or("missing fixture file count")?,
    )?;
    let rows = usize::try_from(
        fixture["rows_per_file"]
            .as_u64()
            .ok_or("missing fixture row count")?,
    )?;
    let (first, expected_count, expected_sum) = fixture::expected(files, rows, config.survivors)?;
    let sql =
        format!("SELECT count(*) AS n, coalesce(sum(id), 0) AS total FROM t WHERE id >= {first}");
    let store = MeasuredStore::new(
        LocalObjectStore::new(root)?,
        Duration::from_millis(config.delay_ms),
    );
    let table = Table::new(store.clone());
    let mut phases = vec![
        json!({"name":"fixture_validation","pass":0,"elapsed_ms":ms(worker_started),"io":Counts::default(),"reader":null,"provider":null,"pool_reserved_bytes":0}),
    ];
    let options = ReaderOptions {
        cache_budget_bytes: config.metadata_budget,
        maximum_record_bytes: config.record_budget,
        engine_page_cache_bytes: config.engine_page_cache_bytes,
        max_inflight_reads: config.metadata_inflight,
        ..ReaderOptions::default()
    };
    let started = Instant::now();
    let io_before = store.snapshot();
    let opened = OtmpTableProvider::open(
        &table,
        MetadataSelection::Current,
        SnapshotSelection::Ref("main".into()),
        options,
        ProviderOptions {
            planning_budget_bytes: config.planning_budget,
            file_pruning: config.pruning,
            ..ProviderOptions::default()
        },
    )
    .await;
    phases.push(phase(
        "registration",
        0,
        started,
        &store,
        &io_before,
        opened
            .as_ref()
            .ok()
            .map(|p| reader_delta(p.reader().statistics(), ReaderStatistics::default())),
        None,
        0,
    ));
    let provider = match opened {
        Ok(provider) => Arc::new(provider),
        Err(error) => {
            let details = error_details("registration", &error);
            let started = Instant::now();
            let before = store.snapshot();
            drop(table);
            phases.push(phase(
                "teardown", 0, started, &store, &before, None, None, 0,
            ));
            let violations = teardown_violations(&store.snapshot(), 0);
            return Ok(
                json!({"violations":violations,"outcome":"error","fixture":fixture,"config":config,"phases":phases,"error":details,"result":null,"worker_elapsed_ms":ms(worker_started),"sql":sql,"target_partitions":4}),
            );
        }
    };
    let started = Instant::now();
    let before = store.snapshot();
    let pool = Arc::new(GreedyMemoryPool::new(config.df_pool_bytes));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(pool.clone())
        .build_arc()?;
    let context =
        SessionContext::new_with_config_rt(SessionConfig::new().with_target_partitions(4), runtime);
    context.register_table("t", provider.clone())?;
    phases.push(phase(
        "setup",
        0,
        started,
        &store,
        &before,
        None,
        None,
        pool.reserved(),
    ));
    let mut error = None;
    let mut result = None;
    for pass in 0..config.passes {
        let started = Instant::now();
        let before = store.snapshot();
        let reader_before = provider.reader().statistics();
        let provider_before = provider.metrics();
        let planned = async { context.sql(&sql).await?.create_physical_plan().await }.await;
        phases.push(phase(
            "planning",
            pass,
            started,
            &store,
            &before,
            Some(reader_delta(provider.reader().statistics(), reader_before)),
            Some(provider_delta(provider.metrics(), provider_before)),
            pool.reserved(),
        ));
        let plan = match planned {
            Ok(plan) => plan,
            Err(e) => {
                error = Some(error_details("planning", &e));
                break;
            }
        };
        if config.execute {
            let started = Instant::now();
            let before = store.snapshot();
            let reader_before = provider.reader().statistics();
            let provider_before = provider.metrics();
            let batches =
                datafusion::physical_plan::collect(plan.clone(), context.task_ctx()).await;
            phases.push(phase(
                "execution",
                pass,
                started,
                &store,
                &before,
                Some(reader_delta(provider.reader().statistics(), reader_before)),
                Some(provider_delta(provider.metrics(), provider_before)),
                pool.reserved(),
            ));
            match batches {
                Err(e) => {
                    error = Some(error_details("execution", &e));
                }
                Ok(batches) => {
                    let values = (batches.len() == 1)
                        .then(|| &batches[0])
                        .filter(|b| b.num_rows() == 1)
                        .and_then(|batch| {
                            let n = batch.column(0).as_any().downcast_ref::<Int64Array>()?;
                            let total = batch.column(1).as_any().downcast_ref::<Int64Array>()?;
                            Some((n.value(0), total.value(0)))
                        });
                    if values == Some((expected_count, expected_sum)) {
                        result = Some(json!({"count":expected_count,"sum":expected_sum}));
                    } else {
                        error = Some(
                            json!({"stage":"execution","code":"QUALIFICATION_RESULT_MISMATCH","message":format!("expected ({expected_count},{expected_sum}), received {values:?}")}),
                        );
                    }
                }
            }
        }
        let started = Instant::now();
        let before = store.snapshot();
        drop(plan);
        phases.push(phase(
            "plan_release",
            pass,
            started,
            &store,
            &before,
            None,
            None,
            pool.reserved(),
        ));
        if error.is_some() {
            break;
        }
    }
    let reader = provider.reader().statistics();
    let mut violations = Vec::new();
    if reader.peak_cache_bytes > config.metadata_budget {
        violations.push("metadata cache exceeded its budget");
    }
    if provider.metrics().peak_footer_cache_bytes > ProviderOptions::default().footer_cache_bytes {
        violations.push("footer cache exceeded its budget");
    }
    let started = Instant::now();
    let before = store.snapshot();
    drop(context);
    drop(provider);
    drop(table);
    tokio::task::yield_now().await;
    phases.push(phase(
        "teardown",
        0,
        started,
        &store,
        &before,
        None,
        None,
        pool.reserved(),
    ));
    violations.extend(teardown_violations(&store.snapshot(), pool.reserved()));
    if error.is_none() && !violations.is_empty() {
        error = Some(
            json!({"stage":"validation","code":"QUALIFICATION_INVARIANT_VIOLATION","message":violations.join("; ")}),
        );
    }
    Ok(
        json!({"violations":violations,"outcome":if error.is_some(){"error"}else{"success"},"fixture":fixture,"config":config,"phases":phases,"error":error,"result":result,"worker_elapsed_ms":ms(worker_started),"sql":sql,"target_partitions":4}),
    )
}
#[cfg(test)]
mod tests {
    use super::super::fixture::{self, PrepareConfig};
    use super::*;
    #[tokio::test]
    async fn optimized_and_unpruned_runs_match_with_warm_pins_and_full_phase_accounting() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 4,
                rows_per_file: 3,
                batch_size: 2,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        for pruning in [true, false] {
            let result = run(
                &root,
                RunConfig {
                    survivors: Some(2),
                    pruning,
                    passes: 2,
                    ..RunConfig::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(result["outcome"], "success", "{result}");
            assert_eq!(result["violations"], json!([]));
            assert_eq!(result["result"], json!({"count":6,"sum":51}));
            let phases = result["phases"].as_array().unwrap();
            let planning: Vec<_> = phases.iter().filter(|p| p["name"] == "planning").collect();
            assert_eq!(planning.len(), 2);
            assert_eq!(planning[0]["provider"]["files_considered"], 4);
            assert_eq!(
                planning[0]["provider"]["files_pruned"],
                if pruning { 2 } else { 0 }
            );
            assert!(planning[0]["pool_reserved_bytes"].as_u64().unwrap() > 0);
            assert!(phases.iter().all(|p| p["io"]["full_reads"] == 0));
            assert_eq!(phases.last().unwrap()["pool_reserved_bytes"], 0);
        }
    }
    #[tokio::test]
    async fn planning_exhaustion_is_a_recorded_outcome_and_releases_reservations() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 2,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let result = run(
            &root,
            RunConfig {
                planning_budget: 1,
                ..RunConfig::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result["outcome"], "error");
        assert_eq!(result["violations"], json!([]));
        assert_eq!(result["error"]["stage"], "planning");
        assert_eq!(result["error"]["code"], "DATAFUSION_RESOURCES_EXHAUSTED");
        assert_eq!(
            result["phases"].as_array().unwrap().last().unwrap()["pool_reserved_bytes"],
            0
        );
    }
    #[tokio::test]
    async fn large_current_commit_failure_and_small_tail_success_are_both_preserved() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 0,
                property_bytes: 2 * 1024 * 1024,
                small_tail: false,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let failed = run(&root, RunConfig::default()).await.unwrap();
        assert_eq!(failed["outcome"], "error");
        assert_eq!(failed["violations"], json!([]));
        assert_eq!(failed["error"]["stage"], "registration");
        assert_eq!(failed["error"]["code"], "OTMP_RESOURCE_EXHAUSTED");
        fixture::tail(&root).await.unwrap();
        let ok = run(&root, RunConfig::default()).await.unwrap();
        assert_eq!(ok["outcome"], "success", "{ok}");
        assert_eq!(ok["result"], json!({"count":0,"sum":0}));
    }
}

use super::{
    fixture,
    transport::{Counts, MeasuredStore},
};
use datafusion::error::DataFusionError;
use datafusion::execution::{
    memory_pool::{GreedyMemoryPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion::{
    arrow::array::Int64Array,
    prelude::{SessionConfig, SessionContext},
};
use otmp::{
    LocalObjectStore, MetadataSelection, ReaderOptions, ReaderStatistics, SnapshotSelection, Table,
};
use otmp_datafusion::{OtmpTableProvider, ProviderOptions, ProviderStatistics};
use std::{
    error::Error as StdError,
    sync::Arc,
    time::{Duration, Instant},
};
fn ms(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}
#[allow(clippy::too_many_arguments)]
fn phase(
    name: &str,
    pass: usize,
    start: Instant,
    store: &MeasuredStore<LocalObjectStore>,
    before: &Counts,
    reader: Option<Value>,
    provider: Option<Value>,
    reserved: usize,
) -> Value {
    let elapsed = ms(start);
    let mut value = json!({"name":name,"pass":pass,"elapsed_ms":elapsed,"io":store.snapshot().delta(before),"pool_reserved_bytes":reserved});
    value["reader"] = reader.unwrap_or(Value::Null);
    value["provider"] = provider.unwrap_or(Value::Null);
    value
}
fn reader_delta(after: ReaderStatistics, before: ReaderStatistics) -> Value {
    json!({"bytes":after.bytes-before.bytes,"requests":after.requests-before.requests,"pages":after.pages-before.pages,"cache_hits":after.cache_hits-before.cache_hits,"cache_bytes":after.cache_bytes,"peak_cache_bytes":after.peak_cache_bytes})
}
fn provider_delta(after: ProviderStatistics, before: ProviderStatistics) -> Value {
    json!({"planning_micros":after.planning_micros-before.planning_micros,"files_considered":after.files_considered-before.files_considered,"files_pruned":after.files_pruned-before.files_pruned,"files_opened":after.files_opened-before.files_opened,"parquet_bytes":after.parquet_bytes-before.parquet_bytes,"parquet_requests":after.parquet_requests-before.parquet_requests,"footer_cache_hits":after.footer_cache_hits-before.footer_cache_hits,"footer_cache_bytes":after.footer_cache_bytes,"peak_footer_cache_bytes":after.peak_footer_cache_bytes})
}
fn error_details(stage: &str, error: &DataFusionError) -> Value {
    let mut current: &(dyn StdError + 'static) = error;
    let mut code = "DATAFUSION_ERROR";
    loop {
        if let Some(runtime) = current.downcast_ref::<otmp::RuntimeError>() {
            code = runtime.code();
            break;
        }
        if let Some(DataFusionError::ResourcesExhausted(_)) =
            current.downcast_ref::<DataFusionError>()
        {
            code = "DATAFUSION_RESOURCES_EXHAUSTED";
            break;
        }
        if let Some(next) = current.source() {
            current = next;
        } else {
            break;
        }
    }
    json!({"stage":stage,"code":code,"message":error.to_string()})
}

fn teardown_violations(io: &Counts, reserved: usize) -> Vec<&'static str> {
    let mut violations = Vec::new();
    if reserved != 0 {
        violations.push("planning memory remains reserved");
    }
    if io.active != 0 {
        violations.push("I/O remains active");
    }
    if io.full_reads != 0 {
        violations.push("a forbidden full read was attempted");
    }
    violations
}
#[test]
fn teardown_checks_report_all_secondary_failures() {
    let io = Counts {
        active: 1,
        full_reads: 1,
        ..Counts::default()
    };
    assert_eq!(teardown_violations(&io, 1).len(), 3);
    assert!(teardown_violations(&Counts::default(), 0).is_empty());
}
