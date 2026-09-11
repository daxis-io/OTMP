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
    pub preflight_concurrency: usize,
    pub footer_budget: usize,
    pub overlapping_survivors: Vec<Option<usize>>,
    pub data_delays_ms: std::collections::BTreeMap<String, u64>,
    pub data_fault: Option<super::transport::DataFault>,
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
            preflight_concurrency: ProviderOptions::default().preflight_concurrency,
            footer_budget: ProviderOptions::default().footer_cache_bytes,
            overlapping_survivors: Vec::new(),
            data_delays_ms: std::collections::BTreeMap::new(),
            data_fault: None,
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
        || !(1..=32).contains(&config.preflight_concurrency)
        || config.overlapping_survivors.len() > 8
        || config.data_delays_ms.iter().any(|(op, ms)| {
            !matches!(op.as_str(), "stat" | "trailer" | "footer" | "data") || *ms > 10_000
        })
        || config.data_fault.as_ref().is_some_and(|fault| {
            fault.request == 0
                || !matches!(
                    fault.operation.as_str(),
                    "stat" | "trailer" | "footer" | "data"
                )
        })
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
    )
    .with_data_controls(config.data_delays_ms.clone(), config.data_fault.clone());
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
            preflight_concurrency: config.preflight_concurrency,
            footer_cache_bytes: config.footer_budget,
            file_pruning: config.pruning,
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
    let mut overlapping_rounds = Vec::new();
    if !config.overlapping_survivors.is_empty() {
        for pass in 0..config.passes {
            let started = Instant::now();
            let before = store.snapshot();
            let queries = futures_util::future::join_all(
                config
                    .overlapping_survivors
                    .iter()
                    .enumerate()
                    .map(|(query, survivors)| {
                        timed_query(&context, files, rows, *survivors, config.execute, query)
                    }),
            )
            .await;
            let queries: Vec<_> = queries.into_iter().collect::<Result<_, _>>()?;
            if let Some(failed) = queries.iter().find_map(|query| query.get("error")) {
                error = Some(failed.clone());
            }
            overlapping_rounds.push(json!({"pass":pass,"elapsed_ms":ms(started),"queries":queries,
                "io":store.snapshot().delta(&before),"reader":reader_delta(provider.reader().statistics(),ReaderStatistics::default()),
                "provider":provider_delta(provider.metrics(),ProviderStatistics::default()),"pool_reserved_bytes":pool.reserved()}));
            if error.is_some() {
                break;
            }
        }
    }
    for pass in 0..if config.overlapping_survivors.is_empty() {
        config.passes
    } else {
        0
    } {
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
    if provider.metrics().peak_footer_cache_bytes > config.footer_budget {
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
        json!({"violations":violations,"outcome":if error.is_some(){"error"}else{"success"},"fixture":fixture,"config":config,"phases":phases,"overlapping_rounds":overlapping_rounds,"error":error,"result":result,"worker_elapsed_ms":ms(worker_started),"sql":sql,"target_partitions":4}),
    )
}
async fn timed_query(
    context: &SessionContext,
    files: usize,
    rows: usize,
    survivors: Option<usize>,
    execute: bool,
    query: usize,
) -> Result<Value, Error> {
    use futures_util::StreamExt;
    let (first, count, sum) = fixture::expected(files, rows, survivors)?;
    let sql =
        format!("SELECT count(*) AS n, coalesce(sum(id), 0) AS total FROM t WHERE id >= {first}");
    let started = Instant::now();
    let planned = async { context.sql(&sql).await?.create_physical_plan().await }.await;
    let planning_ms = ms(started);
    let plan = match planned {
        Ok(plan) => plan,
        Err(error) => {
            return Ok(
                json!({"query":query,"planning_ms":planning_ms,"complete_ms":ms(started),"error":error_details("planning", &error)}),
            );
        }
    };
    let execution = Instant::now();
    let mut first_result = None;
    let mut values = None;
    let mut batches = 0;
    if execute {
        let result = async {
            let mut stream = datafusion::physical_plan::execute_stream(plan, context.task_ctx())?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                first_result.get_or_insert_with(|| ms(started));
                batches += 1;
                if batch.num_rows() == 1 {
                    values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .zip(batch.column(1).as_any().downcast_ref::<Int64Array>())
                        .map(|(n, total)| (n.value(0), total.value(0)));
                }
            }
            Ok::<_, DataFusionError>(())
        }
        .await;
        if let Err(error) = result {
            return Ok(
                json!({"query":query,"planning_ms":planning_ms,"complete_ms":ms(started),"error":error_details("execution", &error)}),
            );
        }
        if batches != 1 || values != Some((count, sum)) {
            return Ok(
                json!({"query":query,"planning_ms":planning_ms,"complete_ms":ms(started),"error":{"stage":"execution","code":"QUALIFICATION_RESULT_MISMATCH","message":format!("expected ({count},{sum}), received {values:?} in {batches} batches")}}),
            );
        }
    }
    Ok(
        json!({"query":query,"survivors":survivors,"planning_ms":planning_ms,"planning_to_first_result_ms":first_result,
        "execution_ms":ms(execution),"complete_ms":ms(started),"result":execute.then(|| json!({"count":count,"sum":sum}))}),
    )
}
#[cfg(test)]
mod tests {
    use super::super::fixture::{self, PrepareConfig};
    use super::*;
    #[tokio::test]
    async fn small_scan_does_not_queue_behind_an_entire_broad_scan() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 16,
                rows_per_file: 1,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let store = MeasuredStore::new(LocalObjectStore::new(&root).unwrap(), Duration::ZERO)
            .with_data_controls([("footer".into(), 100)].into(), None);
        let table = Table::new(store.clone());
        let provider = OtmpTableProvider::open(
            &table,
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
            ProviderOptions {
                preflight_concurrency: 1,
                ..ProviderOptions::default()
            },
        )
        .await
        .unwrap();
        let context = SessionContext::new();
        context.register_table("t", Arc::new(provider)).unwrap();
        let broad_context = context.clone();
        let broad = tokio::spawn(async move {
            broad_context
                .sql("SELECT * FROM t")
                .await
                .unwrap()
                .create_physical_plan()
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while store
                .snapshot()
                .by_class
                .get("data")
                .is_none_or(|data| data.range_requests < 2)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let small = context
            .sql("SELECT * FROM t WHERE id >= 15")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        assert!(!broad.is_finished());
        assert!(store.snapshot().by_class["data"].stat_requests <= 4);
        drop(small);
        broad.abort();
        assert!(broad.await.unwrap_err().is_cancelled());
        assert_eq!(store.snapshot().active, 0);
    }
    #[tokio::test]
    async fn cancelling_one_scan_keeps_shared_footer_io_alive_for_its_peer() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 1,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let store = MeasuredStore::new(LocalObjectStore::new(&root).unwrap(), Duration::ZERO)
            .with_data_controls([("stat".into(), 100), ("trailer".into(), 100)].into(), None);
        let table = Table::new(store.clone());
        let provider = OtmpTableProvider::open(
            &table,
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
            ProviderOptions::default(),
        )
        .await
        .unwrap();
        let context = SessionContext::new();
        context.register_table("t", Arc::new(provider)).unwrap();
        let query = |context: SessionContext| {
            tokio::spawn(async move {
                context
                    .sql("SELECT * FROM t")
                    .await
                    .unwrap()
                    .create_physical_plan()
                    .await
            })
        };
        let first = query(context.clone());
        let second = query(context);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let io = store.snapshot();
                if io.by_class.get("data").is_some_and(|data| {
                    data.stat_requests == 2 && data.range_requests == 1 && data.active == 1
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        drop(second.await.unwrap().unwrap());
        let io = store.snapshot();
        assert_eq!(io.by_class["data"].range_requests, 2);
        assert_eq!(io.by_class["data"].cancelled, 0);
        assert_eq!(io.active, 0);
    }
    #[tokio::test]
    async fn concurrent_preflight_makes_progress_at_the_sequential_minimum_footer_budget() {
        use otmp::ObjectStore;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 8,
                rows_per_file: 3,
                footer_padding_bytes: vec![0, 4096],
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let store = LocalObjectStore::new(&root).unwrap();
        let mut budget = 0;
        for entry in std::fs::read_dir(root.join("data")).unwrap() {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            let footer =
                u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap())
                    as usize;
            let uri: otmp_protocol::RelativeUri =
                format!("data/{}", path.file_name().unwrap().to_str().unwrap())
                    .parse()
                    .unwrap();
            let metadata = store.stat(&uri).await.unwrap();
            budget = budget.max(
                footer * 128
                    + 65536
                    + 512
                    + uri.as_str().len() * 4
                    + metadata.version.as_opaque().len() * 2,
            );
        }
        for concurrency in [1, 8] {
            let config: RunConfig = serde_json::from_value(json!({
                "preflight_concurrency":concurrency,"footer_budget":budget,"execute":false,
                "data_delays_ms":{"footer":2}
            }))
            .unwrap();
            let output = run(&root, config).await.unwrap();
            assert_eq!(
                output["outcome"], "success",
                "concurrency {concurrency}, budget {budget}: {output}"
            );
        }
        let store = MeasuredStore::new(store, Duration::ZERO);
        let [entered, release] = store.pause_next_trailer();
        let table = Table::new(store.clone());
        let provider = OtmpTableProvider::open(
            &table,
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            ReaderOptions::default(),
            ProviderOptions {
                footer_cache_bytes: budget,
                ..ProviderOptions::default()
            },
        )
        .await
        .unwrap();
        let context = SessionContext::new();
        context.register_table("t", Arc::new(provider)).unwrap();
        let query = |context: SessionContext, sql: &'static str| {
            tokio::spawn(
                async move { context.sql(sql).await.unwrap().create_physical_plan().await },
            )
        };
        let small = query(context.clone(), "SELECT * FROM t WHERE id < 3");
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .unwrap();
        let mut large = query(context, "SELECT * FROM t WHERE id >= 3 AND id < 6");
        tokio::select! {
            result = &mut large => panic!("large preflight completed before the small trailer released its key: {result:?}"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        release.notify_one();
        drop(small.await.unwrap().unwrap());
        drop(large.await.unwrap().unwrap());
        assert_eq!(store.snapshot().active, 0);
    }
    #[tokio::test]
    async fn metadata_enumeration_overlaps_preflight_across_batch_boundary() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 257,
                rows_per_file: 1,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let config: RunConfig = serde_json::from_value(json!({
            "preflight_concurrency": 2,"execute":false,"delay_ms":1,
            "data_delays_ms":{"footer":10}
        }))
        .unwrap();
        let output = run(&root, config).await.unwrap();
        assert_eq!(output["outcome"], "success", "{output}");
        let planning = output["phases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == "planning")
            .unwrap();
        assert_eq!(planning["provider"]["files_considered"], 257);
        assert!(
            planning["io"]["metadata_data_overlaps"].as_u64().unwrap() > 0,
            "metadata and data must overlap at the same instant, not just have different historical peaks"
        );
    }
    #[tokio::test]
    async fn simultaneous_scans_share_one_validated_file_fill() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 1,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let config: RunConfig = serde_json::from_value(json!({
            "preflight_concurrency": 8, "execute": false,
            "overlapping_survivors": [null, null, null, null, null, null, null, null],
            "data_delays_ms": {"stat": 10, "trailer": 10, "footer": 10}
        }))
        .unwrap();
        let output = run(&root, config).await.unwrap();
        assert_eq!(output["outcome"], "success", "{output}");
        let io = &output["overlapping_rounds"][0]["io"]["by_class"]["data"];
        assert_eq!(io["stat_requests"], 1);
        assert_eq!(io["range_requests"], 2);
        assert_eq!(io["active"], 0);
    }
    #[tokio::test]
    async fn admitted_preflights_overlap_and_release_under_a_single_decode_budget() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 8,
                rows_per_file: 3,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        for concurrency in [1, 2, 4, 8, 16, 32] {
            let config: RunConfig = serde_json::from_value(json!({
                "preflight_concurrency": concurrency,
                "footer_budget": 200_000,
                "data_delays_ms": {"stat": 1,"trailer": 1,"footer": 1}
            }))
            .unwrap();
            let output = run(&root, config).await.unwrap();
            assert_eq!(output["outcome"], "success", "{output}");
            let planning = output["phases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|p| p["name"] == "planning")
                .unwrap();
            let peak = planning["io"]["by_class"]["data"]["peak_inflight"]
                .as_u64()
                .unwrap();
            assert!(peak <= concurrency as u64);
            if concurrency > 1 {
                assert!(peak > 1);
            }
            assert_eq!(planning["provider"]["files_considered"], 8);
            assert_eq!(output["result"], json!({"count":24,"sum":276}));
            assert_eq!(output["violations"], json!([]));
        }
    }
    #[tokio::test]
    async fn overlapping_queries_have_direct_timers_and_one_shared_io_scope() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("table");
        fixture::prepare(
            &root,
            PrepareConfig {
                files: 4,
                rows_per_file: 3,
                ..PrepareConfig::default()
            },
        )
        .await
        .unwrap();
        let config: RunConfig = serde_json::from_value(json!({
            "overlapping_survivors": [null, 1, 1, 1], "passes": 2,
            "preflight_concurrency": 1
        }))
        .unwrap();
        let output = run(&root, config).await.unwrap();
        assert_eq!(output["outcome"], "success", "{output}");
        let rounds = output["overlapping_rounds"].as_array().unwrap();
        assert_eq!(rounds.len(), 2);
        for round in rounds {
            let queries = round["queries"].as_array().unwrap();
            assert_eq!(queries.len(), 4);
            assert_eq!(queries[0]["result"], json!({"count":12,"sum":66}));
            for query in queries {
                assert!(
                    query.get("io").is_none(),
                    "physical accounting belongs to the round"
                );
                assert!(query["planning_ms"].as_f64().unwrap() >= 0.0);
                assert!(
                    query["planning_to_first_result_ms"].as_f64().unwrap()
                        >= query["planning_ms"].as_f64().unwrap()
                );
                assert!(
                    query["complete_ms"].as_f64().unwrap()
                        >= query["planning_to_first_result_ms"].as_f64().unwrap()
                );
            }
            assert!(round["io"]["stat_requests"].as_u64().unwrap() > 0);
            assert_eq!(round["io"]["active"], 0);
        }
        assert_eq!(output["violations"], json!([]));
    }
    #[tokio::test]
    async fn targeted_preflight_failures_are_recorded_and_release_requests() {
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
        for operation in ["stat", "trailer", "footer"] {
            let config: RunConfig = serde_json::from_value(json!({
                "data_fault": {"operation": operation, "request": 1},
                "data_delays_ms": {"stat": 1, "trailer": 2, "footer": 3}
            }))
            .unwrap();
            let output = run(&root, config).await.unwrap();
            assert_eq!(output["outcome"], "error", "{output}");
            assert_eq!(output["error"]["stage"], "planning");
            assert_eq!(output["violations"], json!([]));
            assert!(
                output["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("injected")
            );
        }
    }
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
            assert_eq!(
                planning[0]["provider"]["files_considered"],
                if pruning { 2 } else { 4 }
            );
            assert_eq!(planning[0]["provider"]["files_pruned"], 0);
            assert_eq!(
                planning[0]["provider"]["catalog_pruning_scans"],
                u64::from(pruning)
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
    json!({"planning_micros":after.planning_micros-before.planning_micros,"files_considered":after.files_considered-before.files_considered,"files_pruned":after.files_pruned-before.files_pruned,"catalog_pruning_scans":after.catalog_pruning_scans-before.catalog_pruning_scans,"files_opened":after.files_opened-before.files_opened,"parquet_bytes":after.parquet_bytes-before.parquet_bytes,"parquet_requests":after.parquet_requests-before.parquet_requests,"footer_cache_hits":after.footer_cache_hits-before.footer_cache_hits,"validated_file_cache_hits":after.validated_file_cache_hits-before.validated_file_cache_hits,"footer_cache_bytes":after.footer_cache_bytes,"peak_footer_cache_bytes":after.peak_footer_cache_bytes})
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
