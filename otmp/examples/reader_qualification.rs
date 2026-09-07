//! Local qualification for authenticated lazy metadata reads.

use std::process::Command;
use std::time::Instant;

use otmp::reader::ReaderOptions;
use otmp::{LocalObjectStore, MetadataSelection, SnapshotSelection, Table};
use otmp_protocol::{Generation, Head, canonical_json};

fn maximum_rss_kib(stderr: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(stderr).ok()?;
    text.lines().find_map(|line| {
        let bytes: u64 = line
            .trim()
            .strip_suffix(" maximum resident set size")?
            .trim()
            .parse()
            .ok()?;
        // Darwin's `time -l` reports bytes; the JSON contract reports KiB.
        Some(bytes / 1024)
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    let worker = arguments.first().is_some_and(|value| value == "--worker");
    let path = arguments
        .get(usize::from(worker))
        .cloned()
        .unwrap_or_else(|| "conformance/tables/indexed".into());
    if !worker {
        let output = Command::new("/usr/bin/time")
            .args([
                "-l",
                &std::env::current_exe()?.to_string_lossy(),
                "--worker",
                &path,
            ])
            .output()?;
        let json = String::from_utf8_lossy(&output.stdout);
        let peak = maximum_rss_kib(&output.stderr).unwrap_or_default();
        let worker = if json.trim().is_empty() {
            "null"
        } else {
            json.trim()
        };
        println!("{{\"worker\":{worker},\"peak_rss_kib\":{peak}}}");
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
        }
        return Ok(());
    }
    let root = std::path::PathBuf::from(&path);
    let table = Table::new(LocalObjectStore::new(&root)?);
    let head: Head =
        canonical_json::from_slice_canonical(&std::fs::read(root.join("_otmp/HEAD"))?)?;
    let generation: Generation = canonical_json::from_slice_canonical(&std::fs::read(
        root.join(head.metadata_generation.uri.as_str()),
    )?)?;
    let image_bytes = generation.metadata_image.checkpoint.length.0;
    let options = ReaderOptions::default();
    let started = Instant::now();
    let reader = table
        .open_metadata_reader(
            MetadataSelection::Current,
            SnapshotSelection::Ref("main".into()),
            options.clone(),
        )
        .await?;
    let registration_ms = started.elapsed().as_millis();
    let registered = reader.statistics();
    let planning_started = Instant::now();
    let mut cursor = None;
    let mut files_considered = 0_u64;
    loop {
        let batch = reader.files(cursor, &[], 256).await?;
        files_considered += batch.files.len() as u64;
        cursor = batch.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    let planning_ms = planning_started.elapsed().as_millis();
    let planned = reader.statistics();
    if registered.bytes.saturating_mul(10) >= image_bytes {
        return Err(format!(
            "cold registration transferred {} of {image_bytes} bytes",
            registered.bytes
        )
        .into());
    }
    if planned.peak_cache_bytes > options.cache_budget_bytes {
        return Err("reader cache budget exceeded".into());
    }
    println!(
        "{{\"metadata_image_bytes\":{image_bytes},\"registration\":{{\"bytes\":{},\"requests\":{},\"pages\":{},\"cache_hits\":{},\"cache_peak_bytes\":{},\"latency_ms\":{}}},\"planning\":{{\"bytes\":{},\"requests\":{},\"pages\":{},\"cache_hits\":{},\"cache_peak_bytes\":{},\"files_considered\":{},\"latency_ms\":{}}},\"parquet_execution\":null}}",
        registered.bytes,
        registered.requests,
        registered.pages,
        registered.cache_hits,
        registered.peak_cache_bytes,
        registration_ms,
        planned.bytes.saturating_sub(registered.bytes),
        planned.requests.saturating_sub(registered.requests),
        planned.pages.saturating_sub(registered.pages),
        planned.cache_hits.saturating_sub(registered.cache_hits),
        planned.peak_cache_bytes,
        files_considered,
        planning_ms,
    );
    Ok(())
}
