#![cfg(feature = "write-latency-qualification")]

use std::fs;

use otmp::write_latency_qualification::{self as qualification, worker};

#[test]
fn write_latency_phase_guard_stays_compact_for_async_callers() {
    assert!(std::mem::size_of::<qualification::PhaseGuard>() <= 2 * std::mem::size_of::<usize>());
}

#[test]
fn write_latency_probe_rejects_overlap_and_unfinished_phases() {
    let session = qualification::start().unwrap();
    assert!(qualification::start().is_err());
    qualification::add_bytes("temporary_file_bytes", 7);
    qualification::add_bytes("temporary_file_bytes", 5);
    {
        let _outer = qualification::phase("outer");
        let _inner = qualification::phase("inner");
    }
    let report = session.finish().unwrap();
    assert_eq!(report.counters["temporary_file_bytes"], 12);
    assert_eq!(report.phases.len(), 2);
    assert_eq!(report.phases[0].name, "inner");
    assert_eq!(report.phases[0].depth, 1);
    assert_eq!(report.phases[1].name, "outer");
    assert_eq!(report.phases[1].depth, 0);

    let session = qualification::start().unwrap();
    std::mem::forget(qualification::phase("unfinished"));
    assert!(session.finish().is_err());
    assert!(qualification::start().is_ok());
}

#[tokio::test]
async fn write_latency_worker_prepares_runs_and_verifies_both_modes() {
    let directory = tempfile::tempdir().unwrap();
    let prepare_config = directory.path().join("prepare.json");
    fs::write(&prepare_config, r#"{"property_bytes":32}"#).unwrap();
    let fixture = directory.path().join("fixture");
    let prepared = worker::prepare(&fixture, &prepare_config).await.unwrap();
    assert_eq!(prepared.property_bytes, 32);
    assert_eq!(prepared.table_version, 2);
    assert!(prepared.retained_history_verified);
    assert!(
        worker::verify(&fixture)
            .await
            .unwrap()
            .retained_history_verified
    );

    for mode in ["fresh", "pre_pinned"] {
        let sample = directory.path().join(mode);
        copy_dir_all(&fixture, &sample);
        let config = directory.path().join(format!("{mode}.json"));
        fs::write(&config, format!(r#"{{"mode":"{mode}"}}"#)).unwrap();
        let result = worker::run(&sample, &config).await.unwrap();
        assert_eq!(result.mode, mode);
        assert_eq!(result.before_table_version, 2);
        assert_eq!(result.after_table_version, 3);
        assert_eq!(result.measured_property, "measured");
        assert!(result.retained_history_verified);
        let parent_pin = result
            .probe
            .phases
            .iter()
            .find(|phase| phase.name == "parent_pin")
            .unwrap();
        assert_eq!(parent_pin.performed, mode == "fresh");
        assert_eq!(
            result
                .probe
                .phases
                .iter()
                .filter(|phase| phase.name == "head_cas")
                .count(),
            1
        );
    }

    let unknown = directory.path().join("unknown.json");
    fs::write(&unknown, r#"{"property_bytes":0,"extra":true}"#).unwrap();
    assert!(
        worker::prepare(&directory.path().join("unknown"), &unknown)
            .await
            .is_err()
    );
    let too_large = directory.path().join("too-large.json");
    fs::write(&too_large, r#"{"property_bytes":16777217}"#).unwrap();
    assert!(
        worker::prepare(&directory.path().join("too-large"), &too_large)
            .await
            .is_err()
    );

    let invalid_run = directory.path().join("invalid-run.json");
    fs::write(&invalid_run, r#"{"mode":"warm"}"#).unwrap();
    let invalid = worker::run(&fixture, &invalid_run).await.unwrap_err();
    let output = serde_json::to_value(invalid.output()).unwrap();
    assert_eq!(output["ok"], false);
    assert_eq!(output["error"]["code"], "OTMP_QUALIFICATION_CONFIG");
    let unknown_run = directory.path().join("unknown-run.json");
    fs::write(&unknown_run, r#"{"mode":"fresh","extra":true}"#).unwrap();
    assert!(worker::run(&fixture, &unknown_run).await.is_err());

    let tampered = directory.path().join("tampered");
    copy_dir_all(&fixture, &tampered);
    fs::write(tampered.join("_otmp/HEAD"), b"{}").unwrap();
    let config = directory.path().join("tampered.json");
    fs::write(&config, r#"{"mode":"fresh"}"#).unwrap();
    assert!(worker::run(&tampered, &config).await.is_err());

    let identity_tampered = directory.path().join("identity-tampered");
    copy_dir_all(&fixture, &identity_tampered);
    let manifest = identity_tampered.join("write-latency-fixture.json");
    let mut identity: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
    identity["property_bytes"] = 31.into();
    fs::write(&manifest, serde_json::to_vec(&identity).unwrap()).unwrap();
    assert!(worker::run(&identity_tampered, &config).await.is_err());
}

fn copy_dir_all(source: &std::path::Path, destination: &std::path::Path) {
    fs::create_dir(destination).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let target = destination.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir_all(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).unwrap();
        }
    }
}
