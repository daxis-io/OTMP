#![allow(clippy::too_many_lines)]
use object_store::aws::AmazonS3Builder;
use otmp::{ConditionalWriteOutcome, ObjectStore, ObjectVersion};
use otmp_protocol::RelativeUri;
use otmp_s3::S3ObjectStore;
use serde_json::{Value, json};
use std::process::{Command, ExitCode};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum ResultKind {
    Passed,
    Failed,
    NotRun,
}
impl ResultKind {
    fn text(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::NotRun => "not_run",
        }
    }
}
fn case(name: &str, kind: ResultKind, code: &str) -> Value {
    json!({"name":name,"result":kind.text(),"code":code})
}
fn required_pass(cases: &[Value]) -> bool {
    cases.iter().all(|case| case["result"] == "passed")
}
fn present(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}
fn git_sha() -> String {
    present("GITHUB_SHA").unwrap_or_else(|| {
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map_or_else(|| "unavailable".into(), |value| value.trim().into())
    })
}
fn exact(object: &otmp::storage::StoredObject, bytes: &[u8], version: &ObjectVersion) -> bool {
    object.bytes == bytes && object.version == *version
}
fn winning_writer<'outcome>(
    left: &'outcome ConditionalWriteOutcome,
    right: &'outcome ConditionalWriteOutcome,
) -> Option<(&'static [u8], &'outcome ObjectVersion)> {
    match (left, right) {
        (
            ConditionalWriteOutcome::Applied { new_version },
            ConditionalWriteOutcome::Conflict { .. },
        ) => Some((b"a", new_version)),
        (
            ConditionalWriteOutcome::Conflict { .. },
            ConditionalWriteOutcome::Applied { new_version },
        ) => Some((b"b", new_version)),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let provider = present("OTMP_S3_PROVIDER").unwrap_or_else(|| "unknown".into());
    let sha = git_sha();
    let bucket = present("OTMP_S3_BUCKET");
    if bucket.is_none()
        || present("AWS_ACCESS_KEY_ID").is_none()
        || present("AWS_SECRET_ACCESS_KEY").is_none()
    {
        report(
            &sha,
            &provider,
            "not_run",
            false,
            &[case(
                "credential_gate",
                ResultKind::NotRun,
                "credentials_or_bucket_missing",
            )],
        );
        return ExitCode::SUCCESS;
    }
    let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket.expect("checked"));
    if let Some(endpoint) = present("OTMP_S3_ENDPOINT") {
        builder = builder.with_endpoint(endpoint);
    }
    if provider == "cloudflare-r2" {
        builder = builder.with_region("auto");
    }
    let Ok(store) = S3ObjectStore::from_amazon_s3_with_prefix(
        builder,
        format!("otmp-evidence/{provider}/{}", Uuid::now_v7()),
    ) else {
        report(
            &sha,
            &provider,
            "failed",
            false,
            &[case(
                "client",
                ResultKind::Failed,
                "client_initialization_failed",
            )],
        );
        return ExitCode::FAILURE;
    };
    let immutable: RelativeUri = "objects/immutable".parse().expect("constant");
    let create = store.create_bytes(&immutable, b"immutable").await;
    let collision = store.create_bytes(&immutable, b"immutable").await;
    let initial = store.create_head(b"one").await;
    let mut cases = vec![
        case(
            "immutable_create",
            if create.is_ok() {
                ResultKind::Passed
            } else {
                ResultKind::Failed
            },
            if create.is_ok() {
                "ok"
            } else {
                "operation_failed"
            },
        ),
        case(
            "create_collision",
            if collision.is_ok() {
                ResultKind::Passed
            } else {
                ResultKind::Failed
            },
            if collision.is_ok() {
                "ok"
            } else {
                "operation_failed"
            },
        ),
    ];
    if let ConditionalWriteOutcome::Applied {
        new_version: initial_version,
    } = initial
    {
        let read = store.read(&"_otmp/HEAD".parse().expect("constant")).await;
        let initial_ok = matches!(&read,Ok(object) if exact(object,b"one",&initial_version));
        cases.push(case(
            "token_readback",
            if initial_ok {
                ResultKind::Passed
            } else {
                ResultKind::Failed
            },
            if initial_ok {
                "exact_bytes_and_token"
            } else {
                "readback_mismatch"
            },
        ));
        let moved = store.replace_head(&initial_version, b"two").await;
        let moved_ok = matches!(moved, ConditionalWriteOutcome::Applied { .. });
        cases.push(case(
            "move_cas",
            if moved_ok {
                ResultKind::Passed
            } else {
                ResultKind::Failed
            },
            if moved_ok { "ok" } else { "move_failed" },
        ));
        let stale = store.replace_head(&initial_version, b"stale").await;
        cases.push(case(
            "stale_cas",
            if matches!(stale, ConditionalWriteOutcome::Conflict { .. }) {
                ResultKind::Passed
            } else {
                ResultKind::Failed
            },
            "expected_conflict",
        ));
        if let ConditionalWriteOutcome::Applied { new_version } = moved {
            let (left, right) = tokio::join!(
                store.replace_head(&new_version, b"a"),
                store.replace_head(&new_version, b"b")
            );
            let final_read = store.read(&"_otmp/HEAD".parse().expect("constant")).await;
            let ok = matches!((&winning_writer(&left,&right),&final_read),(Some((bytes,version)),Ok(object)) if exact(object,bytes,version));
            cases.push(case(
                "two_writers",
                if ok {
                    ResultKind::Passed
                } else {
                    ResultKind::Failed
                },
                if ok {
                    "one_applied_one_conflict_exact_readback"
                } else {
                    "writer_outcome_or_readback_failed"
                },
            ));
        } else {
            cases.push(case("two_writers", ResultKind::Failed, "head_move_failed"));
        }
    } else {
        cases.extend([
            case("token_readback", ResultKind::Failed, "head_create_failed"),
            case("move_cas", ResultKind::Failed, "head_create_failed"),
            case("stale_cas", ResultKind::Failed, "head_create_failed"),
            case("two_writers", ResultKind::Failed, "head_create_failed"),
        ]);
    }
    let completed = required_pass(&cases);
    report(
        &sha,
        &provider,
        if completed { "completed" } else { "failed" },
        completed,
        &cases,
    );
    if completed {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
fn report(sha: &str, provider: &str, state: &str, completed: bool, cases: &[Value]) {
    let passed = cases
        .iter()
        .filter(|case| case["result"] == "passed")
        .count();
    let failed = cases
        .iter()
        .filter(|case| case["result"] == "failed")
        .count();
    let not_run = cases
        .iter()
        .filter(|case| case["result"] == "not_run")
        .count();
    println!(
        "{}",
        json!({"git_sha":sha,"provider":provider,"dependency_versions":{"object_store":"0.14.1"},"state":state,"completed":completed,"counts":{"passed":passed,"failed":failed,"not_run":not_run,"list_calls":0},"cases":cases})
    );
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn completion_and_writer_classifiers_fail_closed() {
        assert!(!required_pass(&[
            case("a", ResultKind::Passed, "ok"),
            case("b", ResultKind::Failed, "x")
        ]));
        let v = S3ObjectStore::object_version(Some("etag"), None).unwrap();
        let applied = ConditionalWriteOutcome::Applied {
            new_version: v.clone(),
        };
        let conflict = ConditionalWriteOutcome::Conflict {
            current_version: None,
        };
        assert!(winning_writer(&applied, &conflict).is_some());
        assert!(winning_writer(&applied, &applied).is_none());
    }
    #[test]
    fn case_is_sanitized() {
        let value = case("x", ResultKind::Failed, "readback_mismatch");
        assert!(value.get("error").is_none());
        assert_eq!(value["code"], "readback_mismatch");
    }
}
