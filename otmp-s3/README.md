# otmp-s3

`otmp-s3` adapts Apache [`object_store`](https://docs.rs/object_store/0.14.1/object_store/)
to OTMP's `ObjectStore` contract for AWS S3 and Cloudflare R2's S3-compatible
endpoint. It is an evidence-track adapter, not a provider qualification or a
production support statement.

The initial profile accepts only exact-length objects no larger than 64 MiB and
uses one buffered `PutObject`. It uses conditional `PutObject` for immutable
creation and `HEAD` replacement, retains both ETag and version ID in OTMP's
opaque runtime `ObjectVersion`, and verifies an immutable-create collision by
reading the exact expected bytes. It does not use multipart upload and returns
`Unsupported` for conditional deletion: a GET followed by DELETE is not an
atomic condition.

AWS documents `If-None-Match` and `If-Match` conditional `PutObject` behavior,
including 412 and concurrent-write 409 responses. Cloudflare documents its
S3-compatible API and reports failed conditional headers as HTTP 412. See the
[AWS PutObject reference](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html),
[Cloudflare R2 S3 documentation](https://developers.cloudflare.com/r2/get-started/s3/),
and [R2 error codes](https://developers.cloudflare.com/r2/api/error-codes/).

The default test suite uses an in-process scripted S3 HTTP endpoint and does
not require credentials, object listing, or cloud access. `.github/workflows/
provider-evidence.yml` is a separate manual/nightly evidence path. Missing
credentials write a sanitized `not_run` JSON artifact; that outcome is not a
provider pass.

Configure the AWS evidence bucket region with repository variable `OTMP_AWS_S3_REGION` (default `us-east-1`). R2 uses its configured S3 endpoint and region `auto`. Each run writes beneath a fresh UUID prefix.
