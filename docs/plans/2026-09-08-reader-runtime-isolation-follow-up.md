# Reader runtime isolation follow-up

This is a separate implementation and qualification slice. The concurrency
candidate continues to use the caller's Tokio runtime and one serialized Turso
connection per reader. Its preflight and metadata budgets do not bound native
Parquet execution or process RSS.

Before choosing an additional runtime, measure scheduling delay while a broad
query and small queries execute concurrently. Attribute time to metadata engine
admission, storage requests, footer decoding, native execution, and the caller's
stream polling. Keep the current reader and the same immutable fixtures as the
control.

The implementation contract must answer these questions together:

- **Stream-driver placement.** Identify the task that actually polls each
  returned DataFusion stream, including first poll, backpressure, errors, and
  cancellation. Moving plan construction alone cannot isolate stream execution.
- **Complete storage-body routing.** Route response-body polling and decoding
  along with request initiation. Cover metadata pages, trailer/footer requests,
  native Parquet ranges, retries, and cancelled bodies; do not leave body work on
  the application runtime by accident.
- **Runtime lifetime.** Specify ownership when providers, plans, readers, and
  streams outlive their creating request or session. A retained stream must not
  refer to an already destroyed executor. Keep runtime ownership explicit and
  avoid one permanent runtime per file or scan.
- **Shutdown ownership.** Define who stops admission, cancels work, joins tasks,
  and shuts down blocking work. Test shutdown while waiting for metadata
  admission, storage, cache fills, native execution, and a stalled consumer.

Qualify exact result parity and terminal ownership release before measuring
latency. Report planning, first result, complete execution, throughput, explicit
reservation peaks, process RSS, and runtime/thread counts separately. Keep real
provider qualification and application deployment as separate delivery gates.

Rayon, parallel metadata connections, asynchronous Turso storage, schema-binding
memoization, file-group changes, and prepared-statement caching require their own
measured justification and are outside this slice.
