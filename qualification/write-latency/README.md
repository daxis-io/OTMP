# Metadata write-latency qualification

Build the feature-gated worker, then run the one-sample smoke before a final
20-sample matrix:

```sh
cargo build --release --locked -p otmp --example write_latency --features write-latency-qualification
python3 qualification/write-latency/run.py OUTPUT --binary target/release/examples/write_latency --samples 1 --report REPORT --allow-dirty
python3 qualification/write-latency/run.py OUTPUT --binary target/release/examples/write_latency --samples 20
```

The final command refuses an existing output root, an existing report, a dirty
tree, or a source commit that does not descend from `78c3311`. The evidence is
local-only; OS cache state is uncontrolled, and live providers and production
throughput are outside this gate.
