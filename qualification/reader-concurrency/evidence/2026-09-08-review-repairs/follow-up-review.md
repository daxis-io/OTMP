# OTMP reader concurrency repair follow-up

## Verdict

**Go for the repaired implementation and behavioral-test scope.** I found no remaining confirmed production defect in the reviewed repair delta. The original three acceptance gaps are resolved. Final performance qualification is a separate gate and was still being run by the parent at this review cutoff.

## Findings

No open P0-P2 correctness, integrity, concurrency, resource, compatibility, or failure-recovery finding remains in the repair delta.

One additional P1 defect was found and fixed during this follow-up. Before the final repair, an optioned execution reader admitted bytes with `owned_keys=None`, so it ignored a peer's releasable cached-preflight guard and returned `ResourceExhausted` under a budget where the same execution succeeded sequentially. The red reproduction is `/private/tmp/otmp-reader-review-repairs/option-peer-red.log`. The final split between `ReservationPhase::Key` and `ReservationPhase::Preflight`, plus `owned_hits`, is at `otmp-datafusion/src/footer.rs:28-80`, `otmp-datafusion/src/footer.rs:226-313`, and `otmp-datafusion/src/footer.rs:583-602`. It lets option work wait for peer cache-hit preflights without waiting on queued load keys or its own hit. The same test is green in `/private/tmp/otmp-reader-review-repairs/all-lib-green.log`.

## Repair assessment

- Cache-hit admission is race-safe. `FooterCache::get` clones the entry and publishes the zero-byte preflight reservation while holding `entries` (`otmp-datafusion/src/footer.rs:315-342`). `try_reserve` takes the same lock before deciding Pressure versus CannotFit (`otmp-datafusion/src/footer.rs:226-275`). Dropping the reader releases the entry lease and Preflight counter and wakes admission; zero-byte guards do not alter retained-byte accounting.
- The final repeated-URI schema binding now uses a cloned preflight factory while retaining the provider permit (`otmp-datafusion/src/provider.rs:301-336`). This closes the same cache-hit lifetime hole in the late schema-rebinding path without adding stats or changing the execution factory.
- Provider-level coverage now reaches a descriptor at ordinal 256 and tests repeated URI, hash and length conflict, pruned conflict, branch/history selection, concurrency 1 and 8, no partial plan, single pin/footer acquisition, exact execution rows, and permit/cache/pool cleanup (`otmp-datafusion/src/provider_tests.rs:128-208`). Separate tests cover stale version pinning and original cause traversal (`:234-297`), malformed trailer/length/container and projected-away required fields (`:299-378`), cancellation at validation (`:210-232`), one-decode warm/cold progress (`:380-456`), and final repeated-URI schema binding (`:458-517`).
- The integration matrix checks exact selected-file membership and rows at concurrency 1, 2, 4, 8, 16, and 32 (`otmp-datafusion/tests/planning.rs:318-420`). Historical continuation crosses a full pruned batch and a schema evolution at concurrency 1 and 8 (`:124-247`).
- The Loom tests are bounded standalone models of the relevant OTMP-owned transitions. They cover weak-upgrade failure plus remove-if-same replacement, Payload/Key/Preflight/Retained accounting with shrink and active-lease drop, shared-waiter cancellation, and FIFO pressure/wakeup/head cancellation (`otmp-datafusion/tests/concurrency_model.rs:14-289`). Abstract modeling is appropriate here because the modeled transitions correspond to the production ownership rules; production unit tests exercise peer-hit progress and self-wait exclusion.
- Qualification gates now enumerate every expected pass/query position, require exact 20/10 sample totals, require all four latency metric counts and throughput counts, and fail closed for a missing case or metric (`qualification/reader-scale/concurrency.py:43-80`; `qualification/reader-scale/tests/test_concurrency.py:10-83`). The formerly passing incomplete input is retained in `/private/tmp/otmp-reader-review-repairs/gates-red.log`; the frozen V7 archive re-evaluates to all 45 gates in `v7-gates-rechecked.json`.

## Validation reviewed

- `cargo nextest run --locked --workspace --all-features`: 251 passed, 0 skipped (`nextest-final.log`).
- `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`: passed (`clippy-2.log`).
- `cargo fmt --all --check`: passed (`fmt-final.log`).
- Workspace doctests: passed (`validation.json`).
- Footer option-peer red/green, cached-preflight red/green, final-binding red/green, provider matrix, planning evolution, and four Loom models are retained under `/private/tmp/otmp-reader-review-repairs/`.

## Audit appendix

- **Redundant/unused:** No redundant runtime layer or unused production branch introduced by the repairs. Test hooks are `cfg(test)` and compile away.
- **Low value:** No low-value repair identified. The separate Preflight phase is required to avoid both false CannotFit and a deadlock on queued FooterLoad keys.
- **Quality:** The ownership categories and compatibility/budget documentation now match behavior. The added test module is large but keeps authenticated metadata, pagination, native planning, storage, and cleanup paths real.
- **Residual proof boundary:** The option-peer regression is a focused factory-level schedule rather than a full DataFusion query executing concurrently with another provider scan. It invokes the exact `get_metadata(Some(...))` path used by DataFusion 55 and is paired with provider/native-plan tests, so this is not a release blocker. Updated performance results must still be assessed separately because this review did not rerun timing matrices.

