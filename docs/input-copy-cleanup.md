# Worker input-copy cleanup

Workers fetch each submitted object into `jobs/<reservation>/<objectId>.input`.
Before this change, only result objects entered the scratch ledger, so these
copies were never reclaimed. Each finished job permanently lowered free scratch
by its input size, and later reservations held with "insufficient unreserved
scratch". At the planned 10,000-photo / ~50 GB scale, that blocks the next job.

This change adds **opt-in** ledger accounting and exact cleanup for those copies.
It does not delete anything by itself and changes nothing for existing ledgers.
Integrating it into the driver and daemon is a separate step (see below).

## Opt-in, versioned ledger ABI

- `ScratchLedger(..., input_copy_accounting=False)` is the default. Its schema
  and config bytes are unchanged.
- `ScratchLedger(..., input_copy_accounting=True)` plus an explicit call to
  `enable_input_copy_accounting()` latches `inputCopyAccountingVersion: 1` into the
  stored config and creates five additive tables. The latch is idempotent and
  irreversible. There is no backfill and no rollback.
- The marker sits alongside the existing control `accountingVersion`. Both latches
  go through one helper, so neither can drop the other.
- **Fencing.** `validate_accounting` runs at bootstrap and at the start of every
  transaction:
  - The previous source (55cfcd9) accepts only two config shapes, so it refuses a
    marked ledger both on reopen and as an already-running writer, before any
    business mutation.
  - New code without the opt-in profile refuses a marked ledger the same way.
  - `scripts/input_copy_old_image_oracle.py` demonstrates both against the
    actual source55 worker image (see Evidence).
- The table schemas are checked exactly whenever the marker is present.

Tables:

| Table | Purpose |
| --- | --- |
| `input_bindings` | Full input closure per reservation, bound to job/generation/environment/manifest/pipeline and whether a native task could exist |
| `input_copies` | One row per object: bytes, sha, owned path, `fetched`, `cleaned` |
| `input_releases` | The stored canonical terminal release proof |
| `input_pins` | `read` / `worker` / `downstream` pins (`active`, `uncertain`, `released`) |
| `input_cleanup_intents` | Exact cleanup fences, separate from result intents |

Result objects, receipts, pins, intents and every existing result API are unchanged.
Input and result IDs never share a fence.

## Lifecycle

1. **`register_input_binding`** happens before the first download.
   - The closure must match the original reservation exactly, and its byte total
     must equal the reserved input bytes.
   - Each path is canonical inside `jobs/<reservation>/`.
   - Portable case/Unicode and hardlink aliasing is checked across inputs and
     results, in linear time for 10,000 copies. Result registration also checks
     against input paths.
   - Exact replay is idempotent; any change conflicts.
2. **`confirm_input_copy`** runs after the broker download (or `worker._file`)
   verified the bytes. It is idempotent, so a fetch-before-checkpoint crash just
   confirms again. Unconfirmed rows are never candidates.
3. **`release_input_copies(proof)`** requires the original terminal reservation and
   a closed, typed proof:
   - `success` carries the sealed `resultManifest` and the raw original Viewer
     `receipts`:
     - the manifest re-validates with `validate_result_manifest`, and its digest
       and objects equal the registered result;
     - every receipt re-validates with `validate_acceptance` against it;
     - receipts are sorted, unique, cover every object and are exactly this
       reservation's durable ledger receipts, field for field;
     - every result object is durably accepted. Receipt IDs alone are refused.
   - `terminal`: the sealed failure passes `validate_failure` and the receipt
     passes `validate_terminal_acceptance`, both bound to the original identity.
     This covers failed and cancelled jobs.
   - `nativeRemoval` is `{kind:'removed', phase:'removed', identity}`, exported by
     Root from the original engine cleanup journal. Its job/task/reservation/
     generation/environment/manifest/pipeline must match and its `inputs` must
     equal the bound closure. The exporter's exact schema is Root's to finalize.
   - `{kind:'no_native_task'}` is **refused** for now. A `pipeline.odm.requested
     == false` flag is not enough evidence; positive sealed execution-route
     evidence is needed and not yet agreed. Until then TreeDar-only input copies
     stay retained. This is a known remaining gate.

   The proof is stored canonically. An identical replay returns the count; a
   different proof conflicts.
4. **`input_cleanup_candidates`** returns copies that are:
   - released;
   - terminal;
   - fetched;
   - not cleaned and not fenced;
   - without any active or uncertain pin;
   - past grace, counted from the later of release and terminal time.

   Oldest release first. Count and byte limits are hard and never exceeded. A copy
   larger than `max_bytes` is deferred and reported by `oversized_input_copies`,
   staying retained until Root aligns a reviewed object-size policy for admission
   and cleanup. There is no dependence on `output_budget`.
5. **`begin_input_cleanup` / `input_cleanup_intent` / `mark_input_cleaned`** mirror
   the result fence. Pins are refused once an intent exists.
6. **`ScratchCleanupExecutor.execute_input_copy`** runs the same Linux-only
   procedure as result cleanup: verify, no-replace quarantine, unlink, fsync,
   confirm. Input journal rows are keyed `input-copy:<token>` and their binding
   carries `namespace: "input-copy"`, so they can never collide with result rows,
   even for identical object IDs. Result keys and bindings stay byte-identical.

## Driver and daemon integration (Root-owned, not in this change)

- At fetch entry, before the first download, call `register_input_binding` with the
  full closure and the planned `.input` paths. Pass `native_task_possible=True`
  unless the pipeline can never create a native task.
- After each verified `download_object`, or a `worker._file` re-verify of an
  existing file, call `confirm_input_copy`.
- Hold a `worker` pin from start to terminal. Combined and TreeDar stage resolvers
  hold `downstream` pins while they depend on inputs. Readers hold `read` pins.
- Once full acceptance and native removal are reconciled (success), or terminal
  acceptance and removal or no-task are (failure/cancel), call
  `release_input_copies(proof)`. Run it in the background sweep, idempotently,
  without holding completion.
- In `_background`, call `input_cleanup_candidates(max_count=1, max_bytes=<input
  policy>)`, then `executor.execute_input_copy`. Keep it behind the existing
  `cleanupEnabled` daemon setting. Surface `oversized_input_copies` in health or
  logs.
- Provide the read-only removed-phase export from `EngineCacheCleanup`, and
  extend native cleanup to failed and cancelled tasks, so `terminal` releases
  can be proven.
- The daemon enables the profile and latches the marker only through a reviewed
  migration step. The default stays off.

## Evidence

- `tests/test_input_copy_cleanup.py` has 33 tests: 28 ledger tests on any platform
  and 5 executor tests that need Linux. They cover:
  - marker shapes, latch preservation in both orders, profile fencing, schema
    tampering;
  - binding identity, liveness, expiry, path canonicalization, case and hardlink
    aliases across namespaces;
  - confirmation replay;
  - success proofs with tampered manifests, unregistered manifests, changed,
    foreign, extra, duplicate and empty receipts, and IDs-only proofs refused;
  - partial-acceptance, failed and cancelled proofs, with every native removal
    field negative, and no-native release refused;
  - proof replay and change;
  - pins (active, uncertain, after intent);
  - fence recovery, ordering, and hard count/byte limits with the oversized
    report;
  - the capacity reproducer;
  - a bounded 10,000-copy closure;
  - executor exact removal, replaced-file retention, crash-before-confirm
    reconciliation, missing-file refusal and namespaced journal keys.
- Old-writer oracle: `scripts/input_copy_old_image_oracle.py` was run in
  `ledgetop-processing-reference-worker:source55cfcd93850f-20261028` (image
  `sha256:f853927f…`, Python 3.13.17) with network none, a read-only root and
  synthetic data. Results:
  - cold reopen refused;
  - live writer refused;
  - with both the control and input markers latched, a live writer that had
    already latched control accounting is refused, and so is a cold reopen;
  - ledger bytes unchanged in every refused case;
  - unmarked ledgers remain compatible.

No live host, NAS, credential, engine or Viewer state was touched.
