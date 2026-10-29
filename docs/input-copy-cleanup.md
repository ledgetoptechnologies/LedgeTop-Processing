# Worker input-copy cleanup

Workers fetch each submitted object into `jobs/<reservation>/<objectId>.input`.
Before this change, only result objects entered the scratch ledger, so these
copies were never reclaimed. Each finished job permanently lowered free scratch
by its input size, and later reservations held with "insufficient unreserved
scratch". At the planned 10,000-photo / ~50 GB scale, that blocks the next job.

This change adds **opt-in** ledger accounting and an exact cleanup primitive for
those copies. It is a low-level primitive, not runtime acceptance:

- It deletes nothing by itself.
- It changes nothing for existing ledgers.
- It releases nothing until a trusted release authority is configured.

Driver, daemon and authority integration are separate steps (see below).

## Opt-in, versioned ledger ABI

- `ScratchLedger(..., input_copy_accounting=False)` is the default. Its schema
  and config bytes are unchanged.
- `ScratchLedger(..., input_copy_accounting=True)` plus an explicit call to
  `enable_input_copy_accounting()` does three things in one transaction:
  - creates six additive tables;
  - writes the `input_accounting` footprint row;
  - latches `inputCopyAccountingVersion: 1` into the stored config.

  The latch is idempotent and irreversible. There is no backfill and no rollback.
- The marker sits alongside the existing control `accountingVersion`. Both latches
  go through one helper, so neither can drop the other.
- **Fencing.** `validate_accounting` runs at bootstrap and at the start and end of
  every transaction, for every profile:
  - Any input table with no marker is refused. So is a marker without the
    complete footprint (all six tables and the exact `input_accounting` row).
  - New code without the opt-in profile refuses a marked ledger.
  - The previous source (55cfcd9) accepts only two config shapes. It therefore
    refuses a coherent marked ledger cold and live, including alongside the
    control marker; see Evidence.
- **Scope.** The fence covers coherent old and default-off writers, and accidental
  loss of the marker or footprint. It does not defend against a privileged actor
  rewriting the database consistently.
- The table schemas are checked exactly whenever the marker is present.

| Table | Purpose |
| --- | --- |
| `input_accounting` | Singleton version footprint |
| `input_bindings` | Original job/generation/environment/manifest/pipeline and the closure digest `inputsSha256` |
| `input_copies` | One row per object: bytes, sha, owned path, indexed portable `path_key`, `fetched`, `cleaned` |
| `input_releases` | Stored canonical release record, its digest and authority ID |
| `input_pins` | `read` / `worker` / `downstream` pins (`active`, `uncertain`, `released`) |
| `input_cleanup_intents` | Exact cleanup fences, separate from result intents |

Result objects, receipts, pins, intents and every existing result API are unchanged.
Input and result IDs never share a fence.

## Release authority

Terminal metadata acknowledgement, Viewer output receipts and source-retention
validators do not prove that the original source bytes are retained. They are
never deletion authority on their own.

Release therefore needs a configured `InputReleaseAuthority`. The trusted
composition passes it as `ScratchLedger(..., release_authority=...)`. For a compact
record, the authority must independently resolve, from concrete durable stores
and the authenticated issuer:

- positive retained-original-source custody covering every original object until
  its release ACK;
- the durable terminal outcome and its exact acceptance;
- whole-parent pipeline finality.

It then returns the identical canonical record, or raises. **No implementation
ships here.** Without one, every release is refused: success, failure and cancel
alike.

The release record (`RELEASE_FIELDS`) is closed and bounded to 16 KiB, whatever the
closure size:

```
schemaVersion=1, purpose='input-copy-release', scope='worker_fetched_inputs_only',
releaseId, authorityId, reservationId, jobId, generation, environment,
inputManifestSha256, pipelineDigest, inputsSha256, outcome (succeeded|failed|cancelled),
outcomeSha256, terminalReceiptId, sourceCustodyReceiptId, sourceStorageReceiptId,
pipelineClosureSha256
```

Before asking the authority, the ledger checks the record against its own rows.
These checks are necessary but never sufficient:

- the original binding and the confirmed terminal reservation match;
- `inputsSha256` equals the ledger's own closure digest;
- the four receipt roles are distinct, and source roles reuse no result receipt;
- for `succeeded`, the registered result digest matches, every result object is
  durably accepted, and `terminalReceiptId` names this reservation's durable
  receipt.

The stored record is re-checked every time it is used:

- `input_cleanup_candidates`, `oversized_input_copies` and `begin_input_cleanup`
  require bounded canonical bytes, a matching digest, a matching authority ID and
  the full identity checks above;
- missing, corrupted, stale or mismatched records retain every copy;
- `begin_input_cleanup` also asks the authority to re-resolve the record before
  creating a fence, so revoked custody holds deletion.

## Lifecycle

1. **`register_input_binding`** happens before the first download.
   - The closure must match the original reservation exactly, and its byte total
     must equal the reserved input bytes.
   - Each path is canonical inside `jobs/<reservation>/`.
   - Portable case/Unicode identity is checked through the indexed `path_key`, and
     against this reservation's case-variant result paths. Any existing file must
     be single-link.
   - It does no historical scans and is bounded per registration.
   - Exact replay is idempotent; any change conflicts.
2. **`confirm_input_copy`** runs after the broker download (or `worker._file`)
   verified the bytes. It is idempotent. Unconfirmed rows are never candidates.
3. **`release_input_copies(record)`** stores an authority-resolved record (see
   above).
4. **`input_cleanup_candidates`** returns copies that are:
   - released, with a still-valid stored record;
   - terminal;
   - fetched;
   - not cleaned and not fenced;
   - without any active or uncertain pin;
   - past grace.

   Oldest release first. Count and byte limits are hard. A copy larger than
   `max_bytes` is deferred and reported by `oversized_input_copies`, which is
   bounded by its own `max_count`. It stays retained until Root aligns a reviewed
   object-size policy.
5. **`begin_input_cleanup` / `input_cleanup_intent` / `mark_input_cleaned`** mirror
   the result fence. Pins are refused once an intent exists.
6. **`ScratchCleanupExecutor.execute_input_copy`** runs the same Linux-only
   procedure as result cleanup: verify, no-replace quarantine, unlink, fsync,
   confirm. Input journal rows are keyed `input-copy:<token>` with
   `namespace: "input-copy"`. Result keys and bindings stay byte-identical.

## Integration still required (Root/Viewer-owned, not in this change)

- **Authority:** a concrete `InputReleaseAuthority`, backed by an authenticated
  Viewer/NAS retention issuer whose pins are enforced on every purge path. It also
  needs durable, role-unique terminal and source receipts and whole-parent pipeline
  closure. It must send the source release ACK only after the last local copy is
  reconciled.
- **Driver:**
  - call `register_input_binding` at fetch entry and `confirm_input_copy` after
    each verified download or `worker._file`;
  - hold a `worker` pin from start to terminal and `downstream` pins for stage
    resolvers;
  - run `release_input_copies` in the background sweep;
  - call `input_cleanup_candidates(max_count=1, max_bytes=<policy>)` and then
    `execute_input_copy`, behind `cleanupEnabled`. Surface oversized copies.
- **Native failed/cancelled cache cleanup** is a separate scope. It may remove the
  native task cache only, while pinned worker inputs remain. It never authorizes
  deleting the last fetched copy.
- **Enabling:** a reviewed enable/migration step and an object-size policy. The
  default stays off.

## Evidence

- `tests/test_input_copy_cleanup.py` has 37 tests: 31 ledger tests on any platform
  and 6 executor tests that need Linux. They include each review reproduction:
  - terminal metadata alone releases and deletes nothing (ledger and executor
    end to end);
  - a removed marker with retained input rows is refused live, cold and default-off;
  - a tampered stored record (record, digest, authority, release row, binding,
    acceptance or receipts) retains every copy and refuses a fence;
  - a legal 10,000-object closure with 128-character IDs releases with a compact
    record.

  They also cover:
  - authority mismatch and revocation, and every record field negative;
  - footprint and schema tampering, and latch preservation in both orders;
  - binding identity and aliases;
  - pins, fences and hard limits;
  - the capacity reproducer;
  - executor removal, retention, crash reconciliation and journal keys.
- Old-writer oracle: `scripts/input_copy_old_image_oracle.py` was run in
  `ledgetop-processing-reference-worker:source55cfcd93850f-20261028` (image
  `sha256:f853927f…`, Python 3.13.17) with network none, a read-only root, synthetic
  data and `-W error::ResourceWarning`. Results:
  - cold and live refused, both alone and alongside the control marker;
  - ledger bytes unchanged in every refused case;
  - unmarked ledgers remain compatible.

No live host, NAS, credential, engine or Viewer state was touched.
