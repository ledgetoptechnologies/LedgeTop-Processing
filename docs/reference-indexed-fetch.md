# Fresh indexed fetch journal

This source component adds an explicit private worker composition:

```
inputCopyAccounting: {
  schemaVersion: 2,
  fetchJournal: {schemaVersion: 1, policyId: "indexed-worker-fetch-v1"}
}
```

Default workers and `inputCopyAccounting:{schemaVersion:1}` retain their previous configuration, driver binding and one-object fetch paths. The scratch ledger marker remains `inputCopyAccountingVersion:1`; the new version names a separate composition and indexed driver ABI. There is no migration, backfill, adoption of an existing v1 journal, or automatic enable. Combined preparation and configured TreeDar execution are refused in this first indexed profile.

The explicit bootstrap creates a fresh fixed-schema `driver.sqlite`, permanently binds its root/inode, bootstrap ID, physical node identity, private policy and schema digest, and records completion. Normal startup uses `read_only_profile` before opening legacy components. A missing footprint or altered profile is corruption, not a request to reconstruct it. `initialize_fresh_profile`, `read_only_profile` and `compiled_profile_descriptor` are the only bootstrap seam; normal driver construction receives the actual returned `IndexedFetchContext` and the actual daemon state owner.

The database stores the original canonical authenticated claim once, with normalized ordered object rows and immutable reservation/grant records. Seals and immutable triggers prevent ordinary updates, deletions and additions to a closed object set. Constructor checks the exact schema; every new driver connection checks schema version, actual root/file identity, fixed driver binding and page cap. Cold entry validates the original claim, row closure, claim request, reservation, grant, prefix, attempt history and clock. It also reconciles actual scratch rows and rehashes recovered files. Missing, extra or changed rows hold before a new download. Stored digests do not grant source custody or cleanup authority.

During fetching, the hot path runs before the legacy full-claim JSON load. It reads a bounded progress projection, one object descriptor, the current attempt and a CAS cursor. Every download has a committed intent and a bounded request charge before its GET. New and recovered published files pass the actual worker size/hash/regular-single-link check before scratch confirmation. Allocation precedes the atomic normalized prefix commit. A lost publication/confirmation/progress return reconciles those exact records and bytes; it does not repeat an already successful GET or skip an unverified object.

The process caches the exact current-object attempt predecessor (at most three rows), checks it before GET or reconciliation and again inside the attempt transaction. Attempt identity columns and deletion are immutable; spent failed attempts remain retained. One full typed attempt-closure audit runs at cold entry and at the final fetch boundary, so an unowned future or late past row cannot reach processing merely because the hot cursor advanced normally. No full attempt-history scan runs for every object.

The indexed grant factory validates the full original grant once and supplies an opaque O(1) object lookup. Every hot step checks its trusted clock/expiry, including recovery without a GET and the final phase transition. Backwards clock movement or expiry holds the original reservation and files. There is no authenticated renewal route in the current coordinator protocol, so this component never rotates a nonce, resets a claim or invents renewal.

One object still advances per step. `immediate_fetch_progress(result)` proves the last concrete committed advance to the service loop; a copied JSON dictionary is not scheduling authority. The service yields/checks stop between objects instead of adding the idle poll sleep after every successful progress step. Background acceptance polling uses only bounded sealed headers and has a private cadence. It never reloads a retained large manifest merely to poll an unavailable acceptance endpoint. Readiness uses an actual cross-thread `CoordinatorReadinessLease`, exact job/request/phase and a read-only journal snapshot; it does not relax pending-work checks for ordinary callers.

At processing/execution boundaries, the driver reconstructs the original full in-memory claim and path map once. Only whitelisted mutable coordinator fields overlay sealed original data; a journal-supplied submission or result manifest cannot replace it. The reservation and transfer request must equal the sealed reservation. The existing engine adapter still verifies files before engine mutation and retains its initialize/upload/commit uncertainty fences. This patch does not make uncertain engine mutations safe to replay.

## Capacity and lifetime limits

The private compiled descriptor binds 4 KiB pages, a 64 MiB driver database cap and DELETE/FULL SQLite behavior, with at most twice that size for the owned rollback journal. The global physical metadata allowance is 192 MiB, not 192 MiB per job. Actual allocated database/journal blocks are already excluded from free space; only its remaining unmaterialized allowance is reserved.

The v2-only `IndexedCapacityProbe` verifies the actual context, owner and state/scratch device identities. Shared-device scratch admission subtracts that one remaining metadata hold from actual free bytes. Separate-device scratch admission reports actual scratch free bytes unchanged. Before taking a new claim, journal admission also subtracts same-device existing scratch `remaining` reservations and retained control charges. The existing ScratchLedger algorithm and default/v1 probes are unchanged. Uncertain identity or probe failures hold rather than assume capacity.

The current conservative logical charge is 32 MiB per retained job plus a 4 MiB non-job allowance inside the 64 MiB cap. Therefore this initial profile admits only one conservatively charged job before explicit retirement. No TTL or terminal status releases that charge. This is a deliberate bounded-source limitation, not a perpetual production queue or completed retention lifecycle. The configured retained-job ceiling of 64 is additionally constrained by the byte charge; it does not imply 64 large jobs fit.

A near-4-MiB legal manifest, 10,000 bounded object IDs and the maximum 10,010 aggregate attempts are exercised against the actual schema. Per-object attempts are capped at three, while the aggregate GET ceiling is `min(3*N,N+10)`, never above 10,010. These metadata/capacity tests are distinct from actual 50 GB transfer qualification. No public metadata limits, main-node CPU/RAM limits or small-node photo limits change.

## Verification and remaining work

Tests exercise actual one-object `driver.step` calls, real SQLite progress writes, the actual BrokerClient publisher and actual file verification for 10,000 tiny synthetic objects. The real `_save` is wrapped only to count calls, not replaced: the hot loop never rewrites a growing input-path dictionary. Full claim loads, grant loads and full scratch closure checks remain at bounded phase/cold-entry counts. This proves the metadata-work boundary, not real dataset throughput or processing quality.

Negative tests cover original request/projection/reservation drift, immutable schema changes, malformed or oversized cells, cursor/attempt/history and clock changes, quota refusal before claim, publication recovery, shared-device ledger/control holds and a genuinely separate Linux tmpfs. Separate integration tests exercise the canonical worker factory, service, actual Node HTTPS broker fixtures, listener health and coordinator outage. Old-image downgrade gates must be qualified against the actual images before release.

Runtime activation, authenticated original-source custody, purge-path enforcement, input/native cleanup authority, safe retained-metadata retirement and a grant-renewal protocol are separate work. Existing maintenance census intentionally refuses the indexed ABI until its normalized history is qualified. Startup with an unknown/uncommitted rollback sidecar holds for inspection; no sidecar deletion or unqualified repair is introduced. The existing native engine start still performs a full upload loop, whose large-job responsiveness and uncertainty recovery require separate qualification. Gaussian splats remain disabled. No real client dataset or live service is authorized by this source patch.
