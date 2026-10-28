# Reference pipeline implementation checkpoint

This is an additive, opt-in successor to the existing multipart Viewer provider.
It is not enabled in the installed provider. Real-data testing is paused until
Beau supplies a smaller dataset.

## Implemented source foundations

- `reference_protocol.py` validates bounded metadata, canonical manifests,
  immutable environment bindings, engine requests and partial acceptance receipts.
  Gaussian splats requests are rejected; TreeDar availability is opt-in.
- `reference_store.py` puts reference metadata and execution claims in the same
  authoritative SQLite queue transaction. A surviving worker can authenticate its
  result after coordinator restart without receiving permission to rerun the engine.
  Engine completion releases execution capacity separately from Viewer acceptance.
- `scratch_policy.py` records capacity reservations, monotonic clock observations,
  object acceptance, pins and fenced cleanup intentions. It performs no transfers,
  engine calls or physical deletion.
- `reference_api.py` and `reference_http.py` provide authenticated metadata routes
  on an explicit loopback/TLS listener. No autostart or live enablement is installed.
- `reference_transfer.py` verifies scoped grants and streams bounded HTTPS input
  objects with checksum verification, sync and exclusive publication. Redirects
  and caller-selected origins are refused. The store persists grants privately
  and returns only a safe binding acknowledgement for lost-response recovery.
- `worker_outbox.py` persists immutable terminal results separately from local
  file paths. `reference_delivery.py` authenticates the durable execution claim,
  commits completion and returns a replayable coordinator metadata receipt.
  This receipt cannot authorize cleanup or replace Viewer acceptance.
- `reference_worker_api.py` fixes each private bearer to a configured worker and
  node identity. `reference_control.py` assembles separate Viewer/worker routes;
  the credentials cannot overlap. Reservation and phase updates require the
  exact durable worker claim. `reference_sender.py` sends only stored completion
  records and verifies an exact durable receipt before marking delivery complete.
- `reference_engine_worker.py` connects reserved, verified local photo inputs to
  the existing NativeEngine adapter, shared physical NodeGate and durable outbox.
  Mutation intents commit before HTTP calls; uncertain initialization/upload
  outcomes retain the fence. Original result ZIPs remain local after delivery.
- `scratch_cleanup.py` provides Linux-only exact-object cleanup with a durable
  quarantine journal and directory sync. Missing files alone never prove removal.
  Windows fails closed; no live cleanup or worker daemon is enabled.
- `reference_results.py` and `reference_result_api.py` serve exact local result
  objects under configured read policy, authenticated bindings and active ledger
  pins. `reference_result_proxy.py` streams those bytes through the coordinator
  without retaining a second archive. It withholds the final chunk until size
  and SHA verification succeed. HTTP closes streams and releases read leases on
  success, errors and disconnects; it never appends JSON to a binary response.
- `reference_failure.py` provides separate durable failed/cancelled records,
  receipts and retry outboxes without fake output manifests. Success and failure
  seals are mutually exclusive in the queue transaction. Failure bytes count
  against the global metadata quota. Public failure/Viewer acceptance wiring
  remains pending the shared terminal-failure contract.
- `contracts/reference-v1.contract.json` is the identical Python/Viewer fixture.
  Its SHA-256 with LF line endings is
  `2c49ff86b8e20384c400ff56c8365418e643108aa9e6c29b46e938554e989b37`.

## Remaining integration and acceptance

1. Wire the protected HTTP factory into explicit runtime configuration, current
   engine option discovery and the deployed listener; retain the native API.
2. Finish the Viewer storage broker and connect reservation-scoped transfers.
   Waiting jobs contain metadata; bytes transfer only after capacity is reserved.
3. Install a private node agent with persistent scratch, engine adapters and a
   durable result outbox. Never replay uncertain engine commits automatically.
4. Implement Viewer file verification, file/directory durability barriers and a
   durable receipt/outbox transaction before acknowledging results.
5. Connect receipt verification, downstream pins and grace periods to an exact
   owned-object cleanup executor. A disconnect, download or old age is not an ACK.
6. Test disconnects, restart recovery, partial acceptance, full disks, transfer
   expiry, production priority, small-node-first routing and maintenance with
   synthetic fixtures before enabling the reference path or running real data.

Source acceptance: the full 435-test checkpoint passes in an isolated Linux container
with network disabled and a read-only source mount. Linux also exercises the
symlink checks that Windows skips without symlink privileges. Tests include real loopback HTTP, metadata grant
replay, transfer corruption/expiry and completion-commit crash windows. These
are synthetic checks, not NAS hardware power-loss or installed runtime evidence.

Viewer source qualification is separate: its pinned `3e39f647` four-file reference
test run returned 59 passes and one platform-opposite skip in cached Node 24 on
Linux. An earlier broker source-change test failed once and passed later; that
failure remains unresolved pending a deterministic fix by Viewer. Both runs
passed the real file/directory sync and abrupt-process-exit receipt tests.

The subsequent Viewer `e30504c1` source fixed the broker source-change issue with
deterministic same-stat block-hash regressions. Its six reference/HTTP/harness
suites passed 79 tests with one opposite-platform skip on Linux. This supersedes
the earlier unresolved broker result, rather than relying on the earlier green
rerun alone.

An actual cross-language TLS probe used the real Viewer `ReferenceProvider` and
Python control API with a fresh generated test CA inside isolated containers.
Discovery, submit, status, UUID lookup, exact duplicate submit and credential
environment isolation passed. The helper container and certificate volume were
removed and their absence verified. No host trust settings changed. Helpers:
`ops/reference_tls_fixture.py` and `ops/reference_viewer_tls_probe.cjs`. This
proves metadata transport, not engine execution or the complete payload pipeline.

Transfer `byteBudget` is a bounded wire allowance including retries: between
the manifest's total input bytes and `min(2^53-1, 3 * totalBytes)`. Successful
input files still have the exact manifest byte counts and hashes. This allowance
is separate from the worker's input-plus-output scratch capacity reservation.

## Worker integration contract

The deployment driver must configure the actual loopback engine origin, physical
engine ID, coordinator authority/gate generation and a separate worker bearer.
It must reserve scratch before exposing a transfer request, fetch only through
the scoped broker grant, and pass verified reservation-owned input paths to the
engine controller. No submitted path or engine origin is trusted.

The private API supports claim, reservation, transfer/processing phase, grant,
result receipt and Viewer-acceptance relay operations. Public submission lookup
by immutable submission UUID recovers a lost response without creating another
execution. Uncertain initialization/upload outcomes and orphaned gate ownership
remain held for reconciliation; a completed engine's interrupted archive transfer
retries the same bounded cache without rerunning the engine.

After coordinator metadata acknowledgement, the worker may release its engine
fence. It independently validates relayed Viewer receipts against its persisted
manifest and derives accepted object coverage locally. Cleanup still requires
terminal state, exact accepted objects, no active/uncertain pins and grace.
Engine task removal, failed/cancelled terminal receipts, reservation renewal,
result streaming and automatic daemon wiring remain integration work; a retained
engine task may intentionally block the existing strict maintenance census.

Missing delivery acknowledgements retain results. Disk pressure stops intake.
Production priority, the shared execution fence and maintenance gates apply to
both submission modes; an already running staging job is allowed to finish.

## Coordinated ownership

The existing **Viewer** chat owns the storage broker, Viewer adapter, durable
acceptance and customer → project/subcategory → task hierarchy backend. Claude
has acknowledged the workspace frontend audit/redesign on
`claude/viewer-workspace-redesign`; shared workspace/dialog seams require
coordination. Keep the dark theme and use the actual current Operations accent
tokens. Acknowledged ownership is not evidence that the redesign is complete.

TreeDar remains conditional on verified genuine LiDAR and a real worker adapter.
Gaussian splats remain disabled future work. Partner access and new storage pools
are future scope, not deployed features.
