# Reference pipeline implementation checkpoint

This is an additive, opt-in successor to the existing multipart Viewer provider.
The coordinator TLS API is installed at `https://192.168.50.89:18443` under the
original queue authority. The existing Viewer provider still uses its unchanged
native endpoint at port 3005; reference workers, NAS broker and reference Viewer
client are not activated. Real-data testing is paused until Beau supplies a
smaller dataset. See `evidence/reference-installed-20261028.json` for the bounded
installed acceptance and remaining limitations.

## Current integration boundary, October 28

The latest committed source adds optional original-claim restart continuation
with atomic epoch/receipt transactions and coordinator lease protection through
commit. See [reference-claim-recovery.md](reference-claim-recovery.md). This API
is not configured in the installed runtime. Worker recovery, final mixed-pipeline
delivery and outer-v2 integration still require qualification.

Native ODM and TreeDar execution controllers, the concrete parent guard and
native bundle validator/writer have source qualification. The TreeDar controller
prepares its private RAW directory before launch. Local publication and worker
recovery are being integrated separately; their presence in a worktree does not
mean they are deployed or capable of settling the parent pipeline.

Existing image-update discovery services on both PCs remain installed on their
qualified source, independently of these inactive components. Their working
directories must not be changed to newer source without migration acceptance.
The current shared Hub note is
`ledge-top-processing/orchestration/reference-pipeline-20261027.md`; historical
test counts below describe their named source checkpoints.

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
  on an explicit loopback/TLS listener. The coordinator's opt-in TLS listener is
  installed; downstream worker intake and clients remain inactive.
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
  against the global metadata quota. `reference_terminal.py` and the public API
  expose confirmed terminal outcomes and exact durable metadata acceptance.
  Cancelled jobs retain their actual public outcome; terminal ACKs accept no files.
- `contracts/reference-v1.contract.json` is the identical Python/Viewer fixture.
  Its SHA-256 with LF line endings is
  `2c49ff86b8e20384c400ff56c8365418e643108aa9e6c29b46e938554e989b37`.

## Remaining integration and acceptance

1. Mount the implemented Viewer storage broker on the actual NAS HTTPS route and
   connect reservation-scoped transfers. Waiting jobs retain metadata only.
2. Install the private worker daemon with persistent scratch and exact existing
   engine/gate bindings. Never replay uncertain engine mutations automatically.
3. Connect the qualified Viewer acceptance/receipt components to the production
   ingestion closure and verify its actual storage durability barriers.
4. Wire the separate input-retention receipt to enforced NAS retention and worker
   input cleanup; qualify opt-in exact engine-task cleanup on the installed path.
   Missing files, downloads and age are not ACKs.
5. Wire TreeDar into host-driver selection with authenticated NAS provenance and
   sequential mixed-pipeline support before enabling its capability.
6. Repeat relevant synthetic failure/recovery checks against the complete installed path
   before enabling real data; the isolated source checks do not prove deployment.

Source acceptance: the latest full 587-test checkpoint passes in an isolated Linux container
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

The subsequent joint fixture exercised the complete synthetic transport chain
over real TLS: actual Viewer broker → reserved worker input → synthetic engine →
pinned worker result → coordinator stream → actual Viewer client and durable
acceptance ledger → receipt relay → exact worker archive cleanup. A second run
restarted the coordinator after the worker outbox commit, then exited Viewer with
code 86 after its ledger commit and before ACK. The worker retained its archive
and offered zero cleanup candidates until Viewer restarted and replayed its exact
receipt. Engine initialize/upload/commit/download counts stayed one each. The
original 22-byte Viewer ZIP remained after worker cleanup. All exact test
containers, volumes, certificate material and internal network were removed and
their absence verified. See `evidence/reference-joint-20261027.json`.

These runs used a synthetic engine, three-byte input and an empty valid ZIP.
They prove transport/process recovery, not photogrammetry geometry, real TreeDar,
NAS hardware durability or installed runtime readiness. Initial failures exposed
microsecond-versus-millisecond expiry format and a ten-minute reservation outside
the broker's five-minute policy; the fixture now emits millisecond UTC and uses
240 seconds. Runtime policy configuration/negotiation remains explicit work.

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
Confirmed engine failure/cancellation now has a durable terminal-outcome outbox and
Viewer metadata-acceptance relay. That receipt does not authorize file deletion.
Reservation renewal and installed NAS/runtime mounting remain integration work;
retained engine task data may intentionally block the strict maintenance census.
Success-only engine task cleanup is implemented as a separate opt-in described
below; it does not treat terminal metadata acceptance as file acceptance.

## Operational source qualification, October 27

The optional coordinator TLS listener now shares the existing queue authority;
`PROCESSING_REFERENCE_CONFIG` must explicitly name an owner-private configuration.
It remains disabled without that configuration. Separate worker, result-reader,
Viewer and legacy credentials cannot be substituted for each other. Fresh engine
options are exposed through `/ltds/v1/options`; this first reference implementation
admits safe numeric/boolean values. String enums and EPSG settings still need their
typed contract and are not advertised as supported.

`ops/reference_worker_daemon.py` builds the real `NativeEngine` driver, persistent
transfer/result/failure journals and private TLS result service. The pre-existing
physical gate binding is checked independently of the new HTTPS transport origin.
The daemon holds an OS lock across startup and shutdown, stops accepting new reads,
and joins active streams and driver operations before releasing ownership. A forced
process kill can still leave stale durable read pins: they remain held pending
explicit reconciliation, rather than being released merely because of a restart.

The operational-builder joint probe passed with the actual driver and real
`NativeEngine` HTTP requests against an isolated synthetic NodeODM server. Each
initialize/upload/commit/download occurred once. The real Viewer accepted and
retained the 22-byte synthetic archive before automatic worker archive cleanup.
The three-byte input and empty ZIP prove transport, not photogrammetry quality.
All owned fixture resources were removed. This extends the previous process-crash
probe; it does not claim the operational daemon itself has been deployed.

TreeDar admission now verifies bounded LAS/LAZ geometry, projected metre units,
declared vertical datum, classified ground coverage and a trusted acquisition
receipt. A filename or point-cloud header alone cannot prove LiDAR provenance.
LAZ count assurance additionally requires an authenticated source count; decoder
EOF alone is not an independent count proof. The actual cached TreeDar CLI passed
an isolated synthetic 4,555-point test: all three known crowns were recovered at
12, 15 and 18 metres, with finite projected output rasters. This is synthetic
geometry qualification, not genuine sensor acquisition or an integrated TreeDar
worker path. TreeDar availability remains disabled until that integration exists.

See `evidence/reference-operational-20261027.json` for this source checkpoint.
Outstanding deployment work includes the NAS broker HTTPS mount, input-retention
enforcement before input-cache deletion, installed engine-task cleanup qualification,
runtime PKI/service installation and production Viewer ingestion. No real photo trial was
started; Beau will choose a smaller dataset separately.

## Separate source retention and engine cache cleanup

`contracts/source-retention-v1.contract.json` is an additive, LF-stable shared
fixture. `reference_source_retention.py` validates the complete, ASCII-sorted
input object closure against job/generation/environment, manifest and pipeline
digests. Its explicit `source_retention` family and `until_release_ack_v1`
guarantee cannot be substituted by result or terminal receipts. The validator
performs no file deletion, issuer authentication or pin installation. Viewer
must enforce durable source pins against **every** purge path, or use separately
managed immutable storage, before issuing it. A future explicit release handshake
must retire those pins; result acceptance and TTL cannot do so. Global receipt-ID
role uniqueness and durable coordinator relay remain required integration work.

`reference_engine_cleanup.py` independently removes one exact, successful native
engine task only after coordinator metadata acknowledgement and full, locally
persisted Viewer result acceptance. It retains the original worker input files,
verifies their hashes again, waits for grace and released result pins, and holds
the same physical node gate across idle census and removal. A FULL journal intent
precedes the remove call; a lost response permits only read-only exact-UUID
reconciliation. An absent task without an intent is held, never counted as a
successful deletion. Failed/cancelled metadata ACKs authorize no removal.

The worker daemon's `engineCleanupEnabled` is separate from archive
`cleanupEnabled`, and defaults to false. The driver keeps deferred cleanup proof
after execution capacity is released; it does not need to claim a new execution
slot or receive a new Viewer acknowledgement to retry grace/pin checks. No new
cleanup is enabled on either real processing PC by this source change.

## TreeDar host adapter and rollout preparation

`treedar_engine.py` is a main-only Linux host controller, separate from the ODM
HTTP adapter. It uses the existing canonical physical Docker Engine gate and a
configured local Unix socket; the engine container receives no socket, network,
credentials or submission-controlled executable. The image is an exact cached
ID with `--pull never`. Trusted installed bootstrap code, input and control
metadata mount read-only; outputs remain in reserved persistent scratch.

Create/start/stop intentions commit before Docker mutations. Uncertain launches
are reconciled by exact container identity/image/binding inspection, never by
creating another container. Terminal engine and output proofs persist before
scratch settlement; completion acknowledgement and gate release remain the
driver's responsibility. Runtime processing has no CPU, RAM, PID or thread caps,
and no processing timeout by default. Separate synthetic test containers impose
their own 2 CPU / 2 GiB safety bounds.

The fixed in-image bootstrap was executed using actual TreeDar 0.0.1 in the
cached image as UID/GID 1000, with no network, read-only root/source and temporary
input/control/output mounts. It verified the synthetic LAS, recovered all three
known 12/15/18 metre crowns and emitted 16 outputs whose hashes were independently
checked. `ops/qualify_treedar_bootstrap.py` reproduces that synthetic-only check.
Host Docker lifecycle tests additionally cover uncertainty, gate retention and
proof recovery; those use a controlled Docker transport double. They do not prove
installed host execution or genuine LiDAR acquisition.

This adapter currently accepts a TreeDar-only single LAS/LAZ job. Combined
ODM-plus-TreeDar stages, provenance issuance, the general worker driver's TreeDar
dispatch and Viewer ingestion are still required. TreeDar capability remains
disabled. Gaussian splats remain disabled as well.

See [reference-deployment.md](reference-deployment.md) for actual two-host
preflight, source-only staging/import verification and the remaining TLS/NAS,
service installation and rollback steps. Existing coordinator/native provider
and both engine services were left running unchanged.

The final source checkpoint passed all 543 Linux tests with no skips, the actual
bootstrap probe above, and an independent blocking review. Both Python and Viewer
Node validators share the additive source-retention fixture exactly; an independent
isolated Linux run passed all 23 Node protocol/retention tests. Evidence and source
hashes are in `evidence/reference-readiness-20261027.json`. This remains source
qualification and inactive source staging, not completion of runtime acceptance.

The subsequent source checkpoint passed all 572 Linux tests with no skips.
`treedar_delivery.py` now projects exact controller terminal proofs into the
existing result/failure outboxes, result serving and Viewer acceptance interfaces.
It persists and validates the coordinator acknowledgement before physical gate
release. Restart recovery may use only the exact retained gate tombstone; it
cannot alter a newer reservation. If a later completion overwrites that sole
tombstone before the local delivered checkpoint, recovery remains held for
reconciliation. General host-driver selection and combined pipelines are still
unwired, and TreeDar availability remains disabled.

The coordinator now supervises both listeners, handles SIGTERM by requesting a
graceful drain, joins active handlers and the Viewer dispatcher, and releases the
queue lock last. Actual Linux signal and blocked-stream tests cover that ordering.
Unexpected listener failure makes health unavailable and exits with a sanitized
failure. Private TLS/configuration provisioners also passed actual Linux/OpenSSL
checks; deployment preparation is recorded in `reference-deployment.md`.

Missing delivery acknowledgements retain results. Disk pressure stops intake.
Production priority, the shared execution fence and maintenance gates apply to
both submission modes; an already running staging job is allowed to finish.

## Per-job engine routing source checkpoint

The optional `ExecutionRouter` now binds each claimed job to its ODM or TreeDar
adapter and exact runtime pins before reservation, transfer or engine mutation.
The private worker discovery response includes an advisory execution descriptor;
the driver validates the full authenticated claim again before using it. Retained
success/failure acknowledgements and Viewer acceptance use the selected adapter.
Crash tests cover uncertain engine starts and lost acknowledgement checkpoints.
Default ODM behavior remains available without opting into routing. Combined
ODM-plus-TreeDar execution still requires a sequential stage contract.

The broker-bound provenance client rejects stale, revoked, missing or rebound
receipts and rechecks freshness at final physical admission. Its exact wire and
trust boundary are documented in [reference-provenance.md](reference-provenance.md).
This checkpoint passed 634 isolated Linux tests with no skips and the actual
synthetic TreeDar bootstrap. These additions are source qualification, not a live
TreeDar capability or an installed NAS issuer. The installed coordinator remains
on core source `e2ccf489`; current runtime evidence is in
`evidence/reference-installed-20261028.json`. Earlier source-only deployment
paragraphs above describe their historical checkpoints.

## Owned result-reader recovery source checkpoint

The Linux worker now owns a dedicated ledger-bound reader lease. A replacement
process can retire only explicitly registered earlier-generation read pins after
acquiring that same exclusive lease. Anonymous and foreign pins remain held.
Streams record their own descriptor close before pin release, and handler drain
precedes lease release. Uncertain attachment, close or commit outcomes retain
ownership. No reservation, acceptance or cleanup rule is bypassed.

All 663 isolated Linux tests passed without skips, including actual subprocess
death/restart, fork contention, first-bootstrap crashes, FD reuse, release faults
and HTTP HEAD/abort/drain. Independent source review found no blocking issue.
The engine bootstrap source is unchanged from the preceding actual TreeDar
qualification. Details are in [reference-readers.md](reference-readers.md) and
`evidence/reference-readers-20261028.json`. This source is not deployed; NAS
integration and runtime acceptance remain pending.

## Managed TreeDar runtime source checkpoint

The managed factory now preserves one delivery journal while using immutable
controller journals per runtime. Closed idle activation retains old result and
receipt access without an old executable. Exact-runtime restart can recover a
pending job without activating a new runtime or repeating an uncertain launch.
Atomic downgrade fencing rejects pre-managed code after publication. Factory
ownership spans driver and reader drain. Details are in
[reference-treedar-runtime.md](reference-treedar-runtime.md).

All 688 isolated Linux tests passed without skips; independent source review
found no blocking issues. This migration remains source-qualified, not installed.
Separately, TreeDar's actual bootstrap ran successfully on main with 4,555
synthetic points, three recovered crowns and 16 hash-verified outputs. Both nodes
were drained and only owned plans reopened; test data/container were removed.
The image cache and inactive source stage remain, with existing services and
coordinator unchanged. Evidence is in `evidence/main-treedar-bootstrap-20261028.json`.

NAS broker/provenance issuer, retention enforcement and the agreed outer-v2
transport are still incomplete in source as well as installation. The Viewer
chat is active and owns that integration; its existing protected release gate
does not establish processing acceptance. Claude's workspace changes have source
review, while browser/mobile and installed acceptance remain open. Combined
sequential ODM-to-TreeDar final delivery and the selected smaller real-data trial
are still pending; TreeDar/Gaussian capabilities remain disabled.

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
