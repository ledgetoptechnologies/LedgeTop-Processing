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

Source acceptance: the full 336-test suite passes in an isolated Linux container
with network disabled and a read-only source mount. Linux also exercises the
symlink checks that Windows skips without symlink privileges. Tests include real loopback HTTP, metadata grant
replay, transfer corruption/expiry and completion-commit crash windows. These
are synthetic checks, not NAS hardware power-loss or installed runtime evidence.

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
