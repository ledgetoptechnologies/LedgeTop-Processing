# Disabled remote reference integration

This branch composes a reviewed broker/fence candidate with the newer worker
daemon source. An explicit remote photo route now connects the driver to the
claim-bound plan issuer, pinned mTLS broker and proof-fenced result delivery,
but several qualification flags remain false and the existing v1 path remains
the default. Opting in without qualification fails before direct engine use.
The issuer, session factory and daemon route are candidates, not production
endpoints. Both engine cleanup and scratch cleanup are refused for this route.

The protocol has a material large-job limitation: the remote commit response
currently sends the full per-photo inventory in one 100 KiB mTLS frame. A
representative 1,000-record inventory is about 200 KiB, so even the cluster's
1,000-photo target is unsupported. The candidate now rejects an oversize frame
before creating a commit-started intent. A safe extension needs ordered pages
of at most 64 KiB under one fenced session, with a header binding the commit,
claim, manifest, node, epoch, nonce and total count and an incremental digest
over canonical length-prefixed records. The node must
verify every page, unique object and local journal entry against that same
commit/claim identity before forwarding; incomplete, repeated, reordered or
changed pages must hold the started intent. A final digest alone cannot replace
the local per-object verification. The coordinator now uses a joined inventory query and repeats verification
under a ten-second authority window. Large-job behavior on the actual hardware
and complete transfer performance remain unqualified.

A separately disabled paging candidate now sends plan, commit and proof
inventories in bounded authenticated pages. A synthetic one-photo daemon test
passed, as did actual mTLS broker commit and proof tests with 1,000 and 10,000
seeded, signed inventory/journal records (0.675 s and 2.116 s for the full
fixtures on the test host). Those fixtures sent one synthetic upload and mocked
the engine; they do not establish 10,000-photo transfer, storage, deadline
behavior on the real PCs, or production performance. Interruption and tampering
still require verification. Legacy unpaged inventory
records in this candidate also include objectId, so older and newer candidate
peers fail closed when mixed. A future deployment requires a coordinated paired
coordinator/node version rollout or explicitly negotiated compatibility; rolling
mixed versions are not qualified. The existing 100 KiB warning above remains
applicable to the default unpaged path.

The submission and driver control bodies still have 4 MiB limits, the object
limit is 10,000 including non-image objects, and transfer defaults do not enable
ranges and limit a single response to 16 MiB. Serial per-object transfer,
transfer expiry, scratch/backend/Viewer disk accounting and repeated manifest
work remain unqualified for a 50 GB dataset. Paging alone does not remove these
limits. Authority sessions remain capped at ten seconds and filesystem I/O is
not a hard-bounded operation.

A separate disabled transfer journal candidate records object intent, exact
manifest/reservation/directory identity and verified bytes before reconciling
the separate scratch ledger. The driver route requires a process owner and new
empty driver history. Synthetic exception/rebuild and filesystem replacement
tests cover selected recovery boundaries; actual process death, filesystem and
ledger recovery under power loss, nonempty migrations and hostile writers are
not qualified. A further disabled normalized path stores claim/grant once and advances a small
cursor, with claim observation and per-object durable checkpoints. The legacy
path still repeats full grant validation and growing JSON writes per photo.
A full 10,000-object synthetic transfer exceeded 110 seconds, so throughput
qualification remains unresolved. Ranges remain disabled without broker
capability qualification.

A separate new-state-only coordinator claim-observation cache prototype
invalidates verified input metadata on source-row changes and reads current
claim/status on every observation; it remains disabled and never authorizes an
engine action. In a synthetic local API test with 10,000 one-byte metadata
records, first observation took 0.0169 s and the cached observation 0.00096 s.
A subsequent full 10,000-object synthetic one-byte transfer exceeded its
110-second in-test work budget and did not finish; the test did not report a
completed count. This is not a full dataset or network throughput test. The cache assumes a
trusted exclusive SQLite writer and does not qualify old binaries, database
replacement or rollback/restore. Long-running fetches may outlive a claim-state
observation; the independent broker mutation fence remains necessary.

Other remaining qualification includes nonempty state migration, separate-PC
and real NodeODM tests, authenticated result/Viewer handoff, crash recovery,
old-binary/direct-Docker isolation and operations after authority expiry. The
cooperative SQLite fence retains uncertain starts indefinitely; it does not
prove that an isolated physical node has stopped. No deployment is authorized
by this document. Initial node-binding provisioning requires an idle coordinator
with no historical broker intents; later node additions, replacement and
reprovisioning have no qualified procedure.

Each remote proof use currently retains coordinator and node journal rows.
The 100,000-proof cap will eventually stop polling and acknowledgement;
safe bounded retention and compaction are not implemented. At one proof per
minute the cap is reached in about 69 days. This is a production blocker.

Remote failure delivery has an unreconciled crash boundary after the failure
outbox checkpoint but before gate preparation. After gate release but before
the driver checkpoint, exact completed proof, failure receipt and gate
tombstone may be reconciled read-only. If either proof is merely started or
ambiguous, the job remains held for inspection.

A bounded synthetic daemon-child test uses a real daemon process, coordinator
and node mTLS sockets, local ledger/outbox and a loopback NodeODM fixture. It
checks success, failure/cancellation, and a killed daemon after a simulated
lost result receipt. This restart uses the same coordinator epoch; coordinator
restart/epoch recovery remains unqualified. The fixture patches physical Docker
identity probes and does not establish actual NodeODM, separate-PC, live Viewer
custody, or real network-outage behavior.

The local POSIX regression command covered 180 tests across
`test_reference_mtls_authority_candidate`,
`test_reference_broker_authority_candidate`,
`test_reference_adapter_loopback_candidate`,
`test_reference_remote_daemon_candidate`, `test_reference_api`,
`test_reference_runtime`, `test_reference_driver`,
`test_reference_driver_routing`, and `test_reference_worker_daemon_routing`.
It passed under an owned process-group timeout with synthetic fixtures only.
