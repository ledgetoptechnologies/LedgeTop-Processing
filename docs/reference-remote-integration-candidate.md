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
the local per-object verification. The current coordinator also queries the
database per object and repeats the check under a ten-second authority window;
large-job scaling and a measured bounded query strategy are unfinished.

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
