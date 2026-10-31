# Draft Viewer-worker claim boundary

This is a disabled, coordinator-side prototype. It does not reserve a node,
allocate space, transfer inputs, call an engine, accept a result or authorize
cleanup. `VIEWER_WORKER_CLAIM_QUALIFIED` is false; the application refuses
startup with the feature enabled until an independently reviewed migration and
rollback procedure exists. It must not be activated by patching that guard in
production.

When explicitly constructed in isolated tests, the protected reference worker
route `POST /ltds/worker/v1/viewer-claims` authenticates the existing worker
bearer to its configured node, worker and instance. An explicit per-node list
of allowed production/staging environments is mandatory; the request's
environment must agree with that policy and the durable Viewer job. A single
SQLite transaction checks the global production-priority queue head, idle slot,
node maintenance state, conservative small-node-first selection, Viewer task generation,
persisted file rows and option evidence; creates a private claim, token and
engine UUID; and marks both job and Viewer task claimed. The returned state is
`claimed_unenrolled`. It is not permission to start the engine. Same principal,
request and unchanged task binding replay the identical token and response;
conflicting request, node, worker, environment or stored metadata holds. A
replay after a coordinator restart may return the same response while the job
is held; neither replay nor the token authorizes node reservation or dispatch.

The input digest covers the *stored upload-time inventory* (names, byte counts,
recorded hashes and photo flags). The claim checks each path is a regular
single-link file of the recorded size, but does **not** rehash current file
bytes. It is not a validated content manifest or a transfer grant; a future
worker must verify all bytes against an independently bound manifest before
dispatch. The pipeline hash uses the Viewer's canonical stored options and
outputs. Claims may scan many input rows and stat files inside a SQLite write
transaction; performance and starvation on large projects are unqualified.

This draft also freezes the ordered upload-time inventory in the same transaction
as the claim. The authenticated worker may read pages of at most 100 entries at
`GET /ltds/worker/v1/viewer-claims/<job-id>/inventory/<cursor>` using its worker
bearer and exact claim token. Each entry includes an ordinal, opaque identity,
name, byte count, stored SHA-256 and photo flag; it does not expose a source
filesystem path or input bytes. The next cursor is null at the end. Consumers
must iterate from zero without gaps and independently recompute the original
`inputInventorySha256` over the canonical JSON array of `{name, bytes, sha256,
isPhoto}` entries in order. A page or object ID alone does not prove a complete
inventory or current file bytes. A held-on-restart claim permits only the same
authenticated metadata read; it grants no node enrollment or dispatch.

Paging bounds individual responses and does not rescan every file per page.
Claim creation currently loads O(N) filename/hash metadata and stats O(N) files
under one SQLite write transaction. One isolated tmpfs test exercised 10,000
tiny synthetic inputs over TLS and verified 100 bounded pages and the digest;
it does not establish memory, storage, time or correctness for a real 50 GB
dataset, concurrent clients, network interruption or a deployed server. There
is still no direct-Viewer object streaming endpoint, node input allocation,
byte verification, durable input custody or safe dispatch.

Creating the new table is a durable mode marker. In source containing this
draft, the legacy Viewer scheduler and generic Viewer-profile queue claims
refuse to dispatch while it exists. Startup with the marker but without the
matching feature configuration is refused. This does not fence older binaries
that ignore the marker, a pre-existing in-flight dispatcher, direct NodeODM
clients, privileged Docker access or an unrelated coordinator state copy.
Per-node environment policy is shared by the configured workers on that node;
it is not a distinct per-worker authorization policy. Unknown nodes, nodes
whose stored maintenance state blocks work, and wrong roles are held; small
jobs do not fall back to main here. A stored `active` or `scheduled` state is
not a health check, heartbeat, engine reachability check or NodeGate proof: a
physically offline node can still receive a durable claim and remain awaiting
enrollment.

The persisted marker and its new schema have no qualified upgrade, rollback,
backup or host fencing integration. Before this could be activated, a real
authenticated node worker must durably reserve the actual physical NodeGate,
bind the exact claim and engine identity, and reserve qualified output storage;
the coordinator must persist a replayable node-enrollment acknowledgement and
only that worker may dispatch. Input broker, capture, durable results, Viewer
custody, retention and maintenance integration remain unimplemented. Real TLS
tests use synthetic worker credentials and files, not production principals,
node hosts, engine containers, Viewer, power failure or NAS storage.

The isolated suite exercises the actual loopback TLS handler and SQLite state,
including a process killed after the transaction commits but before its TLS
response, followed by restart and exact replay. It injects a database abort,
competing worker principals, altered stored settings, invalid photo metadata,
wrong node/environment and a missing file. The network-none boundary comes from
the test runner invocation, not from an assertion inside the test itself.
