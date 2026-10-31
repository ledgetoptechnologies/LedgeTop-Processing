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
node state, conservative small-node-first selection, Viewer task generation,
persisted file rows and option evidence; creates a private claim, token and
engine UUID; and marks both job and Viewer task claimed. The returned state is
`claimed_unenrolled`. It is not permission to start the engine. Same principal,
request and unchanged task binding replay the identical token and response;
conflicting request, node, worker, environment or stored metadata holds.

The input digest covers the *stored upload-time inventory* (names, byte counts,
recorded hashes and photo flags). The claim checks each path is a regular
single-link file of the recorded size, but does **not** rehash current file
bytes. It is not a validated content manifest or a transfer grant; a future
worker must verify all bytes against an independently bound manifest before
dispatch. The pipeline hash uses the Viewer's canonical stored options and
outputs. Claims may scan many input rows and stat files inside a SQLite write
transaction; performance and starvation on large projects are unqualified.

Creating the new table is a durable mode marker. In source containing this
draft, the legacy Viewer scheduler and generic Viewer-profile queue claims
refuse to dispatch while it exists. Startup with the marker but without the
matching feature configuration is refused. This does not fence older binaries
that ignore the marker, a pre-existing in-flight dispatcher, direct NodeODM
clients, privileged Docker access or an unrelated coordinator state copy.
Per-node environment policy is shared by the configured workers on that node;
it is not a distinct per-worker authorization policy. Unknown or unavailable
node state and wrong role are held; small jobs do not fall back to main here.

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
