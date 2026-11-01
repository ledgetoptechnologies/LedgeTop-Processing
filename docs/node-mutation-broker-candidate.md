# Inactive node mutation broker candidate

This code is disabled and has no listener or deployment configuration. It
must not be enabled on either processing PC. It only supports NodeODM init;
upload, commit, cancellation, result custody and reservation release are absent.

A coordinator helper signs a grant after reading a matching live Viewer job,
claim token and initialization phase. The node checks the signed node, engine,
authority, epoch, job, engine task, environment, generation, operation and
body digest. Its NodeGate reservation and SQLite started intent survive an
uncertain response; exact completed replay does not repeat the mutation.

The broker requires a `LocalCoordinatorAuthority` for the exact same-process
Queue and installed host-wide `AuthorityLease`, checks creator PID, epoch,
authority, both lock inodes and the persistent lease marker, revalidates the
current job and task under the Queue lifecycle mutex, and holds both lease and
Queue ownership across init. Queue.close and lease.close wait for an operation
on another thread; reentrant close is refused. After either closes an old signed
grant is refused. Process death also terminates this same-process broker.
This intentionally cannot operate on the remote main node: no remote witness,
fallback or claim of cross-host revocation is provided. It assumes the trusted
Queue implementation retains its flock until close; checking the descriptor and
inode does not independently attest the kernel lock or prevent other SQLite
writers, privileged peers or direct backend connections.

Caller-provided
engine identity is not yet bound to trusted coordinator configuration. No
qualified handover, reservation reconciliation, grant issuance ledger,
transport authentication, clock policy or cross-host epoch fencing exists.
The loopback adapter uses one absolute deadline across identity checks and the
socket exchange. Broker mutex acquisition, authority locks, SQLite and adapter
each have separate waits; there is no single end-to-end deadline and no
qualified cross-host request timeout.
The adapter preserves the existing NodeODM client's request convention: it
omits the token query for an empty token and URL-encodes a nonempty token.
Docker administrators, local processes, other containers and direct engine
access can bypass these cooperative checks. A broker that already forwarded
init can remain held indefinitely. Database and filesystem ownership are not
qualified against privileged or same-user hostile modification.

The synthetic tests use the actual Queue, NodeGate, persistent SQLite and a
loopback HTTP peer, but Docker identity checks are mocked. They do not establish
production network isolation, actual NodeODM image compatibility, process
power-loss recovery or authenticated remote coordinator transport.
