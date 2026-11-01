# Inactive node mutation broker candidate

This code is disabled and has no listener or deployment configuration. It
must not be enabled on either processing PC. It supports NodeODM init and an
in-process exact-file upload rehearsal; commit, cancellation, result custody
and reservation release are absent.

A coordinator helper signs a grant after reading a matching live Viewer job,
claim token and initialization phase. The node checks the signed node, engine,
authority, epoch, job, engine task, environment, generation, operation and
body digest. Its NodeGate reservation and SQLite started intent survive an
uncertain response; exact completed replay does not repeat the mutation.
For uploads, an explicit offline issuer schema journals the operation ID and
binds the grant to the stored filename, length and SHA-256; its private issuer
journal also pins the Viewer-relative file path and its TaskStore root and task
directory names and device/inode identities. The sender re-reads that
Queue/TaskStore binding before opening the file and after its descriptor check;
these observations do not fence concurrent privileged filesystem writers.
Exact completed replay checks node state before starting a producer. If another
sender completes after that check, the broker reports a replay and the unused
producer is stopped and joined without treating its closed socket as a failure.
The node streams a
dedicated socket into deterministic multipart while checking the same length and
digest. The node retains an `awaiting_sender` hold after backend acceptance
until the local descriptor sender completes its own identity check and the
coordinator signs a separate confirmation. This HMAC is an assertion by the
trusted signer, not independent proof that descriptor verification happened;
any holder of that key could produce it. An interrupted or unconfirmed upload
is held and never resent automatically. This confirmation is only exercised in
one process; it is not yet an authenticated or durable cross-host protocol.

The Queue check is a point-in-time observation. A signed grant is not revoked
when the coordinator later changes state or loses authority. Caller-provided
engine identity is not yet bound to trusted coordinator configuration. No
qualified handover, reservation reconciliation, grant issuance ledger,
transport authentication, clock policy or cross-host epoch fencing exists.
The changed experimental journal schema rejects earlier prototype databases;
no migration is provided. The coordinator has no durable accepted-upload
acknowledgement or recovery mechanism, and there is no broker listener.
The loopback adapter uses one absolute deadline across identity checks and the
socket exchange; this does not qualify an end-to-end cross-host request timeout.
The adapter preserves the existing NodeODM client's request convention: it
omits the token query for an empty token and URL-encodes a nonempty token.
Docker administrators, local processes, other containers and direct engine
access can bypass these cooperative checks. A broker that already forwarded
init can remain held indefinitely. Database and filesystem ownership are not
qualified against privileged or same-user hostile modification.

The synthetic tests use the actual Queue, descriptor sender, NodeGate,
persistent SQLite and a loopback HTTP peer, but Docker identity checks are mocked.
They do not establish
production network isolation, actual NodeODM image compatibility, process
power-loss recovery or authenticated remote coordinator transport.
