# Inactive node mutation broker candidate

This code is disabled and has no listener or deployment configuration. It
must not be enabled on either processing PC. It only supports NodeODM init;
upload, commit, cancellation, result custody and reservation release are absent.

A coordinator helper signs a grant after reading a matching live Viewer job,
claim token and initialization phase. The node checks the signed node, engine,
authority, epoch, job, engine task, environment, generation, operation and
body digest. Its NodeGate reservation and SQLite started intent survive an
uncertain response; exact completed replay does not repeat the mutation.

The Queue check is a point-in-time observation. A signed grant is not revoked
when the coordinator later changes state or loses authority. Caller-provided
engine identity is not yet bound to trusted coordinator configuration. No
qualified handover, reservation reconciliation, grant issuance ledger,
transport authentication, clock policy or cross-host epoch fencing exists.
The adapter uses idle socket timeouts, not an absolute exchange deadline.
Docker administrators, local processes, other containers and direct engine
access can bypass these cooperative checks. A broker that already forwarded
init can remain held indefinitely. Database and filesystem ownership are not
qualified against privileged or same-user hostile modification.

The synthetic tests use the actual Queue, NodeGate, persistent SQLite and a
loopback HTTP peer, but Docker identity checks are mocked. They do not establish
production network isolation, actual NodeODM image compatibility, process
power-loss recovery or authenticated remote coordinator transport.
