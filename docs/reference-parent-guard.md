# Parent guard foundation

`processing.reference_parent_guard` supplies a concrete authorization check for
the inactive sequential parent pipeline. It installs no listener or service and
does not enable a capability, change the daemon factory, or activate processing.
It creates no claims, reservations, grants, outboxes or cleanup intents.

## Wiring and ownership

The caller must already own the durable, private state root through the actual
`DaemonStateLock`. Construct `DriverClaimTokens` over the existing private driver
SQLite database, then construct `ProductionParentGuard` with the actual
`PipelineJournal`, `ScratchLedger`, claim-state client and same state owner.
The driver database, journal and ledger must share that root. Databases must be
private single-link regular files in SQLite rollback mode; read-only WAL access
is refused before SQLite can create shared-memory sidecars.

No configured digest authorizes execution by itself. Every guard invocation
checks the real owner lease/PID, journal binding, private driver claim token,
original submission hashes, exact reservation and existing physical fence.
The private token is read into memory from existing driver state and is never
serialized into this guard's policy, journal, exception or observation.
`WorkerCredential` keeps the separate worker bearer in memory; its representation
excludes the secret. This is process-local handling, not an operating-system
secret store or protection against arbitrary trusted Python code.

`ClaimStatePolicy` pins authority, node, worker, worker instance, credential role,
HTTPS origin, trust-root digest, timeout and response size. The concrete client
disables environment proxies and redirects, verifies TLS hostname/certificate,
requires TLS 1.2 or newer, and rechecks the SSL context policy. Credentials must
come from the trusted factory's existing private configuration. There is no
caller-selected job endpoint or mutation/fallback method.

This transport deliberately requires verified HTTPS, even for loopback. The
existing standalone driver's plaintext loopback allowance is not inherited.
Before factory integration, qualify a verified HTTPS private worker endpoint
and matching driver binding, or separately review an explicit protected-loopback
policy. The current runtime is unchanged by these source files.

## Fresh authorization and lock order

`check(parent_binding, mutation=True)` permits a new engine attempt only when a
fresh authenticated GET `/ltds/worker/v1/jobs/{jobId}/claim-state` reports the exact
parent as running in processing phase. It requires all 16 closed observation
fields: schema/protocol, authority, job, generation, environment, node, worker,
worker instance, claim request, claim-token SHA, original manifest SHA, pipeline
digest, status, phase and held reason. Unknown/missing fields and loosely equal
JSON types are refused.

The worker bearer fixes identity server-side; the private claim token provides
the other credential layer. A former observation is never cached as permission.
An old server's 404, auth failure, timeout, network loss, malformed or oversized
response holds work. No POST claim/reclaim or automatic replay is attempted.
Responses must be finite duplicate-free JSON, bounded and `Cache-Control:
no-store`. Errors use fixed codes without request headers or response bodies.

Call the guard **outside** the Native stage's NodeGate context. The guard takes a
nonblocking real OS flock on the existing gate lock, using a read-only descriptor
and strict private identity checks. It intentionally avoids `NodeGate.__enter__`,
which has directory/lock publication and chmod behavior. While holding that
existing physical lease it reads the ledger, private claim and fresh remote
snapshot, then repeats owner/fence/reservation checks before releasing the lease.
Contention holds immediately. No nested gate acquisition is allowed.

The fence must still match authority, physical engine, node, origin, gate
generation, exact parent receipt/job and absent completion digest. The whole
parent reservation must retain input bytes plus both stage output budgets;
allocated bytes include all original inputs and may grow monotonically to retain
the ODM archive while TreeDar runs. Neither stage receives another reservation.
Terminal/released/mismatched/expired reservations are refused. Reserved expiry
uses the greater of current clock and persisted high-water clock without writing
clock state. Started engine ownership does not expire merely because its initial
transfer TTL elapsed; uncertain ownership forbids new mutations.

`mutation=False` additionally permits an exact `held/coordinator_restart`
observation only when local reservation state already proves `engine_started`
or `uncertain`. This authorizes evidence collection for previously running work,
never starting a next stage. Operator holds, terminal results and merely reserved
work remain held. The guard neither advances the journal nor settles the parent.

## Verification and limits

`tests.test_reference_parent_guard` uses an ephemeral synthetic TLS certificate,
actual loopback HTTP worker API, Queue/ReferenceStore, PipelineJournal,
ScratchLedger, NodeGate and DaemonStateLock. No external network, production
credential, client dataset, NAS or runtime container is used. Tests cover fresh
read-only requests, absent caching/fallback, exact observation and local binding
corruption, real coordinator restart, operator/terminal holds, retained parent
budget, gate contention/release, privacy drift and transport/PID drift.

The HTTP observation and local fence are not a distributed transaction. Local
state ownership and the physical receipt prevent cooperating schedulers and
maintenance paths from changing ownership during the check. Every actual engine
mutation still requires the Native controller's immediate runtime/image/owner/
reservation/gate recheck. Future integration must preserve that ordering and
prove durable root publication, HTTPS identity/trust provisioning, lifecycle
shutdown, driver-origin migration, and restart reconciliation. Passing these
synthetic source tests does not qualify deployment or advertise combined support.
