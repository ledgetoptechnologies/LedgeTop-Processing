# Source grant v3 synthetic candidate

This is an unmounted SQLite model, not an authenticated grant, broker, stream
server, worker, or production enforcement mechanism. No code here lifts v2
activation or source-fetch holds. All input is synthetic. The comparison to
trusted state only has security meaning if an independent authenticated source
establishes the state and a real broker verifies grant authenticity and bearer
possession, expiry, object data, epoch revocation and stream fencing.

Provisional grant is closed: schemaVersion=3, protocol=ltds-source-grant-v3,
accounting={environment,jobId,generation,viewerAttemptId,reservationId,
manifestSha256,sourceVersion}, authority={activationDigest,
coordinatorAuthorityId,executionEpoch}, policy={chunkBytes,requestBudget,
byteBudget,attemptBudget}, expiresAt (integer Unix seconds), bearer, nonce and
objects (closed id,byteSize,sha256 entries). These fields and expiry representation
are proposed, not agreed with Viewer. Transfer/source limits and v2 fields and
semantics require exact inventory and agreement before any live adapter.

The accounting digest is SHA-256 of UTF-8 `ltds-source-accounting-v3`, one NUL
byte and canonical_json(accounting). It excludes epoch, nonce, token and
expiry. Policy and object closure are separately persisted and immutable for
that accounting key. The SQLite model counts requests/bytes and distinct
transfer attempt IDs across epochs and grant renewal. An exact request-id
replay returns stored metadata only: it does not stream bytes or authorize
uncharged retransfer. Changed request bindings fail. It does not implement
range overlap proofs or verified-byte receipts. It does not persist a trusted
current epoch or fence previously admitted streams; caller-supplied trusted
state in this model is a test fixture, not a source of authority.

The canonical request-ID algorithm and compatibility with existing Viewer v2
request IDs are unresolved release blockers. The model accepts bounded opaque
IDs solely to explore accounting; it does not assert cross-version compatibility.
A real design must bind IDs to immutable object/range/attempt identity, retain
existing IDs across renewal, account for retries/uncertain transfers, implement
nonce/bearer authentication, persist epoch authority, and reconcile legacy
ledger state without resetting budgets. Before implementation, inventory v2
expiry, bearer, nonce, object closure, attempt, transfer/source and policy
semantics, and agree on an explicit shared-version contract and test vectors.

A durable synthetic parent lineage keyed by environment/job/generation now
refuses any different accounting identity, including replacement reservation,
source version or Viewer attempt. This is deliberately a hold, not migration;
existing unlineaged ledgers are rejected. Atomic migration, full schema
validation and historical reconciliation remain unimplemented. Caller snapshot
comparison occurs before the SQLite write transaction; it is NOT an atomic
epoch fence, including if authority changes between check and charge. No
production authorization or transfer may rely on this model. Independently
provided synthetic operational caps bound source and policy counts; typed
object closure checks and source/attempt byte-budget relationship are enforced.
