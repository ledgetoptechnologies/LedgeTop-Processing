# Inactive v3 accepted-copy verification-only journal

The owner-scoped `POST /ltds/prepared/v3/submissions/{submissionId}/accepted-copies`
can be configured with an installation-owned `copy_keys_by_identity` map:
principal -> environment -> keyId -> {issuerId, publicKeyPem}. Pins are copied
and validated at construction, never read from the request. Wrong owner is 404;
missing pins or the existing result-custody readiness gate is 503. In the
current runtime `_v3_result_contract_ready()` is always false. No route is
mounted and no request can reach the verification-only write without changing
that hold. Synthetic tests patch readiness only to exercise the candidate.

The closed input is `{acceptance, viewerReceipt}`. Under one coordinator-owned
write transaction, the prototype checks authority, v3 principal/environment,
canonical prepared envelope and recorded activation, sealed result and exact
accepted object closure; it verifies the signed Viewer receipt using configured
Ed25519 pins. The Viewer `ltds-viewer-accepted-copy-v1` signed body has issuer,
key and receipt IDs; job/generation/environment; input, pipeline and result
digests; accepted object IDs with each byteSize/SHA; timestamp; and signature.
The signature domain is `LTDS-VIEWER-ACCEPTED-COPY-ACK-V1` plus NUL. It does not
sign task or attempt directly: the candidate ties the signed job to those IDs
through the independently persisted prepared envelope and activation.

Only exact verified bytes are journaled in a separate `reference_viewer_v3_copy_hold`
table as verification evidence; exact replay returns `verified_held`, while a
changed proof conflicts. This journal never inserts `reference_acceptances`,
never asserts Viewer import completion and never authorizes cleanup or removes
other holds. No automatic migration of pre-existing receipts is attempted.
Before use: implement/verify v3 source and authenticated worker/result custody,
principal-key provisioning and revocation, active authority/epoch fencing,
collision/rotation handling across all receipt namespaces, crash/rollback
semantics, Viewer schema agreement, real peer integration, and independent
review. A synthetic signed receipt and DB-injected result do not establish any
of these production guarantees.

The isolated successor rejects a public-key fingerprint or issuer ID configured
for more than one principal/environment. It checks new/exact held-copy proof
IDs against current legacy acceptance, signed Viewer receipt, terminal and
import-confirmation tables in the same write transaction; legacy acceptance,
terminal and import-confirmation writes likewise check the held-copy table.
Pre-existing historical collisions are not migrated or reconciled: an operator
must audit them before enabling any future use. This candidate does not prove
identity safety across databases, older binaries or unmounted alternative
writers, and retains all source/result/cleanup holds.

The shared scanner now checks bounded canonical proof bytes and SHA, prepared
JCS envelope digest and activation/job/owner binding before using any held IDs.
It fails closed on corruption and caps aggregate scan rows/bytes. Cross-journal
checks only apply to tables in the same SQLite file; the generic standalone
import store can use another database and therefore does not share this fence.
A single pinned authority/database or independently fenced global ID index,
historical collision audit and migration are required before deployment. Key
and issuer uniqueness is enforced only within this API instance; a durable
global registry and key rotation protocol remain missing.
