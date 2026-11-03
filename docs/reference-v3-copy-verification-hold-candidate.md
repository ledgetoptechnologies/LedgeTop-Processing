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
