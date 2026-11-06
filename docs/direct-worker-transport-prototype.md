# Private direct-worker transport prototype

This is an unwired in-process boundary, not a listening HTTP service. Its
`protected_transport` argument is a trusted listener observation; it must never
come from an HTTP header. The boundary checks a configured coordinator bearer
and exact authority before reading a stream, and delegates file writes to the
existing node-gated, pinned worker receiver. Installed TLS, credential storage,
request limits, cancellation, streaming lifetime and peer authorization remain
unimplemented. A caller cannot infer production readiness from this prototype.

After every object in the current combined V1-style manifest is reverified,
the worker can persist an Ed25519-signed local closure under the same durable
writer pin. Its exact closed envelope has `protocol`, `schemaVersion`,
`issuerId`, `keyId`, `closure`, and unpadded base64url `signature`. The signature
input is `LTDS-DIRECT-WORKER-LOCAL-CLOSURE-V1\0` plus the existing Python
canonical JSON encoding of the other fields. The closure binds authority,
node, worker and instance, reservation, job, attempt, generation, environment,
combined manifest and derived inventory hashes. It explicitly records
`sourceCustodyAccepted=false` and `processingAuthorized=false`.

The signer must use a separately provisioned private worker key. Verification
requires an independently pinned issuer, key, public key and expected complete
closure. Replays reverify all local objects and return identical persisted
receipt bytes only for the exact binding/key. A changed key, altered receipt,
missing/changed object or uncertain writer is held. A lost reply after receipt
publication but before pin retirement remains inspection-only; automatic
recovery is not implemented. This is not a Viewer receipt and authorizes no
source release, processing or deletion.

Viewer's proposed **separate coordinator receipt** must additionally bind the
authenticated Viewer principal and destination, authority/environment,
submission/job/attempt/generation, protocol and mode revision, canonical
original-path inventory and derived inventory digests, each object ID/size/hash,
worker closure receipt digest, and `until_explicit_source_release_v1` guarantee.
It must be durably persisted before return, replay identical bytes only for the
same intent, and support authenticated GET reconciliation after a lost reply.
Exact cross-language schema, trust/key lifecycle and test vectors are still to
be agreed. The current V1-style local manifest cannot prove full Viewer path,
raw GCP or attempt provenance, and no coordinator receipt is implemented.
Existing V2 behavior stays unchanged.
