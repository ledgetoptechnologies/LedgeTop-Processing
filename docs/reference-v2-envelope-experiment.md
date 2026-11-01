# Disabled signed-envelope interoperability experiment

This is a proposal, not an approved protocol or runtime integration. The code
is false-qualified and uses the separately installed `cryptography` library for
Ed25519 verification. The only vector private key is public, published test
material and must never be used for any real credential. Grant and observation
have distinct ASCII domain strings terminated by NUL, then exact compact sorted
ASCII JSON. A signature is 64 raw bytes encoded as canonical unpadded base64url.
Raw public keys are 32 bytes; the fixture also supplies DER SubjectPublicKeyInfo
for Node. Times are integer Unix seconds; this experiment accepts at most 300
seconds lifetime without a clock-skew allowance. Envelope schema and algorithm
are not agreed; no HTTP header, URL, or response format is established.

The verifier compares issuer, purpose, key ID, validity and revocation against a
caller-provided registry snapshot and compares all other scope to caller-provided
expected values. It does not prove the registry, clock, epoch or expected values
are fresh or authentic, nor that a nonce is random or unique. The experiment
recomputes `payloadSha256` over exact bounded canonical bytes and compares the
closed payload to a caller-provided expected binding. That binding and its schema
are provisional; matching them does not make either source authoritative. A valid
signature does not prove stream fencing, authoritative absence,
retention, charge, file custody or replay prevention. Fail closed on missing
current authority and never wire this experiment to a live service.

Proposed provisioning: each service generates distinct private keys with its
OS CSPRNG in its own protected store; use least-privilege access and never copy
private keys into source, fixtures or logs. An independently managed trusted
registry binds issuer, key ID, purpose, public key, validity and revocation.
Rotate by provisioning a new key and distributing a verified registry update
before use; accept overlap only within explicit validity, revoke compromised keys
through a current trusted channel, and reject on unavailable/stale registry or
epoch anchor. The experiment does not implement protected storage, registry
distribution, rotation, revocation propagation, time synchronization or an
anti-rollback anchor. Exact operational ownership and recovery require review.

Cross-service state transitions and their crash holds are described in
`reference-v2-paired-authority-state-draft.md`. Network admission requires that
design's real independent fencing and durable ledger, not just this verifier.
