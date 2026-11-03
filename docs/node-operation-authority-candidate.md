# Node operation claims (inactive candidate)

This experimental protocol binds each upload or commit to a previously signed
Viewer-job claim and the exact deterministic operation ID and request digest.
The operation MAC uses domain `ledgetop-node-operation-claim-v1` and the same
independent node job-claim key. Its expiration is separate from the original
admission expiration; it is checked before spooling and immediately before
the native mutation. The durable journal holds duplicate, pending, and
post-expiry operations without replay.

The mTLS test client can be given a job claim and a caller-provided operation
signer. This is not connected to a production signer or scheduler. A bound job
does not accept the legacy unbound commit/upload path while its binding is
intact, even when legacy admission remains available. Enforced admission must
still be configured, old binaries and direct engine access excluded, and the
journal must be protected against replacement or downgrade.

These MACs authenticate possession of a shared key, not an active coordinator
lease or epoch. There is no cross-host revocation, per-peer key isolation,
trusted clock rollback protection, production job signer, or automatically
authorized recovery. Expiry after an intent is recorded safely holds the job;
it does not prove the engine ran. This candidate is disabled by default.
