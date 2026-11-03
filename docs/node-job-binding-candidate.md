# Held-only node job binding candidate

This disabled-by-default experiment binds a signed Viewer queue job ID and
job generation to the engine task UUID, node, authority, gate generation,
nonce, expiration and exact initialization request digest. A MAC under a
separate shared key is validated before the node gate is reserved. The exact
claim is persisted in the gate-bound mutation journal before engine init. The
held-only inspection endpoint compares the presented claim with that journal
and checks the exact requested operation under the node lock.

`require_job_claim=True` rejects legacy unbound initialization. This is only
an adapter setting; no production factory provisions the key or enables it.
A node peer knowing the claim key can forge claims. Peer authentication is not
coordinator lease verification: there is no current-authority epoch, old-writer
exclusion, claim revocation, durable replay ledger, authenticated Viewer source
snapshot, or signed response. Inspection does not grant permission to resume.
The claim itself contains no client data and expiry is checked only for
admission: post-expiry forensic inspection is intentionally held-only.

Upload and commit paths still rely on peer authentication and the existing
node-gate/task binding, not a separately signed per-operation claim. The legacy
unbound mode is maintained for compatibility and must not be exposed alongside
an enforced deployment. Direct engine access and old node binaries must be
excluded independently. A crash after reserving the gate but before creating or
binding the journal remains held for explicit reconciliation. No live state or
services are modified by this candidate.

Replay protection: a separate durable ledger consumes the signed nonce under
NodeGate before the reservation or any native mutation. An exact replay while
its original reservation is active returns held without another mutation; a
changed claim using the same nonce or any replay after retirement is refused.
The ledger marker is persisted with gate schema 2 before its file is created;
missing or incomplete ledgers are held instead of being silently recreated.
A crash after nonce consumption but before reservation likewise requires
reconciliation. Schema 2 excludes the checked 693cb7e gate loader, but later
or unrelated older code, alternate state paths, direct database tampering and
rollback of the whole state still need external controls.

The claim additionally binds production/staging and the exact configured
coordinator origin. This does not independently prove either value: the shared
signing key is still the trust boundary and is not a job-scoped credential.

Admission checks current node ownership, generation, outstanding reservation,
and journal lifecycle under the same held gate lock before consuming a new
nonce. A busy or mismatched node therefore cannot burn an otherwise valid
claim. A read-only ledger check still reports an existing same-reservation
replay as held and rejects a consumed nonce before any reservation attempt.
If the process dies after consumption but before reservation, the claim remains
consumed and requires reconciliation; it is never silently reissued.

Claim expiration is revalidated under the node gate lock before the nonce is
consumed and once more immediately before engine initialization. If it expires
after consumption, the node keeps its reservation and pending journal intent;
no native mutation is issued, and explicit reconciliation is required. This is
not a cross-host lease or protection from system clock rollback.
