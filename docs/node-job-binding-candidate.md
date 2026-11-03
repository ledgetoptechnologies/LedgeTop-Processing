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
