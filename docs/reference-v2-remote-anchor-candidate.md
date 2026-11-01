# Disabled remote checkpoint experiment

This isolated experiment is not a service deployment or trusted admission
protocol. It uses an owner-locked SQLite checkpoint and operation history with
explicit offline bootstrap, fixed domain/ledger/epoch, exact previous and next
revision/digest, unique operation ID, and atomic CAS. Latest exact operation
replay returns the same result; conflicts, stale and superseded replay hold.
SQLite commit can complete after a session deadline or disconnect, so a lost
reply is ambiguous and only the exact original operation may be reconciled.

The one-request framed mTLS helper requires certificate verification and peer
pins and echoes a caller nonce. Both `QUALIFIED` and the separate
`AUTHORITY_QUALIFIED` are false. The latter exists because mTLS identifies a
peer but does not authorize it to choose a ledger digest. A qualified path must
bind the coordinator-owned current claim, durable operation pin and exact
verified Viewer ledger event to CAS under an appropriate authority lifecycle;
the existing NodeODM witness does not authorize this operation. No listener,
daemon construction, actual trusted certificate provisioning, claim verifier,
stream fence or Viewer route exists here. `Store.cas` and the client transaction
helpers are experimental internals, not independently authorizing APIs.

The separate test client journal records exact pending intent before simulated
transport and exact receipt afterward. It relies on its caller to commit and to
provide a genuinely authenticated receipt; neither property is enforced as a
runtime guarantee. It does not grant streaming or custody. History is not yet
bounded or compacted. Epoch/root rotation, cloned coordinators, old writers,
retention proof, durable current-authority/fence and live revocation are not
implemented. A separate coordinator can detect Viewer-only rollback only if
the coordinator checkpoint and its identity independently remain current;
coordinator or coordinated rollback and host compromise need an independent
monotonic anchor or owner-controlled offline re-enrollment with old writers
isolated and nonempty work reconciled.

Synthetic nonroot socketpair mTLS tests used separate client/server certificates
and separate SQLite client/server journals. They exercised CAS, exact replay,
conflict, wrong peer, disabled authority, and a test-only direct-CAS lost-reply
simulation; a separate owned child exited immediately after committed CAS and
reopened state. These do not establish real Viewer integration, process death
during network I/O, filesystem power-loss behavior or production trust. No
client data, live services or TrueNAS changes were involved.
