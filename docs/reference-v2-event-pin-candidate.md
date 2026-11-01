# Disabled coordinator event-pin and transition proposal

The local pin candidate, when explicitly enabled for synthetic testing, verifies
an existing worker claim against the coordinator's actual reference and queue
rows in one SQLite write transaction. It structurally rechecks the stored
transfer reservation, persists exact proposed event bytes and claim binding, and
installs attested cooperative triggers protecting pinned job/claim rows and the
pin itself, including alternate-unique-key `INSERT OR REPLACE` cases. One held
event per job is allowed; exact replay is idempotent and no release exists.
The separate attempt candidate generates a random UUID once inside the existing
claim transaction and persists the attempt with the coordinator's nonsecret
claim identity. Exact replay after reservation returns the same ID. The pin
attests the attempt table and guards and revalidates the stored attempt against
the current claim before recording its local observation. This does not issue a
signed reservation intent or protect an attempt against old writers or restore.
The private pin record contains a claim-token SHA-256; it is not a network
response. A separate local observation digest excludes both token and token hash.
It is not an approved `claimLineageSha256` and must not be published as one.

These guards do not attest the event's Viewer origin, external NAS reservation,
current epoch or physical worker/stream fence. The proposed event is caller
data; the return is explicitly `authorization=unqualified`. Old writers,
administrative schema changes, direct database replacement, coordinated restore
and trigger removal outside the attested entry path are not fenced. Offline
installation only checks queue rows; it cannot prove that old binaries stopped.
The operation is unconnected to the remote checkpoint server, which separately
rejects all unqualified authority. Do not enable these candidates for real jobs.

The separately false-gated transition experiment hashes a strictly bounded
provisional body as SHA-256 of the ASCII domain `ltds-viewer-ledger-transition-v2`,
one NUL, decoded 32-byte previous digest, unsigned 64-bit big-endian canonical
body byte length, and exact canonical ASCII body bytes. It checks safe integer
revision increment, whole/empty bytes and charge, and closed syntax. It does not
check an actual request, stored claim, signed Viewer event or ledger state. Its
fixture uses synthetic placeholders only; operation ID generation, immutable
claim lineage, attempt IDs, epochs and final wire schema remain unapproved.

Before a real CAS can be authorized, jointly define and implement: a durable
coordinator-owned authority epoch and immutable attempt identity, including
replacement/restart/re-enrollment; a verifiable signed Viewer reservation and
ledger event bound to the same immutable request/claim/attempt; independently
current issuer keys and monotonic epoch/retention anchor; deterministic derived
transition digest; durable cross-store pin/receipt ordering; and an independently
enforced physical old-worker/stream fence. Unknown or ambiguous state holds.
No token-derived fingerprint should be sent to Viewer. Matching a local JSON
reservation or synthetic hash is not a substitute for those authorities.

Nine synthetic attempt/pin tests passed with isolated Linux image SQLite 3.40.1;
this matches the separately reported point-in-time production SQLite version,
but is not a production-image or live-node test. Three pure hash-vector tests
also passed; Viewer independently reproduced the synthetic transition digest.
