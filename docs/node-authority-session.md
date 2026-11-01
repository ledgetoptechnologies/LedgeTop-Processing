# Inactive live node authority session

The node broker can be supplied a `RemoteAuthoritySession` instead of the
same-process witness. A caller must establish an already verified mTLS channel
with explicit peer certificate pins. The coordinator-side stream handler pins
the actual Queue lifetime and checks the installed host AuthorityLease on each challenge, and signs a fresh,
sequence-and-nonce-bound challenge only after checking the authority marker and
revalidating the exact init grant in the same Queue read transaction. The node
challenges before forwarding and again before
marking its durable operation complete. Frames and wait times are bounded;
EOF, peer mismatch or timeout refuses the operation. Any already-started
intent and NodeGate reservation are retained. The broker's existing persistent
binding refuses a new coordinator epoch; there is no automatic reset or adoption.
The coordinator's idle timeout is at least 12 seconds, longer than the current
10-second backend exchange; contention and scheduling can still cause a safe
hold. A whole session is limited to 60 seconds. There is no heartbeat during
the backend exchange, and these bounds do not form an end-to-end deadline.

The synthetic test uses a real mTLS stream, Queue, AuthorityLease, broker and
loopback NodeODM-like HTTP server, with mocked Docker identity. Closing the
session after the backend responds leaves one started intent and a reservation,
and retry does not resend. It does not simulate coordinator process death,
TCP partition, power loss or a real NodeODM engine.

This module creates no listener, TLS credentials or connection bootstrap; peer
pins and credentials must be bound to actual node identities by trusted setup.
It currently covers init only. It cannot retract a request already sent when a
connection or authority fails. Importantly, blocking a new epoch on this one
node does not stop a different node from accepting a new job. Before deployment,
all coordinator startups and claims need a qualified durable all-node reconciliation
barrier, and direct Docker/backend routes need independent physical fencing.
Neither boundary is provided here. The existing qualification flag stays false.

## Local integration with coordinator intent barrier

This inactive integration is initialization-only. Before forwarding, the TLS
coordinator handler verifies the exact Viewer claim and commits a coordinator
`started` intent. After a node response the node checks session authority,
records its local completion and requests coordinator completion. Loss before
coordinator completion leaves the coordinator intent started, including if the
node has completed; a restart holds that job and blocks further claims. If the
coordinator commits completion but its acknowledgement is lost, both intents are
complete although the caller sees a session failure. An exact retry through a
fresh valid session in the same coordinator epoch replays the stored node result
without another engine request. There is no automatic reconciliation or epoch
adoption after a coordinator restart.

The mTLS peer is trusted: a completion request is an assertion by that peer, not
independent proof that the intended node has a durable receipt. Node certificate
provisioning, identity mapping, listeners and remote upload are not implemented.
The session holds a queue process pin, but the host authority lease is acquired
only around each challenge and can be lost during the backend exchange. Already
forwarded work cannot be cancelled or fenced physically by this protocol.
The synthetic EOF test restarts the coordinator after an orderly shutdown; it
does not simulate power failure, an actual node process crash, or a network
partition with a still-running backend. This integration is not deployment ready.

## Experimental remote upload data path (inactive)

An mTLS authority session revalidates the issued upload against the coordinator's
exact task, stored file and source-directory scope. A separate, already
established mTLS stream carries an authenticated operation header and bounded
file bytes from the coordinator's descriptor sender to the node broker. Its
coordinator-owned in-memory evidence is recorded only after sender verification;
the authority session requires that evidence before accepting completion. The
node commits its own completion before requesting coordinator completion.
Synthetic tests cover successful upload, interrupted data and an exact completed
replay without another NodeODM call. Interrupted work remains held for inspection.

Evidence is transient and trusted in-process; it is not an independently signed
node receipt. The TLS completion request is a trusted-node assertion, and engine
identity is not bound to trusted coordinator configuration. No listeners, TLS
credential provisioning, durable receipt reconciliation, epoch adoption or
physical backend fence are supplied. Lost evidence or a crash can conservatively
hold work. Tests use one process and synthetic loopback peers with mocked Docker
identity; they do not establish a real cross-host deployment, partition or
power-loss safety. This code stays disabled and requires independent review.

## Experimental durable coordinator sender evidence

An additional explicit offline migration can install an append-only coordinator
sender-evidence table. The coordinator's descriptor sender writes an exact
operation, job, epoch and grant digest after all declared bytes are sent, the
owned sender reports success, the source record is revalidated, and the matching
coordinator intent is still started. The mTLS authority session requires this evidence from its own Queue before
accepting node completion. Each sender rejects overlapping calls and closes its
owned channel on every exit. The synthetic test verifies that the
evidence survives an orderly coordinator restart, remains readable only with
the qualified schema and immutable triggers, and that the restarted job remains
held. A truncated stream writes no such evidence.

Evidence records sender verification, not independent proof of a node receipt.
The authenticated node's completion message remains a trusted-node assertion.
The evidence and its producing objects are trusted within the coordinator
process; a privileged or old writer can alter the SQLite schema or bypass this
path. The evidence has no automatic expiry, deletion, epoch adoption, restart
reconciliation, or qualified downgrade. No process kill during a real transfer,
power failure, or actual remote-node deployment has been tested. This remains
an inactive prototype, not a release or operational migration.

## Experimental node upload receipt

After the mTLS authority session confirms the exact coordinator-owned sender
evidence, the node stores a canonical upload receipt in its own SQLite database
in the same transaction that marks its operation complete. It includes the full
grant, sender-evidence grant hash, and the observed successful engine reply.
Replay checks the receipt content and hash against the exact grant and reply.
The receipt records a node observation; it is not a claim that NodeODM has
fsynced the input or that a later task execution will succeed. It is not a
cryptographic attestation independent of the trusted node.

The new node receipt table is created only for a fresh prototype node database.
Existing databases missing it are refused; no automatic upgrade or downgrade is
provided. If communication fails after the node transaction, the coordinator
intent remains started and the job is held on restart even though the node
receipt is retained. Automatic reconciliation, old-worker fencing, epoch
adoption and independently verified remote receipt signatures are not provided.
The tests simulate session loss and orderly reopen, not power failure or an
actual cross-host node crash. This remains disabled.

## Experimental coordinator copy of node receipt

The node now sends its exact canonical receipt over the authenticated authority
session. After checking the grant and its own durable sender evidence, the
coordinator stores the receipt and completes the matching intent atomically in
one SQLite transaction. An exact retry in the same live coordinator epoch can
recover a lost completion request or acknowledgement without a second engine
upload. If the epoch changes, the old receipt cannot automatically release the
held job; independently fenced recovery is still required. The coordinator
copy survives orderly restart and is immutable through its SQLite triggers.

The receipt is an authenticated assertion by the trusted node, not independent
cryptographic proof of its stored state and not proof that NodeODM fsynced data.
This prototype requires a fresh offline-migrated coordinator evidence schema;
an earlier schema lacking the receipt table is refused, with no automatic
upgrade or downgrade. The tests simulate TLS loss and orderly restarts using
local synthetic peers. They do not prove real cross-host, power-loss, malicious
node, old-writer or physical backend fencing behavior. The path remains disabled.
