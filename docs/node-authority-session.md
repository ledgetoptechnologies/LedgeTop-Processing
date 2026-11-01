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

## Upload authority groundwork (inactive)

The authenticated session can now revalidate an already issued upload grant
against the active coordinator task, exact stored file record and source
directory scope. A prepare challenge commits the same durable coordinator
intent used by init; a check revalidates it. Upload completion challenges are
explicitly refused, and the node broker still rejects remote upload forwarding.
No file bytes cross the session. This intentionally prevents a peer possessing
the signing key from asserting sender completion without coordinator-owned
descriptor evidence.

A working remote upload still requires an authenticated bounded data channel,
exact binding between it and this intent, coordinator-owned sender completion
evidence, a tested lost-response protocol, and independently bound node/engine
identity. The current HMAC by itself is not such evidence.
