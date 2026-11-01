# Inactive live node authority session

The node broker can be supplied a `RemoteAuthoritySession` instead of the
same-process witness. A caller must establish an already verified mTLS channel
with explicit peer certificate pins. The coordinator-side stream handler pins
the actual Queue lifetime and installed host AuthorityLease, and signs a fresh,
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
all coordinator startups and claims need a durable all-node reconciliation
barrier, and direct Docker/backend routes need independent physical fencing.
Neither boundary is provided here. The existing qualification flag stays false.
