# Direct worker TLS listener prototype

`create_direct_worker_server` constructs a TLS-only listener for one already
authenticated, pinned `DirectWorkerBoundary`. It does not start itself, create
a reservation, negotiate profiles, authenticate a Viewer, or acknowledge
coordinator custody. It accepts bounded, fixed-length object PUTs and a
zero-length closure POST; duplicate security headers, transfer encoding and
expect/continue are refused. It takes transport truth from the TLS socket, not
forwarded headers. A bounded request reader owns the logical body while leaving
the socket open for the response; only the trusted synchronous handler holds
the underlying socket. An interrupted or uncertain read leaves the existing
writer pin held. The local closure remains explicitly non-authoritative for
Viewer acceptance and processing.

The listener limits concurrent connections, object length, idle time, an
absolute TLS-handshake-and-header deadline, and the body read deadline. It has
no installed configuration, credential provisioning, authenticated reservation
creation, safe uncertain-pin recovery, coordinated restart, or verified public
response contract. Its in-process `drain(timeout)` blocks new connections and
waits for accepted connections without interrupting their readers; timeout
returns false and leaves drain enabled. Successful drain is not proof that
previously uncertain pins have been reconciled or restart is safe. Do not deploy it until
those contracts and real multi-process/worker failure paths are qualified.
TLS certificate and bearer management stay with the eventual trusted runtime.
The tests use a temporary loopback TLS server, a synthetic worker, tmpfs and
no external network; they do not prove installation or real-data processing.

`DirectWorkerSender` is a separate, unwired coordinator-side client. It uses a
configured HTTPS worker origin, hostname validation and an explicit CA trust
store, a scoped bearer, fixed object lengths and local hashes. It validates the
signed worker closure against the expected full worker binding. Failed or lost
responses are uncertain and never become Viewer custody acknowledgements; no
automatic resend occurs. A caller may explicitly retry only after reconciling
the unchanged job and transfer identity. The local input reader must be trusted
and bounded by its owner: the network deadline does not interrupt a stalled
local reader or DNS resolver. There is no coordinator receipt, authentication
negotiation, job admission, installed client configuration, or verified result
return in this prototype.
