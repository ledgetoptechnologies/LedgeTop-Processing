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

The listener limits concurrent connections, object length, idle time and body
read deadline. It has no installed configuration, credential provisioning,
authenticated reservation creation, safe uncertain-pin recovery, coordinated
shutdown/drain, or verified public response contract. Do not deploy it until
those contracts and real multi-process/worker failure paths are qualified.
TLS certificate and bearer management stay with the eventual trusted runtime.
The tests use a temporary loopback TLS server, a synthetic worker, tmpfs and
no external network; they do not prove installation or real-data processing.
