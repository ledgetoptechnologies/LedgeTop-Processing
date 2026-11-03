# Principal prepared API candidate (unmounted)

No production listener imports this adapter. `POST /ltds/prepared/v2/submissions`
accepts the closed v2 envelope used by the store and returns 201 for new or
200 for exact replay. `GET /ltds/prepared/v2/submissions/{submissionId}`
returns the owner-scoped job plus envelopeSha256, task, attempt and activation
identities, or 404 for another principal/environment. Same-ID collisions on
POST return a generic 409; a caller choosing a known ID can infer a conflict,
but receives no owner or job details. Input and responses contain no bearer.

Credentials are explicit lists of token/principal/environment tuples and an
explicit legacy-token inventory; duplicate or shared bearer values fail setup.
Constant-time token comparison and protected-transport gating are local
primitives, not a complete identity provider. Integrators must prove that the
legacy inventory is complete, protect tokens in memory/logs, authenticate
and rotate principals, and bind TLS/client identity. Policies are server-owned
per-environment immutable snapshots copied on construction, never taken from
request data; provisioning, coherence and authoritative refresh/revocation are
not implemented. `protected_transport` is caller-supplied, so it must only be
set by a trusted transport boundary. No listener, source grant, v2 activation,
or live-system change is included. Existing coordinator and source holds stay.

The loopback test uses an ephemeral localhost server solely to exercise HTTP
serialization; it is explicitly stopped and is not a production transport.
Shared Viewer/Processing canonicalization for negative/floating options remains
unresolved and blocks integration. Endpoint path and wire response are proposals
subject to joint Viewer review.

A listener must preserve repeated wire headers when calling this adapter, or
reject multiple Authorization headers using the server's multivalue API before
any conversion to a dictionary. Collapsing headers can defeat duplicate-token
rejection. The loopback test sends repeated Authorization headers over HTTP and
confirms a 401 with no job inserted.
