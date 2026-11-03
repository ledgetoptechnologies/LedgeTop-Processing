# Held v3 delivery routes and authority request pin

All proposed `/ltds/prepared/v3/submissions` operations require exactly one
`X-LTDS-Expected-Coordinator-Authority` header containing Viewer's independently
configured canonical UUID. Missing, malformed or duplicate headers return 400;
a mismatch against the server's independently configured pin returns 503. The
store checks the server pin against coordinator_identity and Queue authority
again inside the transaction before first INSERT, closing a wrong-endpoint
preflight race. TLS, principal authentication and external clone/rollback fencing
remain separate requirements; an ID copied with a database does not prove
uniqueness. A listener must preserve duplicate wire headers to this adapter.

The proposed authenticated POST `.../{submissionId}/source-grant` and POST
`.../{submissionId}/accepted-copies` are deliberately hard-held. For a
matched owner/environment and authority they return 503 without parsing,
accepting or storing the submitted body; another principal receives 404.
These are not implemented delivery, receipt or durable replay routes.

The currently available v2 grant is a broker-verifiable HMAC bearer; the
Processing candidate has no trusted authenticated broker-verification boundary,
and an untrusted caller must not supply the verification key or binding.
Existing acceptance storage permits legacy unsigned receipts and lacks a
principal-scoped trusted-key/attempt chain for this route. Implementing these
requires authoritative source, task, attempt, job, policy, grant/receipt keys,
current epoch and replay context from reviewed installation-owned state, plus
Viewer pairing, secure transport and durable transactional reconciliation.
Until then neither route authorizes source fetch, ingestion, acceptance,
activation, job execution or cleanup. Both routes and all v3 APIs are unmounted.
