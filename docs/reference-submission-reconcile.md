# Exact original submission reconciliation

`POST /ltds/v1/jobs/reconcile` is an additive, authenticated metadata lookup.
The body is the original unmodified reference-v1 submission envelope. It does
not fetch current capabilities or options, create a job, rotate a submission ID,
create evidence, issue a grant, claim work or authorize execution.

The server structurally normalizes that original envelope against its **own**
recorded fingerprints/options solely for comparison. In the authenticated
environment it must match the entire stored normalized original request and
server-owned request digest. Only then does it return the existing closed
Job DTO with status 200. Valid changed specifications conflict with 409;
absence/cross-environment lookup returns 404, malformed requests 400 and wrong
credentials 401. Original self-validation does not confer engine authority.

This resolves a committed submit whose response was lost before a catalog
epoch changed. A client must persist its original envelope, use the exact
reconciliation endpoint before considering fresh admission, and never silently
rewrite options or submit a new ID. A 404 from an old server is unsupported or
absent, not exact-match proof. Existing `lookupSubmission` semantics remain
unchanged: its opaque pipeline digest alone does not prove original options.

The additive Node implementation is maintained as a separate reviewed source
packet; the historical nine-file Viewer867 fixture remains unchanged. Tests
must distinguish that packet from the historical client and use actual TLS,
catalog change/expiry, both-node outage and coordinator restart. No cross-language
guess of the server's opaque pipeline/request digest is required.
