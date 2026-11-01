# Candidate native acceptance coverage rule

The coordinator currently accepts new receipt IDs with empty or already-covered
object subsets. A client can therefore create more than `MAX_OBJECTS` receipts
for one result, while the worker's relay accepts at most `MAX_OBJECTS` receipts.
The worker must retain its files if it cannot validate the relay.

This candidate rejects a new receipt unless it adds at least one previously
unaccepted result object. Overlap is allowed when the receipt also adds new
coverage. An exact replay of a previously persisted receipt remains idempotent.
SQLite serializes competing new receipts. Valid result manifests always contain
at least one object, so the maximum number of newly recorded receipts is no
greater than the result's object count when no legacy receipts exist. Legacy
receipts count against the `MAX_OBJECTS` relay limit: once full, new receipts
are held for inspection, while exact replay of an existing receipt still works.
Existing overlong receipt sets are not rewritten, discarded or automatically
repaired.

The count limit alone is insufficient: the worker also limits a JSON relay to
`MAX_BODY_BYTES`. Before inserting a new receipt, this candidate measures the
entire worker HTTP relay using the listener's exact ASCII JSON encoding and the
longer `fullyAccepted:false` representation. It holds inserts that exceed the
wire limit; existing receipts and exact replay are preserved. Existing oversized
relay sets still require an explicit recovery procedure.

SQLite reports of BUSY or LOCKED on Viewer acceptance or the worker acceptance
read return sanitized HTTP 503. Other SQLite operational failures remain 500.
Clients must retry an exact receipt identity and reconcile an uncertain response;
503 does not prove that no earlier attempt committed.

This tightens the existing v1 API behavior: clients that generate a new receipt
identity for already-covered objects will receive a conflict and must reconcile
the original receipt identity instead. Coordinate the rule with Viewer before
enabling it. Environment-scoped credentials are not per-client tenant isolation.
The candidate does not prove durable Viewer custody, cleanup safety, or any
runtime deployment qualification.
