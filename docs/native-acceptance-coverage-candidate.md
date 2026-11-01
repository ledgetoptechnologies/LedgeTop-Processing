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
greater than the result's object count. Existing overlong receipt sets are not
rewritten, discarded or automatically repaired.

This tightens the existing v1 API behavior: clients that generate a new receipt
identity for already-covered objects will receive a conflict and must reconcile
the original receipt identity instead. Coordinate the rule with Viewer before
enabling it. Environment-scoped credentials are not per-client tenant isolation.
The candidate does not prove durable Viewer custody, cleanup safety, or any
runtime deployment qualification.
