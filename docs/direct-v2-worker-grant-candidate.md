# Direct worker reservation grant candidate

This unwired candidate binds a pre-enrolled single-job V2 boundary to a
configured coordinator mTLS principal and bearer. It records one exact
operation ID and the worker scratch binding in the scratch ledger before
allowing the configured listener to serve object or closure requests. The
binding covers the exact V2 item and its signed-inventory digests. Exact
replay is accepted; changed principal, operation or item fails closed.

This does not create a reservation, issue Viewer custody, authorize execution,
or restore a crashed worker. The coordinator grant sender, installed key and
principal provisioning, enrollment, worker lifecycle and restart/reconciliation
remain unimplemented. A retained grant has no deletion path and the 4096-row
limit is conservative backpressure, not an archival policy. The low-level HTTP
factory retains legacy mode for other callers; this grant only applies when
explicitly configured, while the opt-in V2 service requires it.

Tests are synthetic. Do not deploy based on this candidate or interpret its
acknowledgement as durable custody of source bytes.
