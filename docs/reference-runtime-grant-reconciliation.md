# Optional runtime grant receipt recovery

The private ReferenceRuntime configuration may opt into the existing read-only
grant receipt route independently of initial-grant issuance:

```json
"grantReconciliation": {"schemaVersion": 1}
```

The object is closed and versioned; booleans, null, extra fields and other
versions are rejected before Queue construction. Absence preserves the disabled
default. Existing `initialGrantDelivery` configurations continue to enable the
receipt route as before. This independent setting neither enables that profile
nor creates its policy, admission, claim-context or delivery-receipt tables.

The actual runtime's authenticated control adapter receives the setting. Viewer
can POST its original closed request to
`/ltds/v1/jobs/{jobId}/transfer-grants/reconcile` after an ambiguous v1 grant
delivery. The existing endpoint validates the complete historical grant and
original bindings, returns only its immutable receipt, and supports expired
historical observations. A 409, 404, timeout or error remains unresolved; it
cannot authorize token replacement, epoch advance, deletion or download.

No runtime configuration is installed by this source change. Real integration
still requires the user's reviewed private listener/Viewer credentials, broker
issuance and source custody, supported worker routing and actual installed
versions. Initial-only grant/context delivery retains its separate explicit
policy and fresh-state latch. Renewal routes remain unavailable. The new tests
exercise the actual runtime adapter and real loopback HTTP with synthetic data;
TLS material loading in the test configuration is mocked, not qualified.
