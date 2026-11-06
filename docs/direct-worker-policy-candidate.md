# Authenticated worker policy lookup

The existing coordinator reference runtime offers a read-only worker-bearer
`GET /ltds/worker/v1/direct-source-policy`. It recomputes the current fleet
options and returns capability and option fingerprints for the authenticated
worker's enrolled node and instance. The worker driver verifies that binding
through its existing protected HTTPS request path.

The response explicitly says direct preparation is disabled and does not
acknowledge source custody. This lookup is not a durable policy lease: it does
not pin a future transaction or authorize a readiness proof. A future direct
preparation integration must authenticate its own mTLS coordinator identity,
recheck policy freshness and node eligibility, and preserve source retention.
No direct listener, worker enrollment or scheduler is started by this change.
