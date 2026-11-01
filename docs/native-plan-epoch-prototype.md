# Disabled internal native-plan epoch prototype

This is an unadvertised, test-only internal state machine, not a frozen wire
protocol. Its qualification flag defaults to false and environment configuration
cannot enable it. There is no v2 HTTP route, migration, upload, dispatch or
physical reservation. Direct construction requires the qualified test config,
the existing manifest/plan objects and a held coordinator authority lease.

Fresh enrollment atomically creates a reservation and an epoch-1 marker bound
to the coordinator authority, environment, task UUID, task capability hash,
unique operation UUID, and predeclared immutable manifest ID and hash. Existing
reservations, tasks, incarnations, bindings or matching orphan directories hold;
they are not classified or migrated. Operation UUIDs and committed revoke
request UUIDs are retained and cannot be reused across those namespaces.
While the v2 tables exist, cooperating v1 reservation, cancellation, manifest,
plan and legacy task mutations hold authority-wide, including unrelated v1 tasks.
This prevents an absent v2 row being mistaken for legacy state; mixed v1/v2
operation is not supported. Older binaries and noncooperating writers do not
honor this guard. This does not stop already-admitted jobs or all background
coordinator work, and is not a quiescence mechanism.

Within the OPEN epoch, manifest binding is first, followed by an exact canonical
empty-options plan intent and then plan acceptance. Each step rechecks the same
authority and epoch in its SQLite write transaction and retains the actual
coordinator manifest/plan records. Only empty options are supported here.
Revocation requires a unique request UUID and exact expected manifest hash,
manifest binding ID (or null) and plan-intent hash (or null). A stale expectation
holds. The close transaction records `closed_without_bind` when no plan was
accepted, even if a manifest or intent exists; it records `bound_then_closed`
with the exact historical plan receipt otherwise. Receipts explicitly include
manifest identity, plan-intent presence and hash, operation kind and epoch.
Exact authenticated revoke replay returns the retained receipt; status returns
the observed open or closed history. Closed epochs cannot be reopened and
records are not automatically deleted. Capacity and dispatch remain false.

The v1 guard and marker checks apply only to cooperating code. Current startup
refuses disabled use when either epoch table exists, but does so after Queue
construction; this is not a pre-write census, a rollback plan or external writer
exclusion. Loss of both epoch tables followed by v2-disabled startup is not
detectable by this SQLite-only prototype; an independently protected durable
authority/version marker is required. The bounded history limits fail closed and do not constitute disk
reservation. Unkeyed digests are not signatures or protection from an actor
that can rewrite the coordinator database. Environment tokens are not user
identities. Local Viewer invalidation is invisible to the coordinator until an
authenticated revoke commits; an already-sent operation can win that race.

Tests exercise synthetic stale expectations, both close outcomes, retained
history, v1 holds, ID reuse, malformed/partial markers and process restart.
Separate client processes race through a test-only adapter in one coordinator;
the adapter is not a production route. Separate processes call `os._exit`
after acceptance and after revoke and subsequent processes recover receipts.
These are not power-loss, partition, old-binary or physical-fence tests.
