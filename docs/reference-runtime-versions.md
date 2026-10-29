# Paired runtime-version publication (source only, default off)

This feature lets an owned container update publish the new **configured runtime
version** without rewriting the coordinator's immutable bootstrap settings or
queued job definitions. It does not establish Docker image identity through
HTTP, authorize engine work, renew grants, or activate a worker. The host updater
remains responsible for its actual container/image/configuration qualification.

The private runtime explicitly adds `"runtimeVersionPublication":{"schemaVersion":1}`
alongside `catalogAdmission` and independent fixed `routingBindings`. An operator
credential and both configured nodes are required. Absence preserves historical
behavior: no registry is opened or created. The actual application passes the
explicit compatible mode into Queue before startup and constructs the registry
before validating a cached catalog against accepted versions. Defaults never
learn or adopt a version from arbitrary health responses.

## Operator API

All routes use existing native operator authentication. Publishing additionally
requires exactly one `X-LTDS-Update-Token` header containing the original token
returned by the real begin-update action. Tokens and their private hashes never
appear in bodies, receipts, current-state responses or logs.

- `POST /v1/prototype/nodes/{node}/runtime-version` returns `{receipt: ...}`.
- `GET /v1/prototype/nodes/{node}/runtime-version/receipts/{requestId}` returns
  the immutable historical `{receipt: ...}`, or 404 when absent.
- `GET /v1/prototype/nodes/{node}/runtime-version` returns `{runtime: ...}`.

The closed schema1 request contains `requestId`, `authorityId`, `planId`,
`expectedRevision`, `ownedScheduleRequestId`, `expectedVersion`, `newVersion`,
`targetImage`, `engineId`, and `gateGeneration`. Image identity is an immutable
`sha256:` value already bound to the actual owned maintenance schedule.
`request_sha256(node, request)` hashes the canonical `{nodeId, request}` envelope.
The shared `validate_version_receipt` and `validate_current_version` functions
give the host controller one exact contract rather than parallel validators.

The 16-field receipt repeats the original closed request and adds `nodeId`,
`requestSha256`, a monotonic `epoch`, `bindingsSha256` and
`verificationScope: configured_worker_and_native_http`. The current response
contains schemaVersion, authorityId, nodeId, engineId, gateGeneration,
engineVersion, targetImage, global epoch, per-node nodeEpoch, bindingsSha256,
scope `configured_runtime` and publicationRequestId. Another node's update may
advance the global epoch; the original node's publication ID, nodeEpoch, image
and version must still match before its controller can finish maintenance.

## Publication protocol

Before network observations, the real Queue snapshot verifies the exact owned
schedule's canonical request/receipt hashes, authority, node, plan, target,
schedule fields and revision; the node must be updating with the original
begin-update token and no running/held work on that node. Engine ID, generation,
origins and worker identity remain fixed to bootstrap configuration. The
expected version must match the previous accepted version.

The Queue transaction and lifecycle lock are then **released**. The registry
reads the real configured Reader-authenticated worker TLS health and native
`/info`, with bounded literal-IP nonblocking transport. It requires the exact
configured service identity, new version, live threads, no pending execution or
acceptance, and native queue count zero. Brief busy health may be sampled again
read-only under one six-second deadline; busy, unknown or authentication errors
never imply idleness. No HTTP wait happens inside Queue/BEGIN IMMEDIATE.

A fresh FULL transaction rechecks original ownership, token, node revision and
the entire runtime epoch. It atomically invalidates the current catalog,
updates accepted bindings and appends the exact request/receipt. Late catalog
HTTP observations are fenced by their old epoch and cancelled pending intent.
Neither publication nor retry changes an old job's selected definitions or
bootstrap scope. Same-version image changes and version ABA still increment
the epoch, so an old routing sample cannot authorize a later claim merely
because the version string matches again.

The host persists its exact publication intent before its single POST. After
an uncertain reply it reads the original receipt and current node state; it
does not blindly repeat a mutation. Same-ID exact server replay returns only
the historical receipt. It never reopens or clears a later hold. An actual
coordinator restart while maintenance is updating still invokes the existing
maintenance hold/revision change and clears the old update claim. A surviving
publication receipt does not override that policy or permit finish.

## Durable bounds and migration

The registry uses a single bounded state row and at most 256 immutable receipt
rows in the authoritative Queue database. Each canonical row is bounded to
16 KiB; receipt fields, full historical chain and current bindings are validated
against original anchors on read. Private row checksums include the associated
token hash; those are accidental corruption checks, not signatures against a
database administrator. All data is charged to the existing metadata quota,
and every compatible Queue connection enforces the 128 MiB physical page cap.
Capacity exhaustion holds; no automatic receipt/history deletion is provided.
This is a **lifetime ceiling of 256 accepted publications**, not a counter
reset at restart. Receipt retirement/export/migration is not yet qualified;
manually deleting rows or resetting an epoch is not an operational workaround.
The independent 64-prepared-job recovery ceiling is also unchanged.

Exact schema and permanent INSERT/UPDATE/DELETE writer-fence triggers are
installed atomically. Only explicit compatible Queue mode registers
`reference_runtime_writer_v1()`. Mode-off cold open refuses the marker; prior
compatible catalog-only writers lack the function and cannot mutate jobs.
Configuration/image rollback is not a state downgrade. A separate reviewed
migration/export is required; no automatic repair, drop, hash rewrite or
bootstrap-version replacement is allowed. Existing jobs and metadata remain.
In particular, do not rewrite the coordinator's base `routingBindings.engineVersion`
to a newly published current version: accepted current versions come from the
durable registry. Rewriting that anchor changes the private scope and holds
cold startup until a separately reviewed migration.

## Canonical worker readiness RPCs

The worker's actual driver retains a whole-step guard. Only live typed
PID/thread/active-DriverStepLease-bound discovery and initial-claim RPC windows
temporarily release the driver state lock; it is reacquired before local SQL,
engine work or return. A second step cannot enter. Mutation guards explicitly
reject the RPC window even if a health reader owns the temporarily released
lock. Errors and unknown replies reacquire and clear the window in `finally`.

Health still takes the real state lock and reads existing bounded DELETE
databases read-only. Only the exact live initial claim with empty local
bookkeeping, no other pending driver work, no nonterminal worker execution and
no reserved/started/uncertain scratch can count as no heavy work. A pending
claim outside that live RPC remains pending. Arbitrary locks, unknown state,
actual execution and reservations remain busy/held. This removes the
self-health deadlock when a canonical worker asks the routing coordinator for
work; it is not a caller-supplied ready flag, cached health or engine permission.

## Qualification scope

Tests use actual Queue/owned schedule/begin tokens, application operator HTTP,
Reader TLS and native HTTP fixtures. They cover repeated updates, same-version
image changes, ABA, restart, immutable prior job evidence, epoch fences, late
catalog observations, lost publication replies, historical receipt replay after
hold, real busy polling outside the Queue lock, quota rollback and writer ABI.
Canonical worker tests exercise the live RPC lock/thread lease, second-step
exclusion, mutation denial, error cleanup and original-request claim replay.
Two-node joint tests use the unchanged pinned Node business modules and actual
canonical worker services; synthetic bytes/backend fixtures do not qualify real
imagery, genuine LiDAR, Docker runtime recipes, NAS custody or live activation.
