# Owned maintenance schedules

This is a source-only coordinator contract for future updater integration. No
detector, supervisor adapter, scheduled update or deployment is installed by it.

Legacy `POST /v1/prototype/nodes/{nodeId}/schedule` keeps its existing payload and
`{node: ...}` response. Its server-generated plan ID cannot prove caller ownership
after a lost response. Existing plans are not adopted or backfilled.

## Operator API

`POST /v1/prototype/nodes/{nodeId}/schedule-owned` requires the maintenance
operator bearer and this closed JSON object:

| Field | Required value |
| --- | --- |
| `schemaVersion` | Integer `1` |
| `requestId` | Canonical UUID, persisted by the caller before sending |
| `authorityId` | Exact pinned coordinator authority UUID |
| `expectedRevision` | Nonnegative safe integer for the current node revision |
| `drainAt`, `notBefore` | Explicit UTC timestamps; earliest start cannot precede drain |
| `afterJobId` | `null`, or a canonical UUID currently running on this node |
| `targetImage` | Immutable lowercase `sha256:` image ID |

The response is `{receipt: ...}`. The immutable receipt contains
`schemaVersion`, `requestId`, `authorityId`, `nodeId`, `requestSha256`,
`expectedRevision`, `revision`, `planId`, `state`, `drainAt`, `notBefore`,
`afterJobId`, `targetImage` and `receiptSha256`.

`revision` and `state` describe the original scheduling commit. Timestamps in
the receipt use the scheduler's normalized UTC representation.

`GET /v1/prototype/nodes/{nodeId}/schedule-owned/{requestId}` returns the exact
receipt under operator authentication. API/observer credentials cannot read it.
Missing receipts or a different node return 404; malformed requests return 400;
changed authority or request-ID bindings conflict with 409. No update token is
included.

## Atomic identity and replay

Node scheduling and the unique request-to-plan receipt commit in the same FULL
SQLite transaction. The generated plan UUID remains server-owned. Additive
`owned_maintenance_schedules` storage uses the existing coordinator database.

An identical request ID, node and JSON values return the original receipt before
any node refresh or scheduling CAS. This remains true after cancellation,
rescheduling, time progression or coordinator restart. Changed JSON values or
node scope with the same ID conflict; no peer plan is modified.

Both hashes use sorted-key compact UTF-8 JSON. `requestSha256` hashes
`{nodeId: ..., request: <exact parsed POST object>}`. `receiptSha256` hashes the
receipt without that hash field. Object key order is immaterial, but changing a
timestamp spelling from `Z` to `+00:00` changes request identity. Persist the
exact request before POST; do not reconstruct its times or revision afterward.

Lookup does not refresh node state or perform host actions. A historical receipt
proves that request scheduled its plan, not that the plan is still current.

## Future adapter boundary

The adapter needs ordinary API credentials for authority/current-node readback
and separate operator credentials for owned schedule/receipt calls. After a lost
POST response it must use receipt lookup, verify authority/node/request hashes,
and compare the current plan and target. Matching a target image alone is not
ownership proof.

Programmatic managed `run(..., expected_plan_id=..., expected_target=...)` accepts
the frozen pair. It checks entry, polling and resume readbacks. Its trusted
`on_reserved` callback closes ingress only after the exact begin CAS and durable
private token receipt; a final post-drain idle check precedes replacement.
Failed/lost reservation or drain does not authorize Docker replay or rollback.
See [managed finalization](managed-update-finalization.md) for success completion.

Qualification and a real supervisor adapter remain separate integration work.
