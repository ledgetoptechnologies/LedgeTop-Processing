# Original-claim recovery before reservation

Design checkpoint only, based on `c59f600`. Claude has yielded this follow-on;
implementation must begin from his final published/tested head. This document
does not implement, install, execute or qualify recovery.

## Gap and intended result

A coordinator restart holds an already claimed job before its reservation POST
commits. The worker currently raises `source_recovery_claim_unreserved`; the
existing negative test deliberately preserves this state. An earlier lost claim
reply is a distinct gap: exact `/claim` replay returns the original token and held
claim, but `claim_pending` rejects it before reaching reservation recovery.

Resume that same authenticated claim, on the same physical node, without
requeueing, new ownership or another engine start. Preserve generation,
environment, worker/instance, claim request/token and all retained source data.
Recovery remains limited to the existing source-range-v2 ODM-only path.

## Three separate cases

1. **Claim reply lost:** replay only the original durable claim request. The Store
   already binds its replay to job/node/worker/instance and returns the original
   token. Validate that exact envelope against the saved job/advice and fresh
   authenticated claim state. Persist it before proceeding. A newly created local
   reservation intent is allowed only when no earlier intent/effect exists; save
   its exact wire bytes before any reservation/recovery POST. Existing intents
   must never be regenerated. Different, terminal, operator-held or overtaken
   claims retain their existing refusal/retirement behavior.
2. **Reservation never committed:** coordinator has `transfer_json IS NULL` and
   phase `queued` or `waiting_capacity`. The worker has one durable original
   transfer request and a matching local scratch reservation. Use the new
   pre-reservation recovery transaction below.
3. **Reservation committed, reply lost:** coordinator already has the exact
   transfer and phase `awaiting_transfer`. Use the existing schema-3 recovery and
   ordinary exact reservation replay. Do not treat this as case 2 or replace the
   reservation. Any mismatch holds.

## Coordinator transaction and wire

Extend the existing authenticated `/recovery-state` and `/recover` contract with
one explicitly versioned closed pre-reservation variant (proposed schema 4).
Keep existing variants/readers intact. The request must include the exact
pending transfer request as well as its digest: a digest alone cannot validate
or install previously unseen bytes. Carry the existing exact claim binding,
current coordinator epoch, expected maintenance revision, accepted source
custody closure and local evidence digest. Bound body/object count/metadata cost
using existing submission and quota ceilings; receipts carry hashes, not tokens
or redundant source manifests.

Factor `ReferenceStore.reserve` validation/transaction work into an internal
existing-transaction helper. Ordinary `reserve` retains its running-only public
guard; recovery reuses the same transfer validation instead of a second policy.
In a single `BEGIN IMMEDIATE`, recheck all authority/state predicates, charge
quota, record the original transfer, set `awaiting_transfer`, restore the exact
claim to `running`, and insert its immutable recovery receipt. No intermediate
running/unreserved state or transaction nesting is permitted.

Required predicates at the writer transaction:

- Actual current Queue authority/startup and open freeze gate; current node
  revision and existing recovery-compatible state (`active`, `scheduled` or
  `draining`). A draining node continues only this already owned claim.
- Exactly this job occupies the physical node's running/held slot; exact
  reference claim request, token hash, node, worker and instance; unchanged
  generation, environment, manifest and pipeline.
- Queue status `held` with reason exactly `coordinator_restart`, reference phase
  `queued`/`waiting_capacity`, no transfer, error, failure, result or terminal
  effects. No grant/token/renewal/range usage, release or reader fence exists.
- Recomputed signed source/derived manifests, accepted retention receipts and
  complete object custody, including raw inputs excluded from processing. Reuse
  existing accepted-custody checks without requiring a nonexistent transfer.
- Full ordinary reservation policy: exact source version/object IDs/chunk size,
  range count/attempt/byte ceilings, generation/manifest, canonical expiry still
  live, and metadata quota. Do not reset any security counter or delete history.

Exact request replay reads its stored receipt without clearing a later hold.
Current state still governs subsequent work. Stale epoch/revision/closure cannot
commit a new receipt. A conflicting reservation appearing concurrently refuses
the new variant; a fresh observation chooses case 3 only when it is digest-exact.

## Worker proof and ambiguity

Before every recovery POST, including replay, revalidate the original private
intent and local scratch reservation: same job/generation/manifest/input/output
budget/environment, live lease, state `reserved`, allocation zero, no transfer
fence/result/input paths/unstarted settlement. There must be no execution row,
physical Gate reservation or native task for this job. Use fresh engine/task
observation and existing host/Gate/start guards; an unreachable engine proves
nothing and spends no source attempt. No engine mutation belongs to recovery.

Persist request/evidence before POST, reuse its exact bytes after worker death,
and settle only its exact receipt. A lost reply is unknown, never grounds to
discard ownership, renew a reservation ID or requeue. Coordinator restart after
commit transitions to existing awaiting-transfer recovery with its exact stored
reservation; it never recreates source or scratch budgets. Full quota, corruption,
expired unfenced lease, ambiguous native effects, operator hold and mismatched
worker/coordinator histories remain holds with all data retained.

## Qualification matrix

Use existing real Store/Queue/ClaimRecoveryService/driver/ledger/Gate fixtures,
then the existing actual child/TLS restart fixture. Keep the synthetic engine
boundary explicit. Tests must not patch claim/start/custody guards to pass.

| Case | Required assertion |
| --- | --- |
| Claim commits, reply lost, coordinator dies before local save | Same request/token/instance recovered; one claim row and one eventual initialize |
| Worker dies after saving claim, before reservation intent | No invented prior reservation; one durable intent created before effect |
| Reservation intent saved, POST never delivered | New atomic transition preserves original request and all budgets |
| Reservation commits, reply lost | Existing awaiting-transfer path; no second transfer/grant/claim |
| Recovery commits, reply/local ACK lost | Exact receipt replay; no new receipt/effect; later restart hold not cleared by old receipt |
| Worker/coordinator die at transaction boundaries | Either no effect or complete transfer/status/receipt; no partial running/unreserved state |
| Concurrent duplicate recovery / ordinary reserve | At most one exact reservation transition; changed wire refuses |
| Maintenance revision, freeze, operator hold, other node occupant race | Writer-transaction compare refuses; no ownership or data removed |
| Wrong job/node/worker/instance/token/generation/environment | Refuse before effect |
| Changed manifest/receipt/raw custody or prior grant/range/release | Refuse; retained data unchanged |
| Expired lease, nonzero allocation/fence/result/engine row/task/Gate | Hold, no initialize or counter reset |
| Engine or coordinator outage / malformed response | Retain exact intent; no new request/source attempt |
| Full metadata quota / stored receipt corruption | Refuse atomically without history eviction |
| Existing v1/schema2/schema3 and overtaken claim | Existing compatibility/retirement contracts unchanged |

End-to-end assertions include stable job/claim/reservation identity, one engine
initialize, eventual normal result delivery/acceptance, no `queued` transition,
unchanged source/range/renewal accounting, and exact physical-node exclusion.
Connected native-engine, real Viewer import and installed recovery remain
separate acceptance evidence; a synthetic pass must not be reported as those.

## Proposed implementation ownership

After final-head handoff: this agent owns the narrow driver/recovery/Store helper
changes and focused tests. Coordinate any shared source-custody helper extraction
with Claude/root first. No activation, installer, transport, NAS, engine cleanup,
new supervisor, credential or runtime configuration changes are part of this
slice. Heavy tests require the existing serialized resource handoff.
