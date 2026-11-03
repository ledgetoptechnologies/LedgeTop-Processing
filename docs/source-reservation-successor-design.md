# Source reservation successor (design, not implemented)

Status: the Processing side is implemented behind disabled gates
(`ReferenceStore._reservation_successor_ready` and
`ReferenceDriver._reservation_successor_ready` both return False). The Viewer
side is not implemented. No runtime flag, TTL or activation gate changes.
Nothing here selects live TTLs.

**As built, the lineage is the `reservationId` itself.** A successor keeps the
same `reservationId` and changes only `expiresAt` and `nonce`, plus
`successorSequence` and `previousTransferSha256` on the request. Every
accounting key, range request id, `.part` name and scratch directory hashes
`reservationId` and not the deadline or nonce, so no new lineage id is needed.
That replaces item 1 below.

What is built:
- **Coordinator:** `ReferenceStore.reserve_successor` and
  `POST /ltds/worker/v1/jobs/{id}/reservation/successor`.
  - Fencing: the running claim token and the node.
  - Compare-and-swap on (sequence, prior digest).
  - An identical replay returns the sealed receipt.
  - A nonce is never reused, and the new deadline must be strictly later.
  - Window zero: it succeeds only an elapsed lease and never extends one ahead
    of time.
  - Prior leases are kept in `reference_reservation_successors`. That table is
    created only by an enabled call, so the gated schema is unchanged.
- **Scratch:** `ScratchLedger.extend` in a new `reservation_extensions` table.
- **Driver:** a `reservationSuccessor` journal, saved before `ledger.extend` and
  the POST, with exact replay after a lost reply or restart. Adoption only
  happens on an exact receipt; ambiguous outcomes hold.

Interoperation is not claimed. After adoption the sealed grant still carries
the prior nonce. In `fetching` the driver holds as
`reservation_successor_grant_unavailable` until the Viewer mints a
successor-bound grant and the coordinator accepts it (see below).

## Problem

A source-range v2 transfer has two deadlines:

- the **grant** expiry, `min(now + maxGrantTtlMs, reservation expiry)` at the
  Viewer broker, which the driver renews through its durable request/ACK
  journal (`ReferenceDriver._source_range_grant`);
- the **reservation** expiry, `transferRequest.expiresAt`, minted once by the
  driver as `clock() + transferTtl` in `_new_transfer_request`.

The reservation deadline is fixed. Nothing extends or succeeds it:

- `ReferenceStore.reserve` freezes it ("Expiry does not reset this
  reservation ... a future reconciliation API. Until then freeze."), and a
  replay does not renew the lease, grant or budget.
- The Viewer copies it into `viewer_reference_transfer_grant_heads`, where a
  trigger makes it immutable. The broker refuses to mint past it
  (`STALE_RESERVATION`), and renewal refuses it (`source_grant_renewal_binding`),
  or refuses a successor grant that cannot outlive its predecessor.
- `ScratchLedger` holds its own `reservations.expires` and expires a reservation
  only while `allocated=0`.

So a transfer that outlives its reservation cannot finish. A successor must
carry the old reservation's accounting, fencing and files, because all of them
are keyed to `reservationId`:

| Piece | Where | Key |
|---|---|---|
| Viewer usage and request ledger | `source_range_usage`, `source_range_requests` | `identity_key` hashes `reservationId` |
| Processing budget and range ledger | `source_budgets`, `source_ranges`, `source_objects` | identity hash includes `reservationId`; a new id for the same job is `source lineage requires reconciliation` |
| Range request ids | both sides | hash of the identity JSON |
| Input and partial files | `scratch/jobs/<reservationId>/`, `.source-range-<identityKey[:24]>-*.part` | reservation directory and identity key |
| Scratch capacity | `ScratchLedger.reservations` | `reservationId` |
| Coordinator record | `reference_jobs.transfer_json`, one `reference_transfer_grants` row per job | job |

A naive new reservation would reset the counters, charge already-fetched ranges
again, orphan the partial files (still counted as allocated) and be refused by
the Processing lineage check and the Viewer single-head constraint.

## Current behaviour (characterized)

Both are tested on `claude/reservation-successor`:

1. **Past the deadline mid-transfer.** The driver's own
   `validate_source_grant` refuses the lineage before any Viewer call
   (`source grant exceeds reservation lease`). The driver holds as
   `source_transfer_grant_unavailable`. No renewal intent is journaled. A
   restarted Viewer and a fresh driver change nothing:
   - broker usage, the grant head (sequence 0), the coordinator reservation and
     the `.part` bytes are unchanged;
   - the hold reason is generic, so an operator cannot tell expiry from a bad
     grant.

   Test: `test_renewal_after_reservation_expiry_holds_with_lineage_frozen_for_a_successor`
   (real Viewer 4bb8d33).
2. **Past the deadline before the coordinator confirmed the reservation**
   (lost reply or outage on `POST /jobs/{id}/reservation`).
   - Before this branch, the driver retried forever as
     `retry_wait/driver_step_unavailable`. `ScratchLedger.reserve` raised
     `PolicyConflict` for the expired row, and a new reservation is minted only
     from stage `claimed`.
   - The branch now holds as `transfer_reservation_expired`, without another
     POST and without minting a new reservation.

   Test: `test_unconfirmed_reservation_past_its_fixed_deadline_holds_instead_of_retrying`.

Known but not changed here: a first object slower than the scratch TTL lets
`ScratchLedger` expire the reservation (`allocated` is recorded per finished
object), so `record_allocated` raises and the driver retries generically.

## Proposed successor contract

The successor is a new lease on the **same transfer lineage**, not a new
transfer.

1. **Lineage id.** Introduce `transferLineageId`, fixed at the first
   reservation. Accounting, request ids, `.part` names and the scratch
   directory key on the lineage, not on `reservationId`. Reservation ids become
   a sequence inside the lineage: `successorSequence`, plus
   `previousReservationSha256`.
2. **Coordinator first.** The driver asks the coordinator, not the Viewer:
   - `POST /jobs/{id}/reservation/successor` with the claim token, the prior
     reservation digest, the successor sequence and a fresh nonce and request
     digest. The coordinator seals it in a per-job successor journal with
     compare-and-swap on (sequence, prior digest).
   - It is idempotent on an identical request digest and a conflict on any
     other.
   - It is accepted only for a running job owned by the same claim, and only
     while the prior reservation is expired or within an explicit renewal
     window.
3. **Viewer authenticity.** The Viewer accepts a successor only through the
   coordinator dispatch path (a sealed successor receipt bound to job,
   generation, `viewerAttemptId`, lineage, sequence and prior digest). It must
   never accept one directly from a worker.
   - Grant head: either a migration that allows a CAS move to the new
     reservation id, expiry and nonce, or a separate reservation-lineage table
     that the head references.
   - The grant-renewal sequence restarts per reservation, or continues
     lineage-wide; this must be decided once, on both sides.
4. **Budgets carry.** Usage rows are keyed by lineage, so request, byte and
   attempt counters continue. A successor never grants new budget implicitly;
   any extra budget is a separate, explicit authority decision.
5. **Fencing.** Succession requires the current claim token and generation. A
   stale worker or a superseded generation cannot mint a successor, and a
   successor never re-fences an existing claim.
6. **Scratch.** `ScratchLedger` gains `extend(lineage, successor)`, which moves
   `expires` and keeps `allocated` and the directory. It must not treat
   `allocated=0` expiry as fatal while a transfer is mid-object.
7. **Driver journal.** A `reservationSuccessor` record mirrors `sourceRenewal`:
   `request_pending` and then `receipt_pending`, saved before each remote
   call, replayed with the same request digest after a lost reply or restart,
   and adopted only after the coordinator receipt and Viewer grant agree.
8. **Errors.**
   - The Viewer returns a distinct `reservation_expired` (successor eligible)
     instead of the generic 409 `renewal_conflict`.
   - The driver holds as `transfer_reservation_expired` until the successor
     contract is enabled.

## Missing cross-service contract

These pieces must be agreed by Processing and Viewer before any
implementation:

- the lineage key definition, and a migration for in-flight rows keyed by
  `reservationId`;
- the coordinator successor endpoint, its journal schema and its receipt
  format;
- the Viewer successor ingestion route, and the grant-head and lineage
  migration (today blocked by the immutability trigger and `job_id UNIQUE`);
- `reference_transfer_grants` accepting a successor's initial grant
  (today one row per job, with a different digest as a Conflict);
- the renewal-sequence scope (per reservation or per lineage);
- the error-code split (`reservation_expired` versus other binding failures);
- who chooses the successor TTL and renewal window. That stays unset here.

## Gates

Keep every existing activation gate held. The successor path should land
behind its own disabled gate, with paired real-Viewer tests for:

- lost successor request reply, lost receipt reply, and Viewer and driver
  restart between them;
- stale-claim and stale-generation refusal;
- unchanged cumulative usage across the successor;
- byte-identical `.part` continuation.
