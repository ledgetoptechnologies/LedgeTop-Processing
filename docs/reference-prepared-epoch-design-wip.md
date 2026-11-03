# Prepared submission and epoch-safe transfer design (WIP)

This is a proposal, not an implemented protocol or deployment approval. Existing
v1 grant holds and environment-only admission rejection stay in place. The
Viewer candidate `c92deb5` includes `viewerAttemptId` in its submission;
`reference_protocol.validate_submission` has a closed v1 field set and rejects
it. Do not ignore or silently strip the field. Viewer and Processing must agree
on a separately versioned wire schema, endpoint, canonicalization and fixtures
before enabling a route. In particular, the current internal prepared method
is not a verified authentication boundary.

## Proposed prepared operation

A dedicated versioned envelope contains an unchanged, closed reference v1
submission plus required `viewerTaskId`, `viewerAttemptId` and a stable
`activationId`. The principal and environment come only from a verified
principal-scoped authentication context, never request JSON or the existing
environment-only bearer. Validate the nested submission against the existing v1
rules; compute a domain-separated digest over the entire canonical envelope,
including all three identifiers, the original submission digest and protocol
version. Define canonical bytes, exact response and error statuses jointly
with Viewer. Under one immediate transaction, persist principal, environment,
full request digest, submission ID, eventual job ID and generation, task,
attempt and activation ID along with the queue entry. Exact same-owner replay
returns exactly the stored identity; any mismatch, reuse across owners or
legacy/no-provenance row fails closed. Preserve production head/FIFO. A
separate idempotent activation can release discovery only after Viewer has
durably persisted the matching job/attempt identity; activation cannot be
withdrawn or rebound while any claim or grant is usable. Lost-response lookup
must require the same principal and return no cross-owner data. Validate the
schema version and constraints at startup and hold incompatible/partial state.
This augments the present candidate, whose prepared record does not yet bind
task, attempt and activation ID at submission time.

## Stable accounting versus renewable authority

Define a stable, domain-separated accounting identity from the immutable
owner/tenant namespace, environment, job ID, generation and input manifest
identity; select and freeze its exact fields and canonical encoding with the
broker and Viewer, including whether a new generation legitimately creates a
new budget. The identity MUST NOT include coordinator startup epoch,
worker instance, transient grant ID, activation refresh, reservation renewal,
retry count or source URL. One durable row keyed by this identity owns the
policy, charged request count, bytes and range records for all authorized
successor epochs. Policy changes, identity collision or ambiguity hold rather
than creating a fresh allowance. Idempotency/range request keys are likewise
stable across epochs for the same logical transfer; a distinct actual transfer
attempt must be charged against the same cumulative budget. Preserve uncertain
charges across crash/restart and never refund on lost responses without proven
transactional reconciliation. Persist charge before serving bytes and make
concurrent charging atomic. The existing disposable
`reference_range_ledger_candidate.py` models stable job accounting but is not
an authenticated production broker or proof of this protocol.

The signed grant separately binds accounting identity, immutable activation
binding digest, principal/authority, coordinator execution epoch, job and
generation, reservation, manifest, policy and authorized worker identity.
A broker must authenticate the grant and check the current authoritative epoch
and activation when admitting every range, atomically with charging or under a
lease/fence whose revocation semantics are specified. Reject stale, mismatched,
withdrawn or unknown authority before serving bytes; a previous successful
claim-state response by itself is not a continuing fence. Specify what happens
to active streams during epoch change and await drain or revoke their trusted
lease before allowing successors; old grants/workers must be fenced, not merely
hidden from new coordinator requests. Same stable request on retry may return a
persisted verified receipt only when it describes exactly the same bytes; it
must not serve fresh uncharged bytes. Never mix authorization epoch into the
accounting key or reset spent counts on restart.

## Migration and verification before any enablement

* Inventory live reservations, range ledgers, outstanding signed grants and
  worker versions, and identify authoritative storage and identity domains.
  Drain/fence old workers and revoke old grants before epoch transition. Keep
  completed inputs/results and uncertain transfers; no deletion on ambiguity.
* Migrate transactionally only where prior immutable identity, policy, charges
  and ranges can be proven complete and map unambiguously to the new key.
  Preserve cumulative counts and uncertain charges; conflicting, partial,
  unknown-version or duplicate mappings hold for operator reconciliation.
  Back up and test rollback; old binaries must remain fenced after rollback.
* Cross-language closed-schema fixtures must prove Viewer/Processing exact
  canonical bytes, principal scoping, wrong/missing/extra fields, idempotent
  retries, conflicts, lost-response recovery and no early claim.
* Broker tests must cover exhausted/partially spent budgets across many epoch
  changes, restart at every charge/serve/ack boundary, simultaneous old/new
  epochs, partial reads, stale signatures, active streams, collision and policy
  changes, same-range replay and independent new attempts. Assert no bytes are
  served by stale grants and total charged budgets never reset.
* Process-death, mixed-version, migration/rollback and real authenticated
  multi-process broker integration tests are release gates. Existing synthetic
  mocks are insufficient to qualify production or client data processing.

The unmounted Python v2 candidate requires a separately supplied trusted_policy
snapshot with capabilityFingerprint, optionsFingerprint, allowedOdmOptions and
availableEngines. Fingerprints, option allowlist and engine availability must
come from one authoritative coherent snapshot, never the submitted envelope.
The caller and its policy provenance are not yet authenticated or implemented.
