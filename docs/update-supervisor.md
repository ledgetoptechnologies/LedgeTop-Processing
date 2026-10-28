# Source-only host update supervisor

`processing/update_supervisor.py` connects the existing `DockerRegistry` to
explicit local qualification and maintenance adapters. It installs no daemon,
timer, approved channel, credential, qualifier, or deployment adapter. Importing
it performs no work. Tests use a real registry component with a fake Docker
transport, SQLite adapter receipts, and synthetic temporary host state. No live
image, NAS, coordinator, ingress, or running container is changed by these tests.

## API and ownership

Construct a frozen `SupervisorPolicy(node_id, channel, current_image_id,
authority_id, qualifier_id, qualification_policy_sha256,
maintenance_adapter_id, capacity_bytes)` and
`UpdateSupervisor(path, policy, registry=..., qualifier=..., maintenance=...)`.
The registry must be an actual `DockerRegistry`, with the exact configured
channel and capacity reservation. The qualifier and maintenance bindings must
match the policy, registry Docker Engine identity, physical node, and sole
coordinator authority. There are no default production selectors.

Use the supervisor as a context manager. It owns one fixed private OS-exclusive
lease beside its SQLite journal. The binding records the immutable policy and
registry configuration/path plus lease/root device and inode. Same-process and
subprocess competitors fail. Replaced, symlinked, hardlinked, foreign-owner, or
publicly accessible lease/journal paths fail closed. An inherited child cannot
use or close its parent's session. Close waits for another thread's operation;
a reentrant callback cannot close or start another operation. The lease file and
journals are retained, including after uncertain operations.

- `discover()` inspects only the frozen channel and records one operation, or
  returns `None` when the exact candidate config image is already adopted.
- `advance(operation_id)` attempts one next stage: stage, qualify, schedule, run.
  Each external mutation has a FULL SQLite intent committed first.
- `reconcile(operation_id)` uses only registry inspection or an adapter's
  read-only receipt lookup. It never repeats a possibly issued mutation.
- `status(operation_id)` returns bounded local state, candidate, sealed plan and
  completion; no credentials or update tokens belong in these records.

The configured current image is the initial baseline. A verified, finalized
completion atomically adopts the new image and increments a durable host
generation. Operation identity binds that generation and previous image as well
as the immutable candidate. A long-lived A→B→C→A→B release sequence therefore
has a fresh final A→B operation. Registry artifact/pull identities remain
separate, so a prior staged immutable image needs no new pull. No new cycle is
allowed while an earlier operation is unresolved, regardless of tag,
descriptor, attestation, or candidate-ID changes.

## Required adapter contracts

`Qualifier.binding` is exactly `{qualifierId, policySha256}`. It implements
`qualify(request)` and read-only `lookup(request)`. The fixed request binds the
operation, candidate/manifest/config, host generation/previous image, physical
engine/node, coordinator authority, supervisor and qualification policies, and
an owned qualification receipt directory. Its `requestSha256` excludes itself.

The closed successful qualification receipt has `schemaVersion:1`,
`requestSha256`, `operationId`, `nodeId`, `engineId`, `authorityId`, `candidateId`,
`manifestDigest`, `configDigest`, `supervisorPolicySha256`, `hostGeneration`,
`qualifierId`, `qualificationPolicySha256`, `passed:true`, bounded
`engineVersion`, `evidenceSha256`, and `qualificationSha256`. The final field is
the canonical SHA256 of all other receipt fields. `seal_receipt(value, field)`
is provided for adapters. No failed, stale, changed-host, or changed-policy
receipt authorizes scheduling.

A real processing qualifier must independently acquire the same physical/global
execution admission used by production processing, or run qualification on an
isolated off-host executor. `--network none` does not prevent concurrent resource
use. This supervisor does not implement that admission. Before enablement, the
qualifier needs reviewed immutable fixture/evidence policy, actual engine and
platform verification, bounded execution/owned-output handling, and durable
lookup semantics. It must not pretend NodeODM and NodeODX are interchangeable.

`MaintenanceAdapter.binding` is exactly `{adapterId, authorityId, nodeId,
engineId}`. Required methods are `inspect_host()`, `schedule(request)`,
`lookup_schedule(request)`, `run(request)`, and `lookup_completion(request)`.
`inspect_host()` returns exactly `{nodeId, authorityId, engineId, imageId}`;
the last two lookup methods are read-only. Schedule receives the accepted
qualification SHA. Its closed plan receipt contains the qualification receipt's
common host/candidate/policy/request fields, `planId` (canonical UUID),
`previousImageId`, `targetImageId`, `qualificationSha256`, and self-excluded
`planSha256`. The schedule request has its own fixed owned receipt directory.

The run request additionally contains that exact sealed plan and a fixed owned
run receipt directory. Completion has the common fields plus `planId`,
`planSha256`, `previousImageId`, `targetImageId`, `qualificationSha256`, `imageId`,
`healthy:true`, `hostPinDurable:true`, `ingressOpen:true`,
`coordinatorFinished:true`, and self-excluded `completionSha256`. The physical
host must independently report the exact target image. A completion claiming
only container health or replacement is insufficient. Unknown or failed
qualification, schedule, replacement, rollback, pin, ingress, or coordinator
finish outcomes remain held; there is no automatic replacement or rollback
retry in this component.

## Sole-coordinator integration boundary

An adapter must use the existing sole coordinator's maintenance authority and
the existing managed local-container update/finalization behavior. It must not
create another scheduler, choose a peer plan, update the sole coordinator as if
it were a processing node, or release unrelated gates. It must preserve main's
processing resource settings. Target/current pins are immutable image IDs;
registry manifest digests and local config IDs remain distinct.

The real scheduling adapter must persist its exact request ID, dynamic
timestamps, expected coordinator revision, and complete schedule body before a
POST. A lost response requires a durable owned-request receipt lookup. Plain
node GET with a matching target does **not** prove ownership. The legacy
maintenance schedule API generates a plan UUID without caller correlation;
that API alone cannot satisfy this recovery contract. The source now provides
an [owned scheduling endpoint](owned-maintenance-schedule.md), but it must be
installed and connected by the production adapter before use. Before run, the adapter must verify the
coordinator still exposes its exact frozen plan/revision and not a later peer
plan. Run and completion lookup must use only the supplied own receipt directory
and actual managed finalization evidence; credentials/update tokens stay private
inside the adapter's existing receipts.

Production adapters, process lifecycle installation, policy/channel approval,
maintenance windows, sole-coordinator authentication, actual qualification
execution admission, and end-to-end acceptance remain required. There is no
claim of unattended runtime installation, automatic qualification, or live
update availability from this source component alone.
