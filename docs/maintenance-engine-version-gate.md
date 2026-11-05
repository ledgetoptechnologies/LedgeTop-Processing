# Maintenance target-version safety gate candidate

This patch is a review candidate. It does not enable unattended updates, change
an installed node, or complete reference-worker pin migration.

New reviewed maintenance schedules may include `targetEngineVersion`, bound to
the immutable target image and plan/CAS identity. It is recorded in node state
and, when supplied, the signed-by-digest owned schedule receipt. Historical
owned receipts keep their exact prior shape and replay identity. Existing node
rows migrate additively with a NULL target version; the running backend is never
used to invent that reviewed target. An unbound plan cannot execute or complete
automatic success. An exact successful finish must name the plan's version.

The managed host path requires an explicit private `workerMode` setting:

- `legacy` declares that this host uses the existing legacy worker path. Known
  reference-worker config or user-unit artifacts veto this declaration.
- `reference` refuses with `reference_worker_handover_unimplemented`. Its
  configured engine pin and actual worker readiness cannot yet be migrated or
  authenticated by this updater. The node stays drained; no container is
  replaced or ingress reopened.
- An omitted or unknown mode refuses with `worker_mode_unbound`. Missing files
  do not imply legacy mode.

The direct updater CLI likewise requires an explicit reviewed `--worker-mode`
before execution. The managed schedule CLI requires `--target-engine-version`.
These settings are server/operator-owned, never job inputs.

For a version-bound legacy operation, direct engine health must match the exact
target version as well as the immutable image and engine kind. The managed
ingress backend and public `/info` must agree with that same plan version before
reopening and before coordinator success. A matching old engine/public pair is
insufficient. Private deferred receipts retain old and target engine versions;
rollback health verifies the recorded prior version rather than applying the
new-version pin to the old image. Lost acknowledgements use the same persisted
plan and result without recreating a container.

Focused Linux qualification passed 67 tests with no skips in a read-only,
network-disabled container, one CPU/512 MiB, with synthetic host transports.
The separate managed finalization/resume suite passed **15/15**, including an
ingress wrong-version refusal before reopen, in the same setup. These
checks establish metadata/host orchestration behavior, not an installed update.

## Known surrounding adapter migration still required

`ops/update_maintenance_adapter.py` currently creates schedules without reviewed
engine-version metadata and reconstructs older completion proofs. Its legacy
unbound plans now refuse execution; four existing broader adapter tests expose
that deliberate missing migration. Do not merge or deploy this patch as a
complete unattended update release. The next patch must use existing qualified
release metadata to bind target engine version and worker mode, migrate exact
schedule/completion validation, and then qualify full supervisor → node update
recovery. A separate digest-to-version registry must not be introduced without
first reviewing existing qualification authority.

The existing `StartupQualifier` already seals `engineVersion` with its exact
staged/runtime-image and probe evidence in the persisted qualification receipt.
`UpdateSupervisor` validates that receipt, but schedule/run requests propagate
only its `qualificationSha256` today. The next patch should carry the validated
receipt's version through those closed request schemas, owned schedule, plan,
completion and recovery. An untrusted runtime `/info` observation or a second
independent version registry is not the source for a new target pin.

Dynamic per-node installation inventory and reference-worker stop/config-pin
handover/restart/readiness proof remain required. No worker config is mutated
and no false readiness endpoint is added by this gate.
