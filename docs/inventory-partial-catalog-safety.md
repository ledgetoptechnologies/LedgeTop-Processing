# Partial catalog candidate: rollout remains blocked

The selected-option journal and full-option journal are persisted together.
The new `viewer_tasks.full_option_evidence_required` flag and
`viewer_full_option_evidence` table are part of the task's safety state.
They must be included in all migration, backup, integrity and cleanup censuses.

**Do not enable or roll back this candidate while an older coordinator can
access its database or engine.** Earlier builds, including parent `e48fa46`,
ignore the full-catalog evidence and can dispatch a task on a node whose
unspecified defaults differ. Reusing the database, deleting the new column or
table, resetting the task, or using another state directory does not enforce
the barrier. A rollout must exclude old writers, reconcile existing tasks,
prove the complete physical-engine ownership boundary, and qualify recovery
and any rollback before activation. No such barrier is implemented here.

The full-catalog equality check is deliberately conservative. A read-only
live catalog observation on 2026-11-03 found that the existing cluster and
main nodes advertise different full option definitions, including enum
defaults/domains. They therefore would not currently be interchangeable under
this candidate. A safe effective-default contract and paired Viewer/engine
validation are required before claiming that partial-fleet admission supports
fallback between them. The per-node route and browser behavior are not
qualified by these unit tests.

`processing/effective_option_plan.py` is an unmounted, non-authorizing
proposal for comparing a source and target catalog. Its caller must explicitly
classify each changed default as a reviewed semantic pin or a node-local
numeric execution setting. It never waives a missing definition, unknown type,
unknown domain or ambiguous default; special workflow/file options cannot be
pinned. It always returns `authorizesDispatch: false`, even for a proposal.
The example `max-concurrency` classification preserves each node's own value;
it does not change a running node or establish that this option is safe to
classify. The actual catalogs still contain missing definitions, so this
proposal holds rather than claiming that the existing PCs can fall back.

An implementation that uses such plans must first authenticate and version the
reviewed classification against the exact catalog versions, persist the
effective options and their provenance with the job, enforce them unchanged at
the actual node, fence stale writers and verify the resulting workflow. The
current comparison does not establish those properties or prove engine-version
equivalence.
