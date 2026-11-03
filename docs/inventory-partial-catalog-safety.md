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
