# Partial catalog candidate: rollout remains blocked

This isolated candidate validates every explicitly requested option against the admission catalog and the actual selected node's current type and domain. It skips an incompatible node without dropping an option. Omitted options are passed through as omitted: each NodeODM uses its own defaults, which can differ, including max-concurrency. This is intentional and does not establish identical results between nodes. The assigned node ID is exposed in task info; the Viewer must still integrate this metadata. Actual effective omitted defaults are not persisted or reported.

Catalog admission remains conservative: when several nodes are online, the currently exposed intersection can omit options whose full descriptors differ even if some explicit values would be compatible. Unknown types or domains fail closed. This candidate is not a complete heterogeneous option discovery solution.

Older experimental task records may include `viewer_tasks.full_option_evidence_required` and `viewer_full_option_evidence`. Preserve these in migration, backup, integrity and cleanup censuses; evidence already recorded remains integrity checked. New tasks store explicit option evidence, not full default snapshots. Existing full-catalog evidence is integrity checked but no longer enforced for matching: migrating tasks created under the prior full-catalog equality policy to this behavior changes their dispatch semantics. Dispatch of tasks carrying full-catalog evidence is explicitly held pending reconciliation under the new node-local-default policy. No migration procedure is implemented; do not silently re-interpret them.

**Do not enable or roll back this candidate while an older coordinator can access its database or engines.** An older writer may ignore recorded evidence and dispatch without this validation. Reusing the database, deleting columns or tables, resetting a task, or using a different state directory does not fence it. A rollout must exclude old writers, reconcile existing tasks, prove physical-engine ownership, and qualify recovery and rollback. No such barrier is implemented here.

`processing/effective_option_plan.py` is an unmounted historical proposal, not the behavior implemented here and not an authorization to dispatch. Its default-pinning approach was superseded by the explicit-option contract above.

These local tests do not qualify the real fleet, live admission, Viewer integration, per-node execution or recovery. Do not activate until independently reviewed and tested end-to-end.

For `json` domain strings this candidate accepts only syntactically valid, finite JSON, including rejecting the empty string. NodeODM-specific empty-string semantics have not been verified, so a task requiring such a value must be held pending verification. Object domain constraints accept only nonempty `min`/`max` bounds; unknown constraints fail closed.
