# Inactive census and recovery-hold runtime integration

This branch combines the indexed upload census and receipt prototypes with the
cooperative recovery-hold candidate from commit `0ec3259`. The receipt and
census branches are already in its ancestry. The hold blocks new cooperative Queue and ReferenceStore claims
and authenticated worker claim snapshots. It also refuses ordinary Queue
completion while held, so results from work already running require a qualified
durable recovery path. It does not stop existing engine work,
observe every node, or fence direct NodeODM/container access. There is no
qualified all-node recovery protocol or release operation.

The experimental runtime configuration can require the incremental census only
alongside an already installed required recovery hold and an authority lease.
The configuration parser and handler are both guarded by false qualification
constants. Required startup checks the existing hold and census under the Queue
transaction; missing/corrupt schema refuses startup. The setting is fixed for a
Queue lifetime. Default configurations do not enable the feature.

No migration is automatic: offline installation must occur only after all old
writers are independently stopped and the authority owner is fenced. The
prototype has no qualified production migration, downgrade, distributed node
identity binding, remotely accessible broker service, restart reconciliation,
or live release path. Do not infer that a held admission means nodes are idle or
data is durable. Synthetic tests exercise configuration, claims and schema
failure only; the user-facing workflow remains unqualified.

### Queued Viewer journal inspection hint

The Viewer-compatible task-info response can include `ledgetopHeld: true` and
`ledgetopHoldReason: transfer_journal_inspection_required` for an existing,
queued, unclaimed task when the global transfer journal cannot be qualified or
its inspection encounters an expected database, ownership, filesystem, or fence
error. The NodeODM-compatible status remains queued (code 10). This is a
point-in-time diagnostic hint, not an engine-started assertion, an operator
resolution, an admission lease or authority to remove evidence. The existing
claim gate remains authoritative. It does not expose raw exception details.
Existing task authentication and authorization still apply; the hint does not
create an operator endpoint or grant access to a task.

Each eligible task-info request performs an additional SQLite connection and
schema/evidence check. The queue's existing SQLite timeout is ten seconds; busy
or unavailable state can therefore delay a poll. High polling rates should be
measured before rollout, and the marker must not be treated as a continuously
held fleet-wide lock. If an earlier task lookup cannot open its database at all,
the ordinary endpoint error path still applies: this hint does not guarantee a
response when the entire queue is unavailable.
