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
