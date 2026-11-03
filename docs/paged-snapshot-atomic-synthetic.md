# Synthetic same-database snapshot staging

The unmounted experiment accepts the current six-field GCP statement shape,
but fails closed if generated GCP or non-null GCP evidence is present: the
current evidence decoder does not supply and semantically verify generated
`gcp_list.txt` bytes against the source and evidence. GCP jobs are not supported.

For synthetic no-GCP inputs only, the signer journal and source, selected and
processing frame staging use one existing coordinator SQLite write transaction.
Internal staging helpers do not commit or roll back their caller's transaction;
the original public APIs still own their transactions. A failure in the outer
transaction rolls back the journal and frame rows. Exact replay rechecks stored
pages; supplied replay streams must be byte-identical, while omitted streams
permit status-only recovery. The signed statement and frames never confer
source access, dispatch, a grant, cleanup, or trusted custody.

This is not a production migration or authority. Its frame/evidence schemas are
initialized by a separate test-only marker and closed schema check; they lack
downgrade protection. External writer/clone fencing, quota, trusted
binding/epoch and signer revocation remain unresolved. The journal marker
detects a single dropped marker or journal, not simultaneous loss of both or
restoration of an old database. Transactions are synthetic and tests do not
establish live deployment, full crash/power-loss durability or GCP semantics.
Synthetic tests kill a child after journal or frame inserts and verify rollback;
another exits immediately after commit before reply, then reopens and replays.
