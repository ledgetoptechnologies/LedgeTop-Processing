# Disabled fetch pin and batching candidate

This opt-in, false-qualified prototype writes a driver fetch intent before
acquiring a purpose-specific, reservation-lifetime scratch pin. The ledger
refuses silent pin disablement or marker/table mismatch; an active pin blocks
reservation expiry and controlled scratch cleanup. It binds reservation,
generation, environment, manifest, input/output budgets and directory inode.
Pin acquisition uses a no-follow directory descriptor, but that descriptor is
not held across later writes or restart: external mount/path replacement, old
binaries, restored ledger copies and administrators are not fenced.

An independently gated batch may pre-record at most four exact inputs as
started before any request. Each request reobserves the claim, rechecks the pin
and grant expiry, verifies bytes, and synchronizes input and directory before
grouped journal and absolute-ledger checkpoints. A process restart with a
missing target holds instead of blindly requesting it again. Viewer’s current
experimental broker charges the full requested response before streaming and
has no qualified idempotency or remaining-budget reconciliation endpoint;
automatic missing-input retry therefore remains unimplemented. Full files are
retained. A separately false-qualified pin-release path attempts exact worker
journal/file plus terminal accepted-output reconciliation, but is not qualified
against external replacement, database restore or real custody.

Synthetic tests exercised 4-item driver delivery, failed/expired requests,
checkpoint reconciliation, and a real daemon-child kill after one of two input
files. The killed child was reaped, its first input retained, and the restarted
worker reached a durable incomplete-batch hold with no second fetch or engine
mutation. A fresh 2-input synthetic daemon run reached coordinator handoff.
The initial 2-input rehearsal was held after the synthetic NodeODM fixture
rejected its second upload; that old fixture was not replayed. The synthetic
fixture now has a separately opted-in multi-upload mode, with duplicate-name
rejection; this is not proof of real NodeODM behavior or Viewer acceptance.

The v2 contract vectors and syntax/hash parser are draft-only, separately
false-qualified, and expose no listener or authorization. Whole/empty object
syntax only is accepted; ranges are rejected pending authenticated chunk-proof
and alignment agreement. Original request identity is immutable; any recovery
actor requires a separate, fenced, audited adoption record which is not yet
implemented. Viewer and Processing are coordinating the separate request and
delivery IDs and durable budget reconciliation contract. Proxy behavior,
wire compatibility, stream fencing, token lifecycle, migrations and live
integration remain unqualified. No deployment is authorized by this document.

An opt-in cProfile sample on the test host took 4.158 seconds for 500
synthetic one-byte inputs in groups of four, compared with 8.725 seconds for
the earlier single-object sample with profiling enabled. These are separate
runs, not a controlled production benchmark. A prior full 10,000-object
single-object synthetic attempt exceeded its 110-second work budget; the batch
version has not completed a 10,000-object test. Neither result represents
50 GB of real image transfer or external storage.
