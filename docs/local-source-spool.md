# Local coordinator source spool

The cluster PC can store incoming and queued input on a private directory on its
local filesystem. The coordinator must receive that directory as its own bind
mount and configure `sourceSpool.root` and `sourceSpool.mountPath` to the mount
target. Keep the identity marker inside it. Do not use a directory that merely
happens to exist beneath the container's writable layer: a missing mount must
stop intake. SourceSpool requires the configured mount, marker and private owner
and mode, and checks their identities on each operation.

Provision the directory outside an existing data tree, with stable host storage,
private access for the coordinator UID/GID, and a dedicated read-write bind.
Do not modify an existing share or Viewer storage. Reserve input bytes and a
free-space floor on the actual disk, and account for the local node's processing
scratch and other users of that same filesystem. Reservations are cooperative,
not a filesystem quota. The file and containing directory publication barriers
must work on the installed filesystem; an ordinary copy is not qualification.

`tests/test_local_spool_mounted.py` is an opt-in test for an empty, task-owned
mount supplied through `LTDS_TEST_LOCAL_SPOOL_ROOT`. It writes a marker and
synthetic bytes and deliberately changes the marker; never point it at an
installed spool or client data. A passing isolated bind-mount test establishes
only the tested environment's behavior, not the cluster PC's disk durability,
power-loss recovery, or successful installation.

This is the queued-input path. Current source intake writes every input to the
coordinator spool, including jobs for which a node is already free. Direct
streaming to a reserved worker is separate work and is not provided by this
configuration. Keep source cleanup disabled until Viewer output acceptance and
the agreed 24-hour retention have been qualified.
