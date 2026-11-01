# Disabled clone lifecycle research

`CloneSupervisor` is an unwired source experiment, not a host service or an
authorization to start a coordinator. It requires the existing outer and
predecessor fences and an explicit assertion that an independently enforced
host policy excludes every candidate launcher and other writer from before
copy until the held clone lock is acquired. The assertion is not evidence of
that policy. Same-UID processes, old binaries, privileged administrators and
Docker can bypass the cooperative locks without external provisioning.

The pending marker is persisted before the existing stopped census and backup.
After the real backup's source/copy/source comparison, the supervisor retains
the clone lock and directory descriptors. The lock is acquired only after the
copy finishes; protection during copy and until lock acquisition depends on
the external launcher policy. A separately provisioned exact
read-only mount can then be observed and compared to the keyed backup census.
The result never grants launch, handoff, cutover or rollback authority. The
marker and copied data remain on failure or close. The existing backup refuses
active jobs and Viewer tasks. It does not provide a hard I/O or elapsed-time
bound; this experiment does not add one.

Synthetic tests use actual SQLite, files, flock and backup logic but mock mount
checks, filesystem separation and the predecessor schema profile. They do not
qualify real bind mounts, host policy, a process or power-loss recovery, or
exclusion of noncooperating writers. If the supervisor dies, its flocks are
released and the persistent marker only holds cooperating launchers that
honor it. No release or cleanup procedure is provided here.
