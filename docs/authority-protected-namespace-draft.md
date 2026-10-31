# Descriptor-based authority handoff is blocked

Passing a lease descriptor with `SCM_RIGHTS` shares its open-file description.
The receiver can call `flock(LOCK_UN)` on that descriptor and release the lock
for the supervisor's descriptor as well. A root-owned marker/socket namespace
does not prevent that operation. The isolated test demonstrates this with real
Unix sockets, separate processes and a real competing lock acquisition.

Do not integrate or enable candidate adoption of the lease descriptor. The
existing prototype predates this finding and is not qualified for runtime use.
The protected namespace experiment was removed rather than presented as a
fence. In addition, the existing `pending_status` reader is not compatible
with that experiment's root-owned directory and group-readable marker mode.

A possible alternative is a separate trusted host authority process that keeps
the lease private and mediates every authority-sensitive write, or another
independently enforced fence. Its process-death, in-flight mutation, queue
admission, old-writer and privileged-administrator behavior still requires
design and qualification. The test does not authorize cutover or rollback.
