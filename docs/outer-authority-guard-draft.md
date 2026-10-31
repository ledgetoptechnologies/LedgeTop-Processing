# Independent outer launcher lock: isolated draft

This opt-in source experiment keeps a separate host-owned outer flock and the
old coordinator's state lock while the supervisor records a durable one-shot
marker and releases its inner authority lease. The candidate must independently
open and lock that same authority inode and its own state lock. No locked FD is
sent to the candidate, so the candidate cannot unlock the supervisor's outer
lock through a shared open-file description.

The observation routine checks a pinned attestor, pidfd and current
`/proc/locks` ownership for the outer owner, old state, candidate lease and
candidate state. It does not launch or stop a container, admit queue work,
authorize promotion or rollback, or provide continuous monitoring. The marker
makes later cooperating outer supervisors hold after a crash; it is never
automatically cleared. A lost or changed marker, PID, inode or lock holds.
The old-state lock remains held while the supervisor is alive, including after
the inner lease is released. There is nevertheless an inner-lease interval
before the candidate acquires it, covered only by the cooperative outer lock
and marker. The candidate's cloned-state lock is not held until that process
starts. A legacy launcher can race these intervals; the old-state lock also
disappears if the supervisor dies.

**This is not a host-wide writer fence.** The existing older coordinator and
direct launchers ignore the outer lock and marker; a synthetic test shows an
old direct lease acquisition succeeds after the supervisor is killed when no
candidate holds the inner lease. Host provisioning must independently require
every launcher/updater to use the outer lock and block legacy starts. Root,
Docker administrators and alternate mounts remain trusted or must be fenced
separately. A process crash can release the outer lock, and a marker does not
stop noncooperating code. Do not activate this as a production cutover.

Tests use real separate processes, real flock and pidfd, actual Queue and
AuthorityLease classes, and mocked Docker identity attestation. They do not
establish Docker container identity, durable power-loss behavior, a safe
candidate admission barrier, result custody or rollback after candidate writes.
