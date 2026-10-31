# Unresolved authority handoff inspection: disabled draft

On startup, the newer operation fence now refuses an existing authority-scoped
handoff intent or ACK, both before and after acquiring its locks. It never
clears those records. `pending_status` inspects an exact private intent, ACK,
lease identity and optional Docker attestor, but returns only `held` and never
authorizes startup, retry, cleanup or cutover. Lock and candidate observations
are point-in-time observations; they are not a continuous physical fence.

Separate-process synthetic tests kill the supervisor after persisting an ACK
but before GO, and immediately after GO while the candidate retains the
transferred descriptor. In both cases newer coordinator and operation-fence
startup refuse the durable markers. The post-GO candidate retains the flock in
that fixture. The tests use mocked Docker process attestation, synthetic state
and local Unix sockets; they do not prove actual container lifecycle, power-loss
durability, host-level old-writer exclusion or admission gating.

Existing PR197 and other older binaries ignore the intent/ACK markers and can
still start if they gain access after the lock is released. No integration in
the packaged application or automatic reconciliation is enabled. An operator
must not treat this status as permission to remove records or start a second
coordinator, and root/Docker administrators remain outside this guard.
