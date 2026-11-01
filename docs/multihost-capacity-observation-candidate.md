# Disabled multi-authority capacity observations

This source experiment is not deployed, and all new protocol/coordinator entry
points are false-qualified. It is not a transfer admission grant, distributed
lease, physical disk reservation, quota, host-wide writer fence, or NAS service.

A coordinator with an owned private journal records a fixed per-attempt plan,
remote endpoint, distinct authority IDs and distinct TLS certificate pins before
contacting per-host authorities. A pinned mTLS session may reserve and later
re-observe the same fixed server plan. A lost reply leaves an intent that is
retried with identical attempt and plan; remote capacity holds remain uncertain
and are not automatically released. Sequential pin observations return only
`observed_only`, because one authority may change while another is being checked.
The local Unix variant verifies a same-UID peer and records a bound historical
receipt; same UID is not individual process or remote host authentication.

Linux local owner experiments hold O_PATH directory descriptors and compare
mount IDs, device and inode from `/proc/self/fdinfo` on checks. The subsequent
SQLite and storage operations still use pathnames, so this is detection at
observation points, not an atomic mount fence. Cleanup attempts to close all
owned descriptors and release the owner even if a close raises; behavior of an
actual kernel close error with uncertain FD state is not proven.

Validation uses synthetic local roots, private loopback TLS endpoints, synthetic
certificates and no NAS. Tests exercise two logical authorities on one physical
machine, distinct cert pins, second-authority lost reply and exact replay, fresh
pin calls, endpoint mutation rejection, and a separate coordinator child killed
with `os._exit` after one authority replied but before receipt persistence. The
parent then reacquires its lock and retries, seeing one reservation. This is not
host restart, separate physical machines, power-loss durability or proof of a
concurrent atomic multi-host barrier. Test listeners/threads and children are
owned, closed/joined and bounded; synthetic roots remain for inspection.

To qualify a transfer prerequisite, first implement and verify a provisioned
host-wide authority that mediates **every managed writer**, exact generation-
bound durable host hold attestations, held sessions or another atomic distributed
barrier across transfer, actual mount/FD-bound storage operations, safe writer
termination and downstream custody/cleanup, authenticated provisioned remote
identity and NAS owner participation. Existing direct ScratchLedger/NodeODM and
Docker/external writers bypass these sketches. Capacity budgets are estimates,
not enforced filesystem quotas; external usage or underestimated NodeODM scratch
can exhaust space. SQLite and filesystem work are not bounded by socket timeouts.
Do not enable or wire this code as a production scheduler.
