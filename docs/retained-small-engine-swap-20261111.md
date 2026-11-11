# Small-engine retention swap candidate

This is a narrow source candidate, not evidence of an installed change. It
targets only the pinned `.89` migration `nodeodm` service and its dedicated
port3001 ingress. Main and the coordinator are never restarted. The generic
empty-task updater and its automatic rollback behavior are unchanged.

`ops/retained_small_engine_swap.py` defaults to read-only inspection. `--prepare`
writes a fresh root-private receipt containing full inspected configuration
(including credentials), the resolved candidate, task identities and exact
current nginx bytes. Public output contains only hashes, counts and fixed image
pins. Keep the receipt private. Preparation does not schedule maintenance.

The private preparation, inspected command/environment and full filesystem
backup can retain credentials as well as client data. Keep the receipt root0700
and its files0600, with root-only local custody. Do not copy these archives or
journals into GitHub, Engineering Hub notes, public artifacts or shared imports.
Public review evidence contains only fixed pins, hashes and counts. Credential
retirement/rotation and backup disposal require separate operator review; this
pilot never automatically expires or publishes those recovery copies.

Apply is one-shot and requires `--apply --receipt /private/new-receipt
--prepared-sha256 REVIEWED_DIGEST`, plus the existing private coordinator worker
and operator credentials file. Run only as root on the verified `.89` host,
after independently reviewing preparation and source. No automatic retry is
provided. Do not delete its operation journal to replay it.

The candidate preserves command/token, max1000, configuration, volume, networks,
limits and restart policy, changing only the immutable image pin and appending
bare `--disable_scheduled_cleanup`. Original source/image:
`sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6`.
Qualified child image:
`sha256:9c9f7ed05e0a8d2e4a370a75dd7b8d8524a0603205e79dfc9046239818237940`.

The flow uses the existing coordinator's exact owned schedule receipt and
begin/finish CAS. It rejects active/unknown engine tasks, cluster running/held
claims, temporary uploads and extra processes, while permitting retained
terminal tasks and unrelated main-node jobs. It closes and drains the dedicated
nginx ingress before gracefully stopping the old engine. No new NodeGate is
provisioned or required by this native Viewer path.

Closing port3001 ingress alone does **not** fence the private backend on13001.
The existing coordinator's owned maintenance state is the new-claim fence:
`Queue.claim` checks node state in the same SQLite write transaction as assigning
the running job, while Viewer scheduling also rejects draining/updating nodes.
The linked job is running before the later ViewerTask engine assignment, so
`begin` sees and rejects an already claimed job even during that assignment gap.
Port13001 remains accessible to trusted host administrators/direct clients;
these cooperating-writer boundaries are not a network firewall against them.
Other direct engine writers must remain quiesced throughout the swap.

Before Compose can remove the old container, the helper retains its complete
filesystem export, including unmounted tmp/log state; separately backs up and
compares the named data volume; captures tmp with numeric ownership; and stores
private source/result hashes and task-index identities. Required disk headroom
is backup size plus8GiB. This is backup admission, not a photo-job byte cap.
It never deletes terminal jobs to satisfy maintenance.

Only then may it create the pinned child without starting it, verify settings,
mounts and patched file hashes, restore the saved tmp, start and independently
check API/options/max1000, identical terminal identities and retained files.
After verification it persists the authoritative Compose pin, reopens and drains
the dedicated ingress, and finishes its own maintenance reservation.

Failure never starts the unpatched old image, performs automatic archive
restoration, stops main, removes retained files or replays uncertain Docker
mutations. It records private failure evidence and attempts to close ingress and
hold **only its provably owned current plan**. A lost schedule/finish response or
unavailable API can prevent that proof; the receipt records what was actually
verified. A finish may already have committed before its response is lost, so
do not describe every failure as a proven held node. Inspect queue, ingress,
actual container and receipt manually before any recovery.

The source tests cover ordered state transitions, failure checkpoints,
retained-terminal admission, command/config preservation, receipt binding,
default read-only behavior and retained-file verification. Native Docker export,
GNU tar comparison, nginx worker draining, recreated-container field equality
and the installed maintenance protocol still require independent Linux review
and qualification before this helper is used for a live swap. These source
tests do not establish installed retention, outage recovery or power-loss safety.

The focused real-Queue tests additionally exercise stale eligible snapshots
against draining/updating state, the linked ViewerTask assignment gap, an actual
claim-transaction/owned-drain interleaving, and unrelated main/queued work.
They use real temporary SQLite queues and no Docker, GPU or live node.

Existing real-server synthetic qualification of child image9c9f completed both
startup and restart, each followed by a graceful stop with exit0 and noOOM.
The accepted record is local
`C:/Users/fstor/.codex/tmp/small-retention-fullserver-20261111/node-retention-fullserver-fabd69197105/result.json`,
SHA256`e1276d1d752d34f34ca8e15bc54740055134dc343dfab9d8147ff0782c1d576f`.
Each phase retained three terminal tasks and five sentinel hashes, exposed82
options and max1000. This qualifies the candidate's scoped idle-server graceful
shutdown behavior; it does not replace live ingress/backup/swap qualification or
establish power-loss/OOM recovery or real-photo processing.

Root also qualified the frozen a8b67600 helper's read-only `Runtime.snapshot()`
on the actual `.89` host using existing credentials only in memory. That dated
checkpoint passed at cluster revision31/stateactive with ten terminal tasks,
the exact9c target cached, one Node process, and only the one-byte `*` tmp
placeholder. Current Compose bytes matched
SHA256`bed3981bb7b967c8e491487b8ea692424bdd5ecced8448b2d840a303a8c1d40e`;
nginx matched
SHA256`a518e6e10e3e5c245456e53935235a878335d4434ffb56426cfa330c968435ef`.
No schedule, preparation, apply, backup, service change or other host write was
performed in that check. This removes a read-only admission uncertainty; the
mutating native steps remain separately unqualified. Recheck actual state
before any later preparation/apply rather than treating revision31 as current.

The subsequent ingress writer correction explicitly restores the original file
mode with `fchmod` after `fchown`, before fsync/atomic replace. Without that step,
the helper's private umask077 turned a0644 nginx configuration into0600, which
would deny a nonroot nginx reader. The regression test uses an actual temporary
file and replacement under umask077; only nginx/HTTP observations are mocked.
It is POSIX-only, and the old read-only a8b67600 observation does not qualify this
new mutating path. No live permissions were changed by the source correction.
