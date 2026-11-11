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
