# Coordinator exclusive-owner provisioning draft

This is an unapplied engineering draft for review, not a deployable runbook.
It assumes host root and authorized Docker administrators are trusted operators;
the intended fence is against unattended restart paths, other known workers,
and network clients bypassing the authorized ingress. It is not intended to
withstand a deliberate privileged administrative override.

Historical read-only observations on the cluster PC are a starting point, not
current authorization or a fresh census: coordinator
`ledgetop-processing-prototype-c3fcb3a`, revision `693cb7e`, image ID
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`;
Docker restart policy `unless-stopped`; direct publications at
`192.168.50.89:3005`, `192.168.50.89:18443`, and `127.0.0.1:18080`;
NodeODM host-loopback `127.0.0.1:13001`; and a separate host-network ingress.
The coordinator has a state volume and two runtime bind mounts. Its historical
container has no Compose labels. Reverify all facts immediately before action.

`python3 ops/exclusive_owner_preflight.py` is read-only and prints fixed check
names only. It verifies some identity/recipe facts for that historical snapshot;
it does not establish an exclusive firewall, runtime recipe, unique writer,
complete backup, or permission to cut over. It prints no raw inspection JSON
or environment. Docker inspection output is size-checked after capture; this
is not a hard memory limit. Run it on the verified cluster PC only after review; a mismatch
or inspection error is a stop condition, not a reason to override the check.

A practical implementation under the trusted-operator assumption needs:

1. An administrator-provisioned dedicated service identity and systemd unit
   as the sole *unattended* launcher, with the known old Docker restart policy,
   old unit/timer paths, and worker launchers accounted for. Bind the Docker
   permissions and state/authority mounts to that unit. Preserve a separately
   controlled operator recovery path. The service must start closed and leave
   ambiguous launches stopped across daemon or host restart.
2. An owner-controlled firewall/network configuration that routes approved
   clients only through the gateway and closes the coordinator's direct host
   publications and NodeODM bypass routes. Enumerate all publishers, host
   networking and local socket routes, not only the historically observed
   ports. Apply an atomic, rollbackable ruleset scoped to these ports; prove
   negative access from other hosts and local known workers. Preserve host
   administrative access and unrelated services. This repo has no reviewed
   production ruleset or systemd unit yet.
3. Authenticated admission closure plus a verified drain of uploads, response
   streams, scheduler executions and node operations. Node maintenance state
   and an empty database queue alone do not prove this drain. Keep the fence
   active through shutdown and verify the exact old process and known worker
   paths cannot automatically restart or write.
4. Retain the exact currently running image ID and existing container; create
   a unique operator-recorded rollback tag and verify it resolves to the same
   image ID. Docker tags can move, so record and recheck the image ID. Preserve
   complete private runtime configuration and secret references without
   printing secrets. Ensure the image exists on the cluster and its available
   storage is adequate before any stop. A previous retained container from an
   older release is not the rollback artifact for the current image.
5. With all writers fenced, take and independently verify a durable consistent
   backup of SQLite including WAL state, queue/input files, result archives,
   permissions, authority binding and runtime configuration. Account for
   storage and cross-filesystem consistency. The existing updater rejects the
   currently observed nonempty Viewer tables and is not an approved copier or
   migration tool for this state.
6. Start a verified immutable candidate under the new owner and keep ingress
   closed until health, authority, state, node routing and bounded synthetic
   checks succeed. Persist each intent and observed outcome. Lost acknowledgments
   or uncertain ownership hold the system closed for inspection.
7. Retain old artifacts and backups. Before the candidate has made any new
   writes, a separately verified reversal may restore the exact old owner and
   runtime configuration. After new writes or migration, preserve the new state
   and use a reviewed forward recovery/reconciliation; never blindly replace
   the state with an older backup and discard accepted jobs.

This cutover requires root or equivalent Docker, systemd and firewall rights.
It will temporarily stop new admissions and result transfers, and may require
waiting for running jobs and downloads. A node drain does not pause an active
photogrammetry job. No TrueNAS commands or changes are included.

Remaining implementation blockers: a reviewed, installed process supervisor
and firewall transaction with restart recovery; a nonempty-state-safe,
WAL-consistent backup and upgrade; exact candidate image/runtime compatibility;
and an authenticated end-to-end small-job validation. No apply or revert
script is supplied until those components can enforce their preconditions.
