# Inactive cooperative cross-filesystem admission experiment

This candidate is disabled and is not connected to the Viewer, legacy scheduler,
reference driver, NAS, or NodeODM. It does not reserve physical extents or set a
filesystem quota. Never use its counters as a guarantee against other writers or
unknown engine scratch consumption.

One local process namespace groups caller-supplied worst-case component budgets
by observed `st_dev`; each distinct device must use a single exact ScratchLedger.
The common journal records deterministic attempt and per-device reservation IDs.
Lost acknowledgements may replay the exact reservation. Partial acquisitions are
retained; each acquired reservation is marked uncertain to avoid automatic TTL
expiry. There is deliberately no release or production recovery protocol.

Seven small tests use independent retained `/tmp` and `/dev/shm` roots and mock
capacity values; they cover same-device grouping, same-journal competing attempts,
expiry before pin, lost reservation acknowledgement, low space, alternate ledger
rejection and a replaced journal after construction. This is not a process-kill,
power-loss, real mount replacement, NAS or physical space guarantee test.

Qualification blockers:

* A per-user Path.home journal is not a host-global authority. Direct ScratchLedger
  users, other OS accounts or namespaces, old versions and external software can
  bypass it. A trusted, provisioned, host-owned canonical authority and exclusive
  integration across all writers is needed.
* Journal inode lstat checks are not atomic SQLite fd binding; directory/mount swaps
  and concurrent path replacement remain possible. A newly created instance has no
  trusted prior inode witness and can accept a replacement. Root paths and mounts
  require pinned identities and tested recovery before qualification.
* Each ledger probes `disk_usage` once; physical free space can change immediately.
  Budgets are estimates and do not enforce NodeODM peak scratch or prevent external
  consumers. Cross-host capacity requires an authenticated per-host authority; an
  SMB mount's observed `st_dev` does not identify remote pool ownership.
* Scratch holds are non-expiring uncertain state without a recovery/release flow.
  Prove idempotent release only with exact writer termination, retained-source and
  downstream custody evidence; never delete files or clear holds merely to unblock.
* Before worker integration, unify these component reservations with the existing
  driver reservation to prevent double charging or unbudgeted same-device copies;
  include Viewer/NAS input, worker staging, NodeODM input/working/output, archive
  and Viewer ingest on their actual filesystems. Persist closed per-attempt policies
  and verify matching generation, environment and manifest in each authority.
* Full 10k input support needs bounded authenticated manifest/inventory transport,
  correct multipart limits, per-volume safety floors, held partial-admission crash
  recovery, external-writer and real power-loss tests. Actual owner-controlled
  TrueNAS configuration, quota policy and coordinated rollout remain separate.

## Disabled local authority wrapper

`LocalCapacityAuthority` requires an already provisioned owner-private binding
file naming a fixed authority ID, existing journal inode/device, and allowed
ledger roots, database inode/devices and floors. It holds `DaemonStateLock`
for its lifetime and rechecks observed binding and inode identities before and
after admission. It is disabled and has no running service or production hook.

A bounded Linux synthetic suite checks exclusive ownership across processes,
identity-change refusal, lock release, idempotent admission and disabled startup.
These observations do not atomically bind SQLite or fence path/mount swaps; a
host-wide provisioned service identity, cross-user protection, root/mount pinning,
authenticated NAS/worker RPC and process-crash recovery are absent. Direct
ScratchLedger and noncooperating Docker writers can bypass this wrapper. Existing
cooperative budgets do not reserve disk blocks or enforce quota. Do not enable
without verified writer termination, downstream custody, release/reconciliation,
space policy and physical-store qualification.

## Disabled local peer protocol

The source-only `LocalCapacityProtocol` handles one caller-owned AF_UNIX socket
and checks Linux SO_PEERCRED against a configured UID. Request bodies contain
only an operation and a preloaded fixed plan ID; callers cannot supply budgets.
`reserve` and `pin` use the existing owner wrapper and journal; `status` reports
only recorded state and is not proof that capacity remains available or that a
caller holds authority. Unknown plans and peers are refused. This authenticates
a cooperating local OS account, not a specific process; every process sharing
that UID can act. It is not mTLS, not a remote protocol and installs no listener.

An absolute monotonic deadline bounds frame recv and send, including slow input.
It does not bound the trusted plan, SQLite lock or filesystem operation; those
may run past the deadline, and an admitted mutation may have committed before a
late response is lost. Retry must use the same exact persisted plan and reconcile
on uncertainty. Tests use owned socket pairs and close both ends; no remote calls
or listener are started. They do not prove process-crash recovery, atomic mount
identity, host-wide enforcement, physical allocation, or durable remote delivery.
