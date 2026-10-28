# One-tick update service

`processing/update_service.py` is a bounded controller around the actual existing
`UpdateSupervisor`. It installs no timer, daemon, SSH connection, job dispatcher
or runtime update. An external reviewed CLI/timer may invoke one tick; its
installation and live activation are separate work. This module adds no
processing CPU, RAM, PID or thread limits.

## Construction and ownership

The caller must already hold the actual supervisor context and retain it through
the tick. Construct:

```python
service = UpdateService(
    state_root / "service.sqlite", supervisor=supervisor,
    config_id=reviewed_config_id, code_binding_sha256=installed_code_sha256,
    capacity_admitter=admit, capacity_releaser=capacity.release_staged,
    idle_probe=idle, can_schedule=may_schedule,
    discovery_interval_seconds=21600, error_cooldown_seconds=300,
    max_transitions=8)
result = service.tick()
```

`config_id` identifies the complete fixed composition, including callback and
capacity policies; change it for a reviewed configuration change. The private
FULL SQLite binding also pins installed code SHA, actual supervisor/registry
paths, frozen supervisor policy, state-root identity, scan/cooldown intervals and
transition budget. Existing supervisor paths, seals, initial policy and adopted
current-image history remain unchanged. The module never resets an adopted
image from `SupervisorPolicy.current_image_id`: that is only the original
baseline. Existing completed operations are history, not pending work.

Only one unresolved operation may be returned by the new public
`UpdateSupervisor.pending()`. It validates the exact existing row and sealed
receipt chain under the supervisor's owner lease. Multiple unresolved rows,
changed binding/code/root, a fork-inherited owner, or an unavailable supervisor
lease fail closed. Concurrent ticks serialize under the supervisor mutex;
same-thread reentrant ticks refuse progress. This is not another scheduler or
execution admission system.

## Fixed callback responsibilities

- `idle_probe(validated_status) -> bool` performs trusted read-only current-host
  admission checks before stage or qualification. A false result waits before
  any corresponding supervisor intent. A point-in-time idle check is not a
  held global/physical execution lease. A future heavy-processing qualifier
  still needs its own production-compatible admission; this service does not
  implement or claim it.
- `capacity_admitter(candidate, registry) -> bool` preflights capacity **before**
  `advance(discovered)` can commit `staging_intent`. False is ordinary
  `waiting_capacity`, keeping the operation discovered without a pull intent.
  A true admission must be a durable, idempotent reservation used by the same
  registry stage callback. A later race/error stays held; it does not authorize
  an unstaged pull retry. Definite cached-image reuse needs an explicit safe
  proof in this callback, not guessing from a tag or config digest.
- `capacity_releaser(candidate, registry) -> bool` is the canonical capacity
  component's `release_staged`. It must verify actual definite staged proof.
  False is an explicitly verified cached/no-reservation no-op; true is a durable
  release. Exceptions or non-booleans stop progress. The service journals its
  idempotent release checkpoint; a lost metadata response may be retried, but
  pull/qualification/schedule/run mutations may not.
- `can_schedule(validated_status) -> bool` reads the sole coordinator's current
  exact node/maintenance state. Manual/foreign scheduled, draining, updating or
  held plans cause a wait before any schedule intent. It never cancels,
  replaces, adopts or reopens another plan. The maintenance adapter still owns
  final CAS/plan validation against a race after this read.

All callbacks are trusted local dependencies fixed by the reviewed composition,
not remote submitted programs. Their errors and false/ambiguous results do not
weaken supervisor seals. The runtime wrapper must also verify the canonical
capacity component's configured legacy-book closure before normal operation;
explicit migration/bootstrap reconciliation is separate from a normal tick.

## Tick behavior

Pending work always takes precedence over discovery. A pending `*_intent` gets
one read-only supervisor reconciliation attempt and the tick ends, even if that
attempt succeeds. Unknown results stay held. No timer tick repeats a possibly
issued pull, qualification, schedule or run.

Normal states may advance at most eight transitions, with a configurable lower
budget, and at most one release cycle per tick. The service waits when idle or
scheduling permission is false. Scheduled work uses the supervisor's existing
`ready_to_run`; an unchanged scheduled state returns `waiting_run` without a
loop or `run_intent`. Completion ends the tick immediately. It never discovers
another candidate after completing a cycle. Durable supervisor generation and
current-image adoption support repeated A→B→C→A→B cycles without replaying an
old completed operation.

After a definite stage, including successful stage-intent reconciliation, the
service calls the capacity releaser before qualification. It also does so when
a transition budget ends at staged. Unknown staging never releases capacity.
Capacity metadata release is recorded once per exact operation/candidate;
callback retry is limited to this explicitly idempotent metadata operation.

With no pending operation, discovery is due only at its durable next timestamp.
The default is six hours; ordinary minute ticks before that timestamp perform
only private state reads/writes and no registry/Docker/network/callback calls.
Before discovery, a FULL committed intent reserves a five-minute cooldown. A
network error, lost response or process exit cannot trigger a tight retry loop.
Success sets the next scan from completion time. A pending supervisor row can
recover a lost service discovery checkpoint without another external scan.

Clock observations commit a durable nondecreasing high-water mark independently
before external work. Backward wall-clock changes cannot revive an expired
throttle; forward jumps permit only one due scan, never a catch-up burst. There
are no sleep loops or background polling in the service.

`tick()` returns bounded `{status, operationId, state, transitions,
nextDiscoveryAt}` metadata. Normal statuses include `not_due`, `no_change`,
`waiting_idle`, `waiting_capacity`, `waiting_schedule`, `waiting_run`,
`transition_budget`, `reconciled`, `completed`, `discovery_cooldown`, `held`, and
`capacity_release_held`. Invalid state/ownership raises sanitized `ServiceHeld`.
No Docker stderr, API body, credential, private update token or exception string
is copied into these results.

## Evidence scope

Tests use actual supervisor/registry SQLite journals and locks, fake Docker and
durable adapter boundaries, plus a real synthetic coordinator HTTP/SQLite,
actual startup qualifier and managed-maintenance pipeline composition. They
exercise waits, foreign plans, future windows, low-capacity recovery, immutable
intent reconciliation, discovery failure/restart/clock rollback, bounded ticks,
repeated cycles and owner/reentrancy/fork guards. They do not access NAS, run
real imagery, install systemd/timers, execute SSH, or change any live container.
