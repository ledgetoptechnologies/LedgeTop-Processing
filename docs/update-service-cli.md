# Host-local update service composition

`python -B -m ops.update_service {bootstrap|status|tick} --config <private-json>`
runs on the configured Linux processing PC as its existing `bkoltz` UID1000
account. The supported hosts are main (`node`) and cluster (`cluster`). It changes
processing-engine containers only through the existing managed maintenance
adapter. It has no OS, reboot, prune, coordinator replacement or credential
provisioning path. No CPU, RAM, PID, GPU or image-count override is introduced.

The configuration is a complete closed JSON object containing:

- `schemaVersion: 1`, a fixed `serviceId`, and reviewed `codeBindingSha256`.
- `registry`, `qualification`, and `supervisor`, each with an explicit
  `statePath` and full dataclass `policy` object. Defaults must be written out.
- `maintenance`, with the existing `managedRoot`, adapter `statePath`,
  `receiptRoot`, and complete `ScheduleWindow` object.
- `capacity.policy`, the complete canonical `CapacityPolicy`, including an
  explicit sorted `legacy_capacity_journals` list, even when empty.
- `service`, with `statePath`, `discoveryIntervalSeconds`,
  `errorCooldownSeconds`, and `maxTransitions`.

The code hash covers all executable Python sources under `processing/` and
`ops/`, including the CLI and unit renderer. Use a byte-identical immutable
source bundle on the host. Source or complete-configuration changes do not
silently rebind existing service history. Tokens do not belong in JSON: the
existing private `managedRoot/maintenance.env` supplies exactly the API and
operator roles. Both clients use the same verified loopback coordinator origin
from the existing private maintenance settings. Docker keeps using its existing
credential setup. No token is printed or copied into a receipt or unit.

Keep the existing registry/qualifier/supervisor/adapter policies and state paths
when resuming a previously accepted pipeline. Their original baseline image is
not the current adopted image: the supervisor's durable current-image record
remains authoritative across generations. The new capacity and service policies
have separate bindings. The CLI checks cross-component node, authority, engine,
paths, channel, capacity and qualification hashes before executing a tick.
State files must be distinct from one another and their lock/SQLite sidecars,
outside managed configuration, executable sources and the entire canonical
image-staging namespace, including other Engine directories. The service journal
also refuses an existing SQLite database containing another component's tables.

## Bootstrap and operation

Main-node runtime acceptance on 2026-10-28 is recorded in
[the installation receipt](evidence/main-update-service-20261028.json): frozen
source `020c15c`, validated legacy-book closure, unchanged current image, one
no-change discovery, throttled later ticks, and enabled deterministic user timer.
The cluster node's later [runtime acceptance](evidence/cluster-update-service-20261028.json)
records its actual immutable-image replacement and enabled user timer. Cluster
retains the existing NodeODM GPU channel and 1,000-photo cap; main retains NodeODX.
The cluster update preserved the original baseline policy and adopted the new
image in the journal. It enabled linger for the existing `bkoltz` account so its
user timer survives logout. No NAS or OS update occurred. This acceptance does not
prove NAS delivery, real processing, or a future image update.

As `bkoltz` on main, inspect the installed timer with
`systemctl --user status ledgetop-processing-image-updates.timer` and recent runs
with `journalctl --user -u ledgetop-processing-image-updates.service -n 30`.
`systemctl --user disable --now ledgetop-processing-image-updates.timer` stops
future timer invocations; it does not cancel an already running update. Preserve
its journals and inspect an in-progress service before taking further action.

Run `bootstrap` explicitly before normal service use. It reconciles every
configured legacy capacity book against actual definite cached-artifact proof,
records closure in the canonical per-Engine budget and leaves the old books and
registry unchanged. Unknown/orphan/mismatched old reservations block bootstrap.
Normal `status` and `tick` verify those closures; a newly empty budget cannot
silently discard old uncertainty.

`status` reports the validated pending operation and actual managed-host image.
It does not discover a release or advance an update. `tick` invokes the bounded
controller described in [the service protocol](update-service.md). A cached
artifact needs a real read-only registry proof before bypassing new capacity;
only typed definite absence may request a new reservation. Low space waits before
a staging intent. Busy processing, existing reservations, and foreign/manual
maintenance plans wait before the corresponding intents. An unprovisioned gate
is rejected without creating it.

Idle probes hold the existing canonical gate only while inspecting the exact
binding and backend. They are point-in-time checks, not a distributed lock across
staging or qualification. The configured qualifier is startup/API/options-only;
a future heavy processing qualifier must provide compatible execution admission.
The managed run still owns its exact coordinator plan and physical gate through
replacement/finalization. Existing unknown mutation intents are reconciled by
read-only lookup, never replayed by the timer.

Outputs contain bounded status metadata. Held outcomes exit2 so the user service
reports failure; ordinary waits and completed/throttled ticks exit0. Raw API
bodies, Docker errors, exception text and credentials are excluded. Current HTTP
clients use socket-idle timeouts; foreground invocation should have an external
wall deadline. The rendered user service supplies a two-hour overall limit. A
timeout preserves the durable intent and does not authorize another mutation.

## Deterministic user units

`ops.update_service_units.render_units(source_root, config_path, node_id=...)`
returns literal `.service` and `.timer` text. It installs or starts nothing.
Source/config paths reject shell/control text and systemd specifiers. The main
unit requires the existing coordinator SSH-tunnel user service; cluster uses its
local coordinator directly. The unit executes one tick with private umask,
without resource overrides or an automatic process restart.

The timer wakes one minute after the prior invocation ends and five minutes after
boot. Durable service state limits ordinary registry discovery to the configured
six-hour interval; pending maintenance waits can resume on a later minute tick.
Successful completion ends that tick, and no second release cycle starts in it.
This is deterministic software supervision and uses no AI agent or inbox monitor.

Before enabling a rendered timer, verify the exact staged source/config and live
host, bootstrap legacy accounting, run a bounded foreground tick, verify its
result and preserved journals, and validate the units with the host's actual
systemd parser. Runtime installation evidence is separate from source tests.
