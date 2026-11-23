# Main node workflow and container maintenance

October 27, 2026 acceptance used the existing cluster coordinator and authority
`7131882e-38ce-4d3e-99cc-4ba5a94d2ee8`. Main is `192.168.50.88`, cluster is
`192.168.50.89`. No second scheduler was installed. The coordinator still admits
one job globally, with production ahead of queued staging and no preemption.

## Installed layout

Main's `ledgetop-processing-tunnel.service` is an enabled systemd **user** service.
The already-enabled user linger allows it to start without an interactive login.
It forwards main `127.0.0.1:18080` directly to cluster `127.0.0.1:18080`, using a
dedicated key, pinned host key and a server-side destination restriction.
It reconnects after transport failure. A service restart recovered the same
authority in acceptance; a host reboot has not been tested. The Windows relay
is no longer needed. No unattended worker or AI inbox polling was installed.

Main's operator root is `/home/bkoltz/.local/share/ledgetop-processing-node`:

- `compose.json`: authoritative private Compose source for project
  `ledgetop-main-processing`, services `nodeodx` and `ingress`.
- `maintenance.json`: non-secret node/authority transport configuration.
- `maintenance.env`: separate worker and operator credentials, mode 0600.
- `ingress/`: private gate state and read-only nginx configuration directory.
- `source/`: installed source for the host commands.
- `original-compose.json`, `bootstrap.json`, `data-backup/`: private bootstrap
  rollback evidence and the original two-byte placeholder data backup.

The original root-owned `/dockerfiles/nodeodm/docker-compose.yml` remains
historical. Do not use it for future managed starts: it selects the old moving
tag and conflicts with the ingress port. The old container
`nodeodm-node-odm-1` is manually stopped and retained, with its original image.
Its original network endpoint was disconnected during the idle bootstrap.
Rollback requires inspecting the private bootstrap record, stopping the new
services and reconnecting the original network before starting that container.
Never run both backends against the same data directory.

The existing LAN endpoint `.88:3004` is preserved by the ingress container.
NodeODX itself binds only `127.0.0.1:13004`; `/dockerfiles/nodeodm/data` stays
mounted at `/var/www/data`. Both new services use `unless-stopped` restart.
Nginx is pinned, non-root, read-only, without capabilities or a Docker socket,
limited to 0.25 CPU / 64 MiB. Request access/error logs are suppressed because
NodeODX can carry credentials in query strings. Health and operation receipts
provide the maintenance diagnostics.

## Schedule and run

Stage and inspect the intended image separately. Only cached immutable image IDs
are accepted; `schedule` and `run` never pull or decide to upgrade. Automatic
upgrades are the separate `auto-deploy` step below, which uses this same path.
Use the installed host command as `bkoltz`:

```sh
ROOT=/home/bkoltz/.local/share/ledgetop-processing-node
python3 "$ROOT/source/ops/node_maintenance.py" \
  --root "$ROOT" --credentials "$ROOT/maintenance.env" status
```

For a new plan, replace the image and UTC times below with reviewed values:

```sh
python3 "$ROOT/source/ops/node_maintenance.py" \
  --root "$ROOT" --credentials "$ROOT/maintenance.env" schedule \
  --target-image sha256:VERIFIED_CACHED_IMAGE_ID \
  --drain-at 2026-11-01T05:00:00Z --not-before 2026-11-01T06:00:00Z

mkdir -m 700 "$ROOT/new-private-receipt"
python3 "$ROOT/source/ops/node_maintenance.py" \
  --root "$ROOT" --credentials "$ROOT/maintenance.env" run \
  --receipt "$ROOT/new-private-receipt" --wait-seconds 86400
```

`--after-job-id` can select the exact currently running coordinator job instead
of allowing more claims before the cutoff. See [maintenance](maintenance.md)
for the full plan semantics. The foreground runner can be supervised in a
separately named systemd user unit for the planned window; no recurring update
timer or new future window was activated during this acceptance.

At the cutoff, the coordinator stops new claims on main. At the earliest update
time, the runner gracefully reloads nginx into maintenance mode, waits for old
workers/connections to finish, waits for the selected node's running/held claims
and genuinely busy host lock, then validates the same frozen plan. It verifies
the exact Engine, Compose service, private port, empty task/upload inventory and
process census before replacement. The sole allowed temporary-file placeholder
is the regular one-byte `.gitignore` containing `*`; other files or symlinks block.

The host gate stays locked through replacement and reopening. Success persists
the immutable image pin in `compose.json`, independently verifies the private
backend image/health, and reopens ingress. A failed candidate attempts the
existing pinned rollback and leaves coordinator admission failed and ingress
closed for inspection. No OS update or PC reboot is performed.

## Automatic updates

`auto-deploy` makes a verified main merge reach a host without a manual step,
through the same drain window. It is opt-in per service: `maintenance.json`
declares the service `kind` and an `autoDeploy` object naming that kind's
package (see [service kinds](#service-kinds)), for example
`"kind": "coordinator", "autoDeploy": {"package": "coordinator", "drainDelaySeconds": 0}`.
The first opt-in of a service is the reviewed [first adoption](#first-adoption)
command, not a hand edit. Run the agent from the host's own timer (for example
a systemd user timer every 10 minutes starting a oneshot unit):

```sh
python3 "$ROOT/source/ops/node_maintenance.py"   --root "$ROOT" --credentials "$ROOT/maintenance.env" auto-deploy --wait-seconds 3600
```

Each pass, in order:

1. If `$ROOT/auto-deploy.hold` exists, it records `paused` and stops before
   any network or Docker call. A `kind` that is missing, unknown or not the
   package's own kind (`coordinator` for the coordinator package, `treedar` for
   the TreeDar package) is `refused` before any pull. `touch` it to hold, delete it to resume. A hold
   does not cancel a window already scheduled; use the coordinator `cancel`
   action for that (the agent then records `cancelled` and never retries it).
2. A failed, held or updating node is left alone (`not_eligible`). So is any
   scheduled or draining plan the agent did not create and record, even one
   for the same image (`other_plan`).
3. It pulls the package's `verified` tag only to learn its digest, then pulls
   `repository@sha256:<digest>` with the host's existing read-only registry
   login and inspects it. The image ID must match the tag's, the digest must be
   on the pulled image, platform must be linux/amd64, user `10001:10001`, and
   the labels must name this repository, this package, a 40-hex commit and a
   publish run. Anything else is `refused`. A non-`push` or non-main label is
   `ignored`.
4. If that image is already the pin, `up_to_date`. The running pinned image
   must itself carry this package's labels, or be the exact pin a person
   recorded with the [first adoption](#first-adoption) command
   (`auto-deploy/adoption.json`); otherwise `refused` ("adopt it manually
   first"). The agent never adopts a service itself. The candidate's publish
   run must be newer than the pin's (0 for an unlabelled adopted pin) and than
   every candidate this host already tried (`highestRunId`), so it never goes
   backwards or retries a failure (`not_newer`).
5. It creates the window (`drainAt` = `notBefore` = now + `drainDelaySeconds`)
   with the coordinator's idempotent `schedule-owned` request, a compare-and-swap
   on the node revision it just read. The request, with its `requestId`, is
   saved in `$ROOT/auto-deploy/schedule-intent.json` before it is sent, and that
   file is removed only when the request's receipt is bound or the request was
   definitely refused; on a refusal `refused.json` is written (atomic, fsynced)
   before the intent is removed, and if that write fails the intent stays and
   the pass returns `held`, `refusal_latch_unsaved`. The client tells a definite answer (`HTTPRefused`, an
   HTTP status and sanitized error code) from an uncertain one (`HTTPUncertain`:
   no complete response). A 3xx/4xx answer means nothing was created: the agent
   writes `$ROOT/auto-deploy/refused.json` and sends no schedule request on any
   later pass (`held`, `refusal_latched`) until a person inspects the node and
   deletes that file. A 5xx, a lost or unreadable response is uncertain: the
   schedule request is never sent again, and later passes only read
   `GET .../schedule-owned/<requestId>`.
   - A receipt binds the plan, even one that committed late; it is never
     treated as another operator's plan.
   - A 404 with the exact `owned_schedule_not_found` code, with the coordinator
     authority equal to the saved request's before and after the lookup, is
     only a point-in-time absence: a timed-out request can still commit. The
     agent keeps the intent, marks it `unresolved` and returns `held`,
     `schedule_unresolved`. It keeps reading on later passes and never clears
     the intent or sends a new request itself. Before deleting
     `auto-deploy/schedule-intent.json` to retry, an operator must confirm the
     original request has settled (the coordinator request timeout has passed,
     or its logs show the request finished) and that no receipt or plan exists
     for it. A momentary absence is not enough.
   - A generic or `prototype_disabled` 404, a 409 authority conflict, a changed
     authority or an uncertain lookup holds (`ownership_unproven`).
   - A receipt whose request ID, node, authority, expected revision, target or
     plan ID does not match the saved request latches `refused.json`.
6. It records the returned plan ID and target and calls `run` with both under a
   private receipt in `$ROOT/auto-deploy/receipts/`. `run` then requires that
   exact plan and target on every poll and again after closing ingress and in
   the update itself; any other plan stops before ingress or the container is
   touched. A `waiting` result resumes the same plan on the next pass. A plan
   cancelled or replaced by someone else is recorded as `cancelled` and never
   retried. A failed candidate is rolled back by `run` exactly as above and
   recorded in `state.json` as `failed`.

`$ROOT/auto-deploy/state.json` keeps `inProgress` (with its plan
ID), `applied`, `failed`, `cancelled` and `highestRunId`; `last-run.json` records every pass. A flock on
`agent.lock` keeps timer passes from overlapping. The exit status is 0 for
normal outcomes, 1 for a failed (rolled back) update and 2 for `refused`,
`held` or a stop that needs inspection; existing receipts are never replayed.

### Service kinds

`maintenance.json` `kind` selects the runner adapter
(`processing/service_adapters.py`). Every adapter answers the same questions
for `processing.container_update.execute`: which image is selected, is it
cached, is the service drained (idle), swap it, is it healthy; rollback is the
runner's own `replace(old)` plus the old image's health check. The plan,
`begin`, `finish`, receipts and no-replay rules are identical for all kinds.

| kind | idle | healthy after swap | rollback |
| --- | --- | --- | --- |
| `odx` (default when `kind` is absent) | ingress closed and drained, host engine gate, no tasks/uploads/extra processes, engine `/info` | exact image, Docker health, engine `/info`, idle again | recreate the old pin |
| `coordinator` | its own `GET /v1/prototype/unsettled` counts no `queued`, `running` or `held` job on any node (`cancelled`, `succeeded`, `failed` are settled) | exact container image, Docker health present and `healthy`, `/healthz` `ok`, unchanged authority ID, its self-update reservation and fence survived the restart, and a new image carries this repository's `coordinator` package labels | recreate the old pin (no labels needed) |
| `treedar` | no running container of the Compose service or of the pinned image | `treedar --version` succeeds in a network-less, read-only, capability-free container of that image (it imports the CLI and its native stack), and a new image carries the `treedar` package labels | keep the old pin (no container is recreated) |

- `odx` is the NodeODX/NodeODM path above, byte for byte. There is no
  published odx package, so an odx service is never an auto-deploy target;
  manual `schedule`/`run` are unchanged.
- `coordinator` and `treedar` have no engine ingress or host engine gate. They
  need `dockerEngineId` (the host's `docker info` ID) in `maintenance.json`;
  `containerPort` (default 8080) is the coordinator's container port, which
  Compose must publish only on `127.0.0.1` at the port of `coordinator`
  (`http://127.0.0.1:18080`). `nodeId` is the coordinator node whose plan
  carries the window, normally the host's own node. The coordinator's node
  plan only fences claims on that node; the idle check waits for every job on
  every node to settle, so a backlog or a held job delays the update
  (`waiting`, resumed on the next pass) rather than interrupting work. A held
  job needs a person.

### Coordinator self-update

The coordinator restarts itself in the middle of its own update. Starting a
coordinator normally turns an `updating` node into `held` (its update token is
dropped) and every `running` job into `held`. A coordinator-kind update
therefore reserves with `begin` and `"selfUpdate": true`, which:

- atomically refuses (409) while any job on any node is `queued`, `running` or
  `held`;
- marks the reservation as a self-update. At startup such a node stays
  `updating` with its token, so the updater's `finish` (success, or failure
  after rollback) is accepted by the restarted coordinator. Engine updates are
  still held at startup exactly as before;
- fences every claim on every node (prototype and reference claims return no
  job) while that node is `updating`. Jobs submitted meanwhile stay `queued`
  and are claimable after `finish`, or after an operator `hold`, which also
  lifts the fence. So no job can start between the idle proof and `compose
  up`, and the restart has no running job to hold.

After the swap, health requires the reservation and fence to still be present.
`GET /v1/prototype/unsettled` (observer token) returns
`{"schemaVersion": 1, "jobs": {"queued", "running", "held"}, "unsettledJobs",
"selfUpdateFence"}`, a bounded answer independent of job history size.

A coordinator image without this API (earlier than this change) cannot survive
its own update and is refused by the idle check and by `adopt`. Install the
first coordinator image that has it manually (idle, `compose up` of the new
pin, update the `compose.json` pin), then adopt it. From then on, the rollback
target is always a coordinator that supports self-update.

### Coordinator database compatibility

Rollback recreates the old coordinator on the same `jobs.sqlite3`; it is not
snapshotted or restored. Instead, migrations must stay forward compatible, and
this is enforced:

- `processing/prototype.py` declares `SCHEMA_READER` (the level this code
  reads correctly) and `SCHEMA_MIN_READER` (the oldest level that may open a
  database this code migrated). The database keeps the highest
  `min_reader` it has seen (`coordinator_schema`).
- A coordinator whose `SCHEMA_READER` is below the stored `min_reader` refuses
  to start. A rollback across a non-compatible migration therefore fails its
  health check and stops for a person (`rollbackHealthy: false`) instead of
  running on a database it misreads.
- Additive migrations (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN` with a
  default, like `nodes.self_update`) keep both at their current value. A test
  fails on any `DROP`, `RENAME` or other `ALTER TABLE` in the coordinator's
  modules unless both numbers were raised.
- A TreeDar service is a Compose service that is only ever run on demand. The
  swap is the Compose pin the runner saves after the new image passed its
  self-check; containers already running keep their image.
- On success the runner saves the new pin to `compose.json` and records
  `managed-update.json` with stage `completed`; on failure the old pin stays
  and the node is left `failed` for review, as for odx.
- Any other `kind` value is refused by `run`, `schedule`, `auto-deploy` and
  `adopt`.

### First adoption

A coordinator or TreeDar service whose current pin predates the package labels
(or was never auto-deployed) is opted in once, by a person, with `adopt`. It
records the reviewed current pin and opts the service in; it never pulls,
schedules or replaces anything. First, `maintenance.json` must already describe
the service: `nodeId`, `project`, `service`, `kind`, `dockerEngineId` (and
`containerPort` if not 8080), with `compose.json` holding the current pin.
Then, as the operator user on that host:

```sh
ROOT=/path/to/that/service/operator-root
touch "$ROOT/auto-deploy.hold"                     # keep the timer idle meanwhile
python3 "$ROOT/source/ops/node_maintenance.py" \
  --root "$ROOT" --credentials "$ROOT/maintenance.env" status
python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["services"][sys.argv[2]]["image"])' \
  "$ROOT/compose.json" SERVICE                       # the current pin
docker image inspect --format '{{json .Config.Labels}}' sha256:CURRENT_PIN
python3 "$ROOT/source/ops/node_maintenance.py" \
  --root "$ROOT" --credentials "$ROOT/maintenance.env" adopt \
  --kind coordinator --image sha256:CURRENT_PIN --drain-delay-seconds 0
cat "$ROOT/auto-deploy/adoption.json" "$ROOT/maintenance.json"
rm "$ROOT/auto-deploy.hold"                          # let the timer run
```

`adopt` refuses unless: the kind is `coordinator` or `treedar` and matches any
kind already declared; the service is not already opted in; `--image` is
exactly the current `compose.json` pin and a cached image; the pin is not
labelled as the other package; the coordinator node is `active` with no plan;
there is no `refused.json`, `schedule-intent.json` or in-progress update; the
kind's adapter accepts the service configuration, the service runs that pin
and passes the kind's health check (labels not required; a coordinator must
also serve the self-update API with no update open). It then writes
`auto-deploy/adoption.json` (kind, package, node, project, service, image and
the pin's publish run if it has one) and, last, adds `kind` and `autoDeploy`
to `maintenance.json`, keeping every other setting. If it stops between the
two writes, rerunning it with the same pin completes the opt-in. The record
applies only to that exact pin, node and service: once the agent has deployed
a labelled image, later upgrades rely on the labels, and any other unlabelled
pin is refused again. To undo before the first upgrade, remove `autoDeploy`
(and `kind`, if it was added) from `maintenance.json` and delete
`adoption.json`.

## Recovery limits

The runner never kills a job to meet a maintenance time. Completed NodeODX tasks
and unfinished uploads also block replacement: their results must first be
accepted and explicitly cleaned by their actual owner. This increment does not
implement the full Viewer artifact-return pipeline or delete client tasks.

An uncertain **close** retains its closed configuration intent and does not
authorize replacement until old workers are proven drained. An uncertain
**open** can already have opened traffic while the receipt says `opening`.
Coordinator finish, source pinning and ingress reopen are separate operations;
a failure can leave coordinator admission active while ingress is closed or
uncertain. Inspect both states, exact containers, pins, claims and receipts.
Do not assume the whole sequence is atomic or replay it automatically. Existing
receipts block another run; never delete one just to bypass that guard.

Ingress controls the existing network endpoint. Host Docker administrators and
direct localhost writers remain trusted and can bypass it. Legacy direct Viewer
jobs do not yet inherit coordinator production/staging labels. Full NodeODX task
bridging, artifact delivery and new Viewer provider registration remain separate
work. No production-ready fleet concurrency claim is made by these small tests.

## Acceptance

- Updated actual main NodeODX API 2.3.1 / ODX 3.8.2 to API 2.4.0 / ODX 3.8.5.
  Cached immutable image `sha256:841177349ebcae8c467787b5a64507497560d346b964b5b157670a76f4251776`;
  RTX 5060 Ti visible; 80 live options. The cluster's existing engine was not updated.
- All 31 coordinator health samples passed during the actual version update.
  A processing node has a maintenance gap; other work/admission stays available.
- Real nginx test: a 30-byte response already in progress finished unchanged;
  new requests received 503; timeout could not attest closure or permit reopening.
  Wrong image/config mount/entrypoint/remote Engine are refused.
- Disposable maintenance fixture passed drain, healthy replacement, deliberately
  failed candidate with healthy pinned rollback, unchanged sibling and persistent
  volume marker, then removed owned containers/volumes/images. The preexisting
  base image is retained; an untagged base keeps a cache tag rather than being deleted.
- Production job `a3e0de32-c64a-43d3-99cf-ac3ae5826da5` ran on main before earlier
  staging `790a4c18-aeef-4faa-8853-6c6889fb8cf5`. Paused main refused dispatch;
  cluster then completed staging through the same coordinator. Both produced
  three trees. Exact ACKs and all 15 indexed output files per job were verified.
- Generated synthetic outputs and the two exact stopped worker containers were
  removed. Main's UID-10001 output directory required cleanup as that same
  non-root UID; no original data or unrelated resources were removed. Private
  completed receipts/dispatch records and rollback resources remain.
- The managed host command also completed an isolated same-image recreation
  against the actual main service. This verified its combined gate/update/pin/open
  path; busy/held/replay/race regressions are separately covered by Linux tests.
- Final verification includes 145 root checks and 34 worker checks on the main
  PC's Python 3.10. The first broader Linux run exposed an existing use of the
  Python 3.11-only `hashlib.file_digest` in the photo smoke helper; bounded
  streaming hashing now supports the actual host runtime. No real-photo run was
  used to test that repair. Static internal review found no remaining blockers.

Sanitized evidence: [version update](evidence/main-container-update-20261027.json),
[ingress drain](evidence/ingress-drain-20261027.json),
[main fixture](evidence/maintenance-workflow-main-20261027.json),
[cluster fixture](evidence/maintenance-workflow-cluster-20261027.json).
The [replacement/rollback fixture](evidence/maintenance-fixture-20261027.json)
also records retention of the preexisting cached base image.
No real photo dataset was processed. ARM, Ollama and node exporter were preserved;
DCGM exporter remains in its preexisting restart loop.
