# Container maintenance with node draining

This prototype can fence a particular processing node before a container update.
OS package updates and PC reboots are excluded. It keeps the existing single
global execution slot; node identity does not yet create a parallel fleet scheduler.

## Scheduling

`PROCESSING_PROTOTYPE_NODE_IDS=cluster,main` declares logical nodes. Default is
`cluster`; that legacy ID must remain configured for old claims and migration.
Workers/dispatchers use `--node-id` (default `cluster`). The new worker/dispatcher
must run with the updated coordinator: an older API lacking node admission is
rejected rather than silently bypassed.

Read `GET /v1/prototype/nodes` or `/v1/prototype/nodes/cluster` using the worker API
credential. Schedule through `POST /v1/prototype/nodes/cluster/schedule` using the
separate operator credential:

```json
{
  "expectedRevision": 0,
  "drainAt": "2026-10-25T05:30:00Z",
  "notBefore": "2026-10-25T06:00:00Z",
  "afterJobId": null,
  "targetImage": "sha256:64_lowercase_hex_image_id"
}
```

The values are examples, not an installed maintenance window. Read the actual
revision first; stale revisions conflict. Times require explicit UTC `Z` or
`+00:00` to avoid local-time/DST ambiguity. Convert the intended local time
explicitly. `notBefore` cannot precede `drainAt`.

At the drain cutoff, that node cannot claim another job, including production.
The cutoff check and claim share the same SQLite transaction. Existing work
continues. For "after this job," supply the exact job currently running on this
node; admission closes immediately and the job finishes normally. Arbitrary
future queued job IDs are not accepted by this first version.

`notBefore` is the earliest update start, not a promise that work finishes by
then. Start draining early enough for long jobs. There is no reliable runtime
estimator and no forced termination to meet a window. Held or unknown execution
blocks an update until reconciled; low CPU usage proves nothing about safe idle.

## States and operator API

| State | New claims | Meaning |
| --- | --- | --- |
| active | allowed | No maintenance gate |
| scheduled | until cutoff | Stored future drain |
| draining | refused | Waiting for work and earliest update time |
| updating | refused | Exclusive update token issued |
| failed | refused | Candidate failed, even if rollback became healthy |
| held | refused | Coordinator restarted during an uncertain update |

Public node records contain revision, plan ID, cutoff, earliest start, optional
current job, target pin and compact result. Update tokens are returned only by
`begin` and stored hashed in SQLite; public reads never expose them. Job records
have `nodeId=null` while queued and the actual node once claimed. Legacy running
rows without identity migrate conservatively to held work on `cluster`.

All mutations below require the distinct operator credential:

| POST suffix on `/v1/prototype/nodes/<id>/` | Exact payload |
| --- | --- |
| schedule | example fields above |
| cancel | `{expectedRevision,planId}`; only scheduled/draining |
| begin | `{expectedRevision,planId}`; returns `{node,updateToken}` only when drained, due and no running/held job on the node |
| finish | `{planId,updateToken,status,result}`; success requires `result.healthy=true` |
| resume | `{expectedRevision,planId,note,workerStopped:true,healthy:true}`; failed/held only, no unresolved active job |

A coordinator restart changes updating to held and invalidates its token. It
never guesses that replacement or rollback finished. Identical finish replay is
idempotent; it is not permission to repeat Docker replacement. Operator resume
attestations require real process and health checks, not just checking boxes.

## Host update runner

`python -m processing.container_update` is a bounded host command for one
NodeODX Compose service. The coordinator gets no Docker socket. It supports
local Unix/npipe Docker endpoints, verifies the pinned Engine identity and exact
Compose project/service labels, rejects hooks/replicas, and checks that the probe
origin is the exact private published port of that inspected container.

The logical node-to-host/Engine/project/service association is trusted operator
configuration, not an enforced fleet registry or global host reservation. Do not
map two node IDs/controllers to the same service. Existing ClusterODX/direct
clients must be fenced before claiming exclusive admission; an empty probe alone
cannot exclude a new request racing from another client.

```sh
python -m processing.container_update \
  --coordinator http://127.0.0.1:18080 --node-id cluster \
  --node-origin http://127.0.0.1:13000 --node-port 3000 \
  --compose-file /private/node/compose.yaml --project processing-node --service nodeodx \
  --docker-engine-id VERIFIED_LOCAL_ENGINE_ID \
  --work /private/new-maintenance-plan-receipt \
  --exclusive-admission --wait-seconds 86400 --health-seconds 30
```

The runner needs the worker API credential plus the separate maintenance operator
credential in environment variables; never put secrets in arguments or Git.
Optional `NODEODX_API_TOKEN` uses the engine's query authentication. Require literal
127.0.0.1/::1 for its bound probe origin. The receipt directory must be private,
existing and new for this attempt. Receipts contain secrets. Use reviewed,
operator-owned Compose sources, not paths/commands submitted by jobs.

The runner waits up to the given budget for draining, the earliest start,
coordinator idle, empty actual task inventory, empty `/var/www/tmp` uploads and
an exact-container process census. Only the NodeODX server, optional shell
wrapper and init are accepted; remaining engine children delay maintenance even
if cancellation/removal already changed task status. This image-specific census
must be revalidated when adopting a different image layout.

Images must already exist locally as immutable image IDs. This version does not
pull a moving `latest` tag or download images during execution. Stage and verify
candidate images separately under the node's storage/capacity policy. Replacement
uses only the selected service with `--no-deps --no-build --pull never`; volumes
and unrelated services remain in place. A cold image load and sparse-SSD impact
are outside the small fixture's performance evidence.

Intent is fsynced before reservation and each Docker mutation. A lost/unknown
Docker mutation response leaves the gate and receipt for inspection, avoiding a
rollback racing a daemon still acting. A definite unhealthy candidate restores
the pinned previous image and checks it independently; the node remains failed
until operator review. Healthy candidate completion returns the node to active.
Health polling budgets are soft: bounded individual Docker/HTTP requests can
extend the total elapsed time. They never justify killing a live processing job.

Existing receipts block automatic replay, even after a successful run; create a
new receipt directory for a new plan. On interruption, inspect exact service,
Engine, worker processes, image, queue and health before operator recovery.
Uncertain coordinator finish can mean the node already became active, so read
back the exact plan/result rather than repeating container actions.

No background service, host reboot or live maintenance window is installed
automatically. The foreground runner must be supervised for its wait period;
if no runner is operating, draining still fences claims but no update occurs.
A permanent node agent/service and registry remain separate deployment work.

## Acceptance

Run `python -m unittest discover -v` plus the worker tests. For a disposable real
Compose replacement/rollback check, run `python scripts/maintenance_smoke.py
/private/new-evidence.json`. It uses a Python NodeODX API fixture, not an actual
NodeODX release or dataset. It verifies drain/idle, healthy replacement, failed
health with pinned rollback, blocked capacity, an unchanged sibling container and
preserved volume marker, then removes its owned fixture resources. It builds
only tiny fixture layers on an already-cached prototype base image.

Before activating updates on real nodes, establish exclusive admission and
approved image pins, validate actual NodeODX process/upload layout, accepted
artifacts/backups and the node's physical ownership/capacity. Existing production
routes and services are untouched by this implementation.
