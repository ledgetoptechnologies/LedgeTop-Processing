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
are accepted; this command does not pull a moving tag or decide to upgrade.
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
