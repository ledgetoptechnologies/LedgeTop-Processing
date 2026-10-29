# Empty real-Docker worker maintenance acceptance plan

This is a review plan, not launch authorization. No lab containers, ports,
services, gates or state have been created. `ops/qualify_worker_maintenance_runtime.py
plan` only renders a draft resource manifest; its run action refuses that draft.
No source may spoof Node/nginx process arguments, replace real Docker calls with
FakeCLI, broaden a cold ownership proof, or suppress a protocol hold.

## Exact proposed resources

- Host: `bkoltz@192.168.50.88`, UID 1000; Docker Engine
  `45bc889d-d645-442b-8fb9-d78b77b14fe9`, local Unix socket only.
- Operation: `93823dba-2c51-4d60-9758-f670b34e1dda`.
- Private root: `/home/bkoltz/.local/share/ledgetop-processing-reference/runtime-maintenance-93823dba-2c51-4d60-9758-f670b34e1dda`.
- Compose project: `ltds-mwq-93823dba2c51`; service `node`; NEW names:
  `ltds-mwq-93823dba2c51-node`, `-worker`, `-ingress`, `-probe-0`, `-probe-1`.
- Initial backend image:
  `sha256:841177349ebcae8c467787b5a64507497560d346b964b5b157670a76f4251776`.
- Replacement backend image:
  `sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`.
- Worker image:
  `sha256:b31c1fb4414144a3d8b9804666f567df1a009c54f8cab7c36755ffba280545a8`
  (inactive accepted recovery source 242dac6).
- nginx image:
  `sha256:84ed8c0dd296c188bfc24ed506f46589e9285fb0b11cf3a04d91d524d3067e11`.
- Loopback IPv4 ports: control 24780, private coordinator TLS 24781, worker TLS
  24782, private backend 24783, nginx ingress 24784. These were bindable at the
  read-only preflight; they must be checked again before creation. No public
  listener or IPv6 wildcard is allowed.

Both cached backend images are real `webodm/nodeodx` images with
`/usr/bin/node /var/www/index.js`. Image metadata does not establish their actual
engine versions. Distinct image digests qualify an image transition even if
both observed version strings are identical. Do not invent a version change.
The real runtime expected-version CAS and target-image/epoch publication must
handle the same-version case.

## First proposed execution: bounded empty image probes

The rendered manifest includes exact `docker create`, single supported configuration copy, `docker start` and `docker exec` argv arrays
for two NEW, labeled, network-none probe containers. They use only cached pinned
images, explicit runc, no GPU devices, a private writable backend container overlay and empty bounded tmpfs storage.
The worker root remains read-only. Backend `--config` selects a nonsecret JSON
configuration whose supported `logger.logDirectory` is `/var/www/logs`, bounded
to two 1 MiB application log files. Docker local logging uses max-size=1m and
max-file=2; max-file=1 is incompatible with its default compression.
Only the newly created exact CID receives this one JSON file via stdin tar
before its single start. It is stopped by exact owned CID after observation
or the 90-second deadline, retained for review, and never removed or restarted.
They publish no ports and mount no host directory, data or credential. Actual
loopback `/info` is read inside the new container by its own Node binary.
Capture only engine/version/empty counts and exact resource identities; also
confirm `/task/list` is empty. No task/initialization/upload/processing API is
called. Startup failures or unexpected processes/credentials remain blockers.
Inspect new resources by exact operation label/CID before any later stop/remove;
never use a broad name match or cleanup an existing container.

Root must review this probe manifest before those commands are run. Its results
then pin actual versions/engine family and the subsequent exact Compose recipe.
That draft recipe uses the same supported configuration through a read-only
bind of the NEW lab-owned `managed/backend-config.json`, plus a private writable
backend overlay. It does not mount existing services, source data or credentials.

## Full lab preparation, still requiring review

After source acceptance/commit, clone that exact Git source into a NEW private
operation workspace and verify raw tracked Python inventory. Do not overlay
production modules. Record all five existing real container IDs, image IDs,
StartedAt, RestartCount and canonical HostConfig hashes before/after each phase;
do not retain raw environments or existing mounts. The plan hardcodes those
five IDs and refuses their use as any test resource.

Generate NEW sandbox API/operator/worker/read credentials and a NEW loopback TLS
CA/server certificate under the private root. No existing credential file,
token, dataset or NAS path participates. Start the actual Python coordinator
service with a fresh Queue/reference state directory and loopback listeners.
No job or source upload is submitted. The actual worker broker boundary exists
only to return an empty queue and publish runtime state.

Use only the task-local `XDG_STATE_HOME=<private-root>/xdg` canonical NodeGate
namespace, provisioned to the fresh authority and actual Engine ID. Do not patch
`canonical_gate_path` or touch the installed production gate. Bootstrap the NEW
v2 state through the actual cached worker image's bootstrap entrypoint with
the exact four proposed private bind mounts. Start a NEW canonical worker with
the original image entrypoint, host networking, read-only root, UID1000:1000,
indefinite graceful stop timeout and no Docker socket. Its TLS listener is
loopback only. Capture exact CID/recipe hash and real host/container inode proof.

The backend Compose recipe must use a NEW empty container, only the exact
`127.0.0.1:24783:3000` mapping and empty tmpfs storage. No real volume/GPU is
attached. Initial startup must prove the production process/upload allowlists,
empty real API task queue and exact Compose ownership. nginx must use the NEW
private configuration bind, actual nginx entrypoint/master/workers and explicit
`listenAddress=127.0.0.1`; close/reopen uses the actual HUP/drain protocol.

Before executing the controller, freeze actual observed versions, source commit
and inventory, exact new CIDs, recipes, new authority and private root identities
in a reviewed completed plan. The run action rechecks source and the five-container
baseline, refuses foreign binds/resources and invokes `node_maintenance.run`
with mandatory worker-aware mode. It does not create, restart or clean resources
outside that actual managed protocol. The installed CLI's fixed port-18080
tunnel guard is not bypassed or claimed tested: this explicit synthetic harness
calls the same production library protocol at its separate reviewed loopback
control origin, as the existing protocol tests do.

## Acceptance sequence

1. Schedule one actual owned plan with immediate drain and a short future
   `notBefore`, then bounded wait-zero observation. Demonstrate draining/deferred
   state and unchanged worker/backend start identities before the due window.
2. Continue that SAME owned request/plan/receipt after it is due. Actual nginx
   drains, the worker exits gracefully, real ownership/census verifies, the
   registered exact Compose artifact executes a distinct cached backend image,
   and the SAME worker CID starts with owned config rebind.
3. Require actual TLS idle health, retained history, target image and version
   CAS, published runtime epoch, actual open ingress, and owned finish before
   claiming success. Preserve a hold rather than add a cold-proof workaround.
4. Perform a second separately scheduled owned transition using a retained
   prior registered artifact; prove repeat-operation provenance and another
   exact runtime epoch. Report image and version transitions independently.

No automatic replay/rollback follows unknown Docker results. No source/result
cleanup or TTL retirement is tested. A cold new-process stopped-mapping hold is
still a hold. Protocol failures retain exact new resources and private receipts
for Root review. Only after terminal review may exact new CIDs/processes be
removed; original five containers and monitoring must remain byte-equivalent
in the recorded fields. This empty lab does not qualify 10k/50GB transfer,
scientific processing, Viewer delivery, live provider activation or client data.
