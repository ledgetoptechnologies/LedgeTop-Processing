# Viewer provider trial

The coordinator exposes a native NodeODM-compatible 2.x HTTP facade over its
durable photo queue. Its advertised engine is `ledgetop-processing`, not an
official ClusterODM build. Dynamic `/options` and node readiness are discovered
from each configured engine; an offline node does not hide a healthy sibling.
TreeDar remains on the same authoritative coordinator, with one active heavy
job per physical node.

`PROCESSING_NODE_ORDER` controls preference and can name future nodes using
`PROCESSING_NODE_<ID>_URL`, `_TOKEN`, `_MAX_PHOTOS` and `_MAX_INPUT_BYTES`.
The cluster defaults to at most 1,000 photos and 20 GiB of inputs; the main node
has no application photo or byte cap, though live engine limits still apply.
Small jobs prefer cluster, then may use an eligible main node when cluster is
busy. Jobs above a node's discovered or configured limit cannot run there. Each
physical node has one heavy-job slot; different nodes may work concurrently.
Production jobs are chosen ahead of waiting staging jobs without preempting a
running task. FIFO is maintained among eligible work.

## Operator setup

Last read-only host check, November 8: cluster coordinator `.89` is running and
healthy at image digest
`sha256:6f648732647e7c1dc74f8b74b71a41714640bfda39548cda70c415df5b43e505`
(source revision `7b996000`); main NodeODX is recorded at digest
`sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`.
The Hub acceptance checkpoint records real 20-photo Viewer-API jobs on both
physical nodes, queued dispatch, retained inputs/results and coordinator
restart recovery. This is API-path acceptance, not a Viewer-workspace
submission. Automatic cleanup remains disabled. Keep NodeODM behind its private
ingress; do not expose the Docker daemon or engine ports to Viewer.

Use the cluster's single existing coordinator and state volume. Never start a
second real dispatcher for staging. `compose.viewer.yaml` requires the existing
coordinator name and state volume, an immutable image, the private runtime env
file, and explicit host paths for the existing authority-lease and coordinator-
TLS directories. It preserves the existing bridge network and loopback operator
port, Viewer port, and
reference TLS listener port during a container replacement. It also restores
the two existing path settings that are injected outside the runtime env file.
The TLS directory is read-only; the state volume and authority lease mount
retain their existing writable modes. The native facade is opt-in through
`PROCESSING_VIEWER_ENABLED=1`.

Viewer registration uses an exact origin with no path or query. Add the origin
to the Viewer's existing processing-provider allowlist if necessary; use its
normal credential UI to enter the private `PROCESSING_VIEWER_TOKEN`. Do not put
the token in manually constructed URLs, notes, source files or chat. The
detected type is `nodeodm`.
Set provider **admissionLimit to 4** for a queued trial: the default1 would keep
later jobs waiting inside Viewer rather than testing the orchestrator queue.
This is only Viewer admission; it does not change the per-node execution slots.

### Protected NodeODM origin (TLS candidate; not installed)

When the explicit `PROCESSING_REFERENCE_CONFIG` TLS runtime is enabled, the
candidate serves the same NodeODM `/info`, `/options` and `/task/...` routes on
the protected reference listener. This lets the existing Viewer provider use
its native protocol over HTTPS without changing its API. The plaintext listener
stops serving those NodeODM routes while the TLS runtime is active; its health
and operator APIs remain available. `/ltds/v1` continues to use its existing
Bearer-authenticated contract. The focused patch is commit `2a0f27e7`, based
directly on installed source `7b996000`; its equivalent diff on this stacked
branch has the same patch ID. PR #369 is stacked on the accepted-copy branch;
its exact source head at the November 8 checkpoint was
`f4f8570cccd422c9623332117210f031eae43c2e`. That source passed 227 local tests
across the queue/recovery, Viewer API, reference runtime and option suites, and
passed an image startup/health smoke on cluster `.89`. The exact image
`sha256:5275ef11703485fb4072775ebf3d45ad5a892424dfb3d1f4490c60ba10e1d2c4` is
preloaded there but is not serving traffic. The live coordinator remains on
`sha256:6f648732647e7c1dc74f8b74b71a41714640bfda39548cda70c415df5b43e505`.
The disposable Compose maintenance smoke also passed against a synthetic
NodeODX fixture: drain blocked new claims, active work delayed the update,
healthy replacement reopened admission, failed health restored the prior
pinned image, and unrelated sibling/state fixtures were preserved. These are
synthetic checks, not a live cluster rollback or real-engine job. See the
[smoke receipt](evidence/maintenance-smoke-tls-facade-20261108.json).

Register the HTTPS listener's exact origin, typically
`https://<certificate-hostname>:18443`, only after the certificate SAN matches
that hostname and the Viewer Node process trusts its issuing CA. Viewer uses
Node's built-in `fetch`; a bounded Node 24 check with the coordinator CA supplied
through `NODE_EXTRA_CA_CERTS` validated the existing `.89:18443` certificate
without credentials (the protected route returned HTTP 401 as expected). For a
Viewer cutover, mount the public coordinator `ca.crt` read-only in both Viewer
API and worker containers, set `NODE_EXTRA_CA_CERTS` to that mount in both, and
restart them only while no Viewer job is active. After the coordinator candidate
is installed, change the existing provider origin to
`https://192.168.50.89:18443` and verify its healthy capability read before
submitting a task. Preserve the current coordinator image digest for rollback.
Keep normal certificate validation enabled. Do not point the NodeODM provider at
the `/ltds/v1` path, disable TLS verification, or place a NodeODM token on the
plaintext port. The existing NodeODM provider protocol transmits its saved
credential in a query parameter, so use it only over HTTPS; this listener
suppresses request and error logging. If a reverse proxy is added later, it
must also omit query strings from access logs.

#### Existing-cluster cutover and rollback

The November 8 cluster installation uses the existing Compose project
`photo-queue-7b996000`, working directory
`/home/bkoltz/.local/share/ledgetop-processing/photo-queue-7b996000`, file
`compose.viewer.yaml`, and project `.env`. The coordinator's `PROCESSING_IMAGE`
value is its immutable image pin. The PR #369 candidate image above is already
preloaded on that host; no second coordinator or state volume is needed.

Immediately before cutover, verify the coordinator has no queued, running or
held work; both physical worker slots and engine locks are idle; and Viewer has
no active task, upload or result transfer. Capture the current container/image
ID, exact `PROCESSING_IMAGE` value, mounts, ports and project configuration for
rollback without printing other environment values. If any check is uncertain,
defer replacement and leave the current service running.

Change only `PROCESSING_IMAGE` in the existing project `.env` to the reviewed
candidate image ID, then recreate only the existing `coordinator` service with
the same Compose project, file, env file and working directory. Do not run
`compose down`, create a new project/state volume, or change the lease, TLS,
network or port settings. Verify the recreated existing service has the exact
candidate image ID, is healthy, and retains the existing volume, writable lease,
read-only TLS mount, bridge network and ports. Verify the protected NodeODM
capability routes over HTTPS with normal CA validation and confirm the plaintext
NodeODM credential route is rejected before switching the existing Viewer
provider.

For rollback before Viewer cutover or any accepted work, restore the captured
old immutable `PROCESSING_IMAGE` value in the same `.env` and recreate that
same service with the same Compose project and mounts. Verify its prior digest
and health; do not roll back or replace the persistent state volume. After
Viewer cutover or any accepted task, stop new admission and reconcile the exact
task and transfer state before changing the image. Never delete input or result
data during rollback. The currently preloaded candidate has passed its
isolated startup check only; no live service replacement or rollback has yet
been performed.

Primary Viewer credentials label jobs production. A separate optional
`PROCESSING_VIEWER_STAGING_TOKEN` labels jobs staging. Names are never used to
guess environments. Environment cannot change when a UUID is reset.

The native endpoint implements capability/options discovery, multipart
init/upload/commit, info/output, cancellation/removal and ZIP download. Requests
use the Viewer's existing query-token protocol. Uploads stream through private
temporary files, with at most 2 GiB per request and 64 parts. The default task
input cap is 20 GiB, and intake reserves space for both the temporary body and
durable spool copy while preserving 20 GiB of free space. The reserve is
configurable. Result archives are bounded separately. These are disk limits;
the main node has no application CPU/RAM cap. Input basenames must be unique and
safe. Current inputs are images plus txt/json/csv auxiliary files.

For pre-dispatch tasks `/info` returns code 10 plus validated
`ledgetopQueueState`, `ledgetopQueueReason` (when known), and `ledgetopNodeId`
fields. Submission remains queued until native NodeODM reports code 20. Viewer
then downloads and validates `all.zip`, and marks ready only after its required
ingest and model checks. Processing retains its own copy and receives no final
Viewer-ready acknowledgment, so automated cleanup must remain disabled.

## Recovery and maintenance

The public task UUID maps durably to a unique private engine UUID per reset.
Each mutation has a durable intent. On coordinator restart, uncertain init is
reconciled by reading that exact UUID; confirmed initialized tasks and fully
acknowledged uploads resume from the durable per-file cursor. A crash during an
upload is held for inspection because NodeODM does not expose an authoritative
uploaded-file inventory; the upload is never blindly replayed. Lost commit
acknowledgments reconcile by status reads for the exact engine UUID; commit is
never replayed. Known processing jobs stay publicly running while their exact
result is reconciled. Read outages back off to 60 seconds.

Running cancellation retains the slot until the engine reports a terminal
state. Queued cancellation prevents dispatch. `remove` hides/resets a public
task but retains prior inputs, execution history and result copies. Download
GETs may be retried; successful downloads do not delete outputs. Viewer retries
ingest from byte zero and verifies the archive and required model products before
marking ready. It does not send a final readiness ACK to Processing. Do not treat
download/remove as an ACK or delete retained data automatically.

Maintenance first drains coordinator admission and waits for the current node's
job/host ownership. Ingress remains open for its uploads/status/results until
that job finishes. Only then does ingress close, idle checks run, the container
change occur and health checks permit reopening. Existing strict task/data
census can block an update while retained engine results await verified cleanup.
Automatic moving-tag update detection is not installed by this provider work.

## Synthetic acceptance

Run stdlib unit/integration checks with `python -m unittest discover -s tests`.
The optional `scripts/viewer_contract_smoke.py --viewer-adapter PATH` executes
the actual Viewer `NodeOdmProvider` JS module against two isolated synthetic HTTP
engines and the real coordinator code. It needs Node.js and the trusted current
Viewer source path. It exercises 20-file upload batches, per-node TreeDar
exclusion, production order, small/main routing, cancellation, duplicate commit,
output cursors, environment authorization, download/hash equality and retained
results.

Synthetic engine doubles are opt-in and never perform photogrammetry. Their ZIPs
are explicitly labelled transport fixtures. Passing these checks does not prove
valid ODM geometry, final Viewer EPT/model ingestion, real-data throughput,
hardware capacity, or unattended reboot recovery. The first real small job is
Beau's next acceptance step after provider configuration.

Acceptance evidence is in [the deployment receipt](evidence/viewer-provider-20261027.json).
The full227-test suite passed inside the Linux image at `f5fb753`; the final
discovery-readiness-only change at `ba01f37` passed its four focused checks.
Final actual-adapter synthetic round trip passed with62invented input files,
three completions, one cancellation and exact repeat-download hashes. Each real
installed engine also accepted an initialized/uploaded synthetic wire fixture;
neither was committed, no photogrammetry ran, and exact owned tmp files were
verified removed. One earlier Windows-only smoke temp directory remains after
automatic approval review rejected its explicit deletion; other successful smoke
directories clean themselves after releasing the queue lock.

The same actual-adapter harness was rerun on November 1 against Viewer commit
`54556f9a34b330bd21c93813441c5c67385aeae2` (`NodeOdmProvider` source SHA-256
`167b7bbd66e36fdba3255ca165877e1b821308d2fc9054f1d9ba3a3b77b3c86b`). It
completed 62 invented uploads, one cancellation and three tasks in this order:
small production on cluster, large production on main, then small staging on
cluster. Global TreeDar-slot exclusion, staging isolation, idempotent commit,
repeatable download and output cursor preservation passed. No live PC, client
dataset or production Viewer storage was used. This remains transport/workflow
evidence for the actual adapter and synthetic coordinator only; it does not
validate real photogrammetry or Viewer result ingestion. See
[November 1 synthetic adapter evidence](evidence/viewer-contract-smoke-20261101.json).
