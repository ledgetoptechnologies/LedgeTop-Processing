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

Last read-only host check, November 7: cluster `.89` had an older healthy
`ledgetop-processing-prototype` container; the reference-worker systemd unit
was inactive on both `.89` and main `.88`. Main NodeODM was running. Before this
staging step, the current photo-queue candidate was not running on either PC.
October 27 provider
deployment details and receipts below are historical evidence for an earlier
build, not acceptance evidence for the current candidate. The exact candidate
image for source commit `7b996000` is now staged in the cluster Docker image
cache as `sha256:6f648732647e7c1dc74f8b74b71a41714640bfda39548cda70c415df5b43e505`.
The running coordinator remains on its previous image; no container replacement
or job submission occurred. The staged image's one-shot read-only probe passed
authenticated `/info` and `/options` checks against both configured engines.
Keep NodeODM behind its private ingress; do not expose the Docker daemon or
engine ports to Viewer.

Use the cluster's single existing coordinator and state volume. Never start a
second real dispatcher for staging. `compose.viewer.yaml` requires the existing
coordinator name and state volume, an immutable image, the private runtime env
file, and explicit host paths for the existing authority-lease and coordinator-
TLS directories. It preserves the loopback operator port, Viewer port, and
reference TLS listener port during a container replacement. It also restores
the two existing path settings that are injected outside the runtime env file.
The TLS directory is read-only; the state volume and authority lease mount
retain their existing writable modes. The native facade is opt-in through
`PROCESSING_VIEWER_ENABLED=1`.

Viewer registration uses an exact origin with no path or query. Add the origin
to the Viewer's existing processing-provider allowlist if necessary; use its
normal credential UI to enter the private `PROCESSING_VIEWER_TOKEN`. Do not put
the token in URLs, notes, source files or chat. The detected type is `nodeodm`.
Set provider **admissionLimit to 4** for a queued trial: the default1 would keep
later jobs waiting inside Viewer rather than testing the orchestrator queue.
This is only Viewer admission; it does not change the per-node execution slots.

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
