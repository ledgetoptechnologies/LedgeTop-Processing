# Viewer provider trial

The coordinator exposes a native NodeODM-compatible 2.x HTTP facade over the
same durable queue/global execution slot used by TreeDar. Its advertised engine
is `ledgetop-processing`, not an official ClusterODM build. Dynamic `/options`
is the fresh exact-definition intersection of configured engines. Engine updates
must not silently introduce options unsupported by the other node.

Small jobs, at most 1,000 photos and the configured local byte ceiling, use
cluster first. If cluster is busy/unavailable/draining, they wait for cluster.
Larger jobs wait for main; they never overflow to cluster. There is one running
or held job globally, production before staging and FIFO within each class.
Running staging is allowed to finish when production arrives.

## Operator setup

Use the cluster's single existing coordinator and state volume. Never start a
second real dispatcher for staging. `compose.viewer.yaml` requires explicit
existing volume, immutable image, private runtime env path and LAN bind address.
The native facade is opt-in through `PROCESSING_VIEWER_ENABLED=1`.

Viewer registration uses an exact origin with no path or query. Add the origin
to the Viewer's existing processing-provider allowlist if necessary; use its
normal credential UI to enter the private `PROCESSING_VIEWER_TOKEN`. Do not put
the token in URLs, notes, source files or chat. The detected type is `nodeodm`.
Set provider **admissionLimit to 4** for a queued trial: the default1 would keep
later jobs waiting inside Viewer rather than testing the orchestrator queue.
This does not change the orchestrator's single execution slot.

Primary Viewer credentials label jobs production. A separate optional
`PROCESSING_VIEWER_STAGING_TOKEN` labels jobs staging. Names are never used to
guess environments. Environment cannot change when a UUID is reset.

The native endpoint implements capability/options discovery, multipart
init/upload/commit, info/output, cancellation/removal and ZIP download. Requests
use the Viewer's existing query-token protocol. Uploads stream through private
temporary files, with at most 2GiB per request, 64parts, and a default20GiB per
job. These disk/transfer bounds are configurable independently of worker CPU/RAM
limits; main processing remains CPU/RAM uncapped. Input basenames must be unique
and safe. Current inputs are images plus txt/json/csv auxiliary files.

## Recovery and maintenance

The public task UUID maps durably to a unique private engine UUID per reset.
Each mutation has a durable intent. Lost init/upload acknowledgements hold the
shared slot for inspection instead of starting another expensive job. Lost
commit acknowledgements reconcile by status reads for that exact engine UUID;
commit is never replayed. After restart, a known processing job stays publicly
running while its exact result is reconciled. Read outages back off to60seconds.

Running cancellation retains the slot until the engine reports a terminal
state. Queued cancellation prevents dispatch. `remove` hides/resets a public
task but retains prior inputs, execution history and result copies. Download
GETs may be retried; successful downloads do not delete outputs. The current
Viewer adapter has no successful-ingestion acknowledgement. Verified cleanup
therefore remains an operator step; do not treat download/remove as an ACK.

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
Viewer source path. It exercises20-file upload batches, global TreeDar exclusion,
production order, small/main routing, cancellation, duplicate commit, output
cursors, environment authorization, download/hash equality and retained results.

Synthetic engine doubles are opt-in and never perform photogrammetry. Their ZIPs
are explicitly labelled transport fixtures. Passing these checks does not prove
valid ODM geometry, final Viewer EPT/model ingestion, real-data throughput,
hardware capacity, or unattended reboot recovery. The first real small job is
Beau's next acceptance step after provider configuration.
