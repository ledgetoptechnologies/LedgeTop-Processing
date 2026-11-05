# Processing node roles

Beau's October 27, 2026 configuration:

| Computer | Role |
| --- | --- |
| Cluster, 192.168.50.89 | Sole coordinator and photogrammetry up to 1,000 photos |
| Main, 192.168.50.88 | Larger photogrammetry and all TreeDar work |

The shared queue allows one active job per configured node, so distinct nodes
can process concurrently without double-booking a node. Production is selected
before staging; running staging work is not interrupted. Each node takes the
oldest queued job it can process, so a large job waiting for a capable node does
not leave a smaller eligible node idle. Maintenance and engine capacity are
rechecked atomically when the worker claims a job.

Set `PROCESSING_PROTOTYPE_NODE_IDS=cluster,main` and
`PROCESSING_TREEDAR_NODE_IDS=main` explicitly in the deployed environment. Node
responses expose `allowedProfiles`; the queue checks eligibility atomically when
claiming. Omitting TreeDar eligibility preserves older fixture configurations,
which permit TreeDar on their configured nodes.

The Viewer-compatible dispatcher and read-only `/v1/plan` probe accept up to
eight ordered node IDs. Set `PROCESSING_PROTOTYPE_NODE_IDS=cluster,middle,main`
to prefer the small cluster, then an intermediate node, then main. For each ID,
configure `PROCESSING_NODE_<ID>_URL` and `PROCESSING_NODE_<ID>_TOKEN` (uppercase
the ID and replace punctuation with underscores); `_MAX_INPUT_BYTES` is an
optional per-node ceiling. `_MAX_IMAGES` accepts a positive integer or the
literal `null` for no configured photo cap. When omitted, cluster inherits
`LOCAL_MAX_PHOTOS` and other nodes have no configured photo cap; live engine
limits still apply. The legacy `LOCAL_NODE_*` and `MAIN_NODE_*` names
remain supported for cluster and main. All configured nodes participate in the
fresh exact-definition options intersection, and a configured unavailable node
closes option admission. The cluster's photo ceiling remains the lower of its
configured `LOCAL_MAX_PHOTOS` and the node's live `/info` maximum. This dynamic
configuration applies to both the Viewer-compatible native dispatcher and the
ReferenceStore worker `/next`/`/claim` path. The worker path refreshes fleet
capacity and option definitions before discovery and rechecks the selected
node/job pair atomically at claim time. This is synthetic-tested draft code; it
has not been deployed or accepted against the live nodes.

For a synthetic main worker, pass `--node-id main --resource-policy
main-unrestricted` to the worker or dispatcher. This omits Docker CPU, RAM and PID
limits and fixed numerical-library thread counts. It retains fixture-specific
timeouts, tmpfs size, isolation, host ownership gate and completion receipts.
The policy is rejected on other nodes. The default `bounded-fixture` policy
remains suitable for small isolated checks. This is not a general real-data
TreeDar worker service.

## Current review versus historical rollout

The current coordinated review branch has newer generic inventory, per-node
limits, worker custody and maintenance code. Its installed acceptance is still
pending; see `coordinated-integration-readiness.md`. A November 5 read-only SSH
refresh still found coordinator image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`
running on cluster and main's existing NodeODX container running. No service was
changed. Do not interpret the following older rollout evidence as installation
of the current coordinated version or its new reference worker service.

### Historical node-role rollout

Core eligibility, dispatcher and resource policy changes are committed as
`6532dbb`. The cluster coordinator was rebuilt locally and deployed, preserving
its authority identity and all 12 historical job records. Main's new worker code
is staged under its private node-role setup directory; it is a foreground tool,
not an installed worker service. Coordinator tests passed on cluster (158) and
Windows (158, 13 platform skips), with 36 focused worker tests passing on main
and Windows. The full TreeDar science suite needs its isolated engine dependencies.

The cluster engine now reports `maxImages: 1000`; its empty task list and
placeholder-only data/tmp directories were verified after network fencing and
before replacement. The old container and data copy were retained privately.
Canonical replacement Compose lives under the cluster's private
`node-roles-6532dbb/cluster-engine/compose.json`; the historical source's cap was
also changed to 1,000. Main's same-image recreation used the maintenance runner,
verified idle/health checks, and reopened ingress successfully. Docker reports
all GPUs exposed (`Count: -1`), `Memory: 0` and `NanoCpus: 0` for its engine.

The existing cluster engine is NodeODM/ODM 3.6.2; main is NodeODX/ODX 3.8.5.
Do not treat their names or capabilities as interchangeable.

Real-photo prototype admission remains disabled. The queue serializes its own
claimed work. Legacy direct NodeAPI/ClusterODX
submission routes can bypass it. Complete ingress fencing and the native Viewer
task/artifact bridge before claiming system-wide exclusivity or opening a live
production provider.

Ollama's main-PC container was stopped and removed; its model bind directory and
cached image were retained. CPU/RAM exporters on both PCs remain in place. Main's
DCGM exporter was recreated with its existing pinned image and basic GPU counters
instead of unsupported profiling counters. Both GPUs returned utilization metrics
and the existing node-exporter container identity was preserved. Central
Prometheus at `192.168.50.80:30104` reports both CPU/RAM endpoints and main's
GPU endpoint healthy after the repair.

Gaussian splats are recorded as a deferred feature in Engineering Hub. No splat
runtime or dataset processing is part of this setup. Use CPU/RAM/GPU telemetry
from future authorized tests before considering TreeDar on cluster.
