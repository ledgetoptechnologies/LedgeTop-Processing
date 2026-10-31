# Viewer provider trial

The coordinator exposes a native NodeODM-compatible 2.x HTTP facade over the
same durable queue/global execution slot used by TreeDar. Its advertised engine
is `ledgetop-processing`, not an official ClusterODM build. Dynamic `/options`
is the fresh exact-definition intersection of configured engines. Engine updates
must not silently introduce options unsupported by the other node.

Viewer photo jobs with at most 1,000 photos prefer cluster regardless of input
bytes. If cluster is busy/unavailable/draining, they wait for cluster. Jobs with
more photos wait for main; they never overflow to cluster. The independent Viewer
upload admission cap still applies. This does not change the separate v1
reference pipeline, which currently retains its local input-byte routing limit.
The `/v1/plan` routing probe follows the same photo-count preference as Viewer.
Viewer has no free-space reservation check; qualify one before raising its upload
cap. There is one running
or held job globally, production before staging and FIFO within each class.
Running staging is allowed to finish when production arrives.

## Operator setup

Verified October27 deployment: **`http://192.168.50.89:3005`**. The actual Viewer
adapter successfully probed this LAN endpoint and detected `nodeodm`, engine
`ledgetop-processing`, 74dynamic options and `maxParallelTasks:1`.
The private production token is on cluster at
`/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/viewer-production.token`
(mode0600). Read it in your own SSH terminal and enter it in Viewer; do not paste
it into chat. The adjacent `viewer-staging.token` is for explicitly staged jobs.

Runtime source: `ba01f372d31a2354ff4321dc5ae7ebf9f2b5f310`, image
`sha256:40a4d54c3b7311384ba031cf05acb3e760237ada4d6d2434ab1392fabd5bc401`.
Existing authority/state and all12historical jobs were preserved, with no active
jobs at handoff. Node backends bind localhost only; ordinary LAN direct access
to their ingress ports returns403. Cluster ingress permits its host/default
Docker bridge; main ingress permits cluster plus loopback. Docker administrators
remain able to bypass host software controls. Legacy ClusterODX is retained stopped.

Use the cluster's single existing coordinator and state volume. Never start a
second real dispatcher for staging. `compose.viewer.yaml` requires explicit
existing volume, immutable image, private runtime env path, LAN bind address,
and `PROCESSING_AUTHORITY_LEASE_HOST_DIR`: the absolute path of an existing
directory writable by container UID/GID 10001. The Compose file mounts this
directory at `/run/ledgetop-authority-lease` and sets
`PROCESSING_AUTHORITY_LEASE_DIR` to that path, overriding the runtime env value.
Compose will not create a missing host directory. The state must already contain
its coordinator authority; the lease is not a fresh-state initialization path.
Compose resolves a relative source against its project directory; it does not
enforce an absolute value, so the operator must provide and verify an absolute
path to avoid accidentally selecting a different lease directory.

Every lease-aware coordinator that could run the same authority must use the
same underlying lease files and a filesystem whose `flock` locks are effective
between those processes. Equal directory names on different PCs do not provide
a shared lock. Do not assume a network filesystem supplies the required lock
semantics without independently qualifying it. This is not a high-availability
or distributed-fencing setup: older software that ignores the lease, a different
lease directory, and privileged writers remain outside its protection.
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

## Optional archive identity

For a receipt-backed verified archive, the download response may include
X-LedgeTop-Archive-SHA256 containing 64 lowercase hexadecimal characters.
The value identifies the full ZIP response body, not a Viewer acceptance or
custody receipt. Existing archives without a coordinator verification receipt
omit the header. Clients must require the full declared Content-Length and
compare the digest before trusting the body. If the bytes change during
streaming, the coordinator closes a truncated response before its final chunk;
the 200 status and headers may already have been sent. TLS and client
authentication remain separate. This adds a complete streaming hash pass in
addition to the existing archive pre-verification and may cost substantial I/O
for large results. It does not authorize deleting inputs or results.

### Reservation-enabled initialization replay scope

For this coordinator's reservation extension, an identical initialization
using the same UUID, reservation capability, environment, name, options and
outputs returns the existing task identity. Different settings conflict, a
wrong capability is hidden as not found, and a cancelled reservation cannot be
initialized again. This behavior is specific to the coordinator extension and
must not be assumed for ordinary NodeODM providers.

For an already committed reservation-bound initialization, the service checks
the exact stored request and durable task/evidence state before consulting the
live option catalog. An identical replay can therefore succeed during a catalog
outage or after a catalog change. New tasks and unreserved legacy requests still
require live validation; an overlapping request that arrives before the first
commit may still fail catalog validation. The pending-drain 503 test applies to a close announced before reading the initialization body;
an arbitrary 503 or disconnected client does not prove no mutation committed.
Reconcile the reserved task identity before deciding what to do next. An
initialization failure after directory creation can leave a private orphan;
a retry chooses a separate directory rather than adopting or deleting it.
Tests cover rollback, client disconnect and abrupt process exit immediately after
directory creation. They do not prove power-loss atomicity or every crash point.
The replay check verifies the task directory exists, any referenced job exists,
and recorded option evidence is structurally consistent when evidence is
required. It does not rehash input or result files or establish their custody.
Older records with `option_evidence_required=0` have no option evidence to
check. The outage and changed-catalog test cases inject errors at the catalog
boundary; they do not prove recovery from every network failure.

Archive intent and receipt markers are published from synced, uniquely named
private temporary files without overwriting existing markers. If a process exits
after linking a valid task-bound marker but before removing its matching staging
link, a later download can finish that unlink after identity checks. A crash
before publication can leave a staging-only orphan; this change does not delete
it automatically. Foreign or invalid markers and unrelated hardlinks remain
held for inspection. These checks do not establish Viewer ingestion, provide an
acknowledgement, or prove power-loss behavior at every filesystem boundary.
Concurrent recovery tests exercise threads in one coordinator service, whose
per-task lock serializes them. They do not establish multi-process exclusion.
