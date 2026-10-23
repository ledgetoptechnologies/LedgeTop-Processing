# Synthetic host worker

`scripts/prototype_worker.py` is a one-shot host process for the isolated
coordinator prototype. Its only admitted workload is `treedar-synthetic-v1`.
Both `production` and `staging` here describe **synthetic queue priority**.
This does not admit real production datasets or replace a NodeODM adapter.

The host needs Python's standard library and the Docker CLI with a Linux engine.
The engine image must already be built locally. Pin its full image ID, obtained
using `docker image inspect treedar:processing-prototype --format '{{.Id}}'`.
No image pull is performed by this worker. Engine dependencies run inside the
image. The helper and existing deterministic three-tree forest fixture are
mounted read-only, and the container runs the actual TreeDar pipeline and
validates its report, manifests, CSVs, raster grid/EPSG, preview files and hashes.
Synthetic success establishes plumbing, not forestry accuracy.

Create an approved private scratch root outside the repository and set
`PROCESSING_API_TOKEN` in the host process environment through the existing
credential mechanism. Never pass it as a CLI argument. The worker does not use
the operator token and passes neither token into Docker. For a coordinator
bound to loopback, run from the repository root:

```powershell
python engines/treedar/scripts/prototype_worker.py `
  --coordinator http://127.0.0.1:8899 `
  --image sha256:REPLACE_WITH_FULL_64_HEX_LOCAL_IMAGE_ID `
  --scratch C:/approved-private/processing-prototype `
  --worker-id cluster-synthetic-1
```

An independently verified remote coordinator origin also requires
`--verified-origin` containing the exact same origin. URLs must have no path,
credentials, query or fragment. Redirects and environment proxies are disabled.
Plain HTTP carries the API token in cleartext; use loopback or a trusted isolated
network for the prototype. There is no scheduler loop: each invocation claims at
most one job, and production-first admission is owned by the coordinator.
Production admission does not preempt a running staging container.

Docker is limited to two CPUs, 2 GiB memory, 128 PIDs, a 512 MiB temporary
filesystem, no network, no capabilities, no privilege escalation, read-only
root filesystem, and the image's non-root user. Only the exact per-job output
directory and two fixed source files are mounted. The fixed container command
accepts no job-supplied executable, path, image, arguments or dataset. Outputs
are retained at `<scratch>/<environment>/<jobId>/synthetic`; each fresh job
requires a new directory. The default wall timeout is 240 seconds, configurable
from 180 to 300 seconds. Containers are retained stopped for inspection.

The host also recomputes every file hash from the output index after Docker
stops. The per-job directory permits the container UID to write; the private
scratch root's permissions/ACL are the access boundary and must be retained.
Image IDs identify exact image bytes. Recording the source revision in a build
label and adding atomic output publication for real datasets remain future
production-adapter work, alongside explicit job metadata and input hashes.

Exit codes are 0 for completed success or no queued work, 1 for a verified
container failure reported to the coordinator, and 2 for an uncertain state or
configuration error. A timeout stops and inspects the exact uniquely named
container. The worker never reports failure if it cannot prove the container
stopped. An ambiguous claim is never retried automatically.

Private receipts under `<scratch>/.prototype-receipts` contain **claim secrets**.
On POSIX they use restrictive permissions; on Windows the scratch root must
have a private inherited ACL. Do not share, commit or print receipt contents.
Unresolved receipts block subsequent claims on that scratch root. A process
lock blocks simultaneous invocations; a stale lock requires checking the old
worker and its named container before manually removing that exact lock file.

If a completion reply is lost, the receipt remains `completion_pending`.
After verifying the coordinator is reachable, rerun the same command with
`--resume <absolute-receipt-path>`. This resends only the saved exact completion
payload using the current API token; it never claims or reruns a job. Other
receipt states require operator inspection. The separately authenticated
coordinator hold/resolve process must verify no old worker/container remains;
never delete an unresolved receipt just to get a new claim.

Run host-only recovery tests from the repository root:

```powershell
python -m unittest discover -s engines/treedar/tests -p test_prototype_worker.py -v
```

These mocked tests cover ambiguous claims, exact completion replay, malformed
job rejection, container stop verification, missing artifact rejection and
runtime resource settings. An actual local Docker run is still required for
engine acceptance, followed by a separate approved cluster smoke test.
