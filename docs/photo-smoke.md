# Bounded NodeODX photo trial

This separate test stack exercises one real upload through the prototype
coordinator into a dedicated NodeODX engine. It is an operator-driven acceptance
test, not a general dataset API or an automatic production dispatcher.
The TreeDar synthetic deployment remains separate.

## Relationship to ClusterODM and ClusterODX

[ClusterODM](https://github.com/OpenDroneMap/ClusterODM) and
[ClusterODX](https://github.com/WebODM/ClusterODX) schedule and proxy requests to
processing nodes. NodeODM runs ODM; NodeODX runs ODX. Similar APIs do not make
their engine identity or output behavior interchangeable. The replacement
coordinator can submit directly to a node without retaining the cluster proxy.
The first trial uses that direct path to avoid an additional upload copy.

The [NodeODX API](https://github.com/WebODM/NodeODX/blob/master/docs/index.adoc)
supports multipart init, file upload, then commit. The worker records an assigned
UUID and each mutation stage before sending. That UUID is a reconciliation key,
not permission to blindly repeat an uncertain init, upload or commit. HTTP 200
can contain a JSON error. NodeODX query authentication must stay out of logs.

Status codes are queued 10, running 20, failed 30, complete 40, canceled 50.
Cancellation can report canceled before the child process exits. The parent
operator must stop and inspect this dedicated container before clearing scratch
after an uncertain response or timeout. Never use the test worker against a
shared processing node.

## Admission and bounds

`PROCESSING_PHOTO_SMOKE_ENABLED=1` explicitly enables `odx-photo-smoke-v1`.
The default remains off, and this profile accepts only staging jobs. Claims can
declare `profiles`; legacy `{workerId}` claims support only TreeDar. The single
global slot still chooses production first, then FIFO. A worker with the wrong
profile cannot bypass that selected job. Priority is nonpreemptive.

The queue accepts no paths or URLs. The operator supplies the worker with an
isolated read-only directory and a manifest of 10–50 JPEGs, at most 512 MiB total:

```json
{"files":[{"name":"photo001.JPG","bytes":12345,"sha256":"64 lowercase hex characters"}]}
```

The directory must contain exactly the manifest files. The worker verifies sizes,
hashes and JPEG headers, then hashes the actual bytes streamed to NodeODX.
Every file is uploaded once. Options are fixed to `end-with=opensfm`,
`feature-quality=low`, `max-concurrency=2`, with `skipPostProcessing=true` and
explicit reconstruction/camera/log outputs. Dense reconstruction, mesh and
orthophoto stages are excluded. The actual node must advertise these options.

The worker has a 600-second polling budget. The server additionally has an
approximate ten-minute watchdog. No uncertain mutation is retried. An uncertain
run leaves its claim blocking the queue for operator recovery.

Success requires the reported input count, an actual nonempty OpenSfM
reconstruction, cameras and points, and registration of at least 80% of inputs.
The ZIP is inspected in memory without extraction, with compressed/expanded size,
member-count and path bounds. A successful upload or completed status alone is
insufficient. The worker removes only its own terminal task, verifies inventory
absence, then completes the coordinator job with compact counts and hashes.

## Deploy an isolated trial

Use `compose.photo-smoke.yaml` with a fresh unique Compose project name and a
private environment file. Required variables:

| Variable | Meaning |
| --- | --- |
| `PROCESSING_SMOKE_IMAGE` | Verified immutable coordinator image ID including this worker |
| `NODEODX_SMOKE_IMAGE` | Verified existing immutable NodeODX image ID |
| `PROCESSING_API_TOKEN` | Fresh private worker credential, at least 32 characters |
| `PROCESSING_PROTOTYPE_OPERATOR_TOKEN` | Separate private operator credential |
| `NODEODX_SMOKE_CONFIG` | Absolute test-only config file path |
| `PHOTO_SMOKE_INPUTS` | Absolute isolated copy directory; originals stay elsewhere |
| `PHOTO_SMOKE_MANIFEST` | Absolute manifest path |
| `PHOTO_SMOKE_WORK` | Absolute initially empty private receipt directory |

Set worker UID/GID to the owner of the private inputs/work directories. The node
runs without capabilities; its bind-mounted config must be readable by its
container user. Start from the pinned image's `config-default.json`, set
`logger.logDirectory=/var/www/logs`, `logger.maxFileSize=1048576`,
`logger.maxFiles=1`, leave test mode false and external outputs/webhooks disabled.
Use an empty node token only on the dedicated internal network, with no host
port; never reuse production configuration or credentials.

```sh
docker compose -p UNIQUE-TEST --env-file PRIVATE.env -f compose.photo-smoke.yaml config --quiet
docker compose -p UNIQUE-TEST --env-file PRIVATE.env -f compose.photo-smoke.yaml up -d coordinator nodeodx
```

Verify readiness, `/info` identity, `/options`, an empty task inventory, actual
image IDs, and resource/mount configuration before submitting one staging job
through `/v1/prototype/jobs`. Then run the worker once:

```sh
docker compose -p UNIQUE-TEST --env-file PRIVATE.env -f compose.photo-smoke.yaml run --no-deps worker
```

The coordinator binds localhost 18081 and has a small persistent queue volume.
The engine has 2 CPUs, 6 GiB RAM, a read-only root and RAM-backed data/uploads/logs.
It has no NAS mount, Docker socket or external network. Inputs should also live
on host tmpfs. RAM may still swap under host pressure; inspect host memory/swap
and Docker block I/O rather than assuming tmpfs guarantees no physical writes.
The worker has 512 MiB RAM. Restart policies are disabled deliberately.

## Evidence and cleanup

Retain metadata only: image IDs, API/engine versions, job/task IDs, input count,
bytes and aggregate digest, registration/point counts, resource settings,
terminal status, removal proof and stopped-container inspection. Keep full
receipts private because they contain claim credentials. Do not commit images,
EXIF, reconstruction geometry, private logs or secrets.

After the worker exits, inspect the job and node task inventory. Stop and inspect
the dedicated engine even after success. Remove this test's engine/worker
containers, selected input copies, upload archive, worker receipt and RAM
intermediates; destroy only this test's named resources. Hash the selected
originals again and compare against the manifest. Preserve originals. On
uncertainty, stop both worker and engine before operator hold/resolve, then
record the failure honestly; do not free capacity while execution may continue.

The dedicated coordinator may remain idle with metadata-only history, or be
stopped after acceptance. Removing its named volume destroys test history, so
export sanitized evidence first. Persistent dispatch, fleet capacity accounting,
lease renewal, broader engine adapters and Viewer integration remain future work.
