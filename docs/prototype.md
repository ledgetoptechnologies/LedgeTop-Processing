# Synthetic TreeDar coordinator prototype

This opt-in prototype exercises durable job submission, production-first
admission and a separate TreeDar worker. It accepts only the fixed
`treedar-synthetic-v1` profile by default. It does not accept client datasets, commands,
paths or URLs, and is not a Viewer-compatible provider or a replacement for
ClusterODM/ClusterODX yet. Existing `/v1/plan` routing behavior is unchanged.

An independent, explicitly enabled staging-only photo trial is documented in
[NodeODX photo smoke test](photo-smoke.md). It uses a separate Compose project
and does not expand the default synthetic deployment's admission rules.

The cluster PC is separate from the Lenovo M720q staging PC. Use the verified
cluster host for deployment; an old address in a note is not current identity
proof. The large main processing PC is outside this first test.

## Start the coordinator

Set `PROCESSING_API_TOKEN` and a distinct
`PROCESSING_PROTOTYPE_OPERATOR_TOKEN` in the host environment, each a fresh
private value of at least 32 printable characters. Never commit or print them.
Use a private host environment file or credential mechanism for repeatable
starts. Both credentials authenticate this isolated prototype only.

```sh
docker compose -p processing-prototype -f compose.prototype.yaml up --build -d
```

This creates a separate stack, persistent state volume and bridge network.
The API is bound to `127.0.0.1:18080` (override `PROTOTYPE_PORT` if occupied).
The coordinator is non-root, read-only except its state volume, capped at
256 MB / 0.5 CPU, with no Docker socket, engine dependencies or NAS mount.
There is no restart policy; restart deliberately after inspecting active work.
An SSH tunnel can reach the localhost API without opening a LAN port.

The standard `compose.yaml` remains a read-only routing probe. The prototype
API is absent unless `PROCESSING_PROTOTYPE_STATE_DIR` is configured.

## API contract

All routes below require `Authorization: Bearer <PROCESSING_API_TOKEN>`, except
the two operator routes, which require the separate operator credential.
Request bodies use `Content-Type: application/json` and are limited to 8 KiB.

| Operation | Request | Result |
| --- | --- | --- |
| POST `/v1/prototype/jobs` | `{requestId,environment,profile}` | `{job}`; 201 new, 200 identical retry, 409 conflicting requestId |
| GET `/v1/prototype/jobs` | none | `{jobs,mode}` |
| GET `/v1/prototype/jobs/<id>` | none | `{job}`; 404 absent |
| POST `/v1/prototype/claim` | `{workerId}` | `{job}` with an opaque claimToken, or `{job:null}` |
| POST `/v1/prototype/jobs/<id>/complete` | `{claimToken,status,result}` | `{job}`; status succeeded or failed, compact result up to 4096 bytes |
| POST `/v1/prototype/jobs/<id>/hold` | `{note}` | running becomes held; does not stop a worker or release capacity |
| POST `/v1/prototype/jobs/<id>/resolve` | `{note,workerStopped:true}` | held becomes failed after operator inspection |

`requestId` and `workerId` are ASCII slugs of 1–64 characters. Environment is
explicitly `production` or `staging`; it cannot change on an existing request.
`profile` is exactly `treedar-synthetic-v1`. Production here labels **synthetic
priority tests**, not permission to process production datasets.

The queue has one global execution slot and retains up to 1000 jobs. Production
is selected before staging, FIFO within each class. Running staging is **not
preempted** if production arrives; production is next after that run finishes.
This is an explicit first-prototype limitation. There is no automatic dispatch,
retry, cancellation, cleanup, promotion or downstream ODM submission.

Worker completion delivery now retries durably while the foreground worker runs;
this never retries engine execution. See [ownership and resilience](resilience.md).

The worker retains its claim token until completion. Status/list responses never
expose it; the database keeps only its hash. An identical completion is safe to
repeat after an uncertain response. A mismatched or stale completion cannot
release a slot. Operator resolution is not an engine cancellation mechanism.

## Recovery and held jobs

Only one coordinator process may own the state directory. On restart, all
running jobs become held and continue blocking the slot. A worker dying without
a coordinator restart leaves its job running. There is no lease-based release:
expiry cannot prove the engine stopped.

Restart-held jobs now carry `heldReason: coordinator_restart`. Their original
worker can complete the unchanged claim, including through the durable outbox.
An operator hold has a different reason and still vetoes completion. The manual
procedure below applies to uncertain execution and explicit operator holds,
rather than requiring successful original work to be discarded after an outage.

1. Inspect job status and the worker's named Docker container and local output.
2. Stop the worker and the exact engine container if still running. Verify that
   they have terminated. Preserve any outputs for inspection.
3. If the job is running, use the operator `/hold` route to fence completion.
   Holding alone does not prove that work stopped.
4. Use `/resolve` only after independently proving the worker/container stopped.
   Record that evidence in `note` and explicitly attest `workerStopped:true`.
   The separate operator credential is never given to a worker. The API trusts
   this operator attestation; it does not inspect the Docker host itself.
5. If a retry is appropriate, submit a new requestId. Do not delete the database
   or reset a live claim to queued.

The prototype's state volume and synthetic artifacts remain separate from
existing datasets, production engines and Viewer job records. Stop this stack
with `docker compose -p processing-prototype -f compose.prototype.yaml down`;
retain its volume unless deliberately retiring only this test's history.

## Verification

```sh
python -m unittest discover -v
python scripts/prototype_api_smoke.py
python engines/treedar/tests/test_prototype_worker.py
python scripts/prototype_treedar_smoke.py
```

The Docker API smoke creates a unique temporary Compose project, generates
in-memory test credentials, verifies priority/idempotency/restart holds/operator
recovery/completion over real HTTP, and removes only its own stack and volume.
It does **not** run TreeDar. The separate `prototype_treedar_smoke.py` builds the
engine image, runs two real TreeDar jobs through the queue and checks production
before staging, three fixture trees per job, validated artifacts and durable
completion. It retains private synthetic outputs/receipts in a newly created
temporary directory and the exact stopped worker containers for inspection.
It removes only its own coordinator stack and state volume after the test.
For persistent one-shot worker usage, see
[the worker guide](../engines/treedar/docs/prototype-worker.md).
For automatically draining a bounded set of synthetic jobs in one invocation,
see [the host dispatcher](dispatch.md). This remains separate from any service
or production dispatcher.

Never mark a host deployment passed based only on these local checks. Record
the exact source revision, host identity, image IDs, resource limits and actual
engine result from that host before calling the cluster prototype working.
