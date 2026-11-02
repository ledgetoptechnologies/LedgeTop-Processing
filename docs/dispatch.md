# Bounded synthetic queue dispatch

`scripts/prototype_dispatcher.py` drains already-queued TreeDar synthetic jobs in
one foreground invocation. It automatically invokes the existing worker once per
job, serially, for at most 1–10 jobs. It exits when empty, running/held work blocks
the slot, the head profile is unsupported, or its job budget is exhausted.
It creates no jobs and never runs the real-photo profile.

The dispatcher now checks the selected node's admission state before each launch
and passes `--node-id` (default `cluster`) to its worker. Upgrade the coordinator
before using this version: a legacy API without node admission fails closed.
See [scheduled node maintenance](maintenance.md) for drain and recovery behavior.
It also requires an explicitly provisioned Engine-bound host gate. Follow
[ownership and outage recovery](resilience.md) before starting a worker.

This is a host command using the Docker CLI through the existing worker. The
coordinator container has no Docker socket. No service, timer, startup policy or
production routing change is installed. A later service needs separate resource,
lifecycle and fleet acceptance. The main processing PC is outside this increment.

## Run

Use a verified localhost coordinator, an immutable TreeDar image already on the
host, and separate private existing directories for worker scratch and durable
dispatcher state. Keep state on persistent storage. Scratch can use RAM for tiny
synthetic runs, but receipts/artifacts there are lost on reboot.

```sh
python scripts/prototype_dispatcher.py \
  --coordinator http://127.0.0.1:18080 \
  --image sha256:VERIFIED_EXISTING_IMAGE_ID \
  --scratch /private/synthetic-scratch \
  --state /private/durable-dispatch-state \
  --worker-id synthetic-dispatch \
  --docker-engine-id VERIFIED_ENGINE_ID --node-generation 1 \
  --max-jobs 2 --timeout 240
```

The API token is supplied only through `PROCESSING_API_TOKEN`. The child worker
does not receive `PROCESSING_PROTOTYPE_OPERATOR_TOKEN`. This is not a general
environment sandbox: run under the intended private host account. Non-loopback
origins additionally require an exact independently checked `--verified-origin`.
Worker timeout is bounded to 180–300 seconds. The dispatcher does not kill the
worker with a parent timeout while the engine may still be running.

The command returns one JSON object with `completed` count, ordered
`completedJobIds`, and `reason`. Empty/blocked/unsupported exits can report zero
completed jobs successfully; inspect `reason`. Exit 2 means inspection is required.
No child output, claim token or server error body is printed.

## Scheduling and uncertainty

Selection matches the queue: production before staging, FIFO within each class.
Running work is not preempted. A higher-priority unsupported job blocks rather
than being bypassed. The worker owns the actual atomic claim. If another worker
or a new production job changes the selected head between the dispatcher read
and child claim, verification may find a different completed job. It stops for
inspection even if that worker succeeded; it never guesses or launches again.
Do not run competing controllers against this prototype.

An OS process lock prevents concurrent dispatchers using the same state directory.
Before each child launch, a durable `launch_pending` record is fsynced and renamed;
POSIX directory metadata is fsynced too. Any nonzero child exit, interruption,
lost response, malformed receipt or missing progress leaves that record unresolved.
The next invocation refuses to launch. A zero exit alone is insufficient:
exactly one new completed private worker receipt must match the expected job,
worker, image, environment, synthetic metrics and coordinator completion readback.

The dispatcher lock is scoped to its state directory. The worker additionally
reserves the canonical Engine-bound host gate before claiming; another scratch
root cannot bypass that reservation. All Docker writers must use the same host
account/state location and gate. This is not protection against direct Docker
administrators or an exactly-once claim API.

## Reviewable per-node control wrapper

`ops/processingctl.py` is the source for a node-local wrapper. Install a copy in
each host's private installation root beside `installation.json` and
`runtime.env`. The metadata identifies that host's local `nodeId`, Docker
`engineId`, gate `generation`, pinned `workerImage`, and paths for the matching
source, scratch, and dispatcher state. The directories must already exist,
belong to the service account, be private, and remain under the installation
root. The wrapper reads `PROCESSING_API_TOKEN` from the private environment
file and passes a small OS/Docker environment allowlist plus that one token to
the dispatcher child. An optional, separate
`PROCESSING_PROTOTYPE_OPERATOR_TOKEN` enables `cancel-queued`; it is used only by
the local wrapper for that operator request and is never passed to a child.

The wrapper refuses non-loopback coordinator origins and non-digest image IDs.
It derives the worker policy from the installed node identity: cluster uses the
bounded fixture policy, main uses `main-unrestricted`. It does not accept a
remote node override; Docker operations run on the same host whose Engine and
gate are recorded. Example invocations are:

```sh
python3 /private/processing/processingctl.py --root /private/processing status
python3 /private/processing/processingctl.py --root /private/processing dispatch --max-jobs 2
python3 /private/processing/processingctl.py --root /private/processing cancel-queued --job-id UUID --note 'wrong node profile'
```

`cancel-queued` requires the separate operator token and only transitions a job
that is still queued. If a worker wins the atomic claim race, cancellation
returns a conflict and does not stop that worker. Retrying cancellation after a
successful response loss returns the same terminal audit record. It does not
delete the job or any input/output data.

This refactor is reviewable source only. It has not been installed on `.88` or
`.89`; the November 1 main-node acceptance used the existing one-shot dispatcher.
The wrapper is still a foreground command, not a supervised polling worker, and
does not provide automatic recovery after an uncertain child result. Do not
describe it as unattended production dispatch.

The complete repository suite was run on November 1, 2026, in WSL Ubuntu 24.04
as an unprivileged user: `python3 -B -m unittest discover -s tests -q` reported
1,313 tests passed and 22 skipped. The skips are platform/optional-dependency
gates. This is local repository evidence; it does not install the wrapper,
activate the v2 TreeDar contract or prove Viewer acceptance. The optional
Draft 2020-12 oracle was skipped because `referencing` is not installed in that
WSL environment.

## Recovery

1. Stop dispatching. Inspect the saved launch record, child process, private worker
   receipt and exact named Docker engine container. Do not remove a stale-looking
   lock or guard simply because its process is absent.
2. Stop and inspect any old worker/container. Resolve a running/held coordinator
   claim only through the documented operator procedure after proving execution
   stopped. A completion-pending worker may replay its exact saved completion;
   this does not rerun the engine.
3. Reconcile the actual job with its expected dispatch record. Preserve the old
   state/receipts and document the outcome. There is no automatic guard reset or
   retry command. To retire an uncertain dispatcher instance after reconciliation,
   archive its entire private state directory and use a new explicit state root;
   never relabel an uncertain record as successful merely to proceed.
4. Keep unresolved worker receipts out of a new worker invocation until their
   outcomes have been reconciled. The worker independently refuses unresolved
   receipts. Submit a new request ID only if a new execution is intended.

Successful jobs retain stopped worker containers, synthetic artifacts and private
receipts for acceptance. Normal dispatch does not delete them. After exporting
metadata and verifying stopped containers, clean only that run's resources.
Never prune shared Docker resources or delete original datasets.

## Acceptance without image builds

`scripts/prototype_dispatch_smoke.py` creates a separate tiny coordinator and two
synthetic submissions (staging first, production second), invokes the dispatcher
once, verifies both real TreeDar outputs and execution order, then removes its
exact containers and scratch. It uses existing pinned images and performs no
builds, pulls or actual-data processing:

```sh
python scripts/prototype_dispatch_smoke.py \
  --coordinator-image sha256:VERIFIED_EXISTING_COORDINATOR_ID \
  --engine-image sha256:VERIFIED_EXISTING_TREEDAR_ID \
  --work-parent /private/approved-test-parent \
  --evidence /private/new-dispatch-acceptance.json
```

The acceptance coordinator/state use tmpfs and are destroyed afterward; that
tests serial dispatch, not durable coordinator restart. The separate API smoke
and earlier host evidence cover restart holds. Failure preserves private scratch
and uncertain worker containers for inspection. No claim token or image content
belongs in the metadata-only acceptance export.

On Linux, bind-mounted helper scripts must be readable by engine UID10001; keep
their source deployment parent private while using readable script files. Engine
outputs belong to UID10001. After inspected-stop verification, the acceptance
harness uses a small network-disabled helper under that same UID, mounting only
each verified job output directory, to remove its contents. It checkpoints safe
evidence before cleanup and marks cleanup verified only after all removals pass.

NodeODX/TreeDar stage chaining still requires
[artifact and ground/reference admission](engine-handoff.md).
