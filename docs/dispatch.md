# Bounded synthetic queue dispatch

`scripts/prototype_dispatcher.py` drains already-queued TreeDar synthetic jobs in
one foreground invocation. It automatically invokes the existing worker once per
job, serially, for at most 1–10 jobs. It exits when empty, running/held work blocks
the slot, the head profile is unsupported, or its job budget is exhausted.
It creates no jobs and never runs the real-photo profile.

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

The local lock is scoped to that state directory, not a global host reservation.
The coordinator's single execution slot and worker receipts provide additional
fencing, but this is not a fleet-wide resource manager or exactly-once claim API.

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

NodeODX/TreeDar stage chaining still requires
[artifact and ground/reference admission](engine-handoff.md).
