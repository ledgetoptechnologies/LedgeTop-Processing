# Eager result custody

`PROCESSING_VIEWER_EAGER_RESULT_CUSTODY=1` copies a normal success's `all.zip` from
the engine into the task directory before Viewer asks, because NodeODM cleans
finished tasks up. It is prototype-only, off by default, and not enabled by any
Compose file. With it off nothing here runs. Viewer's own download is the same with
it on or off, and a custody failure never changes the job.

## What an operator must size

Turning the flag on requires both values below as positive whole bytes. There are
no defaults: the coordinator refuses to start without them, before it takes a lease
or touches state.

| Variable | Meaning |
| --- | --- |
| `PROCESSING_VIEWER_EAGER_CUSTODY_RESERVE_BYTES` | Free space a fetch must leave on the filesystem that holds the archive. |
| `PROCESSING_VIEWER_EAGER_CUSTODY_MAX_BYTES` | Total bytes of `all.zip` that eager custody may retain across all tasks. |

Size them for the real filesystem, on the host that will run the flag:

- Free space is read with `statvfs` on the task's own directory each time a fetch
  checks, so a dataset quota or a separate mount counts. Do not assume the state
  volume shares a pool with anything; check it.
- The reserve must cover whatever else needs that filesystem: uploads, the Viewer
  spool, engine work if it is shared, and any free space an update must find.
- The maximum must fit within free space less the reserve. Nothing deletes a
  retained archive, so reaching it stops custody until an operator removes tasks.
- A file over the updater census limit is refused outright, as a larger one would
  stop an update. That limit is fixed at 20 GiB per file and is not configurable.

Examples only, observed on the live cluster host and likely stale by the time you
read this: about 113 GiB free, against that 20 GiB per-file census limit. At that
figure the 164 GiB reserve assumed by earlier code could never be met. Measure again.

## Reading the state

`GET /v1/prototype/result-custody`, with the API or observer credential, exists only
while the flag is on. It returns `reserveBytes`, `maxBytes`, `fileLimitBytes`, `lastFree`
(`bytes` and `observedAt`, the last reading taken), `pendingTasks` and
`resumeScanned`. Startup logs the policy and the free space it found.

Each task's info carries `ledgetopResultCustody`:

| State | Reason | Meaning |
| --- | --- | --- |
| `fetching` | | A transfer is running. |
| `pending` | | Owed a transfer. A reason says why it has not happened yet. |
| `pending` | `custody_record_unwritten` | The sidecar could not be written. The job is unaffected; it is retried, and this survives a restart. |
| `deferred` | `insufficient_scratch` | Free space would fall below the reserve. Retried. |
| `deferred` | `free_space_unavailable` | The filesystem could not be read. Retried. |
| `deferred` | `custody_budget_exceeded` | The maximum would be passed. Retried. |
| `verified` | | The bytes are held and bound to the finished run. |
| `unavailable` | `engine_identity_unavailable` | The engine omitted evidence needed to bind a result. |
| `unavailable` | `engine_identity_mismatch` | The engine reported other inputs than this task submitted. |
| `unavailable` | `engine_result_changed` | The engine reported another run after the first read. |
| `unavailable` | `result_bound_too_late` | Viewer's download came before any record of the run. |
| `unavailable` | `archive_members_unreadable`, `archive_exceeds_census_limit`, `archive_requires_inspection`, `engine_task_missing` | No further attempts. |

`unavailable` ends the attempts and proves nothing about the job: Viewer's download
works as it did before.

Generic Viewer removal is refused while eager custody is pending or marked
unavailable. An operator may request `POST /v1/prototype/jobs/<job-id>/retry-custody`
with `{"note":"inspection summary"}` and the operator credential only when the
reason is `engine_task_missing`. The current engine task must again report code 40
and every required result-identity field must match the original complete
observation. The operation records a durable retry intent before changing the
sidecar; it does not reassign or reprocess the job. A changed or unavailable
identity remains held. The operator note is emitted in the application log;
this is not a durable audit journal. Other refusal reasons require inspection and
have no retry route here. This policy is candidate-only and has not been deployed.

Legacy rows whose intent was cleared remain blocked from removal when their
identity-bound sidecar is unavailable or pending, even if eager custody is off.
An operator retry can restore the database intent lazily for an eligible legacy
unavailable sidecar. A Viewer download alone does not settle that hold: the
operator retry must recheck the original engine result before the archive can be
verified. A missing sidecar cannot establish whether a legacy row was ever held;
upgrade still needs a separate census and review, including previously removed
tasks. A local verified archive is not downstream Viewer acceptance and does not
authorize deleting the last copy.

Removal of a task with a verified custody sidecar requires a checked, complete
sidecar plus an identity-bound receipt and a fresh archive hash check. Older
verified sidecars with `checked: false` require inspection; they are not silently
upgraded. Rehashing can take time for large archives; it runs outside the global
service and database locks. Removal holds the task's download lock and rechecks
the record, receipt and pinned file identity before committing.

## What a verified receipt means

A result is bound by the engine's `uuid`, node, `dateCreated`, `imagesCount`,
`options` and `processingTime`. All must be present and agree with what the task
submitted. They are read at code 40 and again after every member has been read, and
both reads must match. Bytes Viewer's own download left need the same second read.

## Restart and shutdown

The intent to take custody is the fixed value `eager_custody_pending` in a finished
task's `viewer_tasks.error`, written in the transaction that makes the job succeed.
Nothing else reads that field on a finished task, and info never shows it. There is
no new table, column or file, so the update census allowlist does not change. A
restart resumes every task holding it, without a limit; a task finished before the
flag has none and is not backfilled.

Shutdown wakes every custody read, including one blocked on a socket, and waits for
it. If a writer still survives its ten-second join, the coordinator keeps the queue
lock and authority lease, reports `result_fetch_drain_failed` and exits 2. Process
death then ends that writer and releases both. Exit 2 here is a safe stop, not a
corrupted state: restart after checking the engine, and the fetch resumes.
