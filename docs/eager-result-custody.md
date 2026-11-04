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
(`bytes` and `observedAt`, the last reading taken), `pendingTasks` (every task not yet
verified), `resumeScanned`, and one page of those tasks in `tasks`.

The page is `?after=<position>&limit=<1-500>`, default 100. `next` is the `after` for the
following page and is null only on the last, so read until it is null. Nothing is left
out for being old, refused, abandoned or removed by Viewer. Each entry has `taskId`,
`jobId`, `removed`, `state`, `reason` if any, `retained` if any, `attempts`, `abandoned`
and `operatorActions`.

Each task's info carries `ledgetopResultCustody` with the same state:

| State | Reason | Meaning |
| --- | --- | --- |
| `fetching` | | A transfer is running. |
| `pending` | | Owed a transfer. A reason says why it has not happened yet. |
| `pending` | `custody_record_unwritten` | The sidecar could not be written. The job is unaffected; it is retried, and this survives a restart. |
| `pending` | `archive_fetch_failed` | The node could not be reached or the transfer failed. Retried. |
| `pending` | `engine_task_missing` | The engine does not know the task. Retried, in case it is back from an outage. |
| `pending` | `guard_unavailable`, `production_held`, `admission_closed` | A gate is closed. Retried, and not counted as a failure. |
| `deferred` | `insufficient_scratch` | Free space would fall below the reserve. Retried. |
| `deferred` | `free_space_unavailable` | The filesystem could not be read. Retried. |
| `deferred` | `custody_budget_exceeded` | The maximum would be passed. Retried. |
| `verified` | | The bytes are held and bound to the finished run. |
| `refused` | `engine_identity_unavailable` | The engine omitted evidence needed to bind a result. |
| `refused` | `engine_identity_mismatch` | The engine reported other inputs than this task submitted. |
| `refused` | `engine_result_changed` | The engine reported another run after the first read. |
| `refused` | `result_bound_too_late` | Viewer's download came before any record of the run. |
| `refused` | `archive_exceeds_census_limit` | The file would stop an update. |
| `refused` | `archive_members_unreadable`, `archive_requires_inspection`, `custody_record_invalid` | Files a person must read. |
| `abandoned` | `operator_abandoned` | An operator ended what was owed. |

`retained: true` on any state but `verified` means bytes are here anyway: Viewer's own
download, or a retry below. So a refused job's status changes when Viewer downloads it.

None of this changes the job or Viewer's download, which works as it did before.

## Retries, refusals and abandonment

Nothing automatic ever gives a result up, and no fetch failure, timeout or Viewer call
abandons one or deletes anything.

- **Transient** (`pending`, `deferred`) is retried without end. A failure backs off from
  two minutes to an hourly cap; a disk or budget wait repeats every five minutes. The
  count is in `attempts` and survives a restart, so a restart does not reset the rate.
  An outage of any length ends in the fetch succeeding if the engine still has the task.
  A task the engine has truly cleaned up therefore stays `pending` at one read an hour
  until an operator abandons it.
- **Hard refusals** (`refused`) make no more attempts by themselves, and are listed until
  an operator acts.
- **Viewer's remove** does not end custody. Remove keeps its meaning, hides the task from
  Viewer and returns as it always did, because Viewer has no acknowledgement of ingestion
  and also uses remove as an upload reset, so it says nothing about the bytes. Custody
  carries on and the list shows `removed: true`. Re-initializing a UUID that is still
  listed is refused with 409, as that would replace the row that owes the custody.

Both actions below need the operator credential, take exactly `{"note": "..."}` (printable
ASCII, up to 256 characters) and exist only while the flag is on. Each is refused with 409
if the job is verified or owes nothing, or while a transfer or Viewer download of that task
is running, and with 503 while admission is draining. `who` is `operator_credential`: there
is one operator token, so the note must name the person.

`POST /v1/prototype/jobs/<job id>/custody/retry` evaluates the task again at once, from any
listed state, including a mistaken abandonment. It takes whatever run the engine reports
now. If the task then binds, it is verified. If a refusal remains, the bytes are fetched
and held anyway and the state stays `refused` with `retained: true`: the operator has asked
to keep them, not vouched for them. The census limit always holds. If the engine cannot
answer, this is a transient failure and the retry waits for it.

`POST /v1/prototype/jobs/<job id>/custody/abandon` ends what is owed. It records who, when,
why and the state it ended, in the sidecar's `abandoned` and `operator` fields. It deletes
nothing and repeating it changes nothing. The task stays listed.

Releasing retained bytes is not part of this. Nothing here deletes an archive. Viewer's
acceptance (`viewer_staged_ack`, the accepted-copy receipts) is what may later permit a
release, and only a verified result qualifies; a failed fetch never does.

## What a verified receipt means

A result is bound by the engine's `uuid`, node, `dateCreated`, `imagesCount`,
`options` and `processingTime`. All must be present and agree with what the task
submitted. They are read at code 40 and again after every member has been read, and
both reads must match. Bytes Viewer's own download left need the same second read.

## Restart and shutdown

The intent to take custody is the fixed value `eager_custody_pending` in a finished
task's `viewer_tasks.error`, written in the transaction that makes the job succeed.
Nothing else reads that field on a finished task, and info never shows it. There is
no new table, column or file, so the update census allowlist does not change. Only
`verified` clears it, which is what makes it the list above, and the sidecar holds the
state, so a restart preserves every state. It resumes every task holding it, without a
limit, and leaves refused and abandoned tasks waiting; a task finished before the flag
has none and is not backfilled.

Shutdown wakes every custody read, including one blocked on a socket, then waits
for every dispatch, adoption and custody writer before the queue closes. A dispatch
is waited out however long it takes. A result fetch gets ten seconds and then a
sixty-second grace, which covers the engine read timeout. If one still survives, the
coordinator keeps the queue lock and authority lease, reports
`result_fetch_drain_failed` and exits 2. Process death then ends that writer and
releases both. Any fetch that outlasts the ten seconds makes the exit 2, even if it
leaves in the grace and the queue closes normally. Exit 2 here is a safe stop, not a
corrupted state: restart after checking the engine, and the fetch resumes.

The socket wake is Linux behaviour. On Windows `os.dup` refuses a Winsock handle, and
`shutdown` on any handle of a socket leaves a blocked `recv` asleep; only closing every
handle wakes it, which a reader thread cannot safely have done to it. A blocked fetch
there ends at the read timeout or takes the fail-safe exit above. Production is Linux.
