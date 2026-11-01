# Proactive Viewer-compatible archive capture (draft)

This opt-in feature captures a successful direct Viewer-compatible engine task's
`all.zip` before Viewer asks for it. It does not delete the engine result or
the input photos and does not assert that Viewer has accepted the result.
Existing Viewer download and task status contracts remain unchanged.

`PROCESSING_VIEWER_ARCHIVE_AUTOCAPTURE=1` requires Linux/POSIX allocation, an
active host coordinator authority lease, enabled Viewer, persistent state, and
literal IP addresses for both configured node origins. It is off by default.
`PROCESSING_VIEWER_ARCHIVE_FREE_FLOOR` sets the positive free-byte floor checked
before allocation (default 20 GiB). The archive response must carry a valid
`Content-Length` within `PROCESSING_VIEWER_MAX_ARCHIVE_BYTES`; the child uses
`posix_fallocate` before transferring. An unknown length, low space, allocation
failure, unavailable engine, or uncertain archive is retained for retry or
inspection. The free-space floor is a **point-in-time check**, not an atomic
reservation shared with other uploads, services, or users. Thin-provisioned
storage and external writers may still exhaust the filesystem. Do not enable
this until an operator has reviewed capacity and concurrent-writer policy.

Automatic capture and automatic receipt-bound restore require exactly one
ASCII decimal `Content-Length` and reject any `Transfer-Encoding` before
creating a data slot. An engine or intermediary using other framing will be
held even if its response could otherwise be valid. Existing manual download
behavior is unchanged; framing alone does not authenticate archive contents.

The scheduler scans the existing durable task state, so successful unfinished
captures are rediscovered after restart. A single child performs one capture at
a time. It inherits both the coordinator state flock and host authority lease;
the parent holds its existing per-task download lock while it runs. Shutdown
kills and waits for the child; if it cannot be reaped, the parent thread retains
the task lock and the surviving child retains the OS authority flocks. A new
coordinator must not bypass these locks. Old coordinators or direct filesystem
writers that ignore those locks remain outside this guarantee.

The parent records `archive_path` only after the child has captured, fsynced,
validated and receipt-bound the ZIP. A crash after file publication but before
that database write is retried from the existing receipt, without starting a
new engine task. Tests use synthetic local servers and ephemeral data; this is
not proof of power-loss behavior, real NodeODM retention, Viewer ingestion,
or end-to-end custody. **It cannot prevent engine cleanup while the coordinator
is offline beyond the engine's retention period.** Existing engine 48-hour
cleanup and unsafe restart behavior require their own qualified solution.

When a recorded archive still has its capture intent but its receipt is
missing, delivery holds for inspection instead of treating that file as a
legacy unverified archive. If both intent and receipt disappear, the old and
new formats cannot be distinguished; this safeguard cannot detect that case.

Interrupted automatic transfers without a verified receipt are preserved and held for operator inspection; automatic retry will not remove or overwrite those partials. Failures before a partial is created can retry automatically. This limits availability until a separate safe recovery procedure exists.

The task info response reports `ledgetopArchive` separately from processing success: `pending`, `inspection`, `recorded`, or `disabled`, with only fixed sanitized reasons. Pending/inspection state is persisted and survives coordinator restart. Recorded means an archive path was recorded; the file may now be missing or corrupt and Viewer has not necessarily acknowledged it. A sanitized process-local health signal indicates status persistence failures. Since that signal resets on restart and no complete durable reconciliation exists, enabled capture always reports inspection required with status_unverified_after_restart until such reconciliation is implemented; it never asserts healthy merely because the process restarted. A failed database can also make the API unavailable. Enabling capture alters the Viewer task table; the existing strict update and rollback census rejects this migration. Do not enable or deploy until the update and rollback controller is separately qualified for this schema.

Schema installation uses one immediate SQLite transaction and refuses a partial or unrecognized prior archive schema. The reference coordinator updater in this repository is pinned to a historical one-off environment; its synthetic census tests do not qualify any current live deployment.

Retry backoff is scoped to the Viewer task generation and Processing job as well as its public UUID. Once the identity can be read, reusing a legacy UUID for a new incarnation does not inherit the prior job's delay. If identity is unavailable, the bounded metadata-read retry delay can also delay discovery of a replacement, by up to 256 seconds under the current exponential backoff. Backoff remains process-local, and capture still depends on coordinator availability and NodeODM retaining its result.

Deferred entries are scanned without a quarter-second delay per entry, with a 100ms yield each 64 entries and a quarter-second pause when the cursor wraps. This reduces backlog-induced delay while limiting the scan rate, but a larger backlog still delays discovery; database latency, capture time, and time before engine cleanup are not bounded. If identity becomes unreadable during a remove/recreate gap, the resulting unknown-identity delay can also postpone the replacement until that delay expires.

If cleanup cannot confirm that a capture child exited, the service retains its process handle, refuses further captures and on-demand downloads, and reports shutdown failure. This uncertainty is process-local; it is not a durable reconciliation or a whole-system quiescence guarantee. A download already entered for another task is not drained by this guard. The normal child timeout path may wait indefinitely for reaping while retaining the task lock; it does not provide a hard shutdown deadline.

If Viewer shutdown fails, the coordinator does not explicitly close its own queue authority while that process remains alive. The current main process still exits on failed shutdown, releasing its descriptor, so this is not an authority fence across process exit and does not qualify an update or rollback. External writers and engine tasks remain outside this safeguard.
