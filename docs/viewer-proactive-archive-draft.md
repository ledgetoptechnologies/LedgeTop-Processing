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

Interrupted automatic transfers without a verified receipt are preserved and held for operator inspection; automatic retry will not remove or overwrite those partials. Failures before a partial is created can retry automatically. This limits availability until a separate safe recovery procedure exists.

The task info response reports `ledgetopArchive` separately from processing success: `pending`, `inspection`, `recorded`, or `disabled`, with only fixed sanitized reasons. Pending/inspection state is persisted and survives coordinator restart. Recorded means an archive path was recorded; the file may now be missing or corrupt and Viewer has not necessarily acknowledged it. A sanitized process-local health signal indicates status persistence failures. Since that signal resets on restart and no complete durable reconciliation exists, enabled capture always reports inspection required with status_unverified_after_restart until such reconciliation is implemented; it never asserts healthy merely because the process restarted. A failed database can also make the API unavailable. Enabling capture alters the Viewer task table; the existing strict update and rollback census rejects this migration. Do not enable or deploy until the update and rollback controller is separately qualified for this schema.

Schema installation uses one immediate SQLite transaction and refuses a partial or unrecognized prior archive schema. The reference coordinator updater in this repository is pinned to a historical one-off environment; its synthetic census tests do not qualify any current live deployment.
