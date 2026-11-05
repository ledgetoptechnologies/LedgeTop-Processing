# Installed predecessor metadata diagnostic — November 5

The reviewed metadata-only probe (`54ff936` plus connection-local read-only
hygiene `266757f`) ran over existing SSH into the running original coordinator
container. The trusted script and reviewed schema traveled only through Python
stdin; no package, script or contract file was written into the container.
The ephemeral private census key and generated source were not logged or saved.

Bounds: five seconds cooperative time, one MiB direct receipt/metadata reads,
eight MiB SQLite file cap, one MiB logical metadata and an outer 20-second SSH
deadline. SQLite used immutable read-only mode, query-only enforcement and
in-memory temporary storage. Photo and ZIP content hashing was disabled.

Observed original authority was the expected configured UUID; the state database
was 278,528 bytes. Expected aggregate counts were 22 terminal jobs, seven Viewer
tasks, zero reservations, seven option-evidence rows, 155 input rows and 16 owned
maintenance receipts. These are point-in-time observations, not writer exclusion.

The probe exited with the bounded refusal `current_node_plan_not_closed` in
under one second. Its exact process/start identity was checked afterward and
was no longer alive. There was no stderr or retained task process.

Follow-up aggregate-only reads found:

| Node | Current revision | Owned receipts | Latest owned revision | Current plan matches any owned receipt |
| --- | ---: | ---: | ---: | --- |
| cluster | 25 | 8 | 22 | No |
| main | 34 | 8 | 31 | No |

Both nodes were active with null result, update hash, finish status and after-job
binding. The frozen predecessor's direct schedule/cancel API can legitimately
leave exactly this residual plan shape: cancel changes state and revision while
retaining plan dates/target. The first validator incorrectly required every
current plan to derive from the latest owned schedule. A separately reviewed
closed-direct-history classification is being added; it must preserve all
original fields/receipts and must not synthesize ownership or grant update,
execution, cleanup or cancellation authority.

This refusal is not a successful custody census or cutover approval. No service
was stopped, lease acquired, journal recovered, file removed, permission changed
or TrueNAS configuration altered. Full content verification, stopped-writer
ownership, declared startup transforms and rollback remain separate gates.

## Follow-up after the closed-direct-history correction

`ba62b79` integrates the exact closed-direct maintenance classification from
`a0a35cc`. A second bounded, read-only stdin probe passed that classification
and then exited 2 with `unknown_predecessor_path`. There was no stderr; its own
PID/start identity was verified absent afterward. This is another safe refusal,
not a successful custody or activation check.

Read-only path metadata identified `pilot-journals`: three private 0700
directories, each holding eight private 0600 files owned by UID 10001. They are
node journals, cancellation intents and child-start/exit records; content was
not changed or treated as proof of a completed lifecycle.

Historical `da629ae` and `c43192f` already describe bounded opaque retention for
these records, but no trustworthy committed producer/schema was found. The
current census must explicitly retain their closed, keyed byte inventory under
strict path/owner/type/size bounds. Filenames alone cannot prove cancellation,
child exit or supervisor absence. That custody work and stopped-writer proof
remain open; no file was deleted or wildcard allowlist introduced.
