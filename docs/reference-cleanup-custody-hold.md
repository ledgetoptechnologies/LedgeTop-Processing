# Hold unqualified result cleanup

Reference-v1 acceptance records identify coordinator metadata and accepted
object coverage. They do not independently establish durable Viewer custody or
exclusive authority over Viewer writers. The Viewer synthetic rehearsal sends
acceptance before ingest, so accepting it must not authorize deletion.

This candidate refuses worker daemon configuration with `cleanupEnabled` or
`engineCleanupEnabled` set to true. The reference driver also rejects cleanup
executors at construction and holds if an executor is attached later. Receipt
processing continues when both cleanup options are disabled. Existing engine
tasks, input files and result objects are retained; this is not automatic
reclamation or permission to alter previously running workers.

The lower-level cleanup APIs still exist for isolated tests and are not a host
security boundary: direct calls to `ScratchCleanupExecutor` or
`EngineCacheCleanup`, legacy/previously running workers, and privileged host
writers are not prevented here. Attaching an executor after initialization
holds the entire driver step, including further acceptance and execution; this
is intentional until configuration is corrected. A separate qualified release
must establish actual Viewer
durable custody, authenticated attempt and generation binding, writer fencing,
and a reviewed release and recovery protocol before enabling any deletion.
This change does not establish any of those prerequisites.

## Local verification

On Ubuntu 24.04 under WSL, `python3 -m unittest tests.test_reference_driver
tests.test_reference_worker_daemon_routing` ran as non-root: 45 tests, 43
passed, two legacy cleanup tests intentionally skipped. An earlier run as root
had four TreeDar routing configuration errors; the same four also occurred on
the unchanged base. The fixture explicitly rejects root for that path; rerunning
as an unprivileged account passed all 16 routing tests. Windows ran 45 tests
with 20 skips (18 Linux-only and the two intentionally held cleanup tests).
All are local synthetic tests, not deployed-worker or power-loss qualification.
