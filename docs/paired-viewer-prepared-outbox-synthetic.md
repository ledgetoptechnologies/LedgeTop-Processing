# Paired Viewer prepared outbox synthetic test

This test runs Viewer `7dbd175d1b501edc4b8414328e9d7ff2bfd2837f`'s actual
`openDatabase`, `ProcessingRepository`, `PreparedSubmissionOutbox` and
`PreparedSubmissionClient` against the Processing prepared-v3 candidate based on
`99bf106`. The Viewer checkout must be clean at that commit and is only read.
Both databases, source manifest, credentials, lease and TLS certificates are
synthetic. A local private CA is installed into only the test child process.

The test persists Viewer intent under a real submit lease in one Node process and
reopens it in another. The actual client POST reaches the local TLS Processing
endpoint and commits to its real SQLite queue; a test wrapper then discards the
POST reply to simulate uncertainty. The client performs its own authenticated
GET and stores the returned exact binding. A local replay makes no further
request. Processing independently checks exact POST replay, conflicting replay,
wrong principal/environment, missing/duplicate/wrong authority header and wrong
configured authority with zero pre-admission jobs. An expired Viewer submit lease
fails before new network activity. A separate Processing process takes the closed
coordinator lease, reopens the SQLite state and recovers the same job/digest; the
original test process then reopens it and repeats the authenticated lookup.

This is an orderly process/lease handoff, not a kill during a SQLite transaction.
The wrapper discards an already received POST response; it is not a real network
partition. The HTTPS listener is a bounded test wrapper calling the unmounted
candidate API with `protected_transport=True`, not an installed production
listener. No Viewer source-grant or accepted-copy route is enabled; no source
fetch, activation, execution, Viewer custody, cleanup or client data is tested.
The test still depends on authenticated principal configuration, correct trusted
CA and independently provisioned coordinator authority; copied database identity,
rollback and real cross-host deployment need external fencing.

Run with a clean Viewer checkout, Node and the isolated JCS dependency:

```
VIEWER_7DB_ROOT=<viewer-7dbd-checkout> PYTHONPATH=<pinned-rfc8785-dir> \
  python -m unittest tests.test_paired_viewer_prepared_outbox -v
```
