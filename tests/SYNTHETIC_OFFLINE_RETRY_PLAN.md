# Pending real offline acceptance retry rehearsal

This is an unexecuted extension of the synthetic clean restart rehearsal. An
independent review of the exact fixture, runner and watchdog changes is required
before any containers are created, started or signalled.

A third uniquely named and labelled, task-owned namespace-holder container
runs only a bounded sleep under `--network none`; it has no listener and no
published ports. The synthetic Processing and Viewer containers join that
holder's isolated network namespace. After Processing commits and the fixture
discards the first response, both application containers exit cleanly while the
namespace holder stays running. The runner verifies the exact Processing
container is stopped, starts the same Viewer container while Processing is
offline, and requires its real sender to exhaust retry while retaining the same
SQLite receipt. Only then does it restart Processing, require the one existing
receipt from its file-backed SQLite database, and restart Viewer again for a
matching exact replay and one retained acceptance record.

The namespace-holder exists only for this fixture; after successful checks, the
runner kills that exact full-ID, matching-label owned container, verifies exit,
and retains it. On any failure the existing `finally` and independent Windows
Job Object outer watchdog inspect only the three exact planned names with exact
run labels before acting on full IDs. The holder also has a 75-second
self-termination fallback. No containers, images, SQLite files, exported source
snapshots or roots are deleted. This changes the number of owned containers
from two to three and changes the outer watchdog's reconciliation list; the
exact new signal paths require review.

This tests a genuine local connection failure while the synthetic Processing
service is stopped, followed by clean restart and exact receipt replay. It does
not test physical network partitions, power-loss durability, real engine jobs,
Viewer publication or crash recovery. Linux socket/TLS behavior, current
snapshots, fixture hashes and exact stopped resource IDs must be recorded if run.

## Synthetic result (November 1, 2026)

The independently reviewed harness passed with Processing snapshot
`9afdb87e63fa21cadabc988e36ca1c7cda3d6e4d` and Viewer
`46f3471748ade908773ea6cf60bba02e2568dabf`. After the response was lost and the
Processing container stopped, the restarted Viewer process attempted delivery
while the server was verified offline and retained its exact held payload.
Processing then reopened one file-backed SQLite receipt and the next Viewer
restart received a matching replay response with `created=false`; one receipt
remained. The namespace holder was confirmed running before its exact-ID stop.

Sender fixture SHA-256:
`0643f08b450de9662203f0dff2ec8aee72fb15c1b42b36059b9dc4c7359843b2`.
Server fixture SHA-256:
`69704348f99a7abaef5c91bccd15b5820f1dfa2cc06152dffd695ec26cf9ff1d`.
Retained Viewer container:
`2ff33f4cfee7c129792b46d454a1fefa7afe3d54660722204dc6f8d938c280d3` (exit 0).
Retained Processing container:
`c297a1ad1838a70c5c7616f990b9d2cec213bb84d0c6afebabc96c7e3d58ce0a` (exit 0).
Retained namespace holder:
`f03330529f0bb1a59c8c04330bd9bddb2ccd59ae322b3428c73e72e548d42d21` (expected
exit 137 after its explicitly owned stop). The outer Job Object reported zero
active processes and no uncertain names. Retained root:
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-a14yxy0s`.
No artifacts were deleted. The above synthetic scope limits still apply.
