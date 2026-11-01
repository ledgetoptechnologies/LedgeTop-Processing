# Pending synthetic lost-reply and sender-restart rehearsal

This is an unexecuted extension of `SYNTHETIC_VIEWER_ACCEPTANCE_PLAN.md` and
requires independent review of the exact modified fixtures and host runner
before any containers are created or stopped.

The Processing server runs the actual HTTPS handler, API and SQLite acceptance
store. For the first successful 200 response only, a fixture reply wrapper
closes the owned server-side TLS connection after the real acceptance transaction
returns and before any response bytes are sent. This simulates an ambiguous
lost response after commit; it is not an outage of the entire host. The server
then accepts one real exact replay and self-terminates after the second request
or a 35-second fallback timer.

The first Viewer container process stages three bytes and an empty result in a
private Linux directory, creates the real held outbox and sends once. It must
observe retry exhaustion and leave the held receipt in its SQLite database. It
records a synthetic hash of the real held payload in a local SQLite evidence
table and exits normally. The host runner checks the same owned container ID
has exited successfully and the retry marker exists, then starts that same
container again. This creates a new Node process while preserving its container
filesystem. On restart the actual `readHeld` payload must hash identically, and
the real Viewer sender must observe the matching response to its exact replay.
The Processing store must report created=true then created=false and one row.
This tests a clean sender process restart; it does not simulate abrupt power
loss, unflushed storage, Viewer publication, engine work, or a server restart.

Ownership and signals: the same already-reviewed, uniquely labeled two
containers and independent Windows Job Object outer watchdog remain in use.
The runner uses only full recorded IDs to start, inspect or kill owned
containers; the outer reconciles only exact unique planned names and matching
run labels, then uses their full inspected IDs. Images, containers, exported
commit snapshots, SQLite state and result files are retained. No recursive
cleanup. Review the modified files, compare the runner against its previous
reviewed version, and confirm timeout behavior before execution.

## Synthetic result (November 1, 2026)

The reviewed harness passed using Processing snapshot
`427c22f633643e4a05d13b3da21c764700f44a39` and Viewer
`46f3471748ade908773ea6cf60bba02e2568dabf`. The first real acceptance committed;
the fixture discarded its response. Viewer retained the held receipt and
reported retry exhaustion. A new Node process read the same held payload from
SQLite and successfully replayed it; Processing returned created=false and
retained exactly one acceptance row. Both owned containers exited zero and the
outer Job Object reported zero active processes.

Server fixture SHA-256:
`05578234817c885a1f3f37c9c04bf9fe1ab9860f4fa088817b06c157e692a05b`.
Sender fixture SHA-256:
`a537e4c43f36b442c6075b2fcff5fa049a32332a81a670bd5447d7e72b90850e`.
Retained sender container:
`828cbbe18bd4b7383a07a2c4d6177fbfb8d5998ecb848ceb30e4b41e4ee8477b`.
Retained Processing container:
`cef68fb9416279cc6da2f012dccf7ce66b53472c3ac034fd185575284b8c3a75`.
Retained root:
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-0vzi_cfk`.
The documented scope limits above apply; no artifacts were deleted.
