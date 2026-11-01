# Pending synthetic Processing restart acceptance rehearsal

This unexecuted extension requires independent review of the exact changes
before any container action. It retains the ownership, watchdog, snapshots,
resource limits and no-cleanup rules of `SYNTHETIC_VIEWER_ACCEPTANCE_PLAN.md`.

The first Processing process uses a file-backed SQLite database under its own
retained container filesystem. After a real acceptance commit, the fixture
discards the response, shuts down the server normally, closes the database and
exits. The first Viewer process reports retry exhaustion, retains its held
receipt and exits. The host runner verifies both exact container IDs have
stopped with zero exit status and Processing reported one retained row.

The runner then starts the same Processing container again. It must reopen the
same SQLite file and report one pre-existing receipt before accepting a request.
The runner then starts the same Viewer container again. Its new process checks
the persisted held payload SHA-256 and sends the replay to the actual Processing
HTTPS endpoint. Processing must report `created=false` and one retained row.
Both containers must exit zero and the outer Windows Job Object must report no
active owned processes. The DNS/TLS setup and synthetic three-byte data are the
same as the prior rehearsal. Note that Docker's shared-container network
namespace may prevent reuse across a server container restart; a failure there
is a fixture limitation rather than proof about the protocol.

This is a clean, deliberately induced Processing interruption and process
restart on a filesystem-backed SQLite database. It does not test host power
loss, abrupt kill or database durability across hardware failure, actual
processing, Viewer publication, or migration. No container operations or
signal tests have been performed for this version.

## Synthetic result (November 1, 2026)

The reviewed run passed with Processing snapshot
`7f0c24996e3ae1701c62df8384e3d9019009e7cd` and Viewer
`46f3471748ade908773ea6cf60bba02e2568dabf`. Processing committed once and the
fixture discarded the response; both processes exited cleanly. The restarted
Processing process reported one existing SQLite receipt, and the restarted
Viewer process verified its held payload was unchanged. Replay returned
`created=false`, with one retained receipt.

Server fixture SHA-256:
`69704348f99a7abaef5c91bccd15b5820f1dfa2cc06152dffd695ec26cf9ff1d`.
Sender fixture SHA-256:
`a537e4c43f36b442c6075b2fcff5fa049a32332a81a670bd5447d7e72b90850e`.
Retained Viewer container:
`2009da9a4da3bf3d6832d44ddf5660d2161fa7019153e2ea9bffa2fd858ec729`.
Retained Processing container:
`06440fe457232b9adeda3dc39128dccae0beea0d82292d3f3b106db7fe536c7e`.
Both exited zero; the outer job reported zero active processes. Root retained:
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-yhgw5993`.
No artifacts were removed. This is evidence of clean process restart only;
it does not establish crash or power-loss durability, nor make a claim about
SQLite synchronous settings of other connections.
