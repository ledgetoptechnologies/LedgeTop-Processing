# Pending empty Viewer schema upgrade plus acceptance rehearsal

This unexecuted extension pins committed Viewer
`0c2ffce69856a9b873bddd77aa660abd8b83897c` and uses the existing reviewed
three-container synthetic offline retry watchdog. An independent review of the
exact modified sender and runner is required before any container operations.

Within the retained synthetic Viewer SQLite database, the sender installs the
committed historical `native-reference-schema-46f.json` DDL, checks its exact
SHA-256 `1f48269844fcca486135322b240a7fb94949c354ed046b29ad571d97c299e62a`,
and creates a separate synthetic unrelated row. It calls the actual
`upgradeEmpty46f(db,true)` and `attest`, checks the unrelated row survived,
then uses the upgraded schema for real synthetic staging, held receipt,
credential reload, offline retry and Processing HTTPS replay. The runner
requires an explicit schema-upgrade success marker and logs the schema hash.

This proves only the empty exact historical candidate upgrade in a synthetic
SQLite database combined with the previous three-byte acceptance rehearsal. It
does not prove migration of any nonempty candidate schema or production Viewer
database, or authorization to deploy. The original unit tests separately hold
nonempty and altered legacy schemas; those tests are not invoked by this joined
fixture. No container ownership, signal or cleanup logic was intentionally
changed. All roots, snapshots, data, images and containers remain retained; no
recursive cleanup.

## Synthetic result (November 1, 2026)

The reviewed harness passed with Processing snapshot
`bbd6d2b445fb1b9a113a58c01c91874c552b4288` and Viewer
`0c2ffce69856a9b873bddd77aa660abd8b83897c`. The exact empty 46f candidate
schema upgraded and attested while preserving unrelated synthetic data. The
actual bound sender reloaded synthetic credentials through an offline retry,
then replayed its unchanged held receipt after clean process restarts. The real
Processing HTTPS endpoint returned created=true then created=false, with one
receipt retained. This does not qualify nonempty schema migration.

Legacy schema SHA-256:
`1f48269844fcca486135322b240a7fb94949c354ed046b29ad571d97c299e62a`.
Sender fixture SHA-256:
`98bf51a899d312ee505bc4b0a01fc6cce91eb79d819f06fe03816b3fd33d2108`.
Server fixture SHA-256:
`a7d53a7e2f26f0802dc3305a7e6943c5a6228d51ba4acee02afb75c8707bc99a`.
Retained Viewer container:
`b74298209ddf9b4d7adc89f7616b8fdf6ed5a7db6f22203404b082d55c4a8442` (exit 0).
Retained Processing container:
`c488315c1a794d18a023b552ec5b4c6d563ec313441aaf74584e168418bc2244` (exit 0).
Retained namespace holder:
`8b0dba2e46dca6a76866adb5ca26fdd5b8296878c87ce079299fa54ef0aa36c4` (exit 137
following its verified owned stop). The outer job had zero active processes and
no uncertain names. Retained root:
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-71z99ayo`.
No test resources were deleted. The synthetic scope limitations above apply.
