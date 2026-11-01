# Read-only archive-state preflight draft

`ops/archive_state_preflight.py` composes the stopped-volume census from the
existing #238 rehearsal. A caller supplies a trusted prior keyed census and an
independently qualified, exact candidate schema-object fingerprint map. It
collects the census twice, compares both to the prior census, and holds for
uncertain partials, rejected tree entries, spool content, active jobs, SQLite
sidecars, invalid database checks or an unqualified schema. It writes no state.

This is an unwired library experiment. It does not authenticate its caller's
manifest or schema, associate either with an actual image, or establish the
candidate's compatibility. The candidate-specific schema map and manifest
generation/authentication workflow still need design and qualification.
Never generate an allowlist from the same untrusted current state and treat it
as approval. It does not acquire a writer fence; two matching snapshots cannot
detect changes that were reverted between reads, and the result becomes stale
immediately if writes remain possible. No preflight result authorizes cutover,
backup, restore, cleanup or rollback. Do not run this on live data until an
independent authority and safe operational integration are qualified.
