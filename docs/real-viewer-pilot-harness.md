# Real Viewer pilot harness: review candidate

This is an isolated test harness, not an installed service or production
deployment. `prepare_pilot_input.py` makes a new owned directory with 20–25
sorted JPEG copies, rehashes each copy and its source, and preserves a partial
directory on error. It never deletes or moves the source. The worker uses the
actual Viewer repository and processing/derivative workers, normal 20 GiB / 10%
storage reserves and explicit `gltf` and `pc-ept` options. It consumes a token
from stdin, not the command line, environment, manifest or receipt. Attempt
identity is persisted before provider initialization; neither failures nor
completion automatically delete input, output or backend tasks.

The exclusive, fsynced registry marker rejects another pilot using the same
mounted registry, including a new root and after a process restart. It is a
single-host guard, **not** a global coordinator fence: another machine, another
registry mount, or an ordinary Viewer can still submit a duplicate. The
between-iteration monitoring deadline is advisory; a running upload, download
or derivative can take longer. Timeout or a process exit never implies the node
task stopped. An explicit, independently reviewed recovery/inspection procedure
is required before clearing markers or attempting another submission.

`run_real_viewer_pilot_fake.py` launches the actual worker against a fake
coordinator inside one owned, network-none container, using a hash-verified
converter and a persistent task-owned fixture directory. Its test input is
invented and the fake NodeODM output is a synthetic mesh; it verifies the
archive/tiles hashes and `ready_for_review`, rejects repeated invocation against
the same root and a second root sharing the registry, and removes only its
verified container. Fixture files and database remain for inspection.

`inspect_real_viewer_pilot.py` is an owner-bound inspector for a pilot whose
writers were independently confirmed stopped and SQLite fully checkpointed. It
refuses WAL/shared-memory/journal sidecars and opens an immutable database to
avoid creating them; it cannot prove writers are stopped or resume/clean work.
The fake Viewer left WAL/SHM files even after its container exited, so this
inspector correctly refuses that retained state. A safe WAL snapshot requires a
qualified launcher that records and verifies the exact container/host process
identities and exclusive storage ownership, proves all writers have exited, and
copies the database and WAL together into a separately owned snapshot before
any recovery/checkpoint of that copy. The current harness has no such writer
fence or offline-snapshot procedure; do not assume a stopped container proves
that no other process can write the database.
`advisory_live_pilot_preflight.py` checks a
point-in-time sample of the expected local source/converter and cluster
identity, stage-token binding, queue and free space without copying photos or
printing credentials. It deliberately reports the registry ACL, external
writers, the pilot storage filesystem, actual processing disk requirements and custody as unverified. It
does not attest readiness or enable the worker's live path.

**No live launcher or automated recovery is implemented. Do not point the
worker at a live coordinator.** Before any real run, independently review and
qualify: a launcher that pins the reviewed Viewer/coordinator/converter images,
uses a fixed trusted registry, verifies the staging credential in memory and
actual cluster-only routing, checks node idleness/space and external writers,
starts a uniquely owned supervised worker, persists and reconciles all task
identities, and retains partial copies/results on ambiguity. The source copy and
input directory must receive appropriate host OS access controls. The live
engine's finite terminal retention, possible loss of derived test output during
long outages, and lack of cross-host duplicate fencing remain open. Never use
this harness as evidence of scheduler release readiness or large-data capacity.
