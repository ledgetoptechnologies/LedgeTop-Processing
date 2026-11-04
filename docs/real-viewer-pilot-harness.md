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
