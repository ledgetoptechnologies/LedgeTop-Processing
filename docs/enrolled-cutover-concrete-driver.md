# Concrete first-cutover boundary driver

`ConcreteCutoverDriver` plugs into the existing `FirstEnrolledCutover` journal.
It does not introduce a queue, daemon, alternate supervisor or automatic loop.
Construction/import performs no deployment action. The journal alone chooses
the next action and persists its exact per-attempt dispatch marker first.

The driver implements every forward and rollback action: read-only preflight;
restart disabling; graceful original stop; exact helper creation/acquisition;
full retained custody and immutable TAR backup; bootstrap creation/start;
independent intent/drain/seal RPCs; graceful bootstrap stop and original-lease
reacquisition; declared projection; original retention; final creation/start;
authenticated worker/frozen runtime readiness; same-plan resume and restart
policy restoration. Rollback retains candidates and original data, verifies the
appropriate projection, and restarts the unchanged predecessor. It never
restores SQLite, deletes source files, requeues jobs or contacts an engine to
cancel or initialize processing tasks.

Fresh eight-field boundary proofs join exact container/image/config/recipe,
private holder socket and kernel FD/flock evidence, stopped state-owner census,
authenticated Gate state and actual runtime source custody. The original state
and external lease identities must match the reviewed plan. Worker probes use
the existing authenticated readiness DTO and process/unit/source checks, then
are repeated before admission resumes. Recovery reads exact existing Gate
receipts; a lost reply never triggers another POST automatically.

The pre-stop metadata probe is diagnostic and checks closed legacy records.
If an old-format submission races the stop, post-stop full custody refuses it
before any candidate starts. Original-only rollback preserves that submission:
it checks the original schema/physical provenance and absence of all SQLite
hot-journal/WAL/SHM files without requiring the new old-format row to be terminal.
It does not repair or remove a journal. New-code rollback instead requires the
exact independently generated declared projection. Unknown start/resume effects
remain inspection-only, and rollback after opened/uncertain admission is barred
by the existing journal.

## Current qualification boundary

Five Linux cases passed in 0.263 seconds with the production journal and driver
but synthetic Docker, custody and worker boundaries. They exercise all 22
forward actions, no-repeat lost-stop recovery, retained late submission with
original-only rollback, hot-journal refusal and changed-worker refusal before
resume. This is control-flow evidence, not full Docker/connected acceptance.

The holder/socket/kernel, tracked observer, full-census/TAR, native Docker CLI,
worker TLS and projection implementations have separate component tests. The
complete driver still needs actual composed Docker qualification and review;
no installed coordinator, service, NAS data or real client job was changed by
this checkpoint. The preparation writer described in the bundle document is
not implemented or invoked here.
