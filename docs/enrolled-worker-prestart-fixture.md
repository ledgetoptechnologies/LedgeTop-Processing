# Typed worker pre-start fixture adaptation (preparation tests qualified)

This adapts only the opt-in local full-phase qualifier. It does not install or
activate workers, touch SSH/NAS/systemd, patch INSTALL_BASE/ownership policy, or
weaken the typed review, readonly source or process guards. Root-owned physical
installation remains a separate boundary. There is no old thread/opaque-worker
qualification fallback.

## Source and pure artifact binding

The actual worker/controller source archive revision is independently pinned
with `--worker-source-commit`; the immutable packaged app revision remains
`--candidate-source`. These may differ for ops-only changes. The worker source
archive must match EVERY leaf of the separately frozen source tree, not merely
claim an arbitrary matching footprint. The layout descriptor is separately
pinned by `--worker-layout-sha256` and is required before the qualifier creates
any Docker fixture. Each versioned source and interpreter mount must be truly
read-only, canonical, non-symlinked. A short versioned-interpreter observer reads
actual interpreter/package versions, binary hash, source footprint and
requirements hash before backend/factory startup.

Both native fixture backends and actual ports are chosen first; both final
runtime worker origins are sealed before either preparation. Each node's Gate
is provisioned using the real NodeGate API in its own HOME/XDG environment.
`prepare_reference_worker_config.prepare` consumes schema6 final runtime bytes,
nonempty valid Viewer trust keys, role-disjoint native/TLS inputs and those
actual Gate/archive bindings. It writes its completed enrollment manifest,
config and exact preparation receipt. No receipt/manifest is synthesized to
match expected values. The exact config is copied exclusively to its derived
stable home config path, and the exact installer.render_unit bytes are written
to the derived home unit path. Units are never enabled or loaded.

The core `worker_prestart_review.compose(bundle,plan,entries)` consumes those
real metadata references with install-intent scope. Private config/native keys
are never embedded or read by that artifact producer. Its candidate object is
independent of the deployment hash: compose -> retain artifact -> pin existing
plan gate/bundle descriptor -> compute deployment hash -> validate final bundle.
There is no plan/deployment digest in the artifact, hence no cyclic fixed point.
Other existing simulated NAS/legacy/pilot gates retain their explicit fixture
scope; the worker gate specifically has no fixtureOnly boolean substitute.

## Future resource plan — none created by this patch

Use exactly one NEW labelled UID10001 seed helper and one NEW owned dependency
layout volume, under the reviewed candidate image. No build/pull/root/sudo/chown.
Read-only mounts: frozen source at `/qualification`, its archive at a separate
fixed source-archive path. The empty new volume at `/prototype-state` must be
owned10001; the seed function refuses nonempty/replaced roots.

`qualify_enrolled_worker_preparation.seed_layout` copies the real running
interpreter and the four actual pinned dependency distributions, writes a
venv configuration excluding global site-packages, and retains the complete
dependency TAR, source TAR and descriptor. Bounds: source/dependency archive
256MiB, dependency expansion128MiB/8192 leaves, individual source leaf4MiB.
Versions and hashes are observed rather than guessed. The seed does not claim
root-owned installation or activation. Its container must be proven exited
and the output metadata/archives reviewed before any controller start.

For each node derive the installation UUID from cutover operation + node. Outer
controller mounts the same frozen source read-only at
`/opt/ledgetop-processing-reference/<node-install-uuid>/source` and the shared
seed venv read-only at the sibling `venv` path. Mount the layout volume itself
read-only at `/run/qualification-worker-layout`. Native source mounts may use
the daemon's exact owned named-volume subpath or an exact derived volume
mountpoint; never infer another host path or permit mutable source overlays.
Private homes/config/Gates/worker-state/scratch remain under the outer's NEW
private artifact volume. They are simulated UID10001 homes, not installed host
homes. Complete source/dependency archive proof must be retained privately
before eventual seed/layout resource retirement.

Keep one CPU/768MiB outer and the existing bounded inner app profiles. The seed
helper runs sequentially first (one CPU/512MiB, no network). The outer launches
exactly two factory worker children using the derived versioned interpreters,
isolated startup environment and separate HOME/XDG paths. They run the real
daemon factory/WorkerService/ReaderSession/TLS APIs; native backend and empty
coordinator transport remain explicitly synthetic. No job is claimed/processed.
Each readiness observer runs with the same versioned interpreter/source and
uses production probe validation plus the actual owned child PID/start before
and after TLS. SSH/systemd is an explicit local process-control seam.

Public child stdin is bounded64KiB; readiness subprocess output64KiB/error4KiB,
15 seconds per observation, bounded 503-only startup retry10 seconds. Worker
stderr/stdout stays in private logs with a256KiB checked bound. On shutdown the
parent verifies exact owned PID/start, sends SIGTERM once and joins for10s;
nonzero exit, identity change or uncertain closure holds all later cleanup.
No SIGKILL, generic process scan or broad cleanup exists. Seed/container/volume
ownership and namespace census follow the existing qualification discipline;
unknown outcomes retain resources. Seed/layout cleanup must occur only after
outer/child retirement and a full named/bind alias zero-reference census.

## Tests proposed before any actual run

The preparation-only selection has passed; seed-volume, actual child-factory
and full-phase stages remain unexecuted. First bounded Linux selection:

```text
tests.test_enrolled_worker_preparation_fixture
tests.test_enrolled_phase_configuration_validation
```

Provide frozen archive/revision through `LTDS_QUALIFICATION_SOURCE_ARCHIVE` and
`LTDS_QUALIFICATION_SOURCE_COMMIT`, immutable readonly source, UID10001 and
bounded /tmp. These tests seed real dependency/archive artifacts on NEW tmpfs,
use the actual pure composer/Gate/render_unit/typed review, refuse changed
bindings, and make no installed or actual factory claim. Missing required
inputs explicitly skip the preparation class and must not count as coverage.

Frozen source `e83f22d1503b3ec1c3bc86437233bf73133e52d1`, canonical archive
SHA256 `7cef02c703bae31eec546af981eb1c4c7e44df188e0d027446421a806b30011e`,
passed **11/11 tests, ZERO skips, 10.090 seconds**. All five preparation-class
tests ran with real source/dependency artifacts and real pure composer/Gate/
unit/typed review. Immutable image5d466, one CPU/768MiB, network none, UID10001,
readonly source/archive, /tmp192MiB, pids64, no socket or host ports. No factory,
volume, installer, systemd, SSH or full phase was started. Full2,355-byte log
SHA256 `c3ede83628de44259e420654fe67a65503180227fe53e99e47b9cc00d692ac1e`;
stdout/stderr and receipt retained privately in the root handoff. Exact owned
test container5b661e6c exited0/PID0/no OOM, was removed after ownership checks,
and its task inventory was empty. This source predates the later9ab installer
changes; final integrated source still needs its own qualification.

Next, only after review/first result, qualify the actual two-child fixture with
the separately seeded reviewed readonly mounts using
`tests.test_enrolled_phase_fixture_profile.ActualWorkerFixtureTests`. Supply
the exact layout descriptor/digest and controller ID. This covers actual factory
TLS/loaded-source/readiness, swapped planned workers and engine-pin mismatch.
No actual full forward/rollback rerun is authorized by this patch. Current649b
image proofs stay closed and source-specific; typed prestart adaptation remains
unqualified until these new checks and independently reviewed actual phases pass.
