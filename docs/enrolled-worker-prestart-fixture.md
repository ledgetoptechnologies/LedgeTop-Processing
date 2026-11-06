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

The preparation-only selection and separately granted seed helper have passed;
actual child-factory and full-phase stages remain unexecuted. Linux selection:

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

## Actual seed-only qualification

Operation `4b532a2c-78f4-4dbd-8b3a-bf8f05dbf3e2` ran the exact tested e83
source/archive in one separately granted helper, image5d466/source649b,
UID/GID10001, one CPU/512MiB, network none, pids32, readonly root/source/archive,
32MiB tmpfs, no Docker socket or ports. Only one new labelled layout volume was
writable. Reviewed launcher SHA256
`541aba88bedf8d0c8472383d942ddc472fe09dfe477e4f096a94d53524dbb5d3`;
literal helper-body SHA256
`693b7dacaacbed8ccc6ef58158fb90020c8ab4fac1d7d9976986a8965feeafdf`.
The O_EXCL dispatch marker remains consumed; no create/start was retried.

Exact helper `b0e38e2486773039112f1e70f76a7724b0709ce6d8fd82da2db8195309093537`
started `2026-11-06T04:40:10.267829145Z`, finished `04:40:15.405100026Z`:
5.137 seconds, exit0/PID0/no OOM. Fresh inspection verified exact image, labels,
user/resource/network policy and only the two readonly source binds plus the
single owned volume. It and `ltds-worker-layout-4b532a2c78f44dbd` remain retained
for independent root inspection; no worker/factory/controller was started.

Cold layout SHA256 `ad3d1d1dc5b62a548a2eba96f691f44dafc3ea1923a3172652e5266fda75bd8a`;
source TAR7cef02 above was copied byte-exactly. Independently recomputed actual
146-Python-file worker footprint:
`f67c6b3d88f8ddb847d419a0b7040405b4a3f68f316fc90243574d3faeadb5a1`.
Dependency TAR SHA256 `0e5e30fca9be95dfb06861c6ea9a278980c9e90aabd0f5b30e15f6571357e571`;
all 181 regular leaves (179 package files plus interpreter/venv config) matched
the cold venv bytes, UID/GID10001 headers, no symlink/hardlink. Python3.13.17
binary SHA256 `73b68fc2dac176af16098180a2c98a03ee9f1f1b02db25d42d44d782e79e2330`;
observed cryptography50.0.2, cffi2.1.1, pycparser3.12, rfc8785 0.1.4.
Complete cold copy: 186 files/42,277,469 bytes, manifest SHA256
`3e39e7390f4ded4570d7158622369bbf84a5daa21265b7dc800b81c8c0a8a2ed`;
helper log SHA256 `96b658b64d1146163f8efdb7882e4dac14571bc02649c84dee68b3e5a77839b5`.

This proves seed construction/readback only. It predates later9ab installer
fixes and does not prove the versioned interpreter can start the real factory,
root-owned installation, actual activation, SSH/systemd, native jobs or full
cutover. No frozen e83 bytes were modified and no factory run is authorized by
this seed-only receipt.

## Test-only persistent artifact lifetime follow-up (not executed)

The inherited TemporaryDirectory factory context is replaced by the test-only
`RetainedQualificationArtifacts`, requiring explicit
`LTDS_QUALIFICATION_ARTIFACT_ROOT`: an original owned mode0700 persistent task
parent. It creates one new private child directory and pins both original
inodes. A failed test or uncertain child shutdown records retention outside the
child and preserves every remaining byte. Success cleanup is unavailable until
both actual Popen child handles report successful reap and their original
PID/start identities are absent. A complete bounded ownership/type/link/inventory
preflight precedes any deletion. Cleanup is anchored to original directory
descriptors, verifies the snapshot at each effect, and never uses a generic
rmtree, glob, chmod/adoption, kill or unrelated path. Root replacement, symlink,
foreign leaf, missing/live child or partial cleanup failure holds remaining
artifacts and records the uncertainty. A cleanup-started receipt is fsynced
before effects and cleanup-complete only after the original directory is gone.

Explicit unit cases cover confirmed-success cleanup, uncertain-shutdown
retention, unconfirmed/live child refusal, symlink and root-swap refusal. These
are controlled child-handle tests, not actual worker readiness proof; only static
AST/diff checks have run for this follow-up.

The seed producer, all ops/Processing source and the actual factory child shim
remain byte-identical to e83. To reuse the retained seed safely, a future test
controller may run this newer test-only archive but MUST mount the original
frozen e83 tree at each versioned worker source root and use the e83 worker
archive/revision/footprint pins. Do not mount a newer whole tree while claiming
the old archive digest merely because runtime Python hashes happen to match.
The new retention helper is controller-test-only and is not a worker input.
Final combined9ab source still requires a fresh reviewed seed and qualification.
