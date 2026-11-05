# Coordinated Processing integration readiness

This record describes the review branch
`codex/viewer-accepted-copy-integration-candidate-20261103`. It is not a deployment
approval. Runtime acceptance remains incomplete.

## Latest combined and native-host preparation checkpoint

Processing `89c3dc4` passed **542/542 selected Linux tests, no skips**, in
146.084 seconds across 35 modules. This combines the observation/recovery path
below with receipt-bound ordinary V2 GCP execution admission in Driver, Router
and ODM worker. Actual Viewer Emlid parsing, correspondence/snapshot generation,
signed intake, whole/range transfer and captured native multipart run together.
Captured GCP bytes match the receipt, selected coordinates/height/CRS and pixel
`25 75 photo.jpg`; raw CSV hashes remain unchanged in Viewer and the spool after
worker cleanup. Combined/staged GCP remains explicitly rejected before stage
creation. See `evidence/reference-v2-derived-gcp-input-qualification-20261105.md`.

The bounded combined setup remains frozen Viewer `2317d400`, the old synthetic-only
converter and simulated native engine processing. It proves neither real-photo
GCP processing nor rendered GCP UI acceptance. The container and owned temporary
volume were removed. No runtime, NAS, source-release or publishing gate changed.

Processing `54c28d8e646f95937e6950596f42fdf2cc0b1e82` previously passed
**505/505 selected Linux tests, no skips**, in 127.949 seconds across 33 modules.
This adds durable worker acceptance-observation and immutable lookup binding to
the combined scope below. The actual loopback HTTPS lost-reply, committed replay,
driver reopen and result-cleanup cases are included. One CPU/2 GiB, read-only
sources and dependencies, disabled external networking and an owned temporary
disk volume were used; the container and labeled volume were removed.

This checkpoint still uses frozen Viewer `2317d400` and the old synthetic-only
converter. It does not prove source release/deletion, Viewer hold retirement,
installed/NAS acceptance, licensed conversion or real-photo/GCP engine execution.
The image and inactive native-PC preparations below remain at `51cff44`; they do
not yet include the observation changes.

Processing `51cff44f019eab81ba482adf3f1f298ecd32f571` passed **485/485 selected
Linux tests, no skips**, in 122.705 seconds across 32 modules. This combines
actual Viewer intake/result/conversion/publication, live-description option
policy, source-reader leases/fences, routing/recovery and the version-bound
maintenance/supervisor/HTTP service path on one revision. The first combined
attempt passed 477/478: the composed fixture's qualifier advertised `3.8.3`
while replacement/ingress probes advertised `new-version`. Aligning that
fixture resolved the failure without weakening checks. A genuine mismatch test
now proves rollback, held service and zero ingress reopens.

The successful run used frozen Viewer `2317d400` and the **old synthetic-only**
converter identified below, one CPU/2 GiB, read-only source/dependencies, external
networking disabled and an owned temporary disk volume. Its container and
labeled volume were removed. It does not qualify the licensed ImageSharp 3.2.0
binary, real photos/NAS, installed reference workers or browser rendering.

The local Processing image built from this revision is
`sha256:4fc1df3ac69e7e73fb4325872e3dff9e9dce5eae396a63a77875a33f7bce4fb4`,
tagged `ledgetop-processing:qualification-51cff44`, with its exact revision and
repository labels verified. Startup smoke passed as UID/GID 10001 with a
read-only filesystem, networking disabled, 256 MiB and half a CPU: actual app
health returned 200, the unconfigured reference route returned 404, the live
`gltf` metadata policy worked, and the child server shut down cleanly. Reference,
Viewer and photo execution stayed disabled. The image was not published or
installed. The owned one-CPU/2 GiB builder was stopped after export; cache kept.

Both PCs also qualified the private inactive `51cff44` source preparation below.
That is source/dependency readiness, not activation or installed acceptance.
Source deletion remains disabled. Durable worker acceptance-observation is
included in the newer source-only `54c28d8` checkpoint above, but is **not** in
these `51cff44` image and native-PC preparations.

GCP boundary: ordinary V2 receipt-bound auxiliary admission and captured multipart
are now qualified synthetically in `89c3dc4` above. Staged/combined GCP support,
real native GCP processing and rendered UI acceptance remain unqualified. The
user's next 10,000-photo test was specified without GCPs.

## Live NodeODX option compatibility checkpoint

A November 5 read-only SSH check found main's direct `/info` reporting engine
`odx`, engine version `3.8.5`, API `2.4.0`, `maxImages:null` and queue count zero.
This is a point-in-time backend observation, not reference-worker readiness.
Cluster's running candidate command still configures `--max-images 1000`;
its unauthenticated `/info` did not return usable engine metadata, so no fresh
authenticated cluster discovery claim is made here.

Main's actual `/options` exposed a compatibility bug that abbreviated synthetic
help strings missed: the global unsafe-word scan rejected boolean `gltf` because
its help describes a "single file" output. The shared policy now restricts option
names for all scalar types and retains semantic help/domain restrictions for
string/enum inputs; strictly typed booleans/numbers do not become path inputs
because their help mentions a file. Known path, credential and command names
remain blocked, as do unsafe text choices and incorrectly typed supplied values.
No per-option output allowlist was hardcoded.

The actual main catalog has 80 definitions. After the fix, 77 are accepted,
including required `pc-ept`, `gltf` and `3d-tiles`; `boundary`, `cameras` and
`sm-cluster` remain excluded. A local read-only catalog probe validated the
three required boolean values using the changed policy. **132/132** focused
Linux policy/runtime/driver/native-stage/protocol/API tests passed in 9.831
seconds, plus **5/5** local policy tests. No engine job or container mutation
was performed. Inactive PC source preparations still need this fix before use.

## Combined regression after intake fixes

Processing `be412f366d53368208ccc5f6df65d207d2ee982a` with frozen Viewer
`2317d40029f5b0dbbb002cd319c8f25d321a1bfb` passed **310/310 selected Linux
tests, no skips**, in 101.077 seconds. This combines the earlier 18-module
conversion/publication suite with HTTP and dynamic source-intake scale coverage
on one integrated revision. The run used one CPU/2 GiB, read-only sources and
dependencies, no external networking and an owned temporary disk volume. Its
container exited and its labeled volume was removed; the shared builder was
preserved. One Linux fork-ownership test emitted Python's multithreaded-fork
deprecation warning; the test completed successfully.

This run deliberately uses the earlier **synthetic-only** ImageSharp 3.1.12
converter identified below. It does not qualify the separate patched Viewer
candidate's actual licensed converter binary, installed nodes/NAS, real
photogrammetry or rendered browser results. The patched candidate's focused
48-test and 35-test records below are separate evidence, not a claim that this
combined run used that candidate.

## Large source submission admission checkpoint

Processing `187f542` integrates the V2-only 16 MiB runtime decoder and a single
nonblocking admission slot for exact HTTP `POST /ltds/v2/jobs`. The HTTP layer
already allowed the larger envelope, but runtime decoding previously retained
the 4 MiB V1 default. A valid signed 10,000-photo metadata body of 9,222,511
bytes exposed that mismatch. V1 remains limited to 4 MiB; V2 requests over
16 MiB remain rejected.

The intake slot is acquired before reading the large body and held through the
response. A competing source admission receives `429`, `Retry-After: 2` and a
closed connection with its body unread. This bounds metadata parsing rather
than changing job execution: different physical nodes may still process one
job each. Capability GETs, worker routes, source PUTs and result delivery are
outside this slot.

The integrated HTTP/scale/runtime/API/option-policy suite passed **93/93 Linux
tests** in 13.917 seconds with one CPU/1 GiB, read-only source, network disabled
and a bounded temporary filesystem. Real TLS exercised incomplete-body timeout,
validation failure, busy admission, responsive GET, valid submission and exact
replay. One queued/source-receiving job stored both 10,000-entry manifests and
a 50,000,000,000-byte reservation, with no uploaded source bytes or object
receipts. Capacity and physical-mount checks were injected, so this is neither
a NAS nor 50 GB transfer qualification. Peak server-only RSS was 156,876,800
bytes. A separate server-only **256 MiB / 0.5 CPU cgroup** experiment also
passed with the producer outside that cgroup: final read-only-container peak
was **181,211,136 bytes (about 173 MiB)**, with `max`, `oom` and `oom_kill` event
deltas all zero. Submission returned 201 in 3.43 seconds; exact replay returned
200 in 2.91 seconds. It exercised the same busy/GET/error recovery cases. The
default Linux HTTP/scale regression passed **20/20** after fixture changes.
See the [reproduction and evidence note](evidence/reference-source-intake-cgroup-qualification-20261105.md).
This qualifies that metadata-only case under the node limit; installed load,
actual source transfers and engine execution still need qualification.

The earlier inactive `504af0e` source directories predate these admission fixes.
They are historical preparations only. The current inactive `51cff44`
preparations described below include the fixes; no active service was switched.

## Controlled conversion, publication and delivery checkpoint

The separate Viewer ImageSharp candidate is frozen at
`6718d1beca0f7fbf8cbaae2bbec14326e1e053bb` on
`codex/viewer-imagesharp-320-20261105`. Its source ZIP SHA-256 is
`5d2ce82d1840bd75172c5b71480853b85ff427221c6fc2e28b5768cf3eb3363d`.
Independent Linux qualification passed **48/48 tests, no skips**, in 20.417
seconds across `obj2tiles-bounded-worker`, `lod-derivative-policy`,
`lod-equivalence-audit`, `lod-provenance` and `lod-provenance-receipt`.
It reused the exact frozen-2317 production dependency lock and Node 24.22.2,
with one CPU/1 GiB, read-only mounts and external networking disabled.

The candidate separates historical receipt validation from current generation:
old upstream/schema-1 binaries cannot generate current 3.2.0 provenance, and
the worker verifies the current executable before spawning it. Checks also
exercise durable receipt authority, changed asset/tree rejection and private
proof verification for published sessions. The GitHub build-secret mapping uses
the action's `secret-envs` syntax, not Buildx CLI grammar in the `secrets` input.
This is a separate reviewable candidate, not a merged Viewer release or actual
licensed 3.2.0 build. The binary/build and installed acceptance gates remain.

The same frozen Viewer candidate also passed **35/35** targeted GCP tests in
8.718 seconds under the same bounded Linux setup. These cover strict Emlid/CSV
parsing and units/CRS handling, import API and immutable raw CSV custody,
attempt-specific ODM serialization and snapshot retention, and workspace source
contracts. Workspace checks in this run are static source assertions, **not a
rendered browser flow**. No client CSV or coordinates were used or printed.

Processing `0327d45bdc83eea4b4a663806b555e2987a291e9` with frozen Viewer
`2317d40029f5b0dbbb002cd319c8f25d321a1bfb` passed **272/272 selected Linux tests,
no skips**, including explicitly enabled real conversion. The independent
three-case roundtrip run also passed: whole-object transfer, range-V2 transfer,
and controlled conversion/publication. Two negative preflight cases confirmed
that explicitly requested conversion fails instead of silently skipping when
the converter or Viewer configuration is absent.

The new path executes actual `processOneDerivative`, Obj2Tiles, surface audit,
verification, persisted derivative receipt, readiness and authenticated
publication. It verifies exact publication replay with one publication audit,
normal published-session issuance/redemption, HTTP tileset and referenced binary
delivery, private OBJ exclusion and invalid/revoked-session rejection. Revocation
uses the actual repository mutation; no generic published-session DELETE route
was invented. This is server and asset-delivery qualification, **not rendered
browser acceptance or real photogrammetry**. Native engine execution and physical
mount/host probes remain synthetic.

The exact converter was built from Viewer's pinned Obj2Tiles target:

- Source v1.6.2 SHA-256:
  `79093e12f6eab2cfcd522aebe670892c5d8874e160956b84f3e55c77b94ac0b5`.
- Patch SHA-256:
  `6d5d99ea1d1e36208e44d0456d35cb0d8c68092dfd4a6ad01288bf85bb67322b`.
- Binary SHA-256:
  `3577688939806dabdf1f4620cc05debe6e178180a99bf0308f4b8483b8a36725`.
- Local build image:
  `sha256:15ac76c4d93548b50292795119e9afcc7c6e4c9d80b9c7eccde39a1d2da99ac9`.

The file hash matches `build-info.json`; it starts as UID 10001 in the bookworm
runtime with read-only files and networking disabled. The dedicated builder was
verified at one CPU/2 GiB memory, then stopped after export. Its cache remains
for the pending dependency rebuild. All qualification containers and labeled
temporary disk volumes exited/were removed; the unrelated shared builder was
preserved.

**Converter release gate:** locked ImageSharp 3.1.12 produces four high and two
moderate advisory warnings. TIFF decoding has an applicable converter entry
path; not all other advisory preconditions were established, and no exploit is
claimed. All six primary advisories identify 3.2.0 as patched; no patched 3.1.x
covers the set. [Package/advisory inventory](https://www.nuget.org/packages/SixLabors.ImageSharp/3.1.12),
[patched package](https://www.nuget.org/packages/SixLabors.ImageSharp/3.2.0).
The vendor requires a valid build key for 3.2.0 direct references; eligibility
has not been established. [Vendor announcement](https://sixlabors.com/posts/security-support-and-license-enforcement/).
No license key was present in the checked shell/worktree/repository-secret-name
contexts; organization secrets could not be listed without additional authority.
That is not proof no usable key exists elsewhere. No permissions were broadened.
This old converter artifact is for synthetic qualification only and is not a
client-release candidate. The Viewer owner owns the separate dependency/lock,
secure build-secret and provenance-compatibility patch. Old registered proofs
must remain bound to exact durable receipts/bytes; new generation must require
the patched binary. No licensing or security guard is bypassed.

The current Processing source is privately staged, **inactive**, on both
PCs at `~/.local/share/ledgetop-processing-reference/dependency-candidates/51cff44/source`.
Archive SHA-256:
`98881a3231b4d0e39f7c111227f1f4196b9e5210510c8e34da7cc3e78917794f`;
archive size 5,027,840 bytes.
Each host parsed all **105** Processing/ops modules, imported the policy,
source-reader and worker/runtime modules and passed **35/35** focused validators
without skips (policy, protocol and source-reader custody),
using the existing `39e0825` venv. Main uses Python 3.10.12; cluster 3.14.4.
Preparation receipts record `workerStarted:false`. The source lives on verified
local ext4, owned by UID 1000 in private candidate directories. No current-version
link, active configuration, service, node container or TrueNAS path was changed.

## November 5 combined candidate checkpoint

Processing `667ee7d9eda11fad1caa0569ba8b967dd84bdd67` paired with immutable Viewer
`2317d40029f5b0dbbb002cd319c8f25d321a1bfb` passed **271/271 Linux tests, no
skips**, across 18 selected modules. This includes the shared option policy,
actual HTTPS provider setup and two initial `processOne` submissions, intake and
range transfer, scheduling, worker recovery, result delivery/copy ACK replay,
cleanup and actual Viewer ingestion in both roundtrip modes.

The Viewer ZIP SHA-256 is
`648c05e55c724c7cc57958b812f22bbd6137cfd1172c121f32401b5e1b4b2532`;
the exact dependency lock SHA-256 is
`a0eaead118642843905756fcfe3f53b5336b5a437e22a2f80677d6f06087fb12`.
Python 3.13.17 and Node 24.22.2 ran with one CPU, 2 GiB memory, read-only source
and dependency mounts and external networking disabled. CommonJS and production
ES-module imports passed preflight. The suite initially demonstrated the ingest
free-space guard on a 512 MiB tmpfs (269 passed, two rejected insufficient space).
The full successful run used a dedicated disposable disk volume with sufficient
space; no application storage guard was weakened. Its owned container exited and
the labeled volume was removed.

Ingestion now verifies actual accepted archive bytes, OBJ/MTL/PNG/GLB closure,
registered asset hashes, a one-point EPT tree and a trusted mandatory pending
`mesh_tiles` job. The synthetic GLB fixture checks its emitted texture reference,
PNG bytes, triangle indices and position bounds. **The model stays staged and
unpublished.** Controlled conversion, its verified receipt and a browser-openable
result remain required, along with NAS and installed-runtime acceptance.

Viewer's six-file focused run at `c19b9fec` passed 57/59 with two incorrect DTO
assertions. The final changed-file rerun at `2317d400` passed 20/20 with no skips;
the other focused files were unchanged. These checks preserve required output
formats and conservative holds without deleting uncertain uploads or raw GCP
CSV files. The fresh client binding fix and parser dependency fix remain in the
final Viewer candidate.

A local image from the qualified Processing source is
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`,
tagged `ledgetop-processing:integration-667ee7d`, revision label `667ee7d`.
It passed imports/shared option policy and an actual health-200 check as UID
10001 with a read-only filesystem and networking disabled. Reference processing
was disabled for that smoke check; this is not configured TLS/NAS startup proof.
The image is local only. Neither running PC, TrueNAS, main nor a registry was
updated; the older inactive host preparations below remain unchanged.

See [the local qualification procedure](local-viewer-pairing-qualification.md)
for dependency/mount, disk preflight and cleanup requirements.

## Follow-up option and provider integration qualification

Processing `f5e2737e89effb31a8c2e7b943b79d53baca3e6a` fixes the shared ODM
option-name policy so the actual `3d-tiles` output survives discovery, HTTP
admission, Driver refresh/validation and native-stage validation. Names remain
bounded to 80 lowercase ASCII letters, digits and hyphens, starting with a
letter or digit. Explicit allowlists and unsafe-option restrictions remain.

The changed protocol/runtime/API/Driver/native-stage modules passed 122 Linux
tests, followed by the additional real NativeEngine HTTP option regression
(123 checks total). The new tests failed against the prior runtime before
passing with the mounted candidate. An independent read-only review found no
actionable issue. This is local synthetic qualification, not installed engine
acceptance; the inactive host preparations and image below still contain the
older `8ff9a72` runtime.

The shared safe-option follow-up is integrated as `33ea01c`: coordinator,
Driver and native stage now use the same current enum/text/type/domain policy.
Public aliases normalize to the documented types, and the options fingerprint
covers those exact returned definitions. Worker-local alias support remains;
file/path/credential definitions and unsafe values remain excluded. The
integrated protocol/API/runtime/Driver/native-stage/policy checks passed
**130/130 Linux tests, no skips**, in a network-isolated, read-only bounded
container; its process exited and the container was removed.

Actual HTTPS provider pairing then exposed Viewer's rejection of raw NodeODM
boolean/numeric string defaults. The Viewer owner prepared
`6a1e729785dd70a54c454100b85a07df6d68cd8b`, including typed-default normalization
and a server-managed-coordinator-only bypass of the legacy provider admission
limit. Ordinary NodeODM/ClusterODM limits remain. The actual two-submission
pairing is still being qualified; provider create/health/enable/refresh alone
does not prove that the coordinator receives both queued tasks.

The fresh-submit test exposed a separate P0 Viewer client defect at `6a1e729`: fresh
`createOrReplay` returned the persisted outbox row, while accepted replay
returned its remote binding. The submit worker expects the binding before
source upload. A successful metadata POST and `queued_upstream` status can
therefore coexist with zero source object receipts. The Viewer owner is fixing
the client return boundary without changing the outbox transaction contract.
Viewer `2e8b03f73cd7afcb0a61bb276a798bc75aa6c23` corrects that boundary. The actual
HTTPS pairing test at `d572c4a` passed with no skips against the `f5e2737`
runtime: two real Viewer `processOne` submissions each sent a nonempty source
object, completed intake and persisted completion/object receipts, leaving both
jobs queued. The exact-lock dependency SHA-256 is
`a0eaead118642843905756fcfe3f53b5336b5a437e22a2f80677d6f06087fb12`.
This test is integrated as `d457e04`; a combined rerun with the shared-policy
runtime remains pending. The earlier 205-test roundtrip exercised accepted-intent
resume; it did not qualify this fresh initial-producer path.

The broader Viewer focused run at `2e8b03f` reported 31/34 passing tests, no
skips. Three stale expectations are being corrected at test-only `c19b9fec`:
catalog defaults can be false while submitted tasks require output formats true;
ambiguous legacy uploads/commits must retain files and hold, rather than blindly
retrying or deleting a provider task. That candidate still requires execution.
The compressed trailing-bytes parser regression passed on the updated lock.

Additional work remains active: qualify the fresh producer fix with the actual
two-submission path; exercise actual Viewer archive ingestion and required
derivative enqueue; qualify the targeted Viewer parser dependency fix. The
existing text-only result archive does not prove model ingestion, conversion,
publication or an openable result. Do not treat any pending candidate as a
deployment or client-processing acceptance.

The linked native EH listener remains subscribed with its process identity
verified. Managed interactive MCP initialization still fails at the HTTP header
helper. No new Hub-note review checkpoint or fresh Claude acknowledgement has
been established; Viewer coordination continues through the authorized app
chat, with GitHub as the existing fallback.

## November 4 combined qualification and inactive host preparation

Runtime code `8ff9a7276493c383f9ef250aeead4fca42117fba` paired with an immutable
Viewer `66031588eb465c8e197acc9db9da3569933a33a3` archive passed **205/205 Linux
tests, no skips**, using Python 3.13.17, Node 24.22.2 and pinned Python runtime
dependencies. The Viewer archive SHA-256 is
`7d0d74625d6041c934b06a005e3160f714771188064000d1c34fdf57ae1f53f5`;
its package/lock bytes matched the isolated `npm ci --ignore-scripts` dependencies.

- Both whole-object and opt-in range-V2 Driver paths pass the actual Viewer
  source-client/outbox, HTTPS spool broker, ReferenceDriver/EngineWorker,
  durable result outbox, worker result API/HTTPS proxy, Viewer result-copy signer,
  Store acknowledgement replay and worker engine-cache cleanup roundtrip.
  Viewer `processSubmit` resumes the accepted source intent and automatically
  queues reconciliation; the fixture uses its normal delay and claim. Lost
  POST/PUT/completion and copy-ACK replies recover without a duplicate engine
  initialization, changed receipt IDs or deletion of the Viewer copy.
- Real transport qualification exposed and fixed dropped ETag/request-ID
  headers, rejection of Viewer's bare canonical dataset UUID, and missing
  configured private-CA trust in the Driver's range client. Signed manifests
  are not rewritten, legacy prefixed identities remain stable, and TLS
  verification stays enabled.
- Range grants/renewal/ACKs use the existing protocol, persist cumulative budgets
  and exact replay, and remain bounded by the fixed reservation deadline. Short
  grants last at most six hours; the existing worker reservation cap is 24 hours.
  Unfinished work holds at that deadline. New range journals, pins and token
  records are included in the metadata quota; quota failure rolls back growth,
  while exact ACK replay does not add metadata.
- Signed V2 intake/receipt tests exercise small-first cluster/main claims and
  leave oversized work queued while an idle cluster takes a later eligible job.
  Counts are scaled to a one-image cluster ceiling in those fixtures; the
  configured installed ceiling remains 1,000. Logs now cross the existing
  public API, fixed-worker proxy, authenticated result reader and execution
  router. A bounded native JSON-prefix reader pages through large synthetic
  tails without downloading the full remainder; TreeDar logs stay unavailable.

These are synthetic execution/transport checks. The native engine, physical
engine probe and mount qualification are injected; Viewer storage resolution
and capacity use a scoped test adapter. The test preserves actual filesystem
copy/fsync and database/outbox/signature code, but does not prove TrueNAS
durability, real photogrammetry, model ingestion/publication, live logs or an
openable result in the installed Viewer. The large-dataset transfer/capacity
boundary and the P1-P13 installed acceptance matrix remain to be qualified.

The exact source is privately staged, inactive, on **both PCs** at
`~/.local/share/ledgetop-processing-reference/dependency-candidates/8ff9a72/source`.
Archive SHA-256:
`cd808291d97314d33f57d70b1dfe5b8a290b6a821cc51b3a0f620905952717ee`.
Using each host's previously prepared `39e0825` dependency venv, all **103**
Processing/ops Python modules parsed and worker/runtime imports passed; **24/24
focused validators per host** passed without skips. Main uses Python 3.10.12
and pycparser 2.23; cluster uses Python 3.14.4 and pycparser 3.12. Both retain
cryptography 50.0.2, cffi 2.1.1 and rfc8785 0.1.4. Each private preparation receipt
records `workerStarted: false`; no current-version link, service, live config,
system package, engine container or TrueNAS setting was changed.

The local coordinator image built from this runtime revision is
`sha256:ed1800bcf8e0eda89ff4f1eb16d98faf7896c8111240fc917801f53432fba73c`.
It passed a network-isolated, read-only UID-10001 import/health-200 smoke check;
the task-owned container and listener were removed/joined. It has not been
published or installed. Reference workers remain an installation gate.

Viewer owner reports 46 focused status/UI/outbox checks at `66031588`, including
persisted node/phase metadata, queued pre-engine phases and progress retention.
Rendered installed acceptance and real ingest/publication remain separate gates.
An exact-lock production dependency audit also reported body-parser
`GHSA-27r3-jxmh-p828` (high, `<1.20.9`); the Viewer owner has been asked to review
the targeted fix before release. No broad dependency update was performed here.

**Immediate Viewer admission code gate:** at `66031588`, provider creation,
refresh/probe and background health still use the legacy `NodeOdmProvider`.
That requests `/info` and query-token `/options`; the coordinated TLS API uses
Bearer-authenticated reference capabilities/options, with the legacy facade
correctly disabled. Fresh health/options are required before HTTP task admission,
so inserting a provider directly in the wire fixture does not prove the UI can
schedule work. The Viewer owner has the concrete shared-adapter/bootstrap and
HTTP acceptance test requirements. This gate must pass before installed Viewer
job scheduling is described as ready; it must not be bypassed by enabling the
legacy executor or fabricating cached health.

## November 4 worker integration checkpoint

### Actual client and host preparation checkpoint

Code `39e0825` passed the expanded **273/273** Ubuntu/pilot combined suite with
no skips, including the V2 metadata budget, worker validation, grant broker,
outside-lock verification/CAS, runtime and service-unit checks. Later source
grant corruption/clock hardening `c45e8b3` passed its 18-test intake module on
Ubuntu; it rejects orphan token history before scanning and at final CAS.

Actual Viewer candidate `b6f03f31208dce2b98314183b7fbc5d5aab45e26` paired with
Processing test integration `e06ccc1` passed once on Windows (Node 24.16/Python
3.13) and once on Ubuntu (Node 24.22.2/Python 3.12), with no skips in either
pair. The test uses real Viewer SQLite migrations, ProcessingRepository,
PreparedSubmissionOutbox, preclaim signing and SourceIntakeClientV2 against the
actual loopback HTTPS control/API/Store/spool. Lost replies after committed POST,
PUT and source completion recover with exact immutable request replay, ACK-based
upload resume, strict persisted receipt replay and the `1e-7` option. The spool's
mount check is explicitly simulated: this proves the wire/persistence seam, not
NAS custody. Listener/child cleanup is asserted; the temporary Linux Node export
container and binary were removed after testing.

The same code/dependency profile `39e0825` is privately staged, inactive, under
`~/.local/share/ledgetop-processing-reference/dependency-candidates/39e0825` on
both PCs. Its source archive SHA-256 is
`d8024c99a411ab646fbba8974d68c8e0d1867ed9104c4cf23df58c74971f4e5e`.
Main Python 3.10.12 and cluster Python 3.14.4 both imported the worker/daemon and
passed 25 selected validator tests with two explicit optional Node checks skipped
(27 total per host). Private dependencies are cryptography 50.0.2, cffi 2.1.1,
rfc8785 0.1.4, and pycparser 2.23 on main / 3.12 on cluster. No system Python
packages, running containers, active-version symlinks, service units or NAS
configuration were changed. These are preparation artifacts, not active workers.

### Source-bound result acceptance checkpoint

Combined code through `32ac9c4` includes the exact signed
`ltds-viewer-accepted-copy-v2` verifier, authenticated source/preclaim binding on
private worker jobs, Store acceptance and exact receipt replay, and worker
acceptance/cache-cleanup verification. V2 source jobs cannot use unsigned or V1
copy acknowledgements; partial or malformed V2 markers cannot select legacy
cleanup. The Store persists receipt identity and rejects conflicting reuse.

Focused worker/cleanup/binding/receipt/result tests passed **59/59 on Linux**
without skips at `e6e0ab1`. Subsequent Store V2 replay tests passed **2/2 on
Windows**, including restart and conflicting receipt reuse. At `32ac9c4`, the
actual Viewer TLS intake pair passed **1/1**, and the source completion test
passed **1/1** after adding worker-progress and reopened-Store coverage. Source
completion now returns its immutable receipt ACK even if a lost reply allows a
worker to progress before Viewer retries; dynamic job state stays on GET job.

The coordinator image built from `32ac9c4776136f26bd51ff6b2d0f125f72d98607` is
`sha256:3ea5232a75c1e00bdd195ac8dd9d7b38c5c6cb68ba330946ef32c907764c011f`.
A network-isolated, read-only container imported the worker/Store/V2 verifier
modules and served health HTTP 200 as UID 10001; its listener was joined and
the task-owned container removed. This proves image construction and basic
startup only, not configured reference execution or installed services.

Runtime follow-up `d05d100` permits an explicit schema-6
`"viewerAcceptance": {}` for the new Viewer pull/copy-ACK path. It avoids
constructing the incompatible legacy push sender while still requiring source
and copy-receipt pins; schemas 4/5 keep their nonempty push-target requirement.
The affected runtime suite passed **38/38 on Ubuntu** without skips, and an
independent read-only review found no actionable defect. The new assembly test
mocks TLS and mount qualification. The image above predates this follow-up;
it must not be described as containing the configuration change.

The production worker daemon already assembles `ReferenceResults` and
`ReferenceResultAPI` on its HTTPS listener. A full synthetic source → worker →
result download → actual Viewer outbox/signer → Store ACK → cleanup test is
being assembled; the separate component tests do not prove that whole path.
The photo workflow remains unqualified for client work until that pairing and
installed acceptance pass. Large transfer lease renewal, installed NAS
qualification and real-data processing remain open too.

### Combined intake/worker checkpoint

Code `e7d63c6` combines source intake `2ba927e`, pinned live-engine worker
checks, explicit V2 digest markers through execution/recovery, and the
parent-claim client shape update. The 260-test Ubuntu/pilot combined suite passed
with no skips: intake, spool, API, runtime, HTTP, driver/routing, engine/cleanup,
parent guard, recovery, store and protocol. An earlier run had one Linux-only
symlink-test setup failure; its fixture created a non-private intermediate
directory. The fixture was corrected, its 11-test module passed, and the full
260-test combined suite then passed on this exact code revision.

The Windows actual-Viewer canonical/proof/intake selection passed 23/23 with no
skips on the joined candidate immediately before that fixture-only correction.
Runtime configuration schema 6 explicitly requires the spool and pinned Viewer
source-attestation keys; existing schemas 3/4 are preserved.

The next missing production seam is source grant/download wiring: the imported
Store mint/read methods initially had no production callsites. That follow-up
must pass a reserved worker's actual HTTPS download before this is an executable
end-to-end workflow. Actual Viewer client pairing is also pending. The 10,000
photo synthetic metadata closure measured 4,920,057 bytes for ordinary filenames,
exceeding the existing 4 MiB V2 intake limit; a bounded V2-only limit and matching
Viewer outbox/node-budget migration are being prepared. Neither of these gaps
is resolved by the passing tests above. No client datasets were processed.

The current NAS preparation is described in
[the source spool checklist](truenas-source-spool-preparation.md). Only the
coordinator needs a NAS spool mount; workers use its authenticated source broker.

Worker integration code `06560c6` (parent `5b2d0f8`) incorporates the pinned
live-engine probe before mutations and cleanup, rejects the alternate legacy
Viewer executor in multi-node reference configuration, and retains the current
V2 admission validator from `49107f3`. The selected Linux source-intake validator,
protocol, engine worker/cleanup, driver/routing, runtime, daemon routing,
execution, results, combined preparation and worker-unit suite passed **179/179
with no skips** on Ubuntu 24.04 as unprivileged `pilot`.

This checkpoint does not include the in-progress Store/HTTP/spool intake
integration or prove installed workers, physical-host attestation, NAS custody,
actual Viewer V2 submission or real processing. The earlier missing live-probe
items below describe the earlier checkpoint and are now implemented in this
candidate, pending installed qualification. See
[the acceptance plan](coordinated-release-acceptance-plan.md) for the current
dependency order and explicit per-host concurrency rule.

### Actual Viewer source contract follow-up

The exact clean Viewer candidate checked on November 4 is
`b25d3234a5875be2be95c99302dfac3605999be4`. Its request includes `viewerTaskId`
and a signed `sourceAttestation`; the historical intake import did not accept
those fields. New source-proof verifier `dff4f44` validates a configured signer
pin, principal/environment, queue authority and exact task/manifest/pipeline
bindings, including the top-level Viewer task ID. A verified proof does not
establish physical NAS custody or grant cleanup authority.

The Viewer signs the pipeline digest using ECMAScript/JCS serialization.
Python's existing V1 JSON digest differs for legal options such as `1e-7`,
`1e-6` and negative zero. V2-only helper `63c6a3b` uses pinned `rfc8785==0.1.4`;
V1 hashing/history remains unchanged. Its upstream implementation is
[rfc8785.py](https://github.com/trailofbits/rfc8785.py).

The combined focused validator/protocol/proof/canonical suite passed 36/36 on
Windows with Node 24.16 and Python 3.13. It explicitly selected that actual
Viewer checkout and paired its exported domain/shape and canonicalization code
with Python verification, including ten numeric-option vectors. On Ubuntu,
34 passed and two optional Node-dependent cases skipped. These are isolated
proof/hash tests, not full application admission, physical custody, or installed
runtime acceptance. The Store/HTTP/runtime wiring is still in progress.

### Read-only installed-state refresh

At 2026-11-05T02:30Z, SSH succeeded to both processing PCs. Both user worker
units reported `LoadState=not-found`, `ActiveState=inactive` and no fragment;
neither expected private worker configuration file exists. The existing
coordinator reported healthy. Main NodeODX and both ingress containers are up.
Cluster's engine candidate exposes container port 3000 only on loopback port
13001; its name alone does not establish qualification or current ownership.
No units, containers, configuration, client jobs or NAS files were changed by
these observations. The new reference workflow is not installed.

Identity follow-up `572cf56` integrates duplicate-origin/daemon-ID checks and
requires daemon bindings for multi-node startup. It preserves schema-v3/v4
result-delivery configuration while requiring their recovery bindings too.
The 121-test Linux node-role/runtime/driver/daemon-routing/updater suite passed
without skips. A node-local live identity probe and physical-host binding
remain missing.
The Ops #430 `50ddd6e8` HTTPS/Python source-range pairing was rerun against
this identity follow-up (`572cf56`, documentation head `08e4e9d`): 20/20
passed with no skips. The earlier 20/20 result below applied to code `c7aa1ce`.

Local image `ledgetop-processing:integration-8422a47` was built from repository
head `8422a47` (code `572cf56`) with the pinned Dockerfile/dependencies. Local
image ID reported by Docker inspection is
`sha256:e616f6f4393dddc0a5536b0551a1cf54313f309de5537f9efe633ff58241c881`.
A bounded container with no external network and a read-only root filesystem
started the app and returned HTTP 200 from `/healthz` as UID 10001; cryptography
50.0.2 loaded. The child process and container exited. This checks basic image
startup only, not configured reference dispatch or a deployed PC worker.

## Integrated source

### Canonical execution path

The coordinated version uses Viewer reference-attempt admission into
ReferenceStore, then the per-node ReferenceDriver/ReferenceEngineWorker pull
path, its persistent completion outbox and the existing Viewer accepted-copy
receipt chain. Source transfer and engine execution must join that same job
and claim identity. The legacy ViewerService direct NodeODM mutation lane does
not have the durable accepted-copy release contract and must not be enabled
as an alternate multi-node executor in the coordinated configuration.
The experimental node-execution gateway is not part of this release branch;
adding another execution/receipt protocol is not the integration plan.
Local worker identity verification, source admission/custody and installed
worker services are still required before this path can run jobs.

Code commit `c7aa1ce66e91b3b3c985ed3d5789d7527f21402f` combines:

| Component | Exact source | Review |
| --- | --- | --- |
| Durable result sender, signed receipt, cleanup predicates and guarded updater | `70312a4d4887f9546930facb854ca5c6b5d6ba91` | Processing #339 |
| Ordered routing, healthy-node fallback and per-node claim revalidation | `1d24c14a6924d9dd09d31c7265ec5cf3f30a1c43` | Processing #364 / #365 |
| Resumable source ranges, renewal intent and acknowledgement replay | `e10326bf3d2003fb7542afe7e52fdcf6bf6dc4ba` | Processing #342 / #349 |

The integration retains both `viewerReceiptKeys` and `sourceRangeV2Enabled`
worker configuration options. Source-range execution is an explicit opt-in;
its driver code exists, but a qualified source producer and NAS custody path
are not operational.

## Evidence on the combined code

- Ubuntu 24.04 / Python 3.12, unprivileged `pilot`: 186 tests passed with no
  skips across store, runtime, driver, daemon routing, source ranges, Viewer
  transfer/receipt, retry worker and coordinator updater modules.
- Ops #452 at `9e52371c02f0798e294b583bbe2cbd07e46585e9`, Linux Node 24.22.2
  and Python 3.12: two actual Python/Node paired tests passed without skips.
  The first covers v1/v2 result acceptance and lost-commit/status replay. The
  second kills both processes after a lost commit response, reopens the real
  Processing Queue and Viewer SQLite/filesystem ledger, recovers the accepted
  result and checks persistent nonce replay rejection.
- These are synthetic process-crash tests. They do not prove power-loss
  durability, production TLS, TrueNAS custody or installed worker services.
- Ops #430 at `50ddd6e88bd1e26b0cd7cfb2bfbebcec77de987a`, incorporating
  broker #429 `d2cb01a`, paired with this integrated Processing code: 20/20
  source-range tests passed without skips, including actual HTTPS Python
  interruption/restart, bounded range reads and source corruption checks.
- Temporary Node executables and export containers used for pairing were
  removed after the processes exited.

## Remaining delivery gates, in dependency order

1. **Node identity:** reject duplicate node origins and duplicate pinned engine
   IDs; verify the local identity before mutation. Docker Engine ID identifies
   a daemon, not a physical PC. Establish a host binding and enforce a single
   reservation per physical host before enabling concurrent dispatch. The
   configured identity checks are integrated in `572cf56`; they do not provide
   live daemon verification or physical-host attestation.
2. **NAS source custody:** qualify the implemented authenticated V2 intake,
   immutable source generation/manifests, reserved transfer, retained receipts,
   recovery and source-reader leases against the installed NAS-backed path.
   The combined synthetic tests above prove code paths, not server/export
   durability, ACL isolation or physical-host power-loss behavior. Authenticated
   explicit source release/deletion and Viewer operational-hold retirement
   remain unimplemented. Beau controls all TrueNAS changes. The selected spool
   subfolder under existing Model-Viewer Storage is not yet provisioned or
   ACL-qualified.
3. **Source pairing:** the V2 signed-intake, whole/range transfer and receipt
   pairing are exercised with actual Viewer/Python code in the combined tests
   above. Qualify source production, grant renewal and custody as one installed
   path; synthetic pairing does not establish the NAS or installed-service
   boundaries. The historical #429/#430 evidence is an earlier checkpoint.
4. **Result custody and cleanup:** qualify runtime credentials, persistent mounts,
   exact acknowledgement replay, disconnected delivery, disk-full/corruption
   handling and cleanup against the actual installed services. Source release
   and result acceptance are distinct operations. Unknown outcomes retain data.
5. **Maintenance:** rollback is supported only before nodes reopen for work;
   `reopeningStarted` prevents later rollback. Do not interpret conservative
   rejection of new receipt data as permission to delete it for a downgrade.
   Qualify unattended drain/update/health/resume with persisted queues/outboxes.
6. **TreeDar and future engines:** genuine LAS/LAZ admission and the complete
   typed output closure still need real runtime qualification. TreeDar stays on
   main; Gaussian splats remain disabled. Jobs on one host must not overlap
   between engines.
7. **Viewer experience:** Ops #406 at `bf5c1dbbb5fb49213e429f131c40ad7403628147`
   independently passed 52 workspace behavior/brand/control tests plus 14
   hierarchy migration/API/permission tests, no skips. These verify customer
   project scoping and unchanged access grants plus DOM navigation/error states;
   rendered browser acceptance, deployment, queue/node/log integration and
   current Claude review ownership remain open. The branch has 15 historical
   Claude co-author trailers; that is not evidence of current peer delivery.
   UI completion does not qualify the processing pipeline.
8. **Installed acceptance:** the last read-only checks found reference worker
   services inactive on both processing PCs. Verify credential rotation,
   immutable installed builds, storage and active workers; restore the staging
   service/grant prerequisites; then run synthetic failure cases and a small
   real dataset. Large client datasets remain unqualified.

Production outranks waiting staging jobs without interrupting running jobs.
Routing prefers the first eligible idle node, with cluster capped at 1,000
photos and main uncapped by photo count. The current user requirement is one
job per physical host, allowing distinct hosts to run concurrently. Capacity,
engine eligibility and maintenance must be rechecked atomically at claim.
