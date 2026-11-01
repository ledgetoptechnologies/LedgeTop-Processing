# Joined synthetic real Viewer ingest

The exact scripts, authority boundaries, ZIP fixture, signal ownership, source
pins and failure cleanup were independently reviewed before the recorded run.
Later changes require renewed review. No production data or service is used. The Windows Job,
two exact-labeled containers, network-none namespace, retained bind root and
outer watchdog remain unchanged; only a new committed fixture snapshot is
overlaid. Processing source is `4020f922990bca17598390868a8d378e80615154`,
Viewer is `f13c4e0465640516305aa3f9eeb1ad0735254bb9`, existing fixtures
`a174b8ae1b00e9f3bb4ab71f517bc80c2fe539ba`, and new fixture sources
`86243ade36958c107bacca9e0488525fa54e19c5`. The runner checks committed blob
hashes and prints module/image IDs; host launcher and image tags are not pinned
by an independent outer authority.

The synthetic engine returns a deterministic nonempty ODM-shaped ZIP with
small text placeholder assets, not a real processing output. The actual worker
retains it while the owned coordinator is killed and restarted; Viewer stages
the real coordinator proxy stream and sends the matching acceptance. In a fresh
real Viewer database, the fixture drives normal submission and ingest job states,
then invokes the disabled `nativeReferenceIngestCandidate.ingestHeld` experiment
with explicit synthetic authorizers and owner-private synthetic storage roots.
It checks the resulting real native registration and real derivative job, then
checks ProcessingRepository will not claim that derivative while unrelated
synthetic work can be claimed. No registration row is manually seeded. No
derivative worker is started, no activation or production readiness is asserted,
and the synthetic authorizers are not production authority.

The first reviewed run `32ef1d68350349a0ba3bfd81be45019f` failed before Viewer
staging because Node slim lacked `unzipper`. Retained root
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-p4z5vgzq`;
exact labeled Viewer container
`edbb819509b06900dd00f75969c182b1c3eb27653fd6e268ef94dada1090fad3` exited 1
and Python container
`359a8a8c21579e3ea81c6d1afe225bd81ef740731e0147f5c5babf46feeac109` was
stopped by its owner (137); Windows Job active 0, inner graceful exit unverified.

For a future review, the runner uses an already local Viewer dependency image
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
Its image history shows `npm ci` under `/src/apps/viewer`; history alone does
not prove module contents or lock equivalence. The Node process will require
the image's package-lock SHA-256 to match the pinned Viewer snapshot and load
`unzipper` from the image's node_modules before invoking the fixture. A mismatch
or missing package is a failure, with no network install or live changes.

The second run `7d08a870232642baa1e47b475ae1efd8` passed that lock and package
preflight and persisted Viewer acceptance but failed the unchanged expansion
capacity gate before ingest. Retained root
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-3qkm_4su`;
exact labeled Viewer container
`75491a33600fe4aae69fc9247818a13d663424c84592578f0ba88f1f45319a7b` exited 1;
Python `23b45e328c17b1549f796270b93588edbd896ba9f93614d0eb81e12ecb3c799b`
was stopped by the owner (137), Windows Job active 0, graceful inner exit not
verified. Later read-only WSL observation showed one million available inodes
on the Windows-backed models path versus 1,001,024 required; it was not an
in-container measurement at failure.

The next staged runner creates an owned Docker local named volume with name
`synthetic-joined-models-<run-id>` and the exact existing run label. It checks
name, label and `local` driver before mounting exclusively at `/synthetic-models`
in the Viewer container with `volume-nocopy`. It retains the volume regardless
of outcome. The outer watchdog separately inspects name, label and driver after
container reconciliation; inaccessible, absent, or mismatched state is reported
as uncertain and fails the run. The sender committed at
`0ce91143bac343d130da103f416a1988f29ec11c` records actual in-container
`statfs` bytes/inodes and unchanged requested budget (96 MiB, 1,000,000 inodes
plus 1,024 inode floor) and `storage.space` byte reserve. It does not reduce or
mock the capacity check. The owned volume remains even on a successful run.
Independent review of the exact runner, watchdog, ownership checks and marker
paths preceded the volume-backed run.
The runner requires a successful Docker volume listing and exact-name absence
before create; an unexpected existing name is a hard stop. Listing is only a
point-in-time observation: a concurrent or delayed create can race it, and an
ambiguous create response must remain explicitly uncertain. Exact label/driver
checks do not prove exclusive creation in the face of such a race.

Run `d9f8db89a58946aa95e53fb10a95ee82` passed capacity checks with 66,677,749
free inodes and 993,498,292,224 free bytes, but actual ingest failed because
Node's ESM resolver ignored NODE_PATH and could not find `geotiff` from the
`/viewer` snapshot. Synthetic acceptance had already persisted before ingest.
The owned runner stopped Python container
`9dc9ba99d11195d9ebe8aeb6d4f84bc95eae5a315ad7c9817f98e6e014db9031` (137);
Viewer `f9297f3a8dd56eda6997addca119666605d6db4132a4bb93276d51f93ccace33`
exited 1, Windows Job active 0, graceful inner exit unverified. Root
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-pboe5t0_` and
matching volume `synthetic-joined-models-d9f8db89a58946aa95e53fb10a95ee82`
remain retained; name/label/driver matched, with the documented creation race.

The next staged variation mounts only the pinned Viewer `server` snapshot
read-only at `/src/apps/viewer/server`, adjacent to the dependency image's
lock-verified node_modules. This allows ordinary ESM package resolution without
code changes, package installs or synthetic import mocks. A dynamic-import
preflight of `explicitSourceUnitInspection.mjs` must succeed before the committed
synthetic sender `1fbea012cb3564f87db96231cec20453e2dc678a` runs. Its blob
SHA-256 is `257a8c75acfdd204694dba6ef145d8a745f61ee7d2d982c8f6de5d45307b4ca4`.
The exact new mount and preflight were independently reviewed before execution.

## Passing run, 2026-11-01

After independent review, run `429303b5a50d477eb7cc6dda6bb463ea` passed with
the exact Processing, Viewer, fixture and sender commits listed above. Actual
worker/coordinator recovery, proxy streaming, Viewer held receipt, coordinator
acceptance, and Viewer `ingestHeld` completed with the synthetic ODM-shaped ZIP.
The real repository registered the model, kept its native derivative held,
and claimed unrelated derivative work. **Acceptance preceded ingest**; the
fixture does not prove ingest succeeded before the coordinator accepted data.
Assets are placeholders, authorizers synthetic, and no derivative activation,
derivative worker, real ODM, production authority or power loss was tested.

Retained root:
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-_evur17r`.
Exact labeled Python container
`fbd9aa429b24d88fb10d599b1af3d678f9cf749f88c659e9eb56e755efb96a83`
and Viewer container
`b34a32227334a52017e9077029b53d90644ca7280918a216a3c4573c9291307b`
both exited 0; inner process group exited 0 and Windows Job active count was
0. No container ownership uncertainty was reported. Local volume
`synthetic-joined-models-429303b5a50d477eb7cc6dda6bb463ea` matched name, run
label and local driver and remains retained; an initial absence check cannot
eliminate a concurrent-create race. All containers and root remain retained.

Actual models `statfs`: 993,494,290,432 available bytes and 66,677,513 free
inodes; requested 100,663,296 bytes and 1,000,000 inodes, inode floor 1,024,
byte reserve 0. Pinned Viewer/image lock hashes both
`77c8593ec3db101429175f2d9755f9b346d47fa58e7651f18e59ce0a37a35c44`;
`unzipper` 0.12.3 and the ESM preflight loaded. Python image
`sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`;
Viewer dependency image
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
The IP-address TLS server-name deprecation warning persists.

New fixture blob SHA-256 values:

* archive `20af7a5f2a2824c842bab12218df5a68fc8c4b70b9bf525122af9948afc32946`
* engine `44e9c5b126344e9b6f0e0db5779e73c3d6e62adbb9e4fb2d945956bcb0787c85`
* sender `257a8c75acfdd204694dba6ef145d8a745f61ee7d2d982c8f6de5d45307b4ca4`
* supervisor `4236057021d15f4e085a0601d28526c544b9fcfa77ef4cc4b48e21d62db468c2`
* watchdog `62535d8618a1f9bbebfb217e8bc07dd3716e1324d0ad4b8ed687d1aebf2b5a05`

Existing fixture/module hashes are documented in `JOINED_WORKER_VIEWER_PLAN.md`
and `JOINED_DERIVATIVE_HOLD_PLAN.md`. Prior failed runs and retained identities
are recorded above. No recursive or Docker cleanup was performed.

The existing fixture must verify metadata and bytes after coordinator restart;
the runner must additionally require the actual ingest/hold marker and exact
exit statuses. All child/container identities, run labels, scope and uncertainty
must be reported; retain all stopped containers and all roots, no deletion. ZIP
file parsing, storage capacity/inodes, image dependencies and deadlines may
cause failure; do not bypass fences to obtain a pass.
