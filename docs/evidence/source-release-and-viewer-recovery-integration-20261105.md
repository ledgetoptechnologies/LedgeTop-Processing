# Source release and Viewer recovery integration

The review branch integrates the original authenticated source-release commits
`3ce5662`, `5e4ca3b` and `be6dce3` as `a0b1b8b`, `ae3d3fd` and `f956050`.
The backend remains disabled by default. No installed service or NAS setting
was changed.

Independent review found the final two lifecycle corrections sound: every new
filesystem effect checks the freeze phase in the same SQLite transaction that
records its prepared authority; actual CoordinatorService shutdown joins source
I/O and timer threads before releasing Queue/source/external leases. Recording
an already-issued outcome remains possible while draining. The owner's earlier
99-test Linux run is documented separately; it is not installed-host proof.

Viewer pairing uses immutable `fe49d665a5b6e9582e3eb7840d6f171f5c707b0d`.
It was exported with `git -c core.autocrlf=false archive`; archive SHA-256 is
`6bc4b18f244b53cb0283d2c764b3378491536e90e2371c868c1d7d0c95322a82`.
All 4,118 exported regular files were compared to archive bytes. Dirty Viewer
worker/UI changes were excluded. Converter inputs are unchanged from the
separately qualified licensed ImageSharp 3.2.0 artifact.

The first eight-test run passed six tests in 82.189 seconds. Both earlier GCP
failures were cleared. Two assertions still expected the old client's network
sequence: they rejected the newly supported submission lookup and expected a
second POST after a lost admission reply. `ac8f4fe` now requires the exact fresh
submission lookup to return 404, and the same lookup to return 200 after the
lost response, with exactly one admission POST and unchanged durable identity.
It does not allow arbitrary extra routes or weaken the one-admission check.

The combined rerun includes all eight Viewer pairings, the ten source-release
and service modules, exact source replay tests and predecessor census tests.
At Processing `ac8f4fe`, all **156/156 tests passed, no skips, in 142.949
seconds**, Python 3.13.17. The task-owned Docker runner used one CPU, 2 GiB,
128 PIDs, a read-only source tree and no external network. Its container and
scratch volume were verified absent after exit 0. Engines and source
mount barriers are synthetic; actual NodeODM processing, installed cutover,
NAS crash durability, typed TreeDar usable-output registration and Viewer
release-worker activation remain separate requirements.

Viewer's committed release helpers at `5cac6d6d` and its dirty worker are not
part of this paired export. Review has identified readiness-proof equality and
authority grammar corrections for the Viewer owner. A terminal release must be
durably recorded before retiring the matching operational hold; Viewer originals,
accepted results and raw GCP files remain retained.
