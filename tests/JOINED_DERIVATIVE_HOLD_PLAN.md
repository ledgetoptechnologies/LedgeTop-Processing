# Joined synthetic native derivative hold

The exact scripts and ownership were independently reviewed before the run.
Later changes require renewed review before execution.
Use the existing owned two-container network-none fixture and Windows Job
watchdog. Source pins: Processing `4020f922990bca17598390868a8d378e80615154`,
Viewer `f13c4e0465640516305aa3f9eeb1ad0735254bb9`, existing fixture blobs
`a174b8ae1b00e9f3bb4ab71f517bc80c2fe539ba`, modified Viewer sender blob at
`1138871548dacde30ff6f6edb8626816095f1e2e` with SHA-256
`d58eda1e34e7ab6bbc0d577a9d619c2dc9c242405979bf49795ecc7106cbaf2f`.
The launcher snapshots both committed objects and prints their hashes, plus
Viewer database and repository module hashes. The host runner/watchdog themselves
are not independently pinned and local image IDs are recorded, not fixed.

After the existing synthetic worker completion and coordinator process restart,
the Viewer sender uses its actual database migrations and repositories to create
a synthetic project, dataset, provider, attempt and model. It stages the actual
proxy stream and sends the held receipt. In that same test database it explicitly
seeds a synthetic native registration row, queues a native derivative using the
real repository, asserts no derivative claim is granted and state stays pending,
then queues an unrelated attempt and asserts that derivative can be claimed.
The synthetic registration row is test setup: no actual ingest, derivative
generation, writer authority, publication, or real data is implied. No existing
production database or shared service is touched.

The runner must require the new hold/unrelated-work marker as well as the
existing joined acceptance markers. Exact owned container identities, labels,
exit states and Windows Job active count must be verified before reporting pass;
failure or uncertain cleanup is not a pass. Retain roots/containers and do not
delete recursively. The Node container's real repository modules and migration
dependencies may reveal missing fixture assumptions; those are test failures,
not permission to bypass a production fence. Do not use synthetic authorizers
as evidence of actual production authorization.

## Passing run, 2026-11-01

Run `f76f18a39a3341ab9b84e4e775518185` passed. The worker and coordinator
completed the synthetic crash/restart, real proxy stream, Viewer staging and
persisted acceptance path. Afterward, the real Viewer ProcessingRepository
kept the explicitly seeded native registration's derivative pending and claimed
an unrelated derivative. No derivative worker, actual model ingest, qualified
writer authority or production publication was exercised.

Retained root:
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-3l0volal`.
Exact labeled Python container
`f160e17b9f2b06473fa18592f80f183bca9a4a390eafde12eb097f7c8d4ff0de`
and Viewer container
`ea1f3f59053d180961254b09b97d6115d6278fec381a824ee4b1b74271dc5585`
both exited 0; inner process group exited 0, outer Windows Job active count was
0, and no ownership uncertainty was reported. Containers and root are retained.
Node emitted the IP-address TLS server-name deprecation warning.

Viewer module SHA-256: `database.js`
`b7f797091ed0dd53e2720d5652c76bff9c2198ea42d5771511d4d279f6e00bbb`,
`processingRepository.js`
`6c169c38cddc7f3b81e584623f332bc7e60a1449bce037be7b07fd852d4ed577`,
and `repository.js`
`5c76c00d78f95e736f6fb4326601a3c2fa385f924485cc601c718358eb8552a4`.
The five original fixture hashes and nine exercised native Viewer module hashes
match those listed in `JOINED_WORKER_VIEWER_PLAN.md`. Local image IDs were
Python `sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`
and Node `sha256:c86ecead9e55a5210ac1727de38a4ca840bf03dcaf34dc9749addbf383b1920c`.
