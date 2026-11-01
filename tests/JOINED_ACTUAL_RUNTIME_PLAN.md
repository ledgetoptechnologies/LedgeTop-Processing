# Joined synthetic actual coordinator runtime

The exact source, configuration, process lifecycle, container/volume ownership
and failure handling were independently reviewed before the recorded run.
Later changes require a new review before execution.
No production data, credentials, host service, or cleanup is involved.

The coordinator child uses actual `processing.app.make_handler` with a private
schema-v1 ReferenceRuntime configuration, distinct synthetic tokens for the
two configured nodes and Viewer, a real TLS certificate/CA, the shared Queue,
its real ReferenceRuntime result proxy and bounded stream semaphore. It starts
the actual reference listener, verifies a request without bearer gets 401, and
is killed/reaped and restarted by the exact-owned supervisor. The synthetic
worker retains its ODM-shaped placeholder archive through that outage; Viewer
uses the real proxy stream, holds/sends its receipt, calls the experimental real
Viewer ingest and checks the derivative hold. The Viewer receipt is acknowledged
before ingest. Authorizers are synthetic; no production activation, derivative
worker, real ODM, cross-host TLS or power loss is tested.

Important boundary: synthetic coordinator setup inserts the one job directly via
the actual runtime's ReferenceStore using the contract fixture; it does not
exercise `_DynamicAPI` admission, capabilities discovery, HTTP job submission,
or actual node option discovery. The input grant is also fixture-controlled.
The required 401, authenticated result retrieval, worker delivery and Viewer
acceptance are real HTTPS requests to the listener. This is runtime wiring
coverage, not a production-ready end-to-end deployment claim.

The new coordinator/supervisor/watchdog blobs are pinned to
`147d015deadf1579d99b703e6562953b9847330d`; Processing runtime source remains
`4020f922990bca17598390868a8d378e80615154`, Viewer source
`f13c4e0465640516305aa3f9eeb1ad0735254bb9`. Their blob SHA-256 values are
`9adce99a53c9231874154786975314312736b7fa4b7d1f5d4bcfcf4bd1759884`,
`12702dd3cf7d634599f4e42d554513c8b92e4cb5694c0bc49b4c8d425ef030b5`, and
`a06345588a0ac45a96e6453bade9e335390bd8f759862ad717ac7d810e9e8888`,
respectively. Existing pinned ZIP/sender blobs, digest-verified package-lock,
local dependency image, synthetic volume capacity gate and named-resource
reconciliation are as in `JOINED_REAL_INGEST_PLAN.md`. The runner requires the
actual-runtime 401 marker plus existing ingestion/receipt/cleanup markers.

Processes and containers are bounded by the previously reviewed inner process
group watchdog and outer Windows Job/owned container runner; creation and kill
actions use exact names/labels and direct handles. The local named volume is
retained without deletion and pre-create race remains a known ownership limit.
`runtime.close()` and HTTP server joins can block; the outer timeout bounds this
case and must report incomplete cleanup/uncertainty rather than pass. Keep all
synthetic roots, logs, stopped containers and volume. Host launcher and locally
resolved image IDs remain outside an independent immutable launcher trust chain.

## Passing synthetic run, 2026-11-01

Run `0715b51ec2954a9dbda40c1f28602fa1` passed with the exact runtime, Viewer
and fixture source pins above. Real `make_handler`/ReferenceRuntime private
configuration and TLS listener were used; an unauthenticated request was denied.
The coordinator process was killed and reaped after engine commit, then
restarted. The worker retained the synthetic archive, Viewer staged the real
proxy stream and sent an acceptance that the coordinator persisted. Viewer then
used experimental `ingestHeld` with synthetic authorizers and registered the
model; its native derivative stayed held while an unrelated derivative could be
claimed. **Acceptance was before ingest.** Cleanup was disabled throughout.

Retained root:
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-fkmggupc`.
The exact labeled Python container
`d3664818c4e979105c8bba12a8e42a8774f9559af3c709dab6214a802922c6a0`
and Viewer container
`b4bb2c1afddcdb153e90aba02a00bed2339c2f91598d4a3dea1b192d92d8d5fd`
both exited 0; inner process group exited 0 and Windows Job active count was 0.
The matching labeled local volume
`synthetic-joined-models-0715b51ec2954a9dbda40c1f28602fa1` is retained, as are
both containers and root. Its pre-create absence was observed; the inherent
concurrent-create race cannot be ruled out. No other ownership uncertainty was
reported. Node emitted the IP-address TLS server-name deprecation warning.

The container reported 993,490,288,640 free bytes and 66,677,277 free inodes;
the unchanged request was 100,663,296 bytes and 1,000,000 inodes plus a 1,024
inode floor, with byte reserve 0. Pinned Viewer and dependency-image lock hashes
matched `77c8593ec3db101429175f2d9755f9b346d47fa58e7651f18e59ce0a37a35c44`;
unzipper 0.12.3 and ESM import preflight loaded. Python image ID was
`sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`,
Viewer dependency image
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
Other committed fixture and exercised Viewer module hashes match those recorded
in `JOINED_REAL_INGEST_PLAN.md` and `JOINED_WORKER_VIEWER_PLAN.md`.
