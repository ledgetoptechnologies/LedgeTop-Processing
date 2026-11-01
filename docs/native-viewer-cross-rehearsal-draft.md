# Native Viewer cross-repository rehearsal draft

Processing is based on `42da801be55b686e43a78e35b88d32de89f8c5b4` (PR 242).
The Viewer owner supplied stable commit `3fd097cadeeb1f3ac6fb2354a031798252c731f2`
for this synthetic test. The launcher exports exact Git bytes read-only.

The fixture uses Viewer `processOne`, SQLite and normal result ingestion
against the native coordinator HTTP server and synthetic HTTP engines. It
intends to test one running staging attempt, two queued attempts with
production priority, an offline coordinator and graceful in-process restart,
and one Viewer model-output row per attempt. In the passing run the same
reconcile row recorded an unavailable observation while the coordinator was
stopped and later completed; synthetic source hashes, coordinator archives,
engine results and the absence of engine-removal calls were also checked. The
restart was graceful and in-process; it was not a process-kill test. The Viewer
provider admission limit was set to two for this test; that is not a statement
about production configuration. Its synthetic ZIP contains
placeholder geometry and does not prove a renderable model, real ODM output,
physical storage fencing, Viewer custody, crash recovery, or client readiness.

The fixture and its launcher are not qualified for deployment. The launcher exports Git bytes,
uses a pinned local dependency image, starts a uniquely labeled container with
`--network none`, limits CPU, memory, PIDs and wall time, and rechecks ownership
and daemon identity before cleanup. Uncertain resources are retained and make
the launcher fail. Evidence and synthetic Git archives remain in a new private
temporary directory; inspect them before any later cleanup. Git, Docker and
filesystem timeouts are individual limits, not a global hard deadline.
The image's preinstalled dependencies are linked into the private Viewer export
for Node ESM resolution; their compatibility with the pinned source lockfile is
not independently established. The 16 GiB tmpfs size is a filesystem ceiling,
not reserved physical capacity; the test container is limited to 768 MiB and
only tiny files were written. The temporary files and control server are synthetic only. Do not point
the fixture at live services, client data, or existing shared storage; do not
run it against the Viewer's uncommitted worktree. No remote cleanup or Viewer
acceptance acknowledgement is part of the intended test.

The passing run used fixture commit `85c7a37ef36c431b1b69bd7b7d12bbcb4800f1a3`,
Viewer `3fd097cadeeb1f3ac6fb2354a031798252c731f2`, and test image
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
The Processing and Viewer archive hashes were respectively
`ce6b97e6eca0aaa21c26a3cf5802354274b3ef8efb9739693267413f0fe8bfa8` and
`1cf6d76a8d0f3b8d7a87f820232892cc558025a376404fada9ade1431ac62141`.
Its retained local evidence directory is
`C:\Users\fstor\AppData\Local\Temp\codex-native-cross-88431bfb892e7fab9e6471e138a9faee-fpidtmmf`.
Owned container `7ef4707755d78275e5224ffed146916f87ab3b59f1aeb4fc071a6144e43abbf7`
was identity-checked and removed; subsequent inspection found it absent.

Earlier runs did not pass: the host logging-driver settings prevented one
container starting; a too-narrow Viewer export missed a tracked import; a
512 MiB tmpfs caused admission to defer; the default provider admission limit
prevented the intended two queued tasks; and ESM could not resolve the test
image's `geotiff` dependency from `NODE_PATH`. Corrections were confined to the
synthetic fixture and launcher. Their per-run evidence directories were retained.
The synthetic image itself does not establish dependency provenance or output
validity for a release.
