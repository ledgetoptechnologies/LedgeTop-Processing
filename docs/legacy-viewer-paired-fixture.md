# Legacy Viewer submission fixture

This branch runs a synthetic result-return scenario only. The node engine,
probe and option catalog are fakes; Viewer storage headroom and available
space are simulated, and the synthetic reconcile job's due time is advanced.
The fake ZIP contains placeholder geometry and does not verify visualization,
photogrammetry or derivative execution. The test requires real Viewer SQLite,
coordinator HTTP and retained archive, result reconciliation and ingest.

This branch injects a failure on the fake engine-to-coordinator result stream
after seven bytes. It inspects the coordinator's owned partial and intent,
then retries Viewer ingest and verifies the complete receipt and retained
archive. This is not a Viewer-to-coordinator connection interruption or a
coordinator process restart. The same synthetic Viewer job lease is retained.

This fixture pairs committed Processing `693cb7eca32295da959a4f42399f317577ef8a8a`
with Viewer `c2bba3d589ad0ea4718e94dc72f742b7c0d0e5ff`. The runner checks
both clean source trees (or its own single fixture commit on the Processing
base), uses a pinned local Node image, disables container networking, mounts
source and an explicitly supplied npm cache read-only, and installs pinned
production dependencies from the Viewer lockfile offline with scripts disabled.
It needs a complete cache, Docker, Python on the host, and the pinned image.
Cache or image unavailability is a test setup failure, not a passed test.

Run `python tests/run_legacy_viewer_pair.py --processing <absolute Processing
worktree> --viewer <absolute Viewer worktree> --npm-cache <absolute npm cache>`.
The test creates one uniquely named and labelled owned container; ambiguous
creation or failed identity checks retain it for inspection. The host runner
times out attachment, verifies identity before stopping/removing it, and does
not restart Docker or alter existing containers. Files inside the container are
synthetic and ephemeral. Source and cache paths must be trusted and stable
throughout the test; path replacement and concurrent edits are not fenced.

On 2026-11-04 a local run passed clean submission and a simulated lost upload
acknowledgement after the complete multipart response: three filenames and
SHA-256 values matched in coordinator storage, and each task was queued. The
Viewer submission phase reached committed after retry in the loss case. The
test uses the real Viewer `processSubmit`, `NodeOdmProvider`, SQLite repository,
coordinator HTTP handler and task store. It substitutes engine discovery,
option catalog, storage-headroom check and an in-memory fake engine. Package
install scripts are skipped. The container was removed after the run.

The partial mode injects a test-only socket shutdown after the first stored
multipart item. The runner inspects its exact filename and digest before
allowing the same Viewer job to retry, then compares the entire final set.
This is one deterministic single-sender interruption, not a natural failure or
proof of all partial upload cases.

The overlap mode also holds the first request inside the handler after its
first stored item, aborts the first client request, and lets the same job
retry. It confirms that the original handler is still blocked when the retry
finishes, releases that handler, waits for its exit, and compares the final
filenames and digests. This models a specific same-job request overlap, not a
real worker lease expiry, a process crash, or arbitrary changed payloads.

This does not cover out-of-order acceptance or stale senders after lease loss,
true engine processing, archive delivery, Viewer ingestion, deployed image
equivalence, host fencing, or real data. In particular, an `imagesCount` cannot
prove file identity in the presence of overlapping senders.
