# Native Viewer receipt rehearsal

On 2026-11-01 the optional cross-language test ran with the committed Viewer
candidate at `73a1ae5959753131293ed36d09604d93653e5de0`. Both tested module
files were clean relative to that checkout:

- `nativeReferenceReceiptCandidate.js` SHA-256:
  `f122561bd28d66d0c5ebb08a2e982ab350b152d9a7f2caa33fcd97dd5d9c216d`
- `nativeReferenceProtocolCandidate.js` SHA-256:
  `9945b37616fa1dd6093eb5511069de203deb698beb8af827dc153580ce4b6d95`

The two focused tests passed both on this branch and on a test branch based on
Processing integration candidate `1b101e1ea531a5c83e60429f08e9e1a30d302212`.
The test generates a receipt with the Viewer candidate and sends it through the
actual Processing HTTP API, store and worker relay, including an exact replay.
The selected module hashes and checkout status are checked again after the Node
call. To run it, point `LT_VIEWER_CANDIDATE_DIR` at the Viewer's `apps/viewer/server`
directory and run `python -m unittest -v tests.test_reference_acceptance_actual_contention`.
Without that variable, the cross-language test is explicitly skipped.

The storage authorization and verification callbacks in this rehearsal are
synthetic. The Viewer sender, production constructors, TLS, persistent disks,
process-crash recovery, physical writer fencing and actual Viewer custody are
not established by this test. This is not deployment approval.
