# Native Viewer acceptance: isolated lost-response pairing

This test pairs the actual native Processing `ViewerFacade`, `ViewerService`
and durable `TaskStore` with the actual Viewer `ProcessingRepository` and
`deliverOneViewerAcceptanceAck` implementation. The Viewer-side model and
archive provenance are seeded into a fresh temporary SQLite database; the
Processing side runs a fake engine with synthetic input and archive bytes.

The first successful native ACK commits in the coordinator database, then a
test-only handler drops its TLS connection before sending the response. The
test verifies that the same Viewer outbox record stays pending, reopens the
same Viewer database, retries the exact event and payload, and becomes
delivered after the coordinator returns `replayed=true`. It checks one
coordinator acceptance row, one Viewer outbox row, one engine commit, and an
unchanged retained archive. The retry due time is moved forward in the isolated
test database; Viewer database reopen is not an actual worker-process restart.

In a network-disabled Linux container, the paired test and six related native
service, HTTPS-only and TLS-listener tests passed. The test used Processing
source parent `ad7f0f57ee9c8e0803430ea817a1349e12c94e01` and clean Viewer
source `80bb4be0a6fe293146dc5cc37091f0a27045211f`. The named disposable
container was removed and its absence confirmed. Synthetic TLS keys and
tokens were generated locally; no live services or client data were used.

Before starting its services, the paired test verifies the full Viewer
`apps/viewer/server` file set, `apps/viewer/lod-converter-policy.cjs`, and
`apps/viewer/package.json` against the 127 SHA-256 entries in
`tests/support/native_viewer_80bb_manifest.json`. Those entries were derived
from `git archive 80bb4be0a6fe293146dc5cc37091f0a27045211f` over those paths,
with CRLF text normalized to LF. The verifier rejects modified, missing,
extra and symlinked server source files and does not print their contents.
It verifies the covered content rather than the entire repository or its
current Git HEAD. The Processing test source, Node image and external Node
dependencies are separate trust inputs. The test's Viewer mount was read-only;
that does not by itself exclude another host-side writer during the run.

This proves the cross-language lost-response boundary for the source versions
under the stated fixture. It does not prove a live response loss, installed
image equivalence, genuine photogrammetry, real Viewer ingestion, natural
backoff timing, independent worker-process restart, or physical-node fencing.
