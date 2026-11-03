# Provisional prepared-envelope interoperability vectors

These synthetic fixtures target Processing base `569b93c6a7825bf2c2c9b1d4d9760baf8b944ac6` and
Viewer `793aa66ba0c515287a8cfe1eebc3b763b01d2cef`'s actual
`apps/viewer/server/referenceProtocol.js` Git blob
`354192ea1a6d902171d3e78c47a96c87ca125896`. Viewer has not confirmed the
prepared-envelope schema. These are proposed shared vectors, not proof of an
integrated Viewer sender or an agreed production protocol.

The fixture records exact UTF-8 canonical bytes as hex and SHA-256 of
`b'ltds-viewer-prepared-envelope-v2\0' + canonicalBytes`. Node generated those
values using the pinned Viewer source; the test independently recomputes them
using that source and Processing's `canonical_json`, and compares the bytes and
digest actually persisted by Processing's `prepare_viewer_envelope_v2`.
The examples cover Unicode option strings, maximum safe integer and 128-byte
identifiers, and wrong/missing/extra envelope and nested fields. The Node source
is checked against the pinned Git blob (normalizing checkout line endings).

**Known wire incompatibility:** Processing currently accepts negative numeric
and floating-point ODM option values, while the pinned Viewer `canonicalBytes`
rejects them. The test records this mismatch as an expected synthetic outcome;
it is not an endorsement to send them. Dynamic numeric option support needs a
mutually agreed, versioned representation before the candidates are wire
compatible. The sample vectors exercise only an accepted common subset. Neither
candidate is connected to a live endpoint by these tests.

Run with Node installed and `VIEWER_PROTOCOL_JS` pointing to the pinned Viewer
file, then `python -m unittest tests.test_prepared_envelope_shared_vectors
tests.test_reference_viewer_activation`. Without the environment variable the
cross-language check skips, and the run cannot establish Viewer compatibility.
The tests exercise an isolated local queue with synthetic data; they do not test
the actual Viewer sender, network transport, authentication, replay recovery
across services, or downstream activation.
