# Held cross-service renewal regression

This isolated test fixture pairs Processing 99bf's attempt-bound source client
with the actual Viewer broker, dispatcher and renewal service at Viewer commit
`76acaa506d8dc528a61535122adcbd0ae3fe5ac7`. It remains synthetic and unmounted.
Set `LEDGETOP_VIEWER_SOURCE_DIR` to that clean checkout and
`PAIRED_RENEWAL_REQUIRED=1`; the latter fails instead of silently skipping when
Linux Node or the checkout is missing. Node must support `node:sqlite`.
The default pin is the unfixed negative control; set `PAIRED_VIEWER_COMMIT` to
`c959ba5627737b9c373b44ef08cfa2d1184a3dec` and point to that exact clean Viewer
checkout for the fixed candidate.

In the renewal tests the Processing claim attempt is injected, and its transfer request
is round-tripped through `reference_protocol.canonical_json` to model the
ReferenceStore persisted JSON order. The real Processing prepared claim, HTTP
provider, driver renewal journal, protected transport and source activation are
not exercised. Viewer DB fixture drops admission triggers and reopens its
migrated database on restart; no production migration or live job is tested.

An additional unmocked test passes the real Viewer-generated submission into
an actual prepared-v3 ReferenceStore: preparation succeeds, but the unpatched
source-provenance gate rejects activation, claim returns no work, reserve rejects
the missing activation, and the real driver activation-bound-grant guard holds.
It verifies no activation, claim or reservation was created. It cannot invoke the
driver's renewal journal because a legitimate claim and activation-bound grant
cannot exist through these held routes. The JCS dependency is required. No
readiness flag or production path is changed.

Current known regression: Viewer 76acaa stores the initial transfer request as
`JSON.stringify` and later compares it byte-for-byte, while its renewal path
reconstructs equivalent fields in a different property order. The real broker
returns `TRANSFER_BINDING_CONFLICT`, exposed as `503 renewal_unavailable`.
The two renewal tests remain failures against the negative control, not expected
failures. Against Viewer candidate `c959ba5627737b9c373b44ef08cfa2d1184a3dec`,
the paired tests plus Processing source-range suite pass 29/29 without skips on
Linux with Node 24 and the JCS dependency. The tests
exercise a retained budget through renewal and Viewer process restart, not a
production ledger migration. Do not enable any
grant, activation, cleanup or live source path based on this fixture.
