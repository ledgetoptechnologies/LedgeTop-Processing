# Held cross-service renewal regression

This isolated test fixture pairs Processing 99bf's attempt-bound source client
with the actual Viewer broker, dispatcher and renewal service at Viewer commit
`76acaa506d8dc528a61535122adcbd0ae3fe5ac7`. It remains synthetic and unmounted.
Set `LEDGETOP_VIEWER_SOURCE_DIR` to that clean checkout and
`PAIRED_RENEWAL_REQUIRED=1`; the latter fails instead of silently skipping when
Linux Node or the checkout is missing. Node must support `node:sqlite`.
The default pin is the unfixed negative control; set `PAIRED_VIEWER_COMMIT` to
`cae26c98d61652abd7157d68b680cfdc6d0ee5af` and point to that exact clean Viewer
checkout for the fixed candidate.

The Processing claim attempt is injected in the test, and its transfer request
is round-tripped through `reference_protocol.canonical_json` to model the
ReferenceStore persisted JSON order. The real Processing prepared claim, HTTP
provider, driver renewal journal, protected transport and source activation are
not exercised. Viewer DB fixture drops admission triggers and reopens its
migrated database on restart; no production migration or live job is tested.

Current known regression: Viewer 76acaa stores the initial transfer request as
`JSON.stringify` and later compares it byte-for-byte, while its renewal path
reconstructs equivalent fields in a different property order. The real broker
returns `TRANSFER_BINDING_CONFLICT`, exposed as `503 renewal_unavailable`.
The two renewal tests remain failures against the negative control, not expected
failures. Against Viewer candidate `cae26c98`, the paired tests plus Processing
source-range suite pass 28/28 without skips on Linux with Node 24. The tests
exercise a retained budget through renewal and Viewer process restart, not a
production ledger migration. Do not enable any
grant, activation, cleanup or live source path based on this fixture.
