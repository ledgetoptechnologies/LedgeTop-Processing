# Paired Viewer source broker synthetic test

`tests/test_paired_viewer_source_broker.py` requires `VIEWER_C92_ROOT` to name
a clean local Viewer checkout at exact commit
`c92deb5ba143fd302bdf787ba832e0ac1bd0e2f0` and Node with node:sqlite. It
starts the actual Viewer `SourceRangeV2BrokerCandidate` and actual handler on
an ephemeral localhost HTTP port using a synthetic photo, private broker
SQLite and synthetic grant key. The independent Processing-side
`viewer_source_v2_reconcile_candidate` validates the minted grant against its
separately supplied synthetic attempt binding, manifest and policy; its
separate synthetic ledger records each attempt before Python sends the real
HTTP range request. The test confirms matching request IDs, correct bytes,
two retries counted once as a range and twice as attempts, revoked bearer
rejection, a capped third attempt and independent broker accounting. The
child server is explicitly shut down; no real credentials or datasets are
used. Loopback transport is marked protected by the harness, not a TLS test.

This is one actual Viewer broker HTTP service plus a Processing test process,
not two installed services. The actual Processing `source_range_transfer.py`
client in candidate 6bafe08 does not support Viewer c92deb5's required
viewerAttemptId field/identity; 0175ff7 supplies a non-I/O reconciliation and
synthetic ledger, not a production downloader. This test therefore proves
cross-language identity and HTTP broker compatibility of those exact pieces,
not an end-to-end installed worker, TLS boundary, authenticated epoch/fencing,
source custody or safe launch. Do not lift source, activation, execution or
cleanup holds. Expiry, signed grant, network outage, partial streams, parallel
requests, revocation races, retry and post-crash reconciliation still require
actual paired implementation tests; one denied revoked request does not fence
an already admitted stream.
