# Reference-v1 acceptance sender golden

`contracts/reference-v1-acceptance-sender.golden.json` is a byte-for-byte
copy of Viewer commit `f28c318e`, path
`apps/viewer/test/fixtures/reference-v1-acceptance-sender.golden.json`.
Its SHA-256 is
`5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41`.

The Processing tests submit an actual normalized synthetic v1 job, register
the exact fixture result, commit an acceptance through a real loopback HTTP
listener and `ReferenceAPI`, observe then discard the response, reopen the
coordinator, and submit the identical receipt over a fresh listener. They
verify a single stored receipt, stable ID/digest, and conflict on changed
receipt bytes. Two cases use the actual Viewer sender's UUID storage receipt
and its digest-derived bridge-shaped candidate. The latter tests Processing's
acceptance of that shape; it does not execute or qualify the Viewer bridge.

The listener is plaintext loopback, not the production TLS deployment. This is
a synthetic metadata test, not a network-loss or power-loss test, and
establishes no production authorization, durable Viewer custody or permission
to delete any worker data. Viewer-side sender and bridge tests remain separate.
