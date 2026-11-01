# Synthetic Viewer upload-capacity rehearsal

This unrun fixture targets the native coordinator source at PR 242 and a
pending, independently reviewed Viewer revision. The launcher refuses to run
until that Viewer commit is pinned. No production or client files are used.

The isolated fixture changes the coordinator's advertised and enforced limits
to 30 input bytes and 400 multipart-body bytes. It exercises the real Viewer
worker and native HTTP parser, checks that oversized input and unsupported
names/types cause no coordinator init or upload, and compares exact body
lengths, source hashes and coordinator stored-byte hashes for a two-photo batch
followed by one photo. A fixture-injected exception occurs after the first real
HTTP upload response has been fully consumed, and the worker resumes from the
coordinator's recorded image count. This is not a network disconnect or a
power-loss test, and does not prove custody, physical reservation, large-file
capacity, or a real ODM run. Real Viewer storage headroom checks remain enabled;
the 16 GiB tmpfs ceiling is not a physical reservation and the container is
limited to 768 MiB.

An earlier run with Viewer `330b6e317f3e3801899c2e9b1876ebb74b71c577` was
inconclusive: after three unrelated model ingests, a subsequent attempt was
still pending when the fixture expected a preflight failure. The run finished
before a requested stop could reach its already-removed owned container. Its
evidence is retained in
`C:\Users\fstor\AppData\Local\Temp\codex-native-cross-428fe9a314fcb037932d43b95a3442ca-xbuxhe79`.
The revised fixture starts from empty state and separates these capacity cases
from the earlier full-ingest rehearsal; it has not yet been run.
