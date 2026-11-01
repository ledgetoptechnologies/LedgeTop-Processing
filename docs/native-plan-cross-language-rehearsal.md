# Synthetic native plan HTTP interoperability

The isolated rehearsal used Processing commit
`0c245a8d1927347dfdbce1f8a9b071f2f24ddeef` and Viewer commit
`b05b51ce659bea101b6ea4231c6bd6dcd749f5c5`. It verified all 78 selected
Processing Git blobs and 131 selected Viewer Git blobs against the source
commits, then checked seven Viewer files against pinned SHA-256 values. The
immutable test image was
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.

One selected synthetic test passed in a network-none, unprivileged, 1-CPU,
768-MiB container. It used the real Viewer migrated SQLite database,
ProcessingRepository and unwired candidate clients against the real loopback
coordinator HTTP routes. A fixture supplied a synthetic healthy catalog. The
Viewer bound a manifest and empty plan using an encrypted capability, then
simulated loss by throwing after consuming the successful plan response. A new
Node process used status rather than re-POST after an in-process coordinator
listener/service restart, and checked the persisted receipt. Wrong environment
and capability held. A separate nonempty Viewer plan held without a plan POST.
No objects were uploaded or dispatched.

Successful run `native-binding-63e5beabeea4c70aeea606250c51b7b2` passed;
its exact owned container was verified exited and removed, and evidence was
retained. The first run `native-binding-c101bd91d4f867b822b563af702efcd5`
failed after the initial recovery path because the second synthetic fixture
reused an active project name. Its exited container and evidence were retained.
The fixture was changed to generate a distinct synthetic project name before
the single corrected run.

Viewer b05b51c has a known pre-POST stale-local-context race. This test does
not resolve it or qualify cancellation and mutation authority. The injected
error is not a network disconnect, process kill or power-loss test. The test
uses loopback HTTP, fixture-patched qualification and a synthetic catalog; it
does not qualify production TLS, external writer fencing, engine behavior,
physical capacity, transfer or dispatch. No user data or live service was used.
The wrapper retains evidence and uncertain containers. Its evidence child is
0755 for the unprivileged container and may be read by local users who can
traverse the supplied parent; use a private parent.
