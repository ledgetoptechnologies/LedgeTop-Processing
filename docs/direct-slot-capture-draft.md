# Draft allocated-slot streaming journal

This is an isolated primitive layered on the draft same-inode allocation. It is
not wired to NodeODM, a network endpoint, NodeGate, Viewer or a worker. It does
not authenticate the caller or assert a source result's integrity or custody.

Before writing, `begin_capture` validates the entire immutable proposed task
binding, locks its preallocated inode, and durably records the binding, slot
identity and optional declared length in a new intent file. `consume` reads a
binary stream sequentially to observed EOF, rejects a length exceeding the
slot or a mismatch with any declared length, syncs the file, rereads the exact
used prefix, checks that its SHA-256 matches the incoming stream's locally
computed SHA-256, and parses the prefix's ZIP central directory. It then
creates and syncs a seal containing the exact length, hash, intent digest and
inode. The prefix reader excludes the unused preallocated tail. ZIP validation
is structural only; it does not decompress entries, verify CRCs, validate
products, protect against all archive bombs, or qualify Viewer ingestion.

NodeODM's existing download adapter does not supply a trusted predeclared
SHA-256; Content-Length, when present, is still a source assertion. This hash
detects a mismatch between the bytes handed to this primitive and the bytes
reread locally; it does not establish that the source provided the correct or
complete result. The caller must provide the actual response stream, enforce
network/total deadlines and attest that EOF came from the intended task under
an independently verified engine identity. An actual Viewer custody receipt
must come from a separate, authenticated ingestion and validation protocol.

After a crash, an intent without a valid seal is held without retry or deletion.
A partial or malformed intent/seal is also held. `verify_sealed` accepts only
the same full binding and inode, rereads and rehashes the exact used bytes,
rechecks ZIP structure, and syncs the file, records and directory before
returning the same seal. It never treats file existence alone as completion.
The original unjournaled slot opener refuses files carrying either journal
marker. External noncooperating access to raw files remains outside this guard.

Filesystem and mount assumptions, same-UID mutation races, thin provisioning,
power loss, root identity reconciliation after reboot and global capacity
limitations remain those in `direct-output-slot-draft.md`. A stream read can
block if its transport does not enforce its own deadline. No source files or
results are deleted. This draft has no qualified recovery/release procedure for
held or orphaned records and no production cleanup policy.
