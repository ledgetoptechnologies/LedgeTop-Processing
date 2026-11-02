# Synthetic paired bridge and partial response

The command is `python -m tests.paired_bridge_drop_outer`. The four fixture files
are `tests/paired_bridge_drop_outer.py`, `tests/paired_bridge_drop_launcher.py`,
`tests/paired_bridge_drop_in_container.py` and `tests/paired_bridge_drop_client.cjs`.
They reuse the reviewed `tests/paired_owned_windows.py` Job Object watchdog.

The Windows launcher checks Processing source/contract against commit
`3e4c5eacfd4df22169b9b08ae5e4b90c9e7da57a` and Viewer server against commit
`7c526b49d78423372c29eac98b95a16a270c2bd3`; its golden SHA-256 is
`5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41`.
It snapshots the two fixture scripts into a fresh retained root and prints
their hashes. It checks the pinned image ID
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
The proposed single container is `--network none`, read-only root, 1 CPU,
1 GiB, 64 processes, no Linux capabilities and no-new-privileges. It has only
read-only Processing/Viewer/script source binds and one new randomly named,
labeled local volume at `/owned`; container root is used only for that volume.
There is no host Docker socket, host network, or writable host bind. Python,
Node v24.22.2 and Node SQLite preflight precede any volume writes.

The coordinator is seeded with a deterministic real ZIP containing one
synthetic file. Its actual `ReferenceAPI` and `ResultProxy` verify and stream
the ZIP over loopback; a synthetic in-memory worker opener and no-op read lease
are used, so no worker TLS or physical producer pin is tested. A bounded proxy
preserves the binary MIME, length and integrity headers. The Viewer client
uses its actual `ReferenceProvider` to fetch the ZIP through that path, then
wx-writes and fsyncs the exact derived journal-key file in the owned volume.
All subsequent provider downloads are refused so a missing pre-staged file
cannot enter the journal's create/link/unlink fallback. The actual
`ReferenceResultJournal.retain` must verify that already-present file.

The actual `handleReferenceResult` and `ReferenceAcceptanceLedger` are used,
with synthetic `assertAuthority`, `ingestRetained` and `verifyIngest` callbacks.
The ingest seam wx-writes/fsyncs a managed ZIP copy and bound receipt and
rehashes them after process restart. This is not archive extraction, actual
catalog publication, a durable production authority or proof of genuine
custody. JS `unlinkSync`, `rmSync`, `rmdirSync` and their promise variants are
replaced with throwing functions, so a JS-level deletion attempt fails; this
does not intercept native SQLite's own possible sidecar behavior or provide a
host writer fence. No `captureProcessingArchive` or `safeZip` call is made.

The proxy forwards one byte of the first committed acceptance response before
closing the connection. First Viewer process verifies UNCERTAIN and a pending
outbox then exits without explicit DB close. A fresh process waits at most 15
seconds for the ledger's backoff, reopens the journal and receipt, verifies the
managed copy, and invokes the bridge again with the same receipt. The harness
requires only one binary transfer and one worker opener across both processes,
two identical POST body digests and one coordinator acceptance. This is a
manual retry of the bridge, not proof of a running production retry scheduler.

The launcher and outer watchdog use unique random names and run labels, a
cidfile and exact container/volume inspection. Outer watchdog and launcher
deadlines are 160 and 120 seconds. The in-container children have bounded
timeouts; local server threads are shut down and joined. The outer watchdog may
stop only the exact inspected owned container after verifying the cidfile,
image, name, run label and volume. Missing or ambiguous ownership is retained
and reported; containers, volume and temp root are never removed. A race between
absence check and volume creation, mutable host binds, Docker daemon failure and
host crashes remain outside the ownership proof. The fixture passed one
reviewed run on 2026-11-01: Python 3.11.2 and Node v24.22.2 preflight passed,
the ZIP was 173 bytes, and the marker was
`SYNTHETIC_BRIDGE_PARTIAL_DROP_RESTART_PASSED`. The exact owned container
exited with code 0; its volume and temp root remain retained for review. This
proves only this bounded local synthetic path, not production TLS, worker
read-pin authority, real data safety, catalog publication or cleanup rights.
