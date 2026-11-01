# Synthetic Viewer-to-Processing acceptance rehearsal (review before execution)

Scope: real Linux Viewer staging and held-outbox candidate plus its HTTPS sender,
against the real Python Processing HTTP/API/store acceptance path. Data and
credentials are synthetic. This does not test actual engine processing, Viewer
publication, production deployment, or durable real dataset custody.

1. Record exact Processing commit and Viewer commit, and SHA-256 of sender,
   receipt, and protocol modules. Create a unique task-owned root, export
   Viewer commit `46f3471748ade908773ea6cf60bba02e2568dabf` with `git archive`
   into a subdirectory, and do not remove it. This avoids dirty shared files.
2. Create an owned synthetic Python container from an already-local image,
   with `--network none`, no published ports, resource and PID limits, and
   read-only binds for the Processing worktree and exported Viewer tree. Set
   its working directory to `/processing` and run
   `python -u tests/synthetic_viewer_acceptance_server.py`. Record exact
   container ID. Confirm its `READY synthetic loopback 17864` log and that it is
   running. The server self-stops after two requests or 35 seconds.
3. Create an owned synthetic Node container from an already-local Node 24
   image, with `--network container:<exact-server-ID>`, no published ports,
   the same read-only binds, resource and PID limits. Run
   `node /processing/tests/synthetic_viewer_acceptance_sender.js`. Record exact
   container ID. It writes its synthetic SQLite database and three staged bytes
   only inside its container filesystem, retained after exit.
4. Bound both container waits with an owned host-side watchdog. After the Node
   container exits, inspect both exact recorded IDs and their exit codes/logs.
   If either exceeds its deadline, stop/kill only the exact task-created ID,
   then verify it has exited; on refusal retain and report ID/uncertainty.
   Never enumerate-and-kill, remove containers/images/volumes, or recursively
   delete any roots. No other session's resources are touched.
5. Pass only if Node logs `VIEWER_SEND_AND_REPLAY_OK`, the actual server logs
   200 created=True followed by 200 created=False, one retained receipt, both
   container exit codes are zero, and both exact IDs are stopped. Record image
   IDs, hashes, IDs, logs and any residual uncertainty.

## Recorded synthetic evidence (November 1, 2026)

The independently reviewed Windows watchdog ran these fixtures using Processing
`320c1d0aeeb69a173934c750937439c81187a0db` and Viewer
`46f3471748ade908773ea6cf60bba02e2568dabf`. The sender staged synthetic bytes,
created a real held outbox entry, verified the TLS certificate and observed a
matching actual acceptance response twice. Processing reported `200 True`,
`200 False` (exact replay), and one retained acceptance row. The Viewer entry
remained held. The three-byte object and empty report are synthetic. This does
not establish engine execution, publication, outages or crash-safe custody.

The successful run used fixture SHA-256 values:

- `synthetic_viewer_acceptance_server.py`: `e004023c8e2437d68f375b86ea85a047465eb86f623a8e0f34a01ca80ce7f208`
- `synthetic_viewer_acceptance_sender.js`: `f11350caae4712ff0828b17586d66dd0b0b9c6a3d2e0d746fc4afeeff8317985`

Viewer sender, receipt and protocol module hashes were respectively
`b86bb694aff87228ff2c014e44fbdddc996f53cb065ae97e8c37f1822d0cfe46`,
`32bd9c2c5d493e5c511e87b8612ca2f63e47ba74b1973cbddf84153825c0a88e` and
`5de970d4ee33e7b802bbe0c1eb99fec1713adfe005a5fcf60d2f4d8afa273028`.
The two successful-run containers (retained, exited zero, matching labels) were
`06a815db57f84f5aa427b3b7e6a02625663e33ad8a68e216eb5788b8e4b94a7b` and
`778e9e60851bc4598a34749a665a0f56d4040155e5b9dade3fe3b7e9b6fe0724`.
The outer job reported zero active processes. Its retained root is
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-dek93_se`.

Earlier runs did not complete acceptance. The first used Viewer `93fa26b3`;
the server script could not import Processing because it was invoked by path.
Its retained server container is
`62a96cd1820ab5b490b7e9292c8f6f9fe8ec39f76c186e2415ebea7146025db8`;
the planned sender was verified absent. Root:
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-s9sx_249`.
The second used `93fa26b3` and failed Viewer receipt staging; retained sender
and server are `1bb6f33afc63a3fcdb70d8bfa903165de1101567b9b2cb800583cfaeb420c3db`
and `8ba60497cbb8ebbc59849d460bf101c07e7466f56e99b0286fd428c4c94882f0`.
Root: `C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-mpevhme7`.
The third used Viewer `46f34717`; its 1 MiB fixture quota was below Viewer's
required base plus per-object reservations, so staging was held. Its retained
sender and server are `769b1758c33b37622b54d4b9904f241810de579bd1fcb7e78733625ccc1492cc`
and `7f251fc91e0360447edf478348dadbd8f4d9eb2228cbb46b0683b7ab7aa37121`.
Root: `C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-uqqpfpw3`.
All recorded containers exited, and each outer job reported zero active
processes. The fixture subsequently raised the synthetic quota to 10 MiB.
No retained test artifacts were removed.

The runner remains a synthetic, opt-in test harness. It may require a fresh
ownership review if changed; a Docker create timeout can still leave an
unverifiable name if the daemon becomes unreachable, and operating-system or
Docker refusal can prevent a hard total-runtime guarantee.
