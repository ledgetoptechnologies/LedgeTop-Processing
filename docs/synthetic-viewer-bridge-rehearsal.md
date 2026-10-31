# Synthetic Viewer-to-Processing bridge rehearsal

This is an isolated integration rehearsal, not a client-data readiness test. It uses the exact Viewer staged Git tree `baaaf2439d5a641ae59c3f550f77dc7512e3b84d` and Processing base commit `dfee21c71b1baad1b4536bf0d2c590df142fc34b`, exported read-only without changing the Viewer worktree. The fixture uses real Viewer `processReconcile` and `processIngest`, the Processing Viewer-compatible HTTP endpoint, worker/coordinator TLS, a synthetic NodeODM endpoint and synthetic local files. The Viewer-facing connection is loopback HTTP, not TLS.

The archive contains intentionally invalid placeholder geometry, textures and EPT metadata. The `reconcile` mode starts after a synthetic task has reached Processing; the `full` mode begins with an empty coordinator and uses Viewer's real `processSubmit` to initialize, upload one synthetic photo, and commit. The synthetic worker checks the exact received photo and `dsm=true` plan both in its local allocation and in the fake NodeODM request, then the Viewer uses its real reconcile and ingest entry points. A successful run establishes only that these synthetic paths completed and queued derivatives. It does not establish a renderable model, a successful converter, real NodeODM output, 10,000-photo performance, production TLS/custody, Viewer acceptance, cleanup or power-loss recovery. No acceptance acknowledgement is sent. Before fixture teardown, both modes verify that the node input, node result and coordinator spool allocations all remain and the coordinator job is still running. The Viewer test database and synthetic files are temporary.

The original bounded run used locally available images `python:3.12-slim` (image ID `sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`) and `viewer-gcp-validation-faa3fd8bca374cb9ba721eea97a4674a:latest` (image ID `sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`). The second image supplies Node 24 and the Viewer dependencies; it is a local fixture image, not a published release. The runner verifies and uses these exact image IDs and never pulls them. Substituting images requires a separately reviewed harness change and dependency validation.

To repeat on a Linux Docker host with the two local images above, use an isolated, clean Processing checkout containing this harness and point `VIEWER_REPO` at a repository containing the exact staged tree. The script verifies the Viewer tree ID, exports it to a new private temporary directory, prints the archive SHA-256, compares the extracted files to that archive, and verifies the Processing base and fixture files before starting containers. It does not read or change live Viewer or NAS storage. Docker `container:` network mode shares the isolated loopback namespace and gives the Viewer container no external network. The script requires `python3`, GNU `tar`, `sha256sum`, and `timeout` on the host.

```sh
VIEWER_REPO=/path/to/viewer/repository PROCESSING=/path/to/isolated/processing \
  sh /path/to/isolated/processing/ops/run_viewer_bridge_rehearsal.sh
```

Set `BRIDGE_MODE=full` in that command to exercise actual Viewer submission before reconciliation and ingest. The default is `reconcile`. The full fixture uses the same fixed synthetic task UUID as the original fixture but creates it through the Viewer provider API rather than inserting the Processing task beforehand. Its Viewer producer-provenance record is synthetic and is not a custody acknowledgement.

Every run prints and durably records a random run ID, label and intended container names before creating containers. Docker create calls are bounded to 30 seconds and ordinary inspect, stop, remove, image inspection and log calls to 15 seconds each. Cleanup verifies each immutable container ID and its run label before stopping or removing it; an identity mismatch or uncertain Docker response is left for manual inspection. If a create times out before returning an ID, the recorded name and label identify what to investigate, but the script never deletes by name alone.

After container creation, readiness polling is limited to 60 seconds, the client wait to 45 seconds in `reconcile` mode or 75 seconds in `full` mode, and the final server wait to 15 seconds; these are separate limits, not an end-to-end deadline. Git export, archive comparison and host filesystem operations have no overall deadline. The Python fixture's own 120-second deadline starts after setup. The script retains its new synthetic export and evidence directory and prints its path even on failure; inspect that directory separately if cleanup is desired. It never reuses or removes another run's export.

The client prints the synthetic archive digest, Viewer attempt phase and pending derivative count. The Processing fixture writes the retention check to `stop.result` before teardown. The fixture tokens are local dummy values. The locally supplied dependency image is not proven to match the exported tree's dependency lockfile; this is a compatibility limitation of the rehearsal.

Full-mode fixture scripts are checked against fixed SHA-256 values, and their actual
hashes are printed and retained before Docker is invoked. The owned fixture worker
receives cancellation and is joined for up to 10 seconds during shutdown. If it
remains active, the isolated fixture process exits rather than tearing down state
under that thread. Container cleanup still depends on verified ownership. This is
not a guarantee about shutdown of production workers.

The full-mode retention check follows the `per-object-v1` input layout. The script pins the updated full fixture hash and requires the reviewed Processing base; the separate default reconcile fixture still targets the previous layout and has not been requalified for this base. No offline/restart behavior is proven by the full-mode run.
