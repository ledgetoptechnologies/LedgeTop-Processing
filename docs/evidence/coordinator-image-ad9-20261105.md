# Local coordinator image qualification

Source: `ad9e9d35e59471e5e080ba82125cb1b4b1f27c18`, the source revision that
passed the combined 682-test qualification. Build context was a `git archive`
of only Dockerfile, requirements, ignore rules and the Processing package.
Archive SHA256:
`dedbf55833bf27babc332e3f477d7a60d531f9d6d5e79f3572d204e0c33b1252`.
The Docker ignore allowlist excluded source archives, worker/operator scripts,
datasets, tests, environments and credentials.

Local tag: `ledgetop-processing:qualification-ad9e9d3`.
Verified runnable image/OCI manifest identity:
`sha256:b2d972605b1e2a86851c434b3d4735a3d71dce56a72fd88eb466756a37147b87`.
Config digest reported by BuildKit:
`sha256:47ff80197085593a3ff700b2ef13535e0c84dcc774768f8d01ba52adf49f2980`.
The revision and repository labels match the exported source. This Docker
installation addresses the runnable image by the manifest identity above,
not by the separately reported config digest.

The build reused the pinned Python base and pinned dependency cache in the
existing task-owned builder, limited to one CPU and 2 GiB. An initial attempt
with build networking disabled missed the dependency-layer cache and failed;
the standard-network build reused that layer successfully. The builder was
stopped after export and its stopped state/restart policy were verified. Cache
and qualified image are retained for subsequent testing, not published.

The final startup smoke used the actual image with half a CPU, 256 MiB, 64 PIDs,
read-only root, no external network, and a private 64 MiB temporary filesystem.
A generated in-memory synthetic API credential satisfied normal startup
validation; no real token or client configuration was provided.

Verified:

- UID/GID 10001; a source-write probe failed with EROFS.
- Actual application `/healthz` returned 200/status ok in routing-probe mode;
  prototype, photo-smoke and Viewer processing remained disabled.
- Unconfigured `/ltds/v2/capabilities` returned 404.
- Explicit per-node numeric/null photo-cap behavior and the release-policy
  module loaded from the packaged source.
- The application child exited cleanly with code zero after SIGTERM.

The first application check omitted the required synthetic API credential and
failed normal configuration validation; the final configured run above passed.
The smoke containers and application child exited and were removed. No package,
service, host, NAS configuration or client data was deployed or modified.

This is the coordinator image. The host worker/operator source bundle and its
read-only installation/readiness requirements are separately qualified. This
smoke does not establish configured TLS, NAS source custody, worker services,
image-update execution, licensed Viewer conversion or real dataset acceptance.
