# Synthetic Viewer-to-Processing bridge rehearsal

This is an isolated integration rehearsal, not a client-data readiness test. It uses the exact Viewer staged Git tree `baaaf2439d5a641ae59c3f550f77dc7512e3b84d` and Processing commit `60ae84f7572f611b03a340d17ceb17d82ff84721`, exported read-only without changing the Viewer worktree. The fixture uses real Viewer `processReconcile` and `processIngest`, the Processing Viewer-compatible HTTP endpoint, worker/coordinator TLS, a synthetic NodeODM endpoint and synthetic local files. The Viewer-facing connection is loopback HTTP, not TLS.

The archive contains intentionally invalid placeholder geometry, textures and EPT metadata. A successful ingest establishes only that the Viewer worker observed the projected completion, downloaded and hashed the exact spool archive, registered its synthetic inputs and queued derivatives. It does not establish a renderable model, a successful converter, real NodeODM output, 10,000-photo performance, production TLS/custody, Viewer acceptance, cleanup or power-loss recovery. No acceptance acknowledgement is sent. Before fixture teardown, the script verifies that the node input, node result and coordinator spool allocations all remain and the coordinator job is still running. The Viewer test database and synthetic files are temporary.

The original bounded run used locally available images `python:3.12-slim` (image ID `sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`) and `viewer-gcp-validation-faa3fd8bca374cb9ba721eea97a4674a:latest` (image ID `sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`). The second image supplies Node 24 and the Viewer dependencies; it is a local fixture image, not a published release. Other images must independently provide compatible Viewer dependencies and Python/openssl.

To repeat on a Docker host, export the staged Viewer tree to a private temporary directory as `VIEWER_EXPORT`, create an empty writable `EXCHANGE` directory, and set `PROCESSING` to an isolated checkout at the Processing commit above. The export must include `apps/viewer` and `packages/shared`. Bind those directories into the containers as shown; do not bind a live Viewer or NAS storage directory. Docker's `container:` network mode shares the isolated loopback namespace and gives the Viewer container no external network.

```sh
git -C "$VIEWER_REPO" archive baaaf2439d5a641ae59c3f550f77dc7512e3b84d apps/viewer packages/shared | tar -x -C "$VIEWER_EXPORT"
docker run -d --name viewer-bridge-processing --network none --cpus 0.5 --memory 384m --pids-limit 64 --user 10001:10001 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=128m \
  --mount "type=bind,src=$PROCESSING,dst=/src,readonly" --mount "type=bind,src=$EXCHANGE,dst=/exchange" \
  -w /src -e PYTHONPATH=/src -e PYTHONDONTWRITEBYTECODE=1 -e BRIDGE_READY=/exchange/ready.json -e BRIDGE_STOP=/exchange/stop \
  python:3.12-slim python tests/viewer_bridge_server.py
# Wait for $EXCHANGE/ready.json and confirm the container is running.
docker run --rm --network container:viewer-bridge-processing --cpus 0.5 --memory 512m --pids-limit 64 --user 10001:10001 --read-only --tmpfs /tmp:rw,nosuid,nodev,size=2g \
  --mount "type=bind,src=$VIEWER_EXPORT,dst=/viewer,readonly" --mount "type=bind,src=$PROCESSING,dst=/processing,readonly" \
  --mount "type=bind,src=$EXCHANGE,dst=/exchange,readonly" -e NODE_PATH=/src/apps/viewer/node_modules \
  -e VIEWER_SOURCE_ROOT=/tmp/viewer -e BRIDGE_READY=/exchange/ready.json --entrypoint sh \
  viewer-gcp-validation-faa3fd8bca374cb9ba721eea97a4674a:latest -c 'mkdir -p /tmp/viewer/apps /tmp/viewer/packages && cp -a /viewer/apps/viewer /tmp/viewer/apps/ && cp -a /viewer/packages/shared /tmp/viewer/packages/ && ln -s /src/apps/viewer/node_modules /tmp/viewer/apps/viewer/node_modules && exec node /processing/tests/viewer_bridge_client.cjs'
touch "$EXCHANGE/stop"
docker wait viewer-bridge-processing
cat "$EXCHANGE/stop.result"
docker rm viewer-bridge-processing
```

The client prints the synthetic archive digest, Viewer attempt phase and pending derivative count. The Processing fixture writes the retention check to `stop.result` before teardown. Its lifetime is capped at 120 seconds; inspect and remove only the containers and temporary directories created for the rehearsal if any command fails. The fixture tokens are local dummy values.
