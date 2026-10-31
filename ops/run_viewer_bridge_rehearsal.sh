#!/bin/sh
# Isolated synthetic test only. Retains its temporary export and evidence.
set -eu
: "${VIEWER_REPO:?set VIEWER_REPO to the source repository}"
: "${PROCESSING:?set PROCESSING to the isolated Processing checkout}"
case "$PROCESSING" in /*) ;; *) printf 'PROCESSING must be an absolute path\n' >&2; exit 1;; esac
VIEWER_TREE=baaaf2439d5a641ae59c3f550f77dc7512e3b84d
PROCESSING_BASE=60ae84f7572f611b03a340d17ceb17d82ff84721
HARNESS_BASE=d9f44b555f7851ea87f5391afc5f75af1cdf5dac
VIEWER_IMAGE=viewer-gcp-validation-faa3fd8bca374cb9ba721eea97a4674a:latest
PYTHON_IMAGE=python:3.12-slim
RUN_ID=$(python3 -c 'import secrets; print(secrets.token_hex(12))')
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/codex-viewer-bridge-${RUN_ID}-XXXXXX")
SERVER_ID= CLIENT_ID=
owned() {
    [ -n "$1" ] || return 1
    [ "$(docker inspect --format '{{.Id}}' "$1" 2>/dev/null || :)" = "$1" ] || return 1
    [ "$(docker inspect --format '{{index .Config.Labels "org.openai.codex.bridge"}}' "$1" 2>/dev/null || :)" = "$RUN_ID" ]
}
cleanup_one() {
    if owned "$1"; then
        docker stop --time 5 "$1" >/dev/null 2>&1 || :
        if owned "$1" && [ "$(docker inspect --format '{{.State.Running}}' "$1" 2>/dev/null || :)" = false ]; then
            docker rm "$1" >/dev/null || :
        fi
    fi
}
cleanup() { cleanup_one "$CLIENT_ID"; cleanup_one "$SERVER_ID"; }
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[ "$(git -C "$VIEWER_REPO" rev-parse "${VIEWER_TREE}^{tree}")" = "$VIEWER_TREE" ]
[ -z "$(git -C "$PROCESSING" status --porcelain --untracked-files=normal)" ]
git -C "$PROCESSING" merge-base --is-ancestor "$PROCESSING_BASE" HEAD
git -C "$PROCESSING" merge-base --is-ancestor "$HARNESS_BASE" HEAD
git -C "$PROCESSING" diff --quiet "$HARNESS_BASE" HEAD -- tests/viewer_bridge_client.cjs tests/viewer_bridge_server.py
git -C "$PROCESSING" diff --quiet "$PROCESSING_BASE" HEAD -- processing ops \
    tests/test_viewer_completion_projection.py tests/test_viewer_spool_provider.py \
    tests/test_viewer_archive_spool.py tests/test_viewer_result_metadata.py \
    tests/test_viewer_synthetic_submission.py tests/test_viewer_node_inputs.py \
    tests/test_viewer_node_enrollment.py tests/test_viewer_worker_claim.py \
    ':(exclude)ops/run_viewer_bridge_rehearsal.sh'
printf 'Viewer tree: %s\nProcessing base: %s\nProcessing checkout: %s\n' "$VIEWER_TREE" "$PROCESSING_BASE" "$(git -C "$PROCESSING" rev-parse HEAD)"
mkdir "$RUN_DIR/export" "$RUN_DIR/exchange"
chmod 0777 "$RUN_DIR/exchange" # synthetic dummy files only
git -C "$VIEWER_REPO" archive "$VIEWER_TREE" apps/viewer packages/shared > "$RUN_DIR/viewer.tar"
sha256sum "$RUN_DIR/viewer.tar"
tar -xf "$RUN_DIR/viewer.tar" -C "$RUN_DIR/export"
tar -df "$RUN_DIR/viewer.tar" -C "$RUN_DIR/export"
docker image inspect "$VIEWER_IMAGE" "$PYTHON_IMAGE" --format '{{.Id}}'

SERVER_ID=$(docker run -d --name "viewer-bridge-server-$RUN_ID" --label "org.openai.codex.bridge=$RUN_ID" \
    --network none --cpus 0.5 --memory 384m --pids-limit 64 --user 10001:10001 --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=128m --mount "type=bind,src=$PROCESSING,dst=/src,readonly" \
    --mount "type=bind,src=$RUN_DIR/exchange,dst=/exchange" -w /src -e PYTHONPATH=/src -e PYTHONDONTWRITEBYTECODE=1 \
    -e BRIDGE_READY=/exchange/ready.json -e BRIDGE_STOP=/exchange/stop "$PYTHON_IMAGE" python tests/viewer_bridge_server.py)
owned "$SERVER_ID"
i=0
while [ ! -f "$RUN_DIR/exchange/ready.json" ]; do
    [ "$i" -lt 60 ] && [ "$(docker inspect --format '{{.State.Running}}' "$SERVER_ID")" = true ] || {
        docker logs "$SERVER_ID"; exit 1;
    }
    i=$((i+1));sleep 1
done
tar -df "$RUN_DIR/viewer.tar" -C "$RUN_DIR/export"
CLIENT_ID=$(docker run -d --name "viewer-bridge-client-$RUN_ID" --label "org.openai.codex.bridge=$RUN_ID" \
    --network "container:$SERVER_ID" --cpus 0.5 --memory 512m --pids-limit 64 --user 10001:10001 --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=2g --mount "type=bind,src=$RUN_DIR/export,dst=/viewer,readonly" \
    --mount "type=bind,src=$PROCESSING,dst=/processing,readonly" --mount "type=bind,src=$RUN_DIR/exchange,dst=/exchange,readonly" \
    -e NODE_PATH=/src/apps/viewer/node_modules -e VIEWER_SOURCE_ROOT=/tmp/viewer -e BRIDGE_READY=/exchange/ready.json \
    --entrypoint sh "$VIEWER_IMAGE" -c 'mkdir -p /tmp/viewer/apps /tmp/viewer/packages && cp -a /viewer/apps/viewer /tmp/viewer/apps/ && cp -a /viewer/packages/shared /tmp/viewer/packages/ && ln -s /src/apps/viewer/node_modules /tmp/viewer/apps/viewer/node_modules && exec node /processing/tests/viewer_bridge_client.cjs')
owned "$CLIENT_ID"
code=$(timeout 45s docker wait "$CLIENT_ID")
docker logs "$CLIENT_ID"
[ "$code" = 0 ]
touch "$RUN_DIR/exchange/stop"
[ "$(timeout 15s docker wait "$SERVER_ID")" = 0 ]
cat "$RUN_DIR/exchange/stop.result"
printf 'Synthetic export and evidence retained at %s\n' "$RUN_DIR"
