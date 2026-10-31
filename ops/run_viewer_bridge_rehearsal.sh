#!/bin/sh
# Isolated synthetic test only. Retains its temporary export and evidence.
set -eu
: "${VIEWER_REPO:?set VIEWER_REPO to the source repository}"
: "${PROCESSING:?set PROCESSING to the isolated Processing checkout}"
case "$PROCESSING" in /*) ;; *) printf 'PROCESSING must be an absolute path\n' >&2; exit 1;; esac
VIEWER_TREE=baaaf2439d5a641ae59c3f550f77dc7512e3b84d
PROCESSING_BASE=dfee21c71b1baad1b4536bf0d2c590df142fc34b
HARNESS_BASE=d9f44b555f7851ea87f5391afc5f75af1cdf5dac
FULL_HARNESS_BASE=044de218b05424776037d355acb963e304c29390
FULL_SERVER_SHA256=2be379b690fc391b5559a7028d31a62f3b7a8b3bf67a5a256092a8fdfb620d98
FULL_CLIENT_SHA256=9cc627374bdfa2eb198232edaa53335e82681ac20e1228e2861d454f3556029a
VIEWER_IMAGE=sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668
PYTHON_IMAGE=sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159
BRIDGE_MODE=${BRIDGE_MODE:-full}
case "$BRIDGE_MODE" in
    reconcile) printf 'reconcile mode is not qualified for the per-object input layout\n' >&2; exit 1;;
    full) SERVER_SCRIPT=viewer_full_bridge_server.py; CLIENT_SCRIPT=viewer_full_bridge_client.cjs; CLIENT_WAIT=75s;;
    *) printf 'BRIDGE_MODE must be reconcile or full\n' >&2; exit 1;;
esac
RUN_ID=$(python3 -c 'import secrets; print(secrets.token_hex(12))')
RUN_DIR=$(mktemp -d "${TMPDIR:-/tmp}/codex-viewer-bridge-${RUN_ID}-XXXXXX")
SERVER_NAME="viewer-bridge-server-$RUN_ID"
CLIENT_NAME="viewer-bridge-client-$RUN_ID"
SERVER_ID= CLIENT_ID=
SERVER_ATTEMPTED=0 CLIENT_ATTEMPTED=0
printf 'Rehearsal run ID: %s\nEvidence directory: %s\nServer name: %s\nClient name: %s\n' "$RUN_ID" "$RUN_DIR" "$SERVER_NAME" "$CLIENT_NAME"
python3 - "$RUN_DIR" "$RUN_ID" "$SERVER_NAME" "$CLIENT_NAME" "$VIEWER_TREE" "$PROCESSING_BASE" "$HARNESS_BASE" "$FULL_HARNESS_BASE" "$PYTHON_IMAGE" "$VIEWER_IMAGE" "$BRIDGE_MODE" <<'PY'
import json,os,sys
root,run,server,client,viewer_tree,processing_base,harness_base,full_harness_base,python_image,viewer_image,bridge_mode=sys.argv[1:]
data=json.dumps(dict(runId=run,label='org.openai.codex.bridge',serverName=server,clientName=client,
                     viewerTree=viewer_tree,processingBase=processing_base,harnessBase=harness_base,
                     fullHarnessBase=full_harness_base,
                     pythonImage=python_image,viewerImage=viewer_image,bridgeMode=bridge_mode),sort_keys=True).encode()
fd=os.open(os.path.join(root,'run-identity.json'),os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
try:
    if os.write(fd,data)!=len(data):raise OSError('short evidence write')
    os.fsync(fd)
finally:os.close(fd)
fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
try:os.fsync(fd)
finally:os.close(fd)
PY
docker_short() { timeout 15s docker "$@"; }
owned() {
    [ -n "$1" ] || return 1
    [ "$(docker_short inspect --format '{{.Id}}' "$1" 2>/dev/null || :)" = "$1" ] || return 1
    [ "$(docker_short inspect --format '{{index .Config.Labels "org.openai.codex.bridge"}}' "$1" 2>/dev/null || :)" = "$RUN_ID" ]
}
cleanup_one() {
    if owned "$1"; then
        docker_short stop --time 5 "$1" >/dev/null 2>&1 || :
        if owned "$1" && [ "$(docker_short inspect --format '{{.State.Running}}' "$1" 2>/dev/null || :)" = false ]; then
            docker_short rm "$1" >/dev/null || printf 'Owned container removal uncertain: %s\n' "$1" >&2
        else
            printf 'Owned container state uncertain; left untouched: %s\n' "$1" >&2
        fi
    elif [ -n "$1" ]; then
        printf 'Container identity unavailable or changed; left untouched: %s\n' "$1" >&2
    fi
}
cleanup() {
    cleanup_one "$CLIENT_ID"; cleanup_one "$SERVER_ID"
    if [ "$CLIENT_ATTEMPTED" = 1 ] && [ -z "$CLIENT_ID" ]; then printf 'Client creation outcome uncertain; inspect recorded name/label in %s\n' "$RUN_DIR" >&2; fi
    if [ "$SERVER_ATTEMPTED" = 1 ] && [ -z "$SERVER_ID" ]; then printf 'Server creation outcome uncertain; inspect recorded name/label in %s\n' "$RUN_DIR" >&2; fi
    printf 'Synthetic export and evidence retained at %s\n' "$RUN_DIR"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[ "$(git -C "$VIEWER_REPO" rev-parse "${VIEWER_TREE}^{tree}")" = "$VIEWER_TREE" ]
[ -z "$(git -C "$PROCESSING" status --porcelain --untracked-files=normal)" ]
git -C "$PROCESSING" merge-base --is-ancestor "$PROCESSING_BASE" HEAD
git -C "$PROCESSING" merge-base --is-ancestor "$HARNESS_BASE" HEAD
git -C "$PROCESSING" diff --quiet "$HARNESS_BASE" HEAD -- tests/viewer_bridge_client.cjs tests/viewer_bridge_server.py
if [ "$BRIDGE_MODE" = full ]; then
    git -C "$PROCESSING" ls-files --error-unmatch tests/viewer_full_bridge_client.cjs tests/viewer_full_bridge_server.py >/dev/null
    git -C "$PROCESSING" merge-base --is-ancestor "$FULL_HARNESS_BASE" HEAD
    server_sum=$(sha256sum "$PROCESSING/tests/$SERVER_SCRIPT")
    client_sum=$(sha256sum "$PROCESSING/tests/$CLIENT_SCRIPT")
    printf '%s\n%s\n' "$server_sum" "$client_sum" | tee "$RUN_DIR/fixture-scripts.sha256"
    [ "${server_sum%% *}" = "$FULL_SERVER_SHA256" ] || { printf 'Full server fixture hash mismatch\n' >&2; exit 1; }
    [ "${client_sum%% *}" = "$FULL_CLIENT_SHA256" ] || { printf 'Full client fixture hash mismatch\n' >&2; exit 1; }
fi
git -C "$PROCESSING" diff --quiet "$PROCESSING_BASE" HEAD -- processing ops \
    tests/test_viewer_completion_projection.py tests/test_viewer_spool_provider.py \
    tests/test_viewer_archive_spool.py tests/test_viewer_result_metadata.py \
    tests/test_viewer_synthetic_submission.py tests/test_viewer_node_inputs.py \
    tests/test_viewer_node_enrollment.py tests/test_viewer_worker_claim.py \
    ':(exclude)ops/run_viewer_bridge_rehearsal.sh' ':(exclude)ops/verify_viewer_export.py'
verifier_sum=$(sha256sum "$PROCESSING/ops/verify_viewer_export.py")
[ "${verifier_sum%% *}" = b075ad603632339d38b31fbdc1de614889546a1014aa814c8c6381e5999b34ef ] || { printf 'Viewer export verifier hash mismatch\n' >&2; exit 1; }
printf 'Viewer tree: %s\nProcessing base: %s\nProcessing checkout: %s\n' "$VIEWER_TREE" "$PROCESSING_BASE" "$(git -C "$PROCESSING" rev-parse HEAD)"
git -C "$PROCESSING" rev-parse HEAD > "$RUN_DIR/processing.commit"
if [ "$BRIDGE_MODE" != full ]; then
    sha256sum "$PROCESSING/tests/$SERVER_SCRIPT" "$PROCESSING/tests/$CLIENT_SCRIPT" | tee "$RUN_DIR/fixture-scripts.sha256"
fi
mkdir -m 700 "$RUN_DIR/export"
mkdir "$RUN_DIR/exchange"
chmod 0777 "$RUN_DIR/exchange" # synthetic dummy files only
git -C "$VIEWER_REPO" -c tar.umask=0022 archive --format=tar --prefix=apps/viewer/ "$VIEWER_TREE:apps/viewer" > "$RUN_DIR/viewer-app.tar"
git -C "$VIEWER_REPO" -c tar.umask=0022 archive --format=tar --prefix=packages/shared/ "$VIEWER_TREE:packages/shared" > "$RUN_DIR/viewer-shared.tar"
sha256sum "$RUN_DIR/viewer-app.tar" "$RUN_DIR/viewer-shared.tar" | tee "$RUN_DIR/viewer-archive.sha256"
python3 "$PROCESSING/ops/verify_viewer_export.py" extract "$RUN_DIR/export" "$RUN_DIR/viewer-app.tar" "$RUN_DIR/viewer-shared.tar"
VIEWER_IMAGE_ID=$(docker_short image inspect "$VIEWER_IMAGE" --format '{{.Id}}')
PYTHON_IMAGE_ID=$(docker_short image inspect "$PYTHON_IMAGE" --format '{{.Id}}')
[ "$VIEWER_IMAGE_ID" = 'sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668' ]
[ "$PYTHON_IMAGE_ID" = 'sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159' ]
printf 'Viewer image: %s\nPython image: %s\n' "$VIEWER_IMAGE_ID" "$PYTHON_IMAGE_ID" | tee "$RUN_DIR/images.txt"

SERVER_ATTEMPTED=1
SERVER_ID=$(timeout 30s docker run -d --pull never --name "$SERVER_NAME" --label "org.openai.codex.bridge=$RUN_ID" \
    --network none --cpus 0.5 --memory 384m --pids-limit 64 --user 10001:10001 --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=128m --mount "type=bind,src=$PROCESSING,dst=/src,readonly" \
    --mount "type=bind,src=$RUN_DIR/exchange,dst=/exchange" -w /src -e PYTHONPATH=/src -e PYTHONDONTWRITEBYTECODE=1 \
    -e BRIDGE_READY=/exchange/ready.json -e BRIDGE_STOP=/exchange/stop "$PYTHON_IMAGE_ID" python "tests/$SERVER_SCRIPT")
printf '%s\n' "$SERVER_ID" > "$RUN_DIR/server.id"
owned "$SERVER_ID"
i=0
while [ ! -f "$RUN_DIR/exchange/ready.json" ]; do
    [ "$i" -lt 60 ] || {
        docker_short logs "$SERVER_ID" || :; exit 1;
    }
    i=$((i+1));sleep 1
done
python3 "$PROCESSING/ops/verify_viewer_export.py" verify "$RUN_DIR/export" "$RUN_DIR/viewer-app.tar" "$RUN_DIR/viewer-shared.tar"
CLIENT_ATTEMPTED=1
CLIENT_ID=$(timeout 30s docker run -d --pull never --name "$CLIENT_NAME" --label "org.openai.codex.bridge=$RUN_ID" \
    --network "container:$SERVER_ID" --cpus 0.5 --memory 512m --pids-limit 64 --user 10001:10001 --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,size=2g --mount "type=bind,src=$RUN_DIR/export,dst=/viewer,readonly" \
    --mount "type=bind,src=$PROCESSING,dst=/processing,readonly" --mount "type=bind,src=$RUN_DIR/exchange,dst=/exchange,readonly" \
    -e NODE_PATH=/src/apps/viewer/node_modules -e VIEWER_SOURCE_ROOT=/tmp/viewer -e BRIDGE_READY=/exchange/ready.json \
    --entrypoint sh "$VIEWER_IMAGE_ID" -c 'mkdir -p /tmp/viewer/apps /tmp/viewer/packages && cp -a /viewer/apps/viewer /tmp/viewer/apps/ && cp -a /viewer/packages/shared /tmp/viewer/packages/ && ln -s /src/apps/viewer/node_modules /tmp/viewer/apps/viewer/node_modules && exec node "/processing/tests/$1"' sh "$CLIENT_SCRIPT")
printf '%s\n' "$CLIENT_ID" > "$RUN_DIR/client.id"
owned "$CLIENT_ID"
code=$(timeout "$CLIENT_WAIT" docker wait "$CLIENT_ID")
docker_short logs "$CLIENT_ID"
[ "$code" = 0 ]
touch "$RUN_DIR/exchange/stop"
[ "$(timeout 15s docker wait "$SERVER_ID")" = 0 ]
cat "$RUN_DIR/exchange/stop.result"
