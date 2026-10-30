#!/usr/bin/env bash
# End-to-end rehearsal of docs/d738-copy-cutover.md on synthetic state.
#
# Usage: ops/cutover_rehearsal.sh D738_IMAGE RELEASE_IMAGE D738_COMPOSE_FILE
#
# The old coordinator is created from d738's own compose.viewer.yaml, as on the
# live host. Every Docker call goes through ops/pinned_docker.py. Only resources
# named claude-reh-<run>* are created, touched or removed. It never restarts the
# Docker daemon: that case is reported as unverified.
set -euo pipefail
umask 077
export MSYS_NO_PATHCONV=1
OLD_IMAGE=$1
NEW_IMAGE=$2
COMPOSE_FILE=$3
HERE=$(cd "$(dirname "$0")" && pwd -W 2>/dev/null || pwd)
w() { cygpath -w "$1" 2>/dev/null || echo "$1"; }
RUN=claude-reh-$RANDOM
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOCKER_LOCAL=npipe:////./pipe/docker_engine;; *) DOCKER_LOCAL=unix:///var/run/docker.sock;; esac
DAEMON_ID=$(python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" -- info --format '{{.ID}}')
test -n "$DAEMON_ID"
docker() { python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" -- "$@"; }
ABSENT=(python "$HERE/container_absent.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID")
E=$(mktemp -d)
step() { echo; echo "== $*"; }
pass() { echo "PASS $*"; }
fail() { echo "FAIL $*"; exit 1; }
owned_cleanup() {
  for c in $(docker ps -aq --filter "name=^$RUN"); do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for v in $(docker volume ls -q --filter "name=^$RUN"); do docker volume rm -f "$v" >/dev/null 2>&1 || true; done
  for n in $(docker network ls -q --filter "name=^$RUN"); do docker network rm "$n" >/dev/null 2>&1 || true; done
  rm -rf "$E"
}
trap owned_cleanup EXIT
token() { python -c 'import secrets;print(secrets.token_hex(24))'; }
cat > "$E/runtime.env" <<EOF
PROCESSING_API_TOKEN=$(token)
PROCESSING_PROTOTYPE_OPERATOR_TOKEN=$(token)
PROCESSING_PROTOTYPE_OBSERVER_TOKEN=$(token)
PROCESSING_PROTOTYPE_STATE_DIR=/prototype-state
PROCESSING_VIEWER_ENABLED=1
PROCESSING_PROTOTYPE_NODE_IDS=cluster,main
PROCESSING_VIEWER_TOKEN=$(token)
PROCESSING_VIEWER_STAGING_TOKEN=$(token)
LOCAL_NODE_URL=http://127.0.0.1:9
MAIN_NODE_URL=http://127.0.0.1:9
EOF
PRODUCTION=$(sed -n 's/^PROCESSING_VIEWER_TOKEN=//p' "$E/runtime.env")
STAGING=$(sed -n 's/^PROCESSING_VIEWER_STAGING_TOKEN=//p' "$E/runtime.env")
VIEWER_PORT=$((20000 + RANDOM % 20000))
compose_env=(env PROCESSING_IMAGE="$OLD_IMAGE" VIEWER_BIND_ADDRESS=127.0.0.1 VIEWER_PORT="$VIEWER_PORT"
             PROCESSING_RUNTIME_ENV="$(w "$E/runtime.env")" PROCESSING_STATE_VOLUME="$RUN-state")
old_compose() { "${compose_env[@]}" python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" \
  -- compose -p "$RUN" -f "$(w "$COMPOSE_FILE")" "$@"; }
healthy() {
  for _ in $(seq 40); do
    docker exec "$1" python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=1)" \
      >/dev/null 2>&1 && return 0
    [ "$(docker inspect -f '{{.State.Running}}' "$1")" = true ] || return 1
    sleep 0.5
  done
  return 1
}
http() {  # method path token [body] -> prints status
  python - "$VIEWER_PORT" "$@" <<'PY'
import sys, urllib.error, urllib.request
port, method, path, token = sys.argv[1:5]
body = sys.argv[5].encode() if len(sys.argv) > 5 else None
request = urllib.request.Request('http://127.0.0.1:%s%s?token=%s' % (port, path, token), data=body, method=method,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status)
except urllib.error.HTTPError as error:
    print(error.code, error.read().decode(errors='replace')[:80])
PY
}
census() { docker run --rm -i --network none --read-only --user 10001:10001 --entrypoint python \
  --mount "type=volume,source=$1,destination=/src,readonly" \
  --mount "type=bind,source=$HERE/state_census.py,destination=/c.py,readonly" "$OLD_IMAGE" -B /c.py census /src < "$E/census.key"; }
same() { python "$HERE/state_census.py" compare "$(w "$1")" "$(w "$2")" "${@:3}" < "$E/census.key" > "$E/last-compare.json"; }
fence() {
  docker run -d --name "$RUN-fence-$1" --network none --init --read-only --user 10001:10001 --cap-drop ALL \
    --security-opt no-new-privileges --restart always \
    --mount "type=volume,source=$2,destination=/fence/state,readonly" \
    --mount "type=bind,source=$HERE/state_fence.py,destination=/state_fence.py,readonly" \
    --entrypoint python "$NEW_IMAGE" -B /state_fence.py /fence/state >/dev/null
}
verify_fence() {
  for _ in $(seq 20); do
    docker exec "$RUN-fence-$1" python -B /state_fence.py --verify /fence/state 2>/dev/null | grep -q '"verified": true' && return 0
    sleep 0.5
  done
  return 1
}
run_spec() {
  mapfile -d '' -t ARGV < <(python -c 'import json,sys;a=json.load(open(sys.argv[1]))["argv"];assert a[0]=="docker";sys.stdout.write("\0".join(a[1:]))' "$(w "$1")")
  [ "${#ARGV[@]}" -gt 1 ]
  docker "${ARGV[@]}" >/dev/null
}
spec() {  # mode name extra...
  python "$HERE/coordinator_container_spec.py" --container "$(w "$E/old-inspect.json")" \
    --old-image "$(w "$E/old-image.json")" --new-image "$(w "$E/new-image.json")" --mode "$1" \
    --volume "$NEW_VOL" --name "$2" --lease-volume "$RUN-lease" --accept-compose-source "${@:3}"
}

step "Setup: the old coordinator from d738's compose definition (synthetic state)"
docker volume create "$RUN-state" >/dev/null
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-state,destination=/s" --entrypoint chown "$OLD_IMAGE" 10001:10001 /s
old_compose up -d --quiet-pull >/dev/null 2>&1 || old_compose up -d
OLD=$(docker inspect -f '{{.Id}}' "$(old_compose ps -q coordinator)")
healthy "$OLD" || fail "old coordinator did not start"
pass "old d738 running from compose, $(docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' "$OLD") restart policy"
VERIFY_VOL=$RUN-verify
NEW_VOL=$RUN-release

step "Step 0: capture"
python "$HERE/state_census.py" keygen "$(w "$E/census.key")"
docker inspect "$OLD" > "$E/old-inspect.json"
docker image inspect "$OLD_IMAGE" > "$E/old-image.json"
docker image inspect "$NEW_IMAGE" > "$E/new-image.json"
VOL=$(docker inspect -f '{{range .Mounts}}{{.Name}}{{end}}' "$OLD")
[ "$VOL" = "$RUN-state" ] || fail "unexpected state volume"
spec isolated "$RUN-dry" --network none --env-out "$(w "$E/env-dry")" > /dev/null || fail "spec refused the compose container"
rm -f "$E/env-dry"
[ "$(docker ps -aq --filter "volume=$VOL" | wc -l)" = 1 ] || fail "another container uses the state volume"
pass "captured; spec accepts the compose-managed container; only the old coordinator uses the volume"

step "Step 1: R2 quiescence inside the old coordinator"
inbound=$(docker exec -i "$OLD" python - < "$HERE/netns_sockets.py" | python -c 'import json,sys;print(json.load(sys.stdin)["activeInbound"])')
[ "$inbound" = 0 ] && pass "no active inbound connection in d738's own namespace (TIME_WAIT excluded)" || fail "active inbound connections: $inbound"

step "Step 2: stop, fence, remove"
docker update --restart=no "$OLD" >/dev/null
docker stop -t 60 "$OLD" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}} {{.State.OOMKilled}}' "$OLD")" = "0 false" ] || fail "d738 did not drain cleanly"
fence original "$VOL"
verify_fence original || fail "original fence not verified"
docker rm "$OLD" >/dev/null
"${ABSENT[@]}" "$OLD" >/dev/null || fail "old container not confirmed absent"
pass "d738 drained (exit 0), original fenced and verified, old container confirmed absent on the recorded daemon"

step "Step 3: verified archive, verification volume and copy"
census "$VOL" > "$E/census-original.json"
docker run --rm --network none --read-only --user 10001:10001 --mount "type=volume,source=$VOL,destination=/src,readonly" \
  --entrypoint tar "$OLD_IMAGE" --numeric-owner -C /src -cpf - . > "$E/state-original.tar"
sha256sum "$E/state-original.tar" > "$E/state-original.tar.sha256"

for v in "$VERIFY_VOL" "$NEW_VOL"; do
  docker volume create --label "ltp.cutover=$RUN" "$v" >/dev/null
  [ "$(census "$v" | python -c 'import json,sys;print(json.load(sys.stdin)["tree"]["entries"])')" = 1 ] || fail "$v not empty"
  (cd "$E" && sha256sum -c state-original.tar.sha256 >/dev/null)
  docker run --rm -i --network none --user 0:0 --mount "type=volume,source=$v,destination=/dst" --entrypoint tar "$OLD_IMAGE" \
    --numeric-owner --same-owner -C /dst -xpf - < "$E/state-original.tar"
  census "$v" > "$E/census-$v.json"
done
fence verify "$VERIFY_VOL"
verify_fence verify && verify_fence original || fail "fences not both verified"
census "$VOL" > "$E/census-original-after.json"
for c in "census-$VERIFY_VOL" "census-$NEW_VOL" census-original-after; do
  same "$E/census-original.json" "$E/$c.json" || fail "$c differs from the original"
done
pass "archive restores identically into both volumes, original unchanged; verification volume fenced by its own holder"

step "Step 5: isolated candidate with the authority lease"
docker volume create "$RUN-lease" >/dev/null
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-lease,destination=/l" --entrypoint sh "$OLD_IMAGE" -c 'chown 10001:10001 /l && chmod 700 /l'
spec isolated "$RUN-candidate" --network none --env-out "$(w "$E/env-candidate")" > "$E/candidate-spec.json"
run_spec "$E/candidate-spec.json"; rm -f "$E/env-candidate"
docker start "$RUN-candidate" >/dev/null
healthy "$RUN-candidate" || fail "candidate did not start"
[ "$(docker exec "$RUN-candidate" sh -c "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' '")" = lo ] || fail "candidate has a network interface"
[ -z "$(docker port "$RUN-candidate")" ] || fail "candidate publishes ports"
pass "candidate healthy with only loopback, no published ports, lease held"

step "Step 6: promotion gate"
docker stop -t 60 "$RUN-candidate" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-candidate")" = 0 ] || fail "candidate did not drain"
census "$NEW_VOL" > "$E/census-candidate-isolated.json"
FIXED=(--allow-path .:mtime --allow-path jobs.sqlite3:bytes,content,mtime
       --allow-new-table viewer_reservations --allow-new-table viewer_option_evidence --allow-lease-marker)
same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" "${FIXED[@]}" || true
OBJECTS=$(python -c 'import json,sys
r=json.load(open(sys.argv[1]))
other=[d for d in r["database"] if "object" not in d]
assert not other and not r["files"], (other, r["files"])
print(" ".join(sorted(d["object"] for d in r["database"])))' "$(w "$E/last-compare.json")") || fail "startup changed more than schema"
echo "schema objects (U9 input for this image and configuration): $OBJECTS"
ARGS=(); for o in $OBJECTS; do ARGS+=(--object "$o"); done
python "$HERE/state_census.py" schema-allowlist "$(w "$E/census-candidate-isolated.json")" "${ARGS[@]}" \
  --output "$(w "$E/startup-schema-allowlist.json")"
same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" "${FIXED[@]}" \
  --schema-allowlist "$(w "$E/startup-schema-allowlist.json")" || fail "gate failed"
pass "gate passes with exact schema hashes and the exact lease marker"

step "Step 7: promote with production held"
"${ABSENT[@]}" "$OLD" >/dev/null || fail "old container reappeared"
verify_fence original && verify_fence verify || fail "fences lost before promotion"
census "$NEW_VOL" > "$E/census-before-promotion.json"
same "$E/census-candidate-isolated.json" "$E/census-before-promotion.json" || fail "copy changed since the gate"
docker rm "$RUN-candidate" >/dev/null
spec promoted "$RUN-promoted" --production-admission held --env-out "$(w "$E/env-promoted")" > "$E/promoted-spec.json"
run_spec "$E/promoted-spec.json"; rm -f "$E/env-promoted"
docker start "$RUN-promoted" >/dev/null
healthy "$RUN-promoted" || fail "promoted release did not start"
held=$(http POST /task/new/init "$PRODUCTION" "")
[[ "$held" == 503*production_admission_held* ]] && pass "production mutation over the published port: $held" || fail "production not held: $held"
staged=$(http POST /task/cancel "$STAGING" "uuid=11111111-2222-4333-8444-555555555555")
[[ "$staged" != 503* ]] && pass "staging admitted over the published port (answer: ${staged%% *})" || fail "staging held: $staged"

step "Accidental restart of the old definition while the release runs"
set +e; out=$(old_compose up -d 2>&1); code=$?; set -e
again=$(old_compose ps -aq coordinator 2>/dev/null || true)
if [ -n "$again" ]; then
  sleep 3
  state=$(docker inspect -f '{{.State.Running}} {{.State.ExitCode}}' "$again")
  logs=$(docker logs "$again" 2>&1 | tail -3)
  [[ "$state" == false* ]] || fail "old definition is running: $state"
  docker rm -f "$again" >/dev/null
  if [[ "$logs" == *"prototype state already owned"* ]]; then pass "compose up of d738 refused by the state fence"
  elif [[ "$out" == *"port is already allocated"* || "$out" == *"address already in use"* ]]; then pass "compose up of d738 refused: release holds its host ports"
  else pass "compose up of d738 did not run (exit $code)"; fi
else
  [ "$code" != 0 ] && pass "compose up of d738 failed to create a running container (exit $code)" || fail "unexpected compose result"
fi
healthy "$RUN-promoted" || fail "promoted release affected by the old definition"
verify_fence original || fail "original fence lost"
pass "release unaffected; original fence still verified"

step "Open production"
docker stop -t 60 "$RUN-promoted" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-promoted")" = 0 ] || fail "held release did not drain"
census "$NEW_VOL" > "$E/census-after-held.json"
same "$E/census-candidate-isolated.json" "$E/census-after-held.json" || fail "held phase wrote state (staging request or other)"
pass "nothing was written while production was held"
docker rm "$RUN-promoted" >/dev/null
spec promoted "$RUN-open" --production-admission open --env-out "$(w "$E/env-open")" > "$E/open-spec.json"
run_spec "$E/open-spec.json"; rm -f "$E/env-open"
docker start "$RUN-open" >/dev/null
healthy "$RUN-open" || fail "opened release did not start"
opened=$(http POST /task/cancel "$PRODUCTION" "uuid=11111111-2222-4333-8444-555555555555")
[[ "$opened" != 503* ]] && pass "production admitted after reopening (answer: ${opened%% *})" || fail "production still held: $opened"

step "Rollback rehearsal (nothing admitted)"
docker stop -t 60 "$RUN-open" >/dev/null
census "$NEW_VOL" > "$E/census-before-rollback.json"
same "$E/census-candidate-isolated.json" "$E/census-before-rollback.json" \
  && pass "copy unchanged since the gate: rollback loses nothing" || fail "copy changed: this would be a hold, not a rollback"
docker rm "$RUN-open" >/dev/null
docker rm -f "$RUN-fence-original" >/dev/null
old_compose up -d >/dev/null 2>&1
BACK=$(docker inspect -f '{{.Id}}' "$(old_compose ps -q coordinator)")
healthy "$BACK" || fail "d738 did not come back"
docker stop -t 60 "$BACK" >/dev/null
census "$VOL" > "$E/census-rolled-back.json"
python - "$(w "$E/census-original.json")" "$(w "$E/census-rolled-back.json")" <<'PY' || fail "rolled-back state lost its authority or history"
import json, sys
a, b = (json.load(open(p)) for p in sys.argv[1:])
assert a['database']['authorityId'] == b['database']['authorityId']
assert a['database']['objects'] == b['database']['objects'], 'schema changed under d738'
assert 'table:coordinator_authority_lease' not in b['database']['objects']
for table, count in a['database']['rowCounts'].items():
    assert b['database']['rowCounts'].get(table, 0) >= count, table
PY
pass "d738 restored from its original volume through compose: same authority, d738 schema, no lease marker, history kept"

step "Not verified here"
echo "UNVERIFIED Docker daemon restart / host reboot (shared daemon left untouched; no isolated daemon available)"
echo "UNVERIFIED exact pinned images (rehearsed with $OLD_IMAGE and $NEW_IMAGE)"
echo "REHEARSAL PASSED"
