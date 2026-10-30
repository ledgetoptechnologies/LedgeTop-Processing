#!/usr/bin/env bash
# End-to-end rehearsal of docs/d738-copy-cutover.md on synthetic state.
#
# Usage:
#   ops/cutover_rehearsal.sh D738_IMAGE_ID RELEASE_IMAGE_ID D738_COMPOSE_FILE COMPOSE_SHA256 [REVIEWED_SCHEMA_ALLOWLIST]
#   ops/cutover_rehearsal.sh --cleanup MANIFEST       # remove a held run's recorded resources
#
# Safety properties:
# - Image arguments must be full local image IDs (sha256:...); nothing is pulled.
# - The d738 compose file must match COMPOSE_SHA256 exactly. It is never used as
#   is: a generated override replaces its ports with two fresh, unused, random
#   loopback ports, and the rendered configuration is validated before use. The
#   live 18080/3005 ports are never bound.
# - The run ID is 96 random bits. Every container, volume and network the run
#   creates, including short-lived --rm helpers, carries the label
#   ltp.rehearsal=<run>. Long-lived ones are recorded by exact ID in the manifest.
# - A label census reconciles the daemon against the manifest on failure, before
#   success cleanup and before --cleanup. A labelled resource that is not in the
#   manifest (an interrupted --rm helper, a partial compose up) is reported as
#   UNRECORDED and never removed. Nothing is ever matched by name prefix.
# - A failed run removes nothing. Cleanup (after success, or --cleanup) removes
#   recorded resources one at a time; it may stop partway. Each removal is
#   dropped from the manifest as it happens, so the manifest always lists what
#   remains, and anything not verified or not removable is held and reported.
# - The private synthetic context (runtime.env, census.key, env-*) is kept until
#   a cleanup has verified that every resource is gone, so a held run can be
#   compared and reconstructed.
# - Every Docker call goes through ops/pinned_docker.py. The daemon is never
#   restarted; that case is reported as unverified.
set -euo pipefail
umask 077
export MSYS_NO_PATHCONV=1
HERE=$(cd "$(dirname "$0")" && pwd -W 2>/dev/null || pwd)
w() { cygpath -w "$1" 2>/dev/null || echo "$1"; }
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOCKER_LOCAL=npipe:////./pipe/docker_engine;; *) DOCKER_LOCAL=unix:///var/run/docker.sock;; esac
DAEMON_ID=$(python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" -- info --format '{{.ID}}')
test -n "$DAEMON_ID"
docker() { python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" -- "$@"; }

label_of() {  # kind id -> the ltp.rehearsal label, or fails
  case "$1" in
    container) docker container inspect -f '{{index .Config.Labels "ltp.rehearsal"}}' "$2" ;;
    network) docker network inspect -f '{{index .Labels "ltp.rehearsal"}}' "$2" ;;
    volume) docker volume inspect -f '{{index .Labels "ltp.rehearsal"}}' "$2" ;;
    *) return 1 ;;
  esac
}
labelled() {  # kind run -> exact IDs (volume names) carrying this run's label
  case "$1" in
    container) docker ps -aq --no-trunc --filter "label=ltp.rehearsal=$2" ;;
    volume) docker volume ls -q --filter "label=ltp.rehearsal=$2" ;;
    network) docker network ls -q --no-trunc --filter "label=ltp.rehearsal=$2" ;;
  esac
}
reconcile() {  # manifest run -> 0 only if the label census succeeded and every labelled resource is recorded
  local manifest=$1 run=$2 found=0 kind id ids
  for kind in container volume network; do
    if ! ids=$(labelled "$kind" "$run"); then echo "UNCERTAIN $kind label census failed"; found=1; continue; fi
    for id in $ids; do
      grep -qx "$kind $id" "$manifest" || { echo "UNRECORDED $kind $id carries this run's label; preserved, not removed"; found=1; }
    done
  done
  return $found
}
none_left() {  # run -> 0 only if the label census succeeded and found nothing
  local run=$1 kind ids
  for kind in container volume network; do
    ids=$(labelled "$kind" "$run") || return 1
    [ -z "$ids" ] || return 1
  done
}
remove_owned() {  # manifest run -> 0 only if every recorded resource was verified and removed
  local manifest=$1 run=$2 held=0 kind id label
  for pass_kind in container network volume; do
    while read -r kind id; do
      [ "$kind" = "$pass_kind" ] || continue
      if ! label=$(label_of "$kind" "$id" 2>/dev/null); then
        echo "HOLD $kind $id: cannot inspect it (already gone or daemon problem); not removed"; held=1; continue; fi
      if [ "$label" != "$run" ]; then echo "HOLD $kind $id: label '$label' is not this run; not removed"; held=1; continue; fi
      if case "$kind" in
           container) docker container rm -f "$id" >/dev/null ;;
           network) docker network rm "$id" >/dev/null ;;
           volume) docker volume rm "$id" >/dev/null ;;
         esac; then
        sed -i "/^$kind $id\$/d" "$manifest"   # removed: the manifest keeps listing only what remains
      else
        echo "HOLD $kind $id: removal failed"; held=1
      fi
    done < <(tac "$manifest")
  done
  return $held
}
drop_private_context() {  # evidence dir: only after a verified complete cleanup
  rm -f -- "$1/runtime.env" "$1/census.key" "$1"/env-*
}
if [ "${1:-}" = --cleanup ]; then
  manifest=$2; run=$(sed -n 's/^run //p' "$manifest" | tr -d '\r')
  [[ "$run" =~ ^claude-reh-[0-9a-f]{24}$ ]] || { echo "manifest has no valid run ID"; exit 2; }
  unrecorded=0; held=0
  reconcile "$manifest" "$run" || unrecorded=1
  remove_owned "$manifest" "$run" || held=1
  if [ "$unrecorded" = 0 ] && [ "$held" = 0 ] && none_left "$run"; then
    drop_private_context "$(dirname "$manifest")"
    echo "removed every resource of $run (label census confirms none left); private synthetic context deleted, other evidence kept"
    exit 0
  fi
  echo "some resources of $run remain (see HOLD/UNRECORDED above); the manifest lists the recorded ones still present"
  exit 1
fi

OLD_IMAGE=$1; NEW_IMAGE=$2; COMPOSE_FILE=$3; COMPOSE_SHA256=$4; REVIEWED_ALLOWLIST=${5:-}
RUN=claude-reh-$(python -c 'import secrets;print(secrets.token_hex(12))')
E=$(mktemp -d)
MANIFEST=$E/manifest
echo "run $RUN" > "$MANIFEST"
FINISHED=0
on_exit() {
  if [ "$FINISHED" = 1 ] && reconcile "$MANIFEST" "$RUN" && remove_owned "$MANIFEST" "$RUN" && none_left "$RUN"; then
    rm -rf -- "$E"
    return
  fi
  [ "$FINISHED" = 1 ] || reconcile "$MANIFEST" "$RUN" || true
  echo "HELD: run $RUN. A failed run removes nothing; a cleanup that stopped partway may have removed some"
  echo "      recorded resources, and the manifest lists those that remain. Evidence, manifest and the private"
  echo "      synthetic context are kept in: $E"
  echo "      inspect, then: ops/cutover_rehearsal.sh --cleanup $(w "$MANIFEST")"
}
trap on_exit EXIT
step() { echo; echo "== $*"; }
pass() { echo "PASS $*"; }
fail() { echo "FAIL $*"; exit 1; }
own() {  # kind id: record an exact ID, after verifying it carries this run's label
  local label
  label=$(label_of "$1" "$2") || fail "cannot inspect new $1 $2"
  [ "$label" = "$RUN" ] || fail "new $1 $2 is not labelled for this run"
  echo "$1 $2" >> "$MANIFEST"
}
LABEL=(--label "ltp.rehearsal=$RUN")

step "Identity: exact images and compose file, no pulls"
for image in "$OLD_IMAGE" "$NEW_IMAGE"; do
  [[ "$image" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "image must be a full local ID: $image"
  [ "$(docker image inspect -f '{{.Id}}' "$image")" = "$image" ] || fail "image $image is not present locally"
done
[ "$(sha256sum "$COMPOSE_FILE" | cut -d' ' -f1)" = "$COMPOSE_SHA256" ] || fail "compose file does not match $COMPOSE_SHA256"
grep -q '"127.0.0.1:18080:8080"' "$COMPOSE_FILE" || fail "compose file does not have the expected live port line"
pass "old $OLD_IMAGE, release $NEW_IMAGE, compose $COMPOSE_SHA256; run $RUN"

step "Synthetic compose: fresh random loopback ports, validated"
read -r OPERATOR_PORT VIEWER_PORT < <(python - <<'PY' | tr -d '\r'
import secrets, socket
ports = set()
while len(ports) < 2:
    port = 20000 + secrets.randbelow(40000)
    if port in (18080, 3005, 18443):
        continue
    with socket.socket() as probe:
        try:
            probe.bind(('127.0.0.1', port))
        except OSError:
            continue
    ports.add(port)
print(*sorted(ports))
PY
)
cat > "$E/override.yaml" <<EOF
services:
  coordinator:
    pull_policy: never
    ports: !override
      - "127.0.0.1:$OPERATOR_PORT:8080"
      - "127.0.0.1:$VIEWER_PORT:8080"
    labels:
      ltp.rehearsal: "$RUN"
networks:
  default:
    labels:
      ltp.rehearsal: "$RUN"
EOF
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
compose_env=(env PROCESSING_IMAGE="$OLD_IMAGE" VIEWER_BIND_ADDRESS=127.0.0.1 VIEWER_PORT="$VIEWER_PORT"
             PROCESSING_RUNTIME_ENV="$(w "$E/runtime.env")" PROCESSING_STATE_VOLUME="$RUN-state")
old_compose() { "${compose_env[@]}" python "$HERE/pinned_docker.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" \
  -- compose -p "$RUN" -f "$(w "$COMPOSE_FILE")" -f "$(w "$E/override.yaml")" "$@"; }
old_compose config --format json > "$E/compose-rendered.json"
python - "$(w "$E/compose-rendered.json")" "$OLD_IMAGE" "$RUN" "$OPERATOR_PORT" "$VIEWER_PORT" <<'PY' || fail "rendered compose is not the expected synthetic configuration"
import json, sys
config, image, run, *ports = sys.argv[1], sys.argv[2], sys.argv[3], *sys.argv[4:]
document = json.load(open(config))
service = document['services']['coordinator']
assert set(document['services']) == {'coordinator'}
assert service['image'] == image, service['image']
assert service.get('pull_policy') == 'never'
assert 'build' not in service
published = sorted((p.get('host_ip'), str(p['published']), p['target']) for p in service['ports'])
assert published == sorted(('127.0.0.1', port, 8080) for port in ports), published
assert all(p not in ('18080', '3005', '18443') for _, p, _ in published)
volumes = document['volumes']
assert list(volumes) == ['state'] and volumes['state']['external'] and volumes['state']['name'] == run + '-state', volumes
assert service['labels'].get('ltp.rehearsal') == run
PY
pass "rendered compose: only 127.0.0.1:$OPERATOR_PORT and 127.0.0.1:$VIEWER_PORT, exact image, pull never, run-owned volume"

healthy() {
  for _ in $(seq 40); do
    docker exec "$1" python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=1)" \
      >/dev/null 2>&1 && return 0
    [ "$(docker inspect -f '{{.State.Running}}' "$1")" = true ] || return 1
    sleep 0.5
  done
  return 1
}
http() {  # method path token [body] -> "STATUS BODY"
  python - "$VIEWER_PORT" "$@" <<'PY'
import sys, urllib.error, urllib.request
port, method, path, token = sys.argv[1:5]
body = sys.argv[5].encode() if len(sys.argv) > 5 else None
request = urllib.request.Request('http://127.0.0.1:%s%s?token=%s' % (port, path, token), data=body, method=method,
                                 headers={'Content-Type': 'application/x-www-form-urlencoded'})
try:
    with urllib.request.urlopen(request, timeout=5) as response:
        print(response.status, response.read().decode(errors='replace')[:120])
except urllib.error.HTTPError as error:
    print(error.code, error.read().decode(errors='replace')[:120])
PY
}
expect_http() {  # label expected actual
  [ "$3" = "$2" ] && pass "$1: $3" || fail "$1: expected '$2', got '$3'"
}
UNKNOWN=uuid=11111111-2222-4333-8444-555555555555
census() { docker run --rm -i --pull never "${LABEL[@]}" --network none --read-only --user 10001:10001 --entrypoint python \
  --mount "type=volume,source=$1,destination=/src,readonly" \
  --mount "type=bind,source=$HERE/state_census.py,destination=/c.py,readonly" "$OLD_IMAGE" -B /c.py census /src < "$E/census.key"; }
same() { python "$HERE/state_census.py" compare "$(w "$1")" "$(w "$2")" "${@:3}" < "$E/census.key" > "$E/last-compare.json"; }
new_volume() {
  docker volume create "${LABEL[@]}" "$1" >/dev/null
  own volume "$1"
}
fence() {
  local id
  id=$(docker run -d --pull never "${LABEL[@]}" --name "$RUN-fence-$1" --network none --init --read-only --user 10001:10001 \
    --cap-drop ALL --security-opt no-new-privileges --restart always \
    --mount "type=volume,source=$2,destination=/fence/state,readonly" \
    --mount "type=bind,source=$HERE/state_fence.py,destination=/state_fence.py,readonly" \
    --entrypoint python "$NEW_IMAGE" -B /state_fence.py /fence/state)
  own container "$id"
}
verify_fence() {
  for _ in $(seq 20); do
    docker exec "$RUN-fence-$1" python -B /state_fence.py --verify /fence/state 2>/dev/null | grep -q '"verified": true' && return 0
    sleep 0.5
  done
  return 1
}
spec() {  # mode name extra...
  python "$HERE/coordinator_container_spec.py" --container "$(w "$E/old-inspect.json")" \
    --old-image "$(w "$E/old-image.json")" --new-image "$(w "$E/new-image.json")" --mode "$1" \
    --volume "$NEW_VOL" --name "$2" --lease-volume "$RUN-lease" --accept-compose-source \
    --label "ltp.rehearsal=$RUN" "${@:3}"
}
run_spec() {  # spec.json -> creates the container and records its exact ID
  mapfile -d '' -t ARGV < <(python -c 'import json,sys;a=json.load(open(sys.argv[1]))["argv"];assert a[0]=="docker";sys.stdout.write("\0".join(a[1:]))' "$(w "$1")")
  [ "${#ARGV[@]}" -gt 1 ]
  local id
  id=$(docker "${ARGV[@]}")
  own container "$id"
}
OLD_VOL=$RUN-state
VERIFY_VOL=$RUN-verify
NEW_VOL=$RUN-release

step "Setup: the old coordinator from d738's compose definition"
new_volume "$OLD_VOL"
docker run --rm --pull never "${LABEL[@]}" --network none --user 0:0 --mount "type=volume,source=$OLD_VOL,destination=/s" \
  --entrypoint chown "$OLD_IMAGE" 10001:10001 /s
old_compose up -d --pull never >/dev/null
OLD=$(docker inspect -f '{{.Id}}' "$(old_compose ps -q coordinator)")
own container "$OLD"
own network "$(docker network inspect -f '{{.Id}}' "${RUN}_default")"
[ "$(docker inspect -f '{{.Image}}' "$OLD")" = "$OLD_IMAGE" ] || fail "old container runs another image"
healthy "$OLD" || fail "old coordinator did not start"
pass "old d738 running from compose (exact image, unless-stopped)"

step "Step 0: capture"
python "$HERE/state_census.py" keygen "$(w "$E/census.key")"
docker inspect "$OLD" > "$E/old-inspect.json"
docker image inspect "$OLD_IMAGE" > "$E/old-image.json"
docker image inspect "$NEW_IMAGE" > "$E/new-image.json"
[ "$(docker inspect -f '{{range .Mounts}}{{.Name}}{{end}}' "$OLD")" = "$OLD_VOL" ] || fail "unexpected state volume"
spec isolated "$RUN-dry" --network none --env-out "$(w "$E/env-dry")" > /dev/null || fail "spec refused the compose container"

[ "$(docker ps -aq --filter "volume=$OLD_VOL" | wc -l)" = 1 ] || fail "another container uses the state volume"
pass "captured; spec accepts the compose-managed container; only the old coordinator uses the volume"

step "Step 1: R2 quiescence inside the old coordinator"
active=$(docker exec -i "$OLD" python - < "$HERE/netns_sockets.py" | python -c 'import json,sys;print(json.load(sys.stdin)["activeInbound"])')
[ "$active" = 0 ] && pass "no active inbound connection in d738's own namespace" || fail "active inbound connections: $active"

step "Step 2: stop, fence, remove"
docker update --restart=no "$OLD" >/dev/null
docker stop -t 60 "$OLD" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}} {{.State.OOMKilled}}' "$OLD")" = "0 false" ] || fail "d738 did not drain cleanly"
fence original "$OLD_VOL"
verify_fence original || fail "original fence not verified"
docker rm "$OLD" >/dev/null
python "$HERE/container_absent.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" "$OLD" >/dev/null \
  || fail "old container not confirmed absent"
sed -i "/^container $OLD\$/d" "$MANIFEST"   # deliberately removed by the runbook, confirmed absent
pass "d738 drained (exit 0), original fenced and verified, old container confirmed absent on the recorded daemon"

step "Step 3: verified archive, verification volume and copy"
census "$OLD_VOL" > "$E/census-original.json"
docker run --rm --pull never "${LABEL[@]}" --network none --read-only --user 10001:10001 \
  --mount "type=volume,source=$OLD_VOL,destination=/src,readonly" \
  --entrypoint tar "$OLD_IMAGE" --numeric-owner -C /src -cpf - . > "$E/state-original.tar"
(cd "$E" && sha256sum state-original.tar > state-original.tar.sha256)
for v in "$VERIFY_VOL" "$NEW_VOL"; do
  new_volume "$v"
  [ "$(census "$v" | python -c 'import json,sys;print(json.load(sys.stdin)["tree"]["entries"])')" = 1 ] || fail "$v not empty"
  (cd "$E" && sha256sum -c state-original.tar.sha256 >/dev/null)
  docker run --rm -i --pull never "${LABEL[@]}" --network none --user 0:0 --mount "type=volume,source=$v,destination=/dst" \
    --entrypoint tar "$OLD_IMAGE" --numeric-owner --same-owner -C /dst -xpf - < "$E/state-original.tar"
  census "$v" > "$E/census-$v.json"
done
fence verify "$VERIFY_VOL"
verify_fence verify && verify_fence original || fail "fences not both verified"
census "$OLD_VOL" > "$E/census-original-after.json"
for c in "census-$VERIFY_VOL" "census-$NEW_VOL" census-original-after; do
  same "$E/census-original.json" "$E/$c.json" || fail "$c differs from the original"
done
pass "archive restores identically into both volumes, original unchanged; verification volume fenced by its own holder"

step "Step 5: isolated candidate with the authority lease"
new_volume "$RUN-lease"
docker run --rm --pull never "${LABEL[@]}" --network none --user 0:0 --mount "type=volume,source=$RUN-lease,destination=/l" \
  --entrypoint sh "$OLD_IMAGE" -c 'chown 10001:10001 /l && chmod 700 /l'
spec isolated "$RUN-candidate" --network none --env-out "$(w "$E/env-candidate")" > "$E/candidate-spec.json"
run_spec "$E/candidate-spec.json"
docker start "$RUN-candidate" >/dev/null
healthy "$RUN-candidate" || fail "candidate did not start"
[ "$(docker inspect -f '{{.Image}}' "$RUN-candidate")" = "$NEW_IMAGE" ] || fail "candidate runs another image"
[ "$(docker exec "$RUN-candidate" sh -c "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' '")" = lo ] || fail "candidate has a network interface"
[ -z "$(docker port "$RUN-candidate")" ] || fail "candidate publishes ports"
pass "candidate healthy on the exact release image, only loopback, no published ports, lease held"

step "Step 6: promotion gate"
docker stop -t 60 "$RUN-candidate" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-candidate")" = 0 ] || fail "candidate did not drain"
census "$NEW_VOL" > "$E/census-candidate-isolated.json"
FIXED=(--allow-path .:mtime --allow-path jobs.sqlite3:bytes,content,mtime
       --allow-new-table viewer_reservations --allow-new-table viewer_option_evidence --allow-lease-marker)
if [ -n "$REVIEWED_ALLOWLIST" ]; then
  same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" "${FIXED[@]}" \
    --schema-allowlist "$(w "$REVIEWED_ALLOWLIST")" || fail "gate failed against the reviewed allowlist"
  pass "gate passes against the supplied reviewed schema allowlist and the exact lease marker"
else
  same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" "${FIXED[@]}" || true
  OBJECTS=$(python -c 'import json,sys
r=json.load(open(sys.argv[1]))
other=[d for d in r["database"] if "object" not in d]
assert not other and not r["files"], (other, r["files"])
print(" ".join(sorted(d["object"] for d in r["database"])))' "$(w "$E/last-compare.json")") || fail "startup changed more than schema"
  ARGS=(); for o in $OBJECTS; do ARGS+=(--object "$o"); done
  python "$HERE/state_census.py" schema-allowlist "$(w "$E/census-candidate-isolated.json")" "${ARGS[@]}" \
    --output "$(w "$E/synthetic-schema-allowlist.json")"
  same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" "${FIXED[@]}" \
    --schema-allowlist "$(w "$E/synthetic-schema-allowlist.json")" || fail "gate failed"
  echo "SYNTHETIC COMPARISON EXERCISE: the allowlist was derived from the observed differences ($OBJECTS)."
  echo "This is not release schema qualification; it needs an independently reviewed allowlist."
  pass "only schema objects and the exact lease marker changed (synthetic exercise)"
fi

step "Step 7: promote with production held"
python "$HERE/container_absent.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" "$OLD" >/dev/null || fail "old container reappeared"
verify_fence original && verify_fence verify || fail "fences lost before promotion"
census "$NEW_VOL" > "$E/census-before-promotion.json"
same "$E/census-candidate-isolated.json" "$E/census-before-promotion.json" || fail "copy changed since the gate"
CANDIDATE=$(docker inspect -f '{{.Id}}' "$RUN-candidate")
docker rm "$CANDIDATE" >/dev/null; sed -i "/^container $CANDIDATE\$/d" "$MANIFEST"
spec promoted "$RUN-promoted" --production-admission held --env-out "$(w "$E/env-promoted")" > "$E/promoted-spec.json"
run_spec "$E/promoted-spec.json"
docker start "$RUN-promoted" >/dev/null
healthy "$RUN-promoted" || fail "promoted release did not start"
[ "$(docker port "$RUN-promoted" | sort | tr '\n' ' ')" = "$(printf '8080/tcp -> 127.0.0.1:%s\n8080/tcp -> 127.0.0.1:%s\n' "$OPERATOR_PORT" "$VIEWER_PORT" | sort | tr '\n' ' ')" ] \
  || fail "promoted ports are not the synthetic ones"
expect_http "production mutation held" '503 {"error":"production_admission_held"}' "$(http POST /task/new/init "$PRODUCTION" "")"
expect_http "staging reaches the service past the hold" '404 {"error":"task_not_found"}' "$(http POST /task/cancel "$STAGING" "$UNKNOWN")"
expect_http "production reads still pass the hold" '404 {"error":"task_not_found"}' \
  "$(http GET /task/11111111-2222-4333-8444-555555555555/info "$PRODUCTION")"

step "Accidental restart of the old definition while the release runs"
set +e; out=$(old_compose up -d --pull never 2>&1); code=$?; set -e
again=$(old_compose ps -aq coordinator 2>/dev/null || true)
if [ -n "$again" ]; then
  again=$(docker inspect -f '{{.Id}}' "$again"); own container "$again"
  sleep 3
  [ "$(docker inspect -f '{{.State.Running}}' "$again")" = false ] || fail "old definition is running"
  logs=$(docker logs "$again" 2>&1 | tail -3)
  if [[ "$logs" == *"prototype state already owned"* ]]; then pass "compose up of d738 refused by the state fence"
  elif [[ "$out" == *"port is already allocated"* || "$out" == *"address already in use"* || "$out" == *"bind"* ]]; then
    pass "compose up of d738 refused: the release holds its host ports"
  else fail "old definition stopped for an unexplained reason (exit $code)"; fi
else
  fail "compose up created no container and gave no reason (exit $code)"
fi
healthy "$RUN-promoted" || fail "promoted release affected by the old definition"
verify_fence original || fail "original fence lost"
pass "release unaffected; original fence still verified"

step "Open production"
docker stop -t 60 "$RUN-promoted" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-promoted")" = 0 ] || fail "held release did not drain"
census "$NEW_VOL" > "$E/census-after-held.json"
same "$E/census-candidate-isolated.json" "$E/census-after-held.json" || fail "held phase wrote state"
pass "nothing was written while production was held"
PROMOTED=$(docker inspect -f '{{.Id}}' "$RUN-promoted")
docker rm "$PROMOTED" >/dev/null; sed -i "/^container $PROMOTED\$/d" "$MANIFEST"
spec promoted "$RUN-open" --production-admission open --env-out "$(w "$E/env-open")" > "$E/open-spec.json"
run_spec "$E/open-spec.json"
docker start "$RUN-open" >/dev/null
healthy "$RUN-open" || fail "opened release did not start"
expect_http "production reaches the service after reopening" '404 {"error":"task_not_found"}' \
  "$(http POST /task/cancel "$PRODUCTION" "$UNKNOWN")"

step "Rollback rehearsal (nothing admitted)"
docker stop -t 60 "$RUN-open" >/dev/null
census "$NEW_VOL" > "$E/census-before-rollback.json"
same "$E/census-candidate-isolated.json" "$E/census-before-rollback.json" \
  && pass "copy unchanged since the gate: rollback loses nothing" || fail "copy changed: this would be a hold, not a rollback"
OPENED=$(docker inspect -f '{{.Id}}' "$RUN-open")
docker rm "$OPENED" >/dev/null; sed -i "/^container $OPENED\$/d" "$MANIFEST"
FENCE_ID=$(docker inspect -f '{{.Id}}' "$RUN-fence-original")
docker rm -f "$FENCE_ID" >/dev/null; sed -i "/^container $FENCE_ID\$/d" "$MANIFEST"
docker rm -f "$again" >/dev/null; sed -i "/^container $again\$/d" "$MANIFEST"
old_compose up -d --pull never >/dev/null
BACK=$(docker inspect -f '{{.Id}}' "$(old_compose ps -q coordinator)")
own container "$BACK"
healthy "$BACK" || fail "d738 did not come back"
docker stop -t 60 "$BACK" >/dev/null
census "$OLD_VOL" > "$E/census-rolled-back.json"
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
echo "UNVERIFIED Docker daemon restart / host reboot (the shared daemon is never restarted)"
echo "REHEARSAL PASSED on images $OLD_IMAGE (old) and $NEW_IMAGE (release)"
FINISHED=1
