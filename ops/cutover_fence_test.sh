#!/usr/bin/env bash
# Isolated container test of the cutover fences, on synthetic state only.
#
# Usage: ops/cutover_fence_test.sh D738_IMAGE RELEASE_IMAGE
# Creates and removes only resources named claude-ct-<run>-*. Needs no network
# access: every coordinator runs with unreachable synthetic engine origins.
set -euo pipefail
export MSYS_NO_PATHCONV=1
OLD_IMAGE=$1
NEW_IMAGE=$2
HERE=$(cd "$(dirname "$0")" && pwd -W 2>/dev/null || pwd)
RUN=claude-ct-$RANDOM
LEASE_DIR=/run/ledgetop-authority-lease
CONTAINERS=()
VOLUMES=()
cleanup() {
  for c in "${CONTAINERS[@]}"; do docker rm -f "$c" >/dev/null 2>&1 || true; done
  for v in "${VOLUMES[@]}"; do docker volume rm -f "$v" >/dev/null 2>&1 || true; done
  rm -f "$ENV_FILE"
}
trap cleanup EXIT
pass() { echo "PASS $*"; }
fail() { echo "FAIL $*"; exit 1; }
volume() { docker volume create "$RUN-$1" >/dev/null; VOLUMES+=("$RUN-$1"); }
ENV_FILE=$(mktemp)
token() { python -c 'import secrets;print(secrets.token_hex(24))'; }
cat > "$ENV_FILE" <<EOF
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
DOCKER_ENV=$(cygpath -w "$ENV_FILE" 2>/dev/null || echo "$ENV_FILE")  # Docker Desktop needs a host path
HARDEN=(--init --read-only --user 10001:10001 --cap-drop ALL --security-opt no-new-privileges --env-file "$DOCKER_ENV")

# name volume network image [extra docker args...] -- start a coordinator container
start() {
  local name=$RUN-$1 vol=$RUN-$2 network=$3 image=$4; shift 4
  CONTAINERS+=("$name")
  docker run -d --name "$name" --network "$network" "${HARDEN[@]}" \
    --mount "type=volume,source=$vol,destination=/prototype-state" "$@" "$image" >/dev/null
}
healthy() {  # wait up to 20 s for /healthz inside the container
  for _ in $(seq 40); do
    if docker exec "$RUN-$1" python -c "import urllib.request;urllib.request.urlopen('http://127.0.0.1:8080/healthz',timeout=1)" >/dev/null 2>&1; then return 0; fi
    if [ "$(docker inspect -f '{{.State.Running}}' "$RUN-$1")" != true ]; then return 1; fi
    sleep 0.5
  done
  return 1
}
exited_with() {  # name pattern: container stopped and its log contains pattern
  for _ in $(seq 40); do
    if [ "$(docker inspect -f '{{.State.Running}}' "$RUN-$1")" != true ]; then
      docker logs "$RUN-$1" 2>&1 | grep -q -- "$2"; return; fi
    sleep 0.5
  done
  return 1
}
db_sha() { docker run --rm --network none --read-only --mount "type=volume,source=$RUN-$1,destination=/s,readonly" \
  --entrypoint sha256sum "$OLD_IMAGE" /s/jobs.sqlite3 | cut -c1-64; }
copy_volume() {  # src dst: verbatim tar copy, like the procedure
  volume "$2"
  docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-$1,destination=/src,readonly" \
    --mount "type=volume,source=$RUN-$2,destination=/dst" --entrypoint sh "$OLD_IMAGE" \
    -c 'cd /src && tar --numeric-owner -cpf /tmp/s.tar . && cd /dst && tar --numeric-owner --same-owner -xpf /tmp/s.tar'
}

# Seed: d738 creates the original state and authority, then stops cleanly.
volume original
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-original,destination=/s" \
  --entrypoint chown "$OLD_IMAGE" 10001:10001 /s
start seed original none "$OLD_IMAGE"
healthy seed || fail "seed d738 did not become healthy"
docker stop -t 30 "$RUN-seed" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-seed")" = 0 ] || fail "seed d738 did not drain cleanly"
copy_volume original copy
copy_volume original spare
volume lease
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-lease,destination=/l" \
  --entrypoint sh "$OLD_IMAGE" -c 'chown 10001:10001 /l && chmod 700 /l'
LEASE=(--mount "type=volume,source=$RUN-lease,destination=$LEASE_DIR" -e "PROCESSING_AUTHORITY_LEASE_DIR=$LEASE_DIR")

# 1. The new lease does not fence d738: both run with the same authority.
start candidate copy none "$NEW_IMAGE" "${LEASE[@]}"
healthy candidate || fail "candidate on copy did not start"
start old-alongside original none "$OLD_IMAGE"
if healthy old-alongside; then
  pass "lease alone does NOT fence d738: d738 and the leased release both run with one authority"
else fail "expected d738 to start alongside the leased release (it has no lease code)"; fi
docker rm -f "$RUN-old-alongside" >/dev/null
[ "$(docker run --rm --network none --mount "type=volume,source=$RUN-lease,destination=/l,readonly" \
  --entrypoint sh "$OLD_IMAGE" -c 'ls /l | wc -l')" = 1 ] || fail "unexpected lease files"
pass "only the release created a lease file; d738 never touched it"

# 2. The lease does fence a second release instance on another copy.
start second spare none "$NEW_IMAGE" "${LEASE[@]}"
exited_with second authority_lease_held && pass "second release on a copy refused: authority_lease_held" \
  || fail "second release was not refused"

# 3. Leased state refuses to start without the lease configured.
docker stop -t 30 "$RUN-candidate" >/dev/null
start unleased copy none "$NEW_IMAGE"
exited_with unleased authority_lease_configuration_required \
  && pass "leased state refuses an unleased start" || fail "unleased start not refused"

# 4. Independent d738 fence: one holder per retained d738 state, verified live.
fence() {  # volume: start a holder for exactly one volume
  CONTAINERS+=("$RUN-fence-$1")
  docker run -d --name "$RUN-fence-$1" --network none --init --read-only --user 10001:10001 --cap-drop ALL \
    --security-opt no-new-privileges --restart always \
    --mount "type=volume,source=$RUN-$1,destination=/fence/state,readonly" \
    --mount "type=bind,source=$HERE/state_fence.py,destination=/state_fence.py,readonly" \
    --entrypoint python "$NEW_IMAGE" -B /state_fence.py /fence/state >/dev/null
}
verified() {  # volume: /proc/locks proves the current holder owns the lock now
  for _ in $(seq 20); do
    docker exec "$RUN-fence-$1" python -B /state_fence.py --verify /fence/state 2>/dev/null \
      | grep -q '"verified": true' && return 0
    sleep 0.5
  done
  return 1
}
fence original
verified original && pass "fence on the original verified live from /proc/locks" || fail "original fence not verified"
fence spare   # added as a second holder; the first is never recreated or released
verified spare && verified original && pass "second holder added without releasing the first; both verified" \
  || fail "fences not both verified"
before=$(db_sha original)
start old-fenced original none "$OLD_IMAGE"
exited_with old-fenced "prototype state already owned" \
  && pass "d738 on the original state refused while fenced" || fail "fenced d738 started"
[ "$(db_sha original)" = "$before" ] && pass "refused d738 did not modify the original database" \
  || fail "refused d738 changed the database"
start old-spare spare none "$OLD_IMAGE"
exited_with old-spare "prototype state already owned" && pass "d738 on the spare copy refused while fenced" \
  || fail "d738 on spare started"
if docker exec "$RUN-fence-original" python -B /state_fence.py --verify /fence/state /fence/missing >/dev/null 2>&1; then
  fail "verify passed for a lock it does not hold"; fi
pass "verify fails for a path the holder does not fence"

# 5. Holder death: Docker restarts it, and verification proves the lock is held again.
restarts=$(docker inspect -f '{{.RestartCount}}' "$RUN-fence-original")
# Kill the holder from inside, as a crash would (not "docker stop"). The exec
# itself is torn down when the container exits, so its status is not checked.
docker exec "$RUN-fence-original" python -c 'import os,signal
for pid in os.listdir("/proc"):
    try:
        if pid.isdigit() and int(pid) not in (1, os.getpid()) and b"state_fence" in open("/proc/%s/cmdline" % pid, "rb").read():
            os.kill(int(pid), signal.SIGKILL)
    except OSError:
        pass' || true
for _ in $(seq 40); do
  [ "$(docker inspect -f '{{.RestartCount}}' "$RUN-fence-original")" -gt "$restarts" ] && \
  [ "$(docker inspect -f '{{.State.Running}}' "$RUN-fence-original")" = true ] && break; sleep 0.5; done
[ "$(docker inspect -f '{{.RestartCount}}' "$RUN-fence-original")" -gt "$restarts" ] || fail "fence was not restarted"
verified original && pass "after the holder was killed and restarted by Docker, ownership verified again" \
  || fail "restarted fence not verified"
docker rm -f "$RUN-old-fenced" >/dev/null
start old-fenced original none "$OLD_IMAGE"
exited_with old-fenced "prototype state already owned" \
  && pass "after the restart, d738 is still refused" || fail "d738 started after fence restart"
echo "NOTE the interval between holder death and its restart is unfenced; this test does not close it"

# 6. A running release on the copy blocks d738 on that copy via its own state lock.
docker rm -f "$RUN-unleased" >/dev/null
start promoted copy bridge "$NEW_IMAGE" "${LEASE[@]}" -p 127.0.0.1:18999:8080 \
  -e PROCESSING_VIEWER_PRODUCTION_ADMISSION=held
healthy promoted || fail "promoted release did not start"
start old-on-copy copy none "$OLD_IMAGE"
exited_with old-on-copy "prototype state already owned" && pass "d738 on the release's copy refused" \
  || fail "d738 on release copy started"

# 7. Host port exclusivity: a d738 publishing the release's port cannot start.
volume portcheck
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-portcheck,destination=/s" \
  --entrypoint chown "$OLD_IMAGE" 10001:10001 /s
CONTAINERS+=("$RUN-old-port")
docker create --name "$RUN-old-port" "${HARDEN[@]}" -p 127.0.0.1:18999:8080 \
  --mount "type=volume,source=$RUN-portcheck,destination=/prototype-state" "$OLD_IMAGE" >/dev/null
if docker start "$RUN-old-port" >/dev/null 2>&1; then fail "d738 started on the release's host port"; fi
pass "d738 publishing the release's host port cannot start"

# 8. Production admission hold, over the real listener.
docker exec -i "$RUN-promoted" python - <<'EOF' || fail "production hold check"
import os, urllib.error, urllib.request
base = 'http://127.0.0.1:8080'
def call(path, token, data=None):
    request = urllib.request.Request(base + path + '?token=' + token, data=data, method='POST' if data is not None else 'GET',
                                     headers={'Content-Type': 'application/x-www-form-urlencoded'})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()
production, staging = os.environ['PROCESSING_VIEWER_TOKEN'], os.environ['PROCESSING_VIEWER_STAGING_TOKEN']
for path in ('/task/new/init', '/task/cancel', '/task/remove', '/task/ledgetop/reserve'):
    status, body = call(path, production, b'uuid=11111111-2222-4333-8444-555555555555')
    assert status == 503 and b'production_admission_held' in body, (path, status, body)
status, body = call('/task/cancel', staging, b'uuid=11111111-2222-4333-8444-555555555555')
assert status != 503, (status, body)
# /info probes the (unreachable, synthetic) engines, so use a task read: it
# must pass the hold and reach the service, which reports the unknown task.
status, body = call('/task/11111111-2222-4333-8444-555555555555/info', production)
assert status == 404 and b'task_not_found' in body, (status, body)
print('ok')
EOF
pass "held production refuses every mutation with 503; staging is admitted; production reads work"

# 9. network none: the isolated candidate's namespace has only loopback.
copy_volume original isolated
start isolated isolated none "$NEW_IMAGE"
healthy isolated || fail "network-none candidate did not become healthy"
ifaces=$(docker exec "$RUN-isolated" sh -c "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' ' | sort | tr '\n' ' '" 2>/dev/null || echo exited)
echo "isolated interfaces: $ifaces"
[ "$ifaces" = "lo " ] && [ "$(docker inspect -f '{{.HostConfig.NetworkMode}}' "$RUN-isolated")" = none ] \
  && pass "network-none candidate has only loopback: no bridge interface or address to reach" \
  || fail "isolated candidate has a non-loopback interface"
# 10. Promotion gate over a real release startup: the lease marker passes only
# through its exact allowance, and the schema changes are listed (U9 input).
copy_volume original gate
volume gatelease
docker run --rm --network none --user 0:0 --mount "type=volume,source=$RUN-gatelease,destination=/l" \
  --entrypoint sh "$OLD_IMAGE" -c 'chown 10001:10001 /l && chmod 700 /l'
w() { cygpath -w "$1" 2>/dev/null || echo "$1"; }  # host Python on Windows needs host paths
OUT=$(mktemp -d)
KEY=$OUT/census.key
python "$HERE/state_census.py" keygen "$(w "$KEY")"
census() { docker run --rm -i --network none --read-only --user 10001:10001 --entrypoint python \
  --mount "type=volume,source=$RUN-$1,destination=/src,readonly" \
  --mount "type=bind,source=$HERE/state_census.py,destination=/c.py,readonly" "$OLD_IMAGE" -B /c.py census /src < "$KEY"; }
census gate > "$OUT/before.json"
start gatecandidate gate none "$NEW_IMAGE" --mount "type=volume,source=$RUN-gatelease,destination=$LEASE_DIR" \
  -e "PROCESSING_AUTHORITY_LEASE_DIR=$LEASE_DIR"
healthy gatecandidate || fail "gate candidate did not start"
docker stop -t 30 "$RUN-gatecandidate" >/dev/null
[ "$(docker inspect -f '{{.State.ExitCode}}' "$RUN-gatecandidate")" = 0 ] || fail "gate candidate did not drain"
census gate > "$OUT/after.json"
FIXED=(--allow-path .:mtime --allow-path jobs.sqlite3:bytes,content,mtime
       --allow-new-table viewer_reservations --allow-new-table viewer_option_evidence)
compare() { python "$HERE/state_census.py" compare "$(w "$OUT/before.json")" "$(w "$OUT/after.json")" "$@" < "$KEY"; }
if compare "${FIXED[@]}" > "$OUT/without-marker.json"; then fail "marker passed without its allowance"; fi
python -c 'import json,sys;r=json.load(open(sys.argv[1]));assert {"table":"coordinator_authority_lease","change":"added"} in r["database"],r' \
  "$(w "$OUT/without-marker.json")" || fail "marker not reported without its allowance"
pass "the lease marker fails the gate unless explicitly allowed"
compare "${FIXED[@]}" --allow-lease-marker > "$OUT/with-marker.json" || true
OBJECTS=$(python -c 'import json,sys
r=json.load(open(sys.argv[1]))
other=[d for d in r["database"] if "object" not in d]
assert not other and not r["files"], ("non-schema differences", other, r["files"])
print(" ".join(sorted(d["object"] for d in r["database"])))' "$(w "$OUT/with-marker.json")") \
  || fail "startup changed more than schema objects and the marker"
echo "startup schema objects needing exact allowances: $OBJECTS"
ARGS=(); for o in $OBJECTS; do ARGS+=(--object "$o"); done
python "$HERE/state_census.py" schema-allowlist "$(w "$OUT/after.json")" "${ARGS[@]}" --output "$(w "$OUT/allow.json")"
compare "${FIXED[@]}" --allow-lease-marker --schema-allowlist "$(w "$OUT/allow.json")" > "$OUT/gate.json" \
  && pass "real release startup passes the gate with exact schema hashes and the exact lease marker" \
  || fail "gate failed with exact allowances"
rm -rf "$OUT" "$KEY"

# 11. Confirmed absence against the real daemon: removed is absent, existing is not.
GONE=$(docker inspect -f '{{.Id}}' "$RUN-gatecandidate")
docker rm "$RUN-gatecandidate" >/dev/null
python "$HERE/container_absent.py" "$GONE" >/dev/null && pass "removed container confirmed absent (census + not-found)" \
  || fail "removed container not confirmed absent"
PRESENT=$(docker inspect -f '{{.Id}}' "$RUN-fence-original")
set +e; python "$HERE/container_absent.py" "$PRESENT" >/dev/null; code=$?; set -e
[ "$code" = 1 ] && pass "existing container reported present" || fail "existing container not reported present ($code)"
echo "ALL PASSED"
