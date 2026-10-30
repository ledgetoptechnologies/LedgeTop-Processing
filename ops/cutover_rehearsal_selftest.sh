#!/usr/bin/env bash
# Assertion-based self-test of ops/cutover_rehearsal.sh: normal run, negative
# inputs, cleanup safety and exit statuses.
#
# Usage: ops/cutover_rehearsal_selftest.sh D738_IMAGE_ID RELEASE_IMAGE_ID D738_COMPOSE_FILE
#
# Exits 0 only if every check passes; every violated expectation is reported
# and makes it exit 1. Runs on synthetic state only. Every Docker call goes
# through ops/pinned_docker.py against the recorded local daemon. It removes
# only the exact evidence folders its own runs print, the resources those runs
# record, and the test containers it creates itself, each checked by label and
# exact ID; it never sweeps by name or prefix.
set -euo pipefail
export MSYS_NO_PATHCONV=1
OLDI=$1
NEWI=$2
COMPOSE=$3
ROOT=$(cd "$(dirname "$0")/.." && pwd)
cd "$ROOT"
WORK=$(mktemp -d)
w() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else printf '%s\n' "$1"; fi; }
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DOCKER_LOCAL=npipe:////./pipe/docker_engine;; *) DOCKER_LOCAL=unix:///var/run/docker.sock;; esac
DAEMON_ID=$(python ops/pinned_docker.py --host "$DOCKER_LOCAL" -- info --format '{{.ID}}')
test -n "$DAEMON_ID"
docker() { python ops/pinned_docker.py --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" -- "$@"; }
SHA=$(sha256sum "$COMPOSE" | cut -d' ' -f1)

FAILURES=0
ok() { echo "ok   $*"; }
bad() { echo "FAIL $*"; FAILURES=$((FAILURES + 1)); }
expect_eq() {  # description expected actual
  if [ "$3" = "$2" ]; then ok "$1 ($3)"; else bad "$1: expected '$2', got '$3'"; fi
}
expect_true() {  # description command...
  local description=$1; shift
  if "$@"; then ok "$description"; else bad "$description"; fi
}
rehearse() {  # output-file args...: always called through status_of
  local out=$1; shift
  bash ops/cutover_rehearsal.sh "$@" > "$out" 2>&1
}
status_of() {  # command... -> prints only its exit status; its own output goes to $WORK/last-command.txt
  set +e
  "$@" > "$WORK/last-command.txt" 2>&1
  local status=$?
  set -e
  echo "$status"
}
evidence_of() { sed -n 's/.*synthetic context are kept in: //p' "$1" | tr -d '\r'; }
run_of() { sed -n 's/^HELD: run \(claude-reh-[0-9a-f]\{24\}\)\..*/\1/p' "$1" | tr -d '\r'; }
run_count() {  # run -> number of containers + volumes + networks carrying exactly that run's label
  local c v n
  c=$(docker ps -aq --filter "label=ltp.rehearsal=$1" | wc -l)
  v=$(docker volume ls -q --filter "label=ltp.rehearsal=$1" | wc -l)
  n=$(docker network ls -q --filter "label=ltp.rehearsal=$1" | wc -l)
  echo $((c + v + n))
}
# Everything this self-test still owes a cleanup is recorded in STATE as it
# appears and dropped only after its removal is verified: rehearsal runs and
# their evidence folders, and the test containers it creates itself.
STATE=$WORK/selftest-state.txt
: > "$STATE"
note() { echo "$*" >> "$STATE"; }
forget() { local line=$1; grep -v -x -F -- "$line" "$STATE" > "$STATE.new" || true; mv -- "$STATE.new" "$STATE"; }
note_run() {  # output-file: record a rehearsal run that left an evidence folder
  local dir run
  dir=$(evidence_of "$1"); run=$(run_of "$1")
  [ -n "$dir" ] && note "evidence $dir run $run"
  return 0
}
remove_dir() {  # exact evidence folder printed by a run -> must be removed
  [ -n "$1" ] && [ -d "$1" ] || { bad "evidence folder '$1' missing"; return; }
  rm -r -- "$1"
  if [ -e "$1" ]; then bad "evidence folder $1 could not be removed"; return; fi
  local line
  line=$(grep -m1 -F -- "evidence $1 run " "$STATE" || true)
  [ -z "$line" ] || forget "$line"
}
remove_test_container() {  # id label: only a container this self-test created, with the label it set
  [ "$(docker inspect -f '{{index .Config.Labels "ltp.rehearsal"}}' "$1")" = "$2" ] || { bad "test container $1 label changed"; return; }
  docker rm "$1" >/dev/null
  forget "test-container $1 label $2"
}
on_exit() {
  local status=$? owed
  set +e
  owed=$(grep -c . "$STATE")
  if [ "$status" = 0 ] && [ "$FAILURES" = 0 ] && [ "$owed" = 0 ]; then
    rm -rf -- "$WORK"
    [ ! -e "$WORK" ] && exit 0
    echo "SELF-TEST HELD: every check passed, but $WORK could not be removed"
    exit 1
  fi
  echo "SELF-TEST HELD: logs, outputs and the IDs below are kept in $WORK (nothing further removed)"
  if [ "$owed" != 0 ]; then
    echo "  still owed a cleanup (see $WORK/selftest-state.txt):"
    sed 's/^/    /' "$STATE"
    echo "  test containers: remove only after checking each exact ID and label shown;"
    echo "  rehearsal runs: ops/cutover_rehearsal.sh --cleanup <evidence>/manifest"
  fi
  [ "$status" != 0 ] || status=1
  exit "$status"
}
trap on_exit EXIT

echo "--- 0 normal run"
expect_eq "normal run exit status" 0 "$(status_of rehearse "$WORK/normal.txt" "$OLDI" "$NEWI" "$COMPOSE" "$SHA")"
note_run "$WORK/normal.txt"
expect_true "normal run reports a verified cleanup" grep -q '^REHEARSAL PASSED: every step passed and cleanup is verified' "$WORK/normal.txt"
NORMAL_RUN=$(sed -n 's/.*; run \(claude-reh-[0-9a-f]\{24\}\)$/\1/p' "$WORK/normal.txt" | tr -d '\r' | head -1)
expect_true "normal run ID found" test -n "$NORMAL_RUN"
expect_eq "normal run leaves no labelled resource" 0 "$(run_count "$NORMAL_RUN")"
expect_eq "normal run leaves no evidence folder" "" "$(evidence_of "$WORK/normal.txt")"

echo "--- 1 a value that is not a full image ID"
expect_eq "exit status" 1 "$(status_of rehearse "$WORK/neg1.txt" not-a-full-image-id "$NEWI" "$COMPOSE" "$SHA")"
note_run "$WORK/neg1.txt"
expect_true "refused before creating anything" grep -q '^FAIL image must be a full local ID' "$WORK/neg1.txt"
expect_eq "no labelled resource" 0 "$(run_count "$(run_of "$WORK/neg1.txt")")"
remove_dir "$(evidence_of "$WORK/neg1.txt")"

echo "--- 2 compose hash mismatch"
expect_eq "exit status" 1 "$(status_of rehearse "$WORK/neg2.txt" "$OLDI" "$NEWI" "$COMPOSE" "$(printf '0%.0s' $(seq 64))")"
note_run "$WORK/neg2.txt"
expect_true "refused before creating anything" grep -q '^FAIL compose file does not match' "$WORK/neg2.txt"
expect_eq "no labelled resource" 0 "$(run_count "$(run_of "$WORK/neg2.txt")")"
remove_dir "$(evidence_of "$WORK/neg2.txt")"

echo "--- 3 a mid-run failure holds every resource"
printf '{"schemaAllowlistVersion": 1, "objects": {"table:viewer_tasks": "%s"}}' "$(printf 'a%.0s' $(seq 64))" > "$WORK/wrong-allowlist.json"
expect_eq "exit status" 1 "$(status_of rehearse "$WORK/neg3.txt" "$OLDI" "$NEWI" "$COMPOSE" "$SHA" "$(w "$WORK/wrong-allowlist.json")")"
note_run "$WORK/neg3.txt"
expect_true "gate failure reported" grep -q '^FAIL gate failed against the reviewed allowlist' "$WORK/neg3.txt"
HELD_DIR=$(evidence_of "$WORK/neg3.txt")
HELD_RUN=$(run_of "$WORK/neg3.txt")
MANIFEST=$(w "$HELD_DIR/manifest")
RECORDED=$(grep -c -v '^run ' "$HELD_DIR/manifest" || true)
expect_eq "manifest records every held resource" 8 "$RECORDED"
expect_eq "every held resource is still present" 8 "$(run_count "$HELD_RUN")"

echo "--- 4 cleanup never removes a resource labelled for another run"
DECOY=$(docker create --label ltp.rehearsal=selftest-decoy --name "claude-selftest-decoy-$(python -c 'import secrets;print(secrets.token_hex(6))')" "$OLDI")
note "test-container $DECOY label selftest-decoy"
printf 'run %s\ncontainer %s\ncontainer %s\n' "$HELD_RUN" "$DECOY" "$(printf 'f%.0s' $(seq 64))" > "$WORK/decoy-manifest"
expect_eq "cleanup with a decoy exits non-zero" 1 "$(status_of bash ops/cutover_rehearsal.sh --cleanup "$(w "$WORK/decoy-manifest")")"
expect_eq "decoy still present" selftest-decoy "$(docker inspect -f '{{index .Config.Labels "ltp.rehearsal"}}' "$DECOY")"
expect_eq "held resources untouched by the decoy cleanup" 8 "$(run_count "$HELD_RUN")"
remove_test_container "$DECOY" selftest-decoy

echo "--- 5 a held run keeps its private synthetic context"
for f in runtime.env census.key manifest; do expect_true "kept $f" test -s "$HELD_DIR/$f"; done

echo "--- 6 an unrecorded resource with this run's label is preserved"
STRAY=$(docker create --label "ltp.rehearsal=$HELD_RUN" --name "$HELD_RUN-selftest-stray" "$OLDI")
note "test-container $STRAY label $HELD_RUN"
expect_eq "cleanup with an unrecorded resource exits non-zero" 1 \
  "$(status_of bash ops/cutover_rehearsal.sh --cleanup "$MANIFEST")"
expect_eq "stray still present" "$HELD_RUN" "$(docker inspect -f '{{index .Config.Labels "ltp.rehearsal"}}' "$STRAY")"
expect_eq "recorded resources removed" 0 "$(grep -c -v '^run ' "$HELD_DIR/manifest" || true)"
expect_eq "only the stray remains" 1 "$(run_count "$HELD_RUN")"
expect_true "private context kept while something remains" test -s "$HELD_DIR/runtime.env" -a -s "$HELD_DIR/census.key"

echo "--- 7 once the stray is dealt with, cleanup completes and only then drops the private context"
remove_test_container "$STRAY" "$HELD_RUN"
expect_eq "cleanup exit status" 0 "$(status_of bash ops/cutover_rehearsal.sh --cleanup "$MANIFEST")"
expect_eq "nothing left" 0 "$(run_count "$HELD_RUN")"
expect_true "private context deleted" test ! -e "$HELD_DIR/runtime.env" -a ! -e "$HELD_DIR/census.key"
remove_dir "$HELD_DIR"

echo "--- 8 passed steps with an incomplete cleanup exit 3"
set +e; CUTOVER_REHEARSAL_FAULT=unrecorded-resource bash ops/cutover_rehearsal.sh "$OLDI" "$NEWI" "$COMPOSE" "$SHA" \
  > "$WORK/neg8.txt" 2>&1; FAULT_STATUS=$?; set -e
note_run "$WORK/neg8.txt"
expect_eq "exit status" 3 "$FAULT_STATUS"
expect_true "CLEANUP INCOMPLETE reported" grep -q '^CLEANUP INCOMPLETE' "$WORK/neg8.txt"
expect_eq "PASSED never printed" 0 "$(grep -c '^REHEARSAL PASSED' "$WORK/neg8.txt" || true)"
FAULT_DIR=$(evidence_of "$WORK/neg8.txt")
FAULT_RUN=$(run_of "$WORK/neg8.txt")
INJECTED=$(docker inspect -f '{{.Id}}' "$FAULT_RUN-injected-fault")
note "test-container $INJECTED label $FAULT_RUN"
FAULT_RECORDED=$(grep -c -v '^run ' "$FAULT_DIR/manifest" || true)
expect_true "recorded resources held" test "$FAULT_RECORDED" -gt 0
expect_eq "every recorded resource and the injected one preserved" $((FAULT_RECORDED + 1)) "$(run_count "$FAULT_RUN")"
remove_test_container "$INJECTED" "$FAULT_RUN"
expect_eq "cleanup after removing the injected resource" 0 \
  "$(status_of bash ops/cutover_rehearsal.sh --cleanup "$(w "$FAULT_DIR/manifest")")"
expect_eq "nothing left" 0 "$(run_count "$FAULT_RUN")"
remove_dir "$FAULT_DIR"

echo
if [ "$FAILURES" = 0 ]; then echo "SELF-TEST PASSED"; exit 0; fi
echo "SELF-TEST FAILED: $FAILURES check(s)"
exit 1
