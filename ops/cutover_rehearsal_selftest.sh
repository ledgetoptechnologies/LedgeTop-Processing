#!/usr/bin/env bash
# Negative, cleanup-safety and exit-status checks for ops/cutover_rehearsal.sh.
#
# Usage: ops/cutover_rehearsal_selftest.sh D738_IMAGE_ID RELEASE_IMAGE_ID D738_COMPOSE_FILE WORK_DIR
#
# Runs on synthetic state only. Removes only the exact evidence folders its own
# runs print, the resources those runs record, and the test containers it
# creates itself; it never sweeps by name or prefix.
set -uo pipefail
export MSYS_NO_PATHCONV=1
OLDI=$1
NEWI=$2
COMPOSE=$3
S=$4
cd "$(dirname "$0")/.."
SHA=$(sha256sum "$COMPOSE" | cut -d' ' -f1)
evidence_of() { sed -n 's/.*synthetic context are kept in: //p' "$1" | tr -d '\r'; }
drop_evidence() { local dir; dir=$(evidence_of "$1"); [ -n "$dir" ] && [ -d "$dir" ] && rm -r -- "$dir"; }
counts() { echo "$(docker ps -a --filter label=ltp.rehearsal -q | wc -l) containers, $(docker volume ls -q --filter label=ltp.rehearsal | wc -l) volumes, $(docker network ls -q --filter label=ltp.rehearsal | wc -l) networks"; }

echo "--- 1 a value that is not a full image ID"
bash ops/cutover_rehearsal.sh not-a-full-image-id "$NEWI" "$COMPOSE" "$SHA" > "$S/neg1.txt" 2>&1
grep -E "^(FAIL|HELD)" "$S/neg1.txt"; drop_evidence "$S/neg1.txt"; echo "labelled resources: $(counts)"

echo "--- 2 compose hash mismatch"
bash ops/cutover_rehearsal.sh "$OLDI" "$NEWI" "$COMPOSE" "$(printf '0%.0s' $(seq 64))" > "$S/neg2.txt" 2>&1
grep -E "^(FAIL|HELD)" "$S/neg2.txt"; drop_evidence "$S/neg2.txt"; echo "labelled resources: $(counts)"

echo "--- 3 a mid-run failure holds every resource"
printf '{"schemaAllowlistVersion": 1, "objects": {"table:viewer_tasks": "%s"}}' "$(printf 'a%.0s' $(seq 64))" > "$S/wrong-allowlist.json"
bash ops/cutover_rehearsal.sh "$OLDI" "$NEWI" "$COMPOSE" "$SHA" "$S/wrong-allowlist.json" > "$S/neg3.txt" 2>&1
grep -E "^(FAIL|HELD)" "$S/neg3.txt"
HELD_DIR=$(evidence_of "$S/neg3.txt")
MANIFEST=$(cygpath -w "$HELD_DIR/manifest")
echo "held after failure: $(counts); manifest lists $(grep -c -v '^run ' "$HELD_DIR/manifest") resources"

echo "--- 4 cleanup never removes a resource labelled for another run"
DECOY=$(docker create --label ltp.rehearsal=someone-else --name "claude-decoy-$RANDOM" "$OLDI")
RUN=$(sed -n 's/^run //p' "$HELD_DIR/manifest")
printf 'run %s\ncontainer %s\ncontainer %s\n' "$RUN" "$DECOY" "$(printf 'f%.0s' $(seq 64))" > "$S/decoy-manifest"
bash ops/cutover_rehearsal.sh --cleanup "$(cygpath -w "$S/decoy-manifest")" 2>&1 | tail -3
docker inspect -f 'decoy {{.Name}} still present' "$DECOY"
docker rm "$DECOY" >/dev/null
rm -f -- "$S/decoy-manifest" "$S/decoy-manifest.resources"

echo "--- 5 a held run keeps its private synthetic context"
for f in runtime.env census.key manifest; do [ -s "$HELD_DIR/$f" ] && echo "kept $f" || echo "MISSING $f"; done

echo "--- 6 an unrecorded resource with this run's label is reported and preserved"
STRAY=$(docker create --label "ltp.rehearsal=$RUN" --name "claude-stray-$RANDOM" "$OLDI")   # stands in for an interrupted --rm helper
bash ops/cutover_rehearsal.sh --cleanup "$MANIFEST" 2>&1 | tail -3
docker inspect -f 'stray {{.Name}} still present' "$STRAY"
echo "manifest now lists $(grep -c -v '^run ' "$HELD_DIR/manifest") recorded resources; private context: $(ls "$HELD_DIR" | grep -c -E '^(runtime.env|census.key)$') files"

echo "--- 7 once the stray is dealt with, cleanup completes and only then drops the private context"
docker rm "$STRAY" >/dev/null
bash ops/cutover_rehearsal.sh --cleanup "$MANIFEST" 2>&1 | tail -2
echo "after cleanup: $(counts); private context: $(ls "$HELD_DIR" | grep -c -E '^(runtime.env|census.key)$') files"
rm -r -- "$HELD_DIR"; rm -f -- "$S/wrong-allowlist.json" "$S/neg1.txt" "$S/neg2.txt"

echo "--- 8 exit status: early failures keep their own status; incomplete cleanup after passed steps exits non-zero"
bash ops/cutover_rehearsal.sh not-a-full-image-id "$NEWI" "$COMPOSE" "$SHA" > "$S/neg8a.txt" 2>&1; echo "tag instead of ID exits $? (expected 1)"; drop_evidence "$S/neg8a.txt"
CUTOVER_REHEARSAL_FAULT=unrecorded-resource bash ops/cutover_rehearsal.sh "$OLDI" "$NEWI" "$COMPOSE" "$SHA" > "$S/neg8b.txt" 2>&1
echo "passed steps + injected unrecorded resource exits $? (expected 3)"
grep -E "^(FAULT INJECTED|UNRECORDED|CLEANUP INCOMPLETE|HELD|REHEARSAL)" "$S/neg8b.txt"
echo "held: $(counts)"
FAULT_DIR=$(evidence_of "$S/neg8b.txt")
FAULT_RUN=$(sed -n 's/^run //p' "$FAULT_DIR/manifest")
INJECTED=$(docker inspect -f '{{.Id}}' "$FAULT_RUN-injected-fault")
[ "$(docker inspect -f '{{index .Config.Labels "ltp.rehearsal"}}' "$INJECTED")" = "$FAULT_RUN" ] && docker rm "$INJECTED" >/dev/null
bash ops/cutover_rehearsal.sh --cleanup "$(cygpath -w "$FAULT_DIR/manifest")" > "$S/neg8c.txt" 2>&1; echo "cleanup after removing the injected resource exits $? (expected 0)"; tail -1 "$S/neg8c.txt"
echo "after cleanup: $(counts)"
rm -r -- "$FAULT_DIR"; rm -f -- "$S/neg8a.txt" "$S/neg8c.txt"
