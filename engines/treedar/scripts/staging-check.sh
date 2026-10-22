#!/usr/bin/env bash
# TreeDar staging acceptance check for a Linux Docker host.
#
# Builds the image from the current checkout, runs a small real-data area in a
# resource-capped, network-less container, and compares the result with the
# expectations in staging-check.json on the Staging share. Prints exactly one
# final line: "TREEDAR STAGING CHECK PASS ..." or "TREEDAR STAGING CHECK FAIL ...",
# and exits 0 or 1.
#
# Usage: scripts/staging-check.sh [STAGING_ROOT]
#   STAGING_ROOT defaults to "/mnt/Staging/TreeDar Test Datasets".
# The check config holds property coordinates, so it lives on the share and
# never in Git. Output goes to $STAGING_ROOT/Scratch/staging-check-<commit>/.
set -euo pipefail

ROOT="${1:-/mnt/Staging/TreeDar Test Datasets}"
CONFIG="$ROOT/staging-check.json"
ENGINE_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# Git Bash may disable automatic argument conversion for Docker bind mounts.
# Native Git and Docker then need the Windows spelling of this same path.
if command -v cygpath >/dev/null; then ENGINE_ROOT="$(cygpath -m "$ENGINE_ROOT")"; fi
COMMIT="$(git -C "$ENGINE_ROOT" rev-parse --short=12 HEAD)"
IMAGE="treedar:staging-check-$COMMIT"
NAME="treedar-staging-check-$COMMIT-$$"
OUT_REL="Scratch/staging-check-$COMMIT"

fail() { echo "TREEDAR STAGING CHECK FAIL commit=$COMMIT reason=$1"; exit 1; }

[ -f "$CONFIG" ] || fail "missing-config"
command -v docker >/dev/null || fail "docker-not-found"
command -v python3 >/dev/null || fail "python3-not-found"
if [ -n "$(git -C "$ENGINE_ROOT" status --porcelain --untracked-files=no -- .)" ]; then fail "uncommitted-changes"; fi

read_cfg() { python3 -c "import json,sys; c=json.load(open(sys.argv[1])); print(eval(sys.argv[2], {}, {'c': c}))" "$CONFIG" "$1"; }
INPUT="$(read_cfg 'c["input"]')"
BBOX="$(read_cfg '",".join(str(v) for v in c["bbox"])')"
ARGS="$(read_cfg '" ".join(c["args"])')"
MEM="$(read_cfg 'c["limits"]["memory"]')"
CPUS="$(read_cfg 'c["limits"]["cpus"]')"
MAX_S="$(read_cfg 'c["limits"]["max_seconds"]')"

docker build --quiet --tag "$IMAGE" "$ENGINE_ROOT" >/dev/null || fail "image-build"

mkdir -p "$ROOT/Scratch"
rm -rf "$ROOT/$OUT_REL"
# timeout only stops the docker client; remove the container itself on any exit.
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
START=$(date +%s)
# Run as the calling user so the output folder is writable on a freshly made Scratch.
# shellcheck disable=SC2086  # ARGS is a space-separated option list from the config
timeout "$MAX_S" docker run --rm --name "$NAME" --network none --memory "$MEM" --cpus "$CPUS" \
  --user "$(id -u):$(id -g)" -e HOME=/tmp -e MPLCONFIGDIR=/tmp \
  -v "$ROOT/Datasets:/data:ro" -v "$ROOT/Scratch:/runs" \
  "$IMAGE" run "/data/$INPUT" --out "/runs/staging-check-$COMMIT" --bbox "$BBOX" $ARGS \
  >"$ROOT/Scratch/staging-check-$COMMIT.log" 2>&1 || fail "pipeline-run (see Scratch/staging-check-$COMMIT.log)"
SECONDS_TAKEN=$(( $(date +%s) - START ))

python3 - "$CONFIG" "$ROOT/$OUT_REL/trees.csv" "$SECONDS_TAKEN" "$COMMIT" <<'PY'
import csv, json, statistics, sys
cfg, trees, secs, commit = json.load(open(sys.argv[1])), sys.argv[2], int(sys.argv[3]), sys.argv[4]
e = cfg["expect"]
h = [float(r["height_m"]) for r in csv.DictReader(open(trees, newline=""))]
n, med = len(h), (statistics.median(h) if h else float("nan"))
problems = []
if abs(n - e["n_trees"]) > e["n_trees_tolerance"]:
    problems.append(f"n_trees={n}(expected {e['n_trees']})")
if not abs(med - e["height_median_m"]) <= e["height_tolerance_m"]:
    problems.append(f"height_median={med:.2f}(expected {e['height_median_m']})")
status = "FAIL" if problems else "PASS"
detail = " ".join(problems) if problems else f"n_trees={n} height_median={med:.2f}"
print(f"TREEDAR STAGING CHECK {status} commit={commit} seconds={secs} {detail}")
sys.exit(1 if problems else 0)
PY
