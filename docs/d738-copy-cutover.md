# d738 copy-on-cutover procedure (draft, not executed, BLOCKED)

> **Status: blocked.** Fourteen review findings are listed under
> [Unresolved blockers](#unresolved-blockers). Each one is a mandatory stop
> condition until real enforcement **and** verification exist. U6 now has an
> implemented, synthetic-tested detector, but no live verification. Nothing in this
> document proves that the candidate is unreachable, that the old coordinator is
> fenced, or that rollback is lossless.
>
> **Enforcement now in source (branch `claude/cutover-enforcement`), tested on a
> Python 3.12 rebuild of the same sources, not on the exact images or live:**
>
> - **Authority:** U4. d738 is fenced by removing its container and holding its
>   own state lock. Release instances are fenced by a host-wide `flock` lease.
> - **Ingress:** U1–U3. The candidate runs with `--network none` until promotion.
> - **Production:** U5. Production is held after promotion.
>
> **Threat classification** (from the review direction of 2026-10-30): the
> release gates on *operational* events:
>
> - a normal reboot;
> - a Docker daemon restart;
> - an accidental restart of an existing container;
> - the runbook's own steps.
>
> It does not gate on a hostile, privileged Docker administrator deliberately
> creating an old container or writing arbitrary host data. That remains a
> documented trust boundary, not a release blocker. Beau has authorized the
> necessary operations.
>
> **Still open before release:**
>
> - **Daemon restart and reboot:** a real Docker daemon restart or host reboot
>   has not been rehearsed. That is an operational event, so it gates release.
>   It needs an isolated test daemon; the shared daemon is not restarted.
> - **Exact image:** an exact-image run of the rehearsal. PR46 head `22525b9`
>   has been built on .89 from the pinned Python 3.13 base, as
>   `sha256:62e441c4…2cfd`, and is under independent entrypoint testing.
> - **U5:** the production hold does not resume Viewer work, so true
>   quiescence and a coordinated Viewer recovery are required.
>
> **Trust boundary, not release blockers:**
>
> - deliberate privileged creation of d738 during an unfenced interval (U4);
> - host root and `docker exec` access (U2, U7);
> - volume ownership against a hostile administrator (U8).

This procedure would replace the live cluster coordinator with a frozen
release image. The live coordinator is:

- source `d738edb2`;
- image `sha256:a04271d0…5837`;
- container `9dcb112d…09ae`.

The design intent:

- The release image runs on a verified copy of the stopped state volume.
- The candidate starts with no published port, and is promoted only after a
  census gate.
- The original volume and container stay untouched, so rollback never starts
  d738 on a schema that the release migrated.

The steps below reduce exposure. They are **not** isolation or fencing
guarantees; see the blockers.

Beau has authorized this work. TrueNAS remains under his control, and nothing
here touches it, the Viewer provider settings, credentials or the engine
containers. This draft has not been run.

Every **stop condition** below is mandatory. Hitting one means: stop, keep
every artifact, and inspect. Never improvise a workaround.

## Why copy, and what remains unfenced

- **Downgrade:** on first start, the release `TaskStore` migrates the database:
  - `ALTER TABLE viewer_tasks ADD COLUMN option_evidence_required`;
  - creates `viewer_reservations`;
  - creates `viewer_option_evidence`.

  [viewer-option-evidence.md](viewer-option-evidence.md) says this is
  unqualified for downgrade. If d738 ran on a migrated database, it would
  ignore reservation capabilities and report `held` as failed. Running only the
  release on the copy avoids the downgrade.
- **Writes to the copy before promotion:** if the candidate published live
  ports before health checks, Viewer retries or other clients could write to
  the copy, and rollback would silently discard those writes. Removing published
  ports narrows that exposure, but does not eliminate it (blockers U1–U3).
- **The same authority twice:** the copy keeps authority `7131882e-…e8`, and
  `coordinator.lock` lives inside each volume, so the lock cannot prevent two
  coordinators. Host port binding does not either: Docker bridge peers reach
  container ports directly. Setting restart policy `no` and disconnecting the
  old container's networks guard only against automatic restarts. They do not
  fence a Docker administrator (blocker U4).

## Facts and assumptions

| Item | Source | Re-verified at |
|---|---|---|
| Live d738 has no Viewer tasks or files; both NodeODM task lists are empty | Codex read-only check, 2026-10-30 | steps 1, 4 |
| Cluster Docker filesystem has about 115 GiB free | Codex, 2026-10-30 | step 0 |
| On SIGTERM, d738 closes both listeners, then joins its non-daemon handlers, the dispatcher and the reference runtime, with no timeout. It exits 0, or 2 if any drain fails | `processing/app.py` at d738 | step 2 |
| Exit 0 proves **only** that handlers ended. It does not prove clients received responses or that requests succeeded; a socket failure leaves the outcome ambiguous | review, 2026-10-30 | steps 3, 4 (census and Viewer reconciliation) |
| Admission drain is unusable in both versions (`ADMISSION_DRAIN_QUALIFIED = False`) | `processing/config.py` | — |
| The coordinator's internal ports: the native/Viewer listener `PROCESSING_PORT` (default 8080) and the reference TLS listener (8443). The host publishes 3005, 18443 and loopback 18080 | reviewer; **confirm from `old-inspect.json`** | step 0 |
| Viewer's active work lives in `processing_jobs.status IN ('pending','leased')` and in `processing_attempts.status NOT IN ('cancelled','failed','published')` | Viewer `processingRepository.js` at 821186f4; **confirm against the deployed Viewer** | step 1 |
| The a042 image has `bash` and GNU `tar` (as the python-slim base does) | assumption | step 0 |

## End-to-end rehearsal

[ops/cutover_rehearsal.sh](../ops/cutover_rehearsal.sh) runs this runbook on
synthetic state:

```
ops/cutover_rehearsal.sh D738_IMAGE_ID RELEASE_IMAGE_ID D738_COMPOSE_FILE COMPOSE_SHA256 [REVIEWED_SCHEMA_ALLOWLIST]
ops/cutover_rehearsal.sh --cleanup MANIFEST
```

**Safety properties**

- **Exact identities:** the images must be full local IDs (`sha256:…`), and the
  compose file must match the given SHA-256 (d738's `compose.viewer.yaml` blob
  is `3f0b7ef4…d9ee`). Every `docker run` and `compose up` uses
  `--pull never`, and the override sets `pull_policy: never`.
- **Never the live ports:** the d738 compose file is never used as is. A
  generated override replaces its ports with two fresh, unused, random
  loopback ports (never 18080, 3005 or 18443). The rendered `compose config` is
  validated before anything starts: one service, the exact image, pull never,
  exactly those two ports on 127.0.0.1, and the run's own volume.
- **Exact ownership:**
  - the run ID is 96 random bits;
  - every container, volume and network it creates is labelled
    `ltp.rehearsal=<run>`, including short-lived `--rm` helpers;
  - long-lived resources are recorded by exact ID in a manifest.
- **Label census reconciliation:** on failure, before success cleanup and
  before `--cleanup`, the run's label is counted on the daemon and compared
  with the manifest. A labelled resource that is not recorded (an interrupted
  helper, a partial `compose up`) is reported as `UNRECORDED` and preserved,
  never removed. A failed census is reported as `UNCERTAIN`. Nothing is ever
  matched by name prefix.
- **Hold on failure:** a failed run removes nothing.
- **Sequential cleanup:** cleanup runs after success, or with `--cleanup
  MANIFEST`. It removes recorded resources one at a time, and **may stop
  partway**. Each removal is dropped from the manifest as it happens, so the
  manifest always lists what remains. Anything that cannot be inspected,
  carries another label, or fails to remove is held and reported, and
  `--cleanup` can simply be run again.
- **Exit status:**
  - `0` only when every step passed **and** cleanup is verified (the label
    census finds nothing left);
  - an earlier failure keeps its own non-zero status;
  - passed steps with an incomplete or unverifiable cleanup exit `3`
    (`CLEANUP INCOMPLETE`);
  - `--cleanup` exits non-zero while anything remains.

  `REHEARSAL PASSED` is printed only after cleanup is verified.
- **Private synthetic context:** `runtime.env`, `census.key` and `env-*` are
  kept with the evidence until a cleanup has removed every resource **and** the
  label census confirms none is left. Until then, a held run can be compared
  and reconstructed.
- **Exact answers:** every HTTP check asserts the exact status and body:
  - held production: `503 {"error":"production_admission_held"}`;
  - staging, production reads and reopened production: an authorized
    `404 {"error":"task_not_found"}` from the service, proving the request
    passed authentication and the hold.
- **Synthetic schema allowlist:** without a reviewed allowlist argument, the
  gate's schema allowlist is derived from the observed differences and the run
  says so explicitly. That is a **synthetic comparison exercise, not release
  schema qualification** (U9). With a reviewed allowlist, the gate must pass
  against it.
- **Every Docker call** goes through the pinned wrapper. The daemon is never
  restarted.

**Results on 2026-10-30.** The run used Python 3.12 rebuilds of the d738 and
PR46 sources, given by exact local ID; not the exact .89 images. The normal run
passed every step:

- **Steps 0–1:** exact-identity checks, the validated synthetic compose,
  capture and quiescence.
- **Step 2:** d738 drained with exit 0, its fence was verified, and the old
  container was confirmed absent on the recorded daemon.
- **Step 3:** the verified copy.
- **Steps 5–6:** the isolated candidate, then the gate. Only 5 schema objects
  and the exact marker changed, as a synthetic exercise.
- **Step 7:** promotion with production held, checked with the exact answers
  above.
- **Accidental restart:** `compose up` of the old definition was refused
  because the release holds its host ports.
- **Opening production:** the held phase wrote nothing.
- **Rollback:** d738 came back through compose with the same authority and
  schema, and its history kept.

It left no labelled resources. The negative checks also passed:

- a tag instead of an ID, or a compose hash mismatch, fails before anything is
  created;
- a gate failure holds all 8 recorded resources;
- `--cleanup` refuses a decoy labelled for another run, and an ID it cannot
  inspect;
- a held run keeps `runtime.env`, `census.key` and the manifest;
- a resource carrying the run's label but missing from the manifest is
  reported `UNRECORDED` and preserved. `--cleanup` removes the recorded ones,
  exits non-zero and keeps the private context;
- once that resource is dealt with, `--cleanup` completes. Only after the label
  census confirms nothing is left does it delete the private context;
- **exit status:** a value that is not an image ID exits `1`. A run whose steps
  all pass, but with an unrecorded labelled resource injected before cleanup
  (`CUTOVER_REHEARSAL_FAULT=unrecorded-resource`, test only), exits `3` with
  `CLEANUP INCOMPLETE`, preserves the injected resource and holds the rest. The
  normal run exits `0`.

These checks are in
[ops/cutover_rehearsal_selftest.sh](../ops/cutover_rehearsal_selftest.sh):

```
ops/cutover_rehearsal_selftest.sh D738_IMAGE_ID RELEASE_IMAGE_ID D738_COMPOSE_FILE
```

- **Real assertions:** every check is an assertion. 36 of them cover the
  normal run and checks 1–8. The self-test exits `0` only if all pass, and `1`
  with a `FAIL` line for each violated one. It runs under
  `set -euo pipefail`, and exit statuses are captured explicitly.
- **Pinned Docker:** every Docker call goes through the same pinned wrapper as
  the runbook.
- **Portable paths:** host paths come from one helper, `cygpath` under Git Bash
  and the plain path elsewhere.
- **Evidence removal checked:** it removes only the exact evidence folders,
  recorded resources and test containers of its own runs, and checks each
  removal.
- **Verified rehearsal cleanup:** the rehearsal also reports a complete
  cleanup only after its evidence folder, or the private context under
  `--cleanup`, is verifiably gone.
- **Lint:** a test checks that both scripts define `docker` as the pinned
  wrapper before any use, that no call bypasses it, and that `cygpath` appears
  only in the path helper.

On 2026-10-30, locally, the self-test passed all 36 checks with no labelled
resources left. With the compose file's live-port line removed as a deliberate
violation, it exited `1` and reported each broken expectation.

**Not rehearsed:** a Docker daemon restart or host reboot (the shared daemon was
deliberately left alone), and the exact images.

## Helpers (all read-only, all in this PR)

- [ops/state_census.py](../ops/state_census.py) takes a census of a
  **stopped** state directory, and compares two such censuses.
  - `keygen` writes a fresh per-run secret key, mode `0600`, refusing to
    overwrite an existing file.
  - `census` reads the key on stdin. For **every** entry it records kind, mode,
    owner, group, whole-second mtime, size and contents. Every client-derived
    value is keyed with HMAC-SHA256 under the run key:
    - path identifiers;
    - file contents;
    - per-table row digests.

    So neither a guessed name such as `DJI_0001.JPG` nor a known photo or row
    can be matched against the output. Schema-object hashes stay plain SHA-256,
    because schema SQL is public source and its exact expected hash must be
    reviewable. Sizes and row counts are printed in clear. It also records:
    - partial `.incoming-*` uploads and spool entries;
    - SQLite, opened `immutable=1`: quick_check, active jobs and Viewer tasks,
      the authority, and any sidecar files.

    It **rejects** symlinks, special files, hard-linked files and unreadable
    entries, exiting 3. It never prints names, addresses or row contents.
  - `compare` reads the key on stdin and diffs two census documents entry by
    entry. Any difference not covered by an explicit, narrow allowance fails
    (exit 1):
    - `--allow-path PATH:FIELD[,FIELD]`: only the named fields of an entry that
      exists on both sides may differ. Adding or removing an entry, or changing
      its kind, is never allowable.
    - `--allow-object TYPE:NAME=SHA256`: that one schema object may change only
      to the exact expected SQL hash. The allowance is also an assertion: if the
      object does not end up at that hash, the comparison fails.
    - `--allow-table NAME`: the content of a table that exists on **both** sides
      may change.
    - `--allow-new-table NAME`: a table may appear, but only if empty. No other
      allowance relaxes that.

    Differences are reported only as counts and opaque IDs. Every result states
    that equal censuses show equality at two moments, not that no transient
    write occurred in between.
- [processing/authority_lease.py](../processing/authority_lease.py): the release
  coordinator takes a host-wide `flock` lease on `<authority>.lease`, in a
  bind-mounted directory (`PROCESSING_AUTHORITY_LEASE_DIR`), before it writes
  state.
  - A second release instance with the same authority refuses to start.
  - State that has been leased refuses to start without a lease.
  - **d738 has no lease code and ignores the lease.**
- [ops/state_fence.py](../ops/state_fence.py): holds d738's own
  `coordinator.lock` on retained d738 state, read-only, so d738 refuses to start
  on that state.
  - Run one holder per volume.
  - `--verify`, run inside the holder, proves from `/proc/locks` that the live
    holder owns the lock now.
- [ops/container_absent.py](../ops/container_absent.py) proves that a
  container is absent. It exits 0 only when all of these hold:
  - a successful daemon census;
  - an explicit "No such container";
  - one pinned local daemon: `--host` must be a local unix socket, every
    `DOCKER_*` variable is removed, `DOCKER_CONFIG` points at a fresh empty
    directory, and `--expect-daemon-id` matches the ID recorded in step 0.

  Any daemon error, inspect error or other daemon exits 2, never 0.
- [ops/pinned_docker.py](../ops/pinned_docker.py): the step 0 `docker`
  function runs **every** Docker operation in this runbook through it:
  inspect, update, stop, run, exec, rm, volume and network commands, the census
  containers and the container-spec argv. It pins `--host` to the local socket,
  removes every `DOCKER_*` variable, uses a fresh empty `DOCKER_CONFIG`, and
  re-checks the recorded daemon ID before each command. It refuses global CLI
  options such as `--context` or `-H`, so a command cannot re-point itself.
  A lint test checks that no runbook command bypasses the function.
- `PROCESSING_VIEWER_PRODUCTION_ADMISSION=held` (in `processing/viewer_http.py`)
  refuses every production Viewer POST with 503. Staging is unaffected.
- [ops/cutover_fence_test.sh](../ops/cutover_fence_test.sh): the isolated
  container test of all of the above, run on synthetic state with a given d738
  image and release image.
- [ops/netns_sockets.py](../ops/netns_sockets.py): run inside a container with
  `docker exec -i`. It counts TCP listeners and peers per local port in that
  container's own network namespace, which covers bridge, loopback and
  published access alike.
- [ops/coordinator_container_spec.py](../ops/coordinator_container_spec.py):
  derives the candidate's `docker create` argv from saved `docker inspect`
  output.
  - It carries reviewed HostConfig fields exactly.
  - It replaces only the image, volume, name, network, ports and restart
    policy.
  - It **refuses** any other non-default setting: bind mounts, compose labels,
    command or user overrides, static network addresses, and so on.
  - It writes the environment to a fresh `0600` file and prints key names only.

Copy the three helpers from the frozen release commit to the host's private
cutover folder, and verify them against that commit by SHA-256.

## Step 0 — private capture, read-only

All commands run on 192.168.50.89 in **bash**:

```sh
set -euo pipefail
umask 077                                  # before any credential-bearing file exists
OLD=9dcb112da75cd9663f87d63fed422979dd9216f46b7035f809673a2d900d09ae
IMG_OLD=sha256:a04271d08115481d5dd3a8a171b9b609567b823c7d5ea8af34b56b8915db5837
IMG_NEW=<digest of the locally built image for the frozen release commit>
RUN=$(date -u +%Y%m%dT%H%M%SZ)-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
E=$HOME/.local/share/ledgetop-processing-prototype/cutover-$RUN
mkdir -m 700 "$E"                          # fails if it already exists
cp state_census.py netns_sockets.py coordinator_container_spec.py state_fence.py container_absent.py pinned_docker.py "$E"/
# Pin every Docker call that proves anything to the local daemon, recorded once:
DOCKER_LOCAL=unix:///var/run/docker.sock
DAEMON_ID=$(python3 "$E/pinned_docker.py" --host "$DOCKER_LOCAL" -- info --format '{{.ID}}')
test -n "$DAEMON_ID"; echo "$DAEMON_ID" > "$E/daemon-id.txt"
# From here on, EVERY Docker operation in this runbook goes through this function:
# pinned --host, all DOCKER_* removed, fresh empty DOCKER_CONFIG, and the recorded
# daemon ID re-checked before each command. Global options are refused.
docker() { python3 "$E/pinned_docker.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID" -- "$@"; }
ABSENT=(python3 "$E/container_absent.py" --host "$DOCKER_LOCAL" --expect-daemon-id "$DAEMON_ID")
docker inspect "$OLD"     > "$E/old-inspect.json"
docker image inspect "$IMG_OLD" > "$E/old-image.json"
docker image inspect "$IMG_NEW" > "$E/new-image.json"
docker inspect -f '{{.State.Status}} {{.RestartCount}} {{.HostConfig.RestartPolicy.Name}} {{.Image}}' "$OLD"
docker inspect -f '{{range .Mounts}}{{.Type}} {{.Name}} {{.Destination}} {{.RW}}{{"\n"}}{{end}}' "$OLD"
docker inspect -f '{{json .HostConfig.PortBindings}}' "$OLD"
docker inspect -f '{{range $n, $_ := .NetworkSettings.Networks}}{{$n}}{{"\n"}}{{end}}' "$OLD" > "$E/old-networks.txt"
VOL=$(docker inspect -f '{{range .Mounts}}{{.Name}}{{end}}' "$OLD")
docker system df -v > "$E/df.txt"; df -B1 "$(docker info -f '{{.DockerRootDir}}')"
```

**Stop conditions:**

- The image is not `IMG_OLD`.
- There is not exactly one writable named volume.
- The port bindings differ from the facts table.
- Free space is less than 3× the volume size plus the new image.
- `coordinator_container_spec.py --mode isolated` refuses the configuration.
  Run it now as a dry check, writing its env file to `$E/env-dry`, then delete
  that file with `shred -u`.

### Every owner and writer of the volume and its mountpoint

```sh
docker ps -a --filter volume="$VOL" --format '{{.ID}}' > "$E/volume-users.txt"   # must be exactly $OLD
MP=$(docker volume inspect -f '{{.Mountpoint}}' "$VOL")
for c in $(docker ps -aq); do docker inspect -f '{{.Id}} {{range .Mounts}}{{.Source}} {{end}}' "$c"; done \
  | grep -F -- "$MP" > "$E/mountpoint-users.txt" || true                          # must list only $OLD
set +e; sudo -n fuser -vm "$MP" 2> "$E/fuser.txt"; FUSER=$?; set -e
echo "fuser exit $FUSER" >> "$E/fuser.txt"   # 0: users listed, 1: none found, anything else: the check failed
test "$FUSER" -le 1                         # never masked; see blocker U7
```

**Stop conditions:**

- Any container other than `$OLD` uses the volume or its mountpoint.
- Any host process other than `$OLD`'s holds the mountpoint.
- `sudo -n` is unavailable. In that case the host-writer check is
  **unproven**, and this is a blocker until Beau provides an equivalent root
  check.

## Step 1 — prove every submission route is quiet (exact mechanisms)

An idle queue is necessary but not sufficient. Each route below needs its own
evidence, saved in `$E`. All checks are read-only.

| # | Route | Exact mechanism | Pass |
|---|---|---|---|
| R1 | Viewer production and staging | The Viewer owner runs `python3 ops/viewer_quiescence.py <viewer.db>` as the Viewer's user. It opens the live WAL database read-only (`--snapshot` only for a backup-API copy) and prints counts only. Tested against the Viewer's own migrations (821186f4), including uncheckpointed WAL writes | Exit `0` and `"quiet": true`: no `pending`/`leased` processing job (retries scheduled later included), no attempt from `pending` through `ingesting`, and, counted conservatively, no `derivatives` attempt or pending/leased derivative job. Exit `1` means work is in flight; exit `2` means the schema or a status is unexpected: stop |
| R2 | Requests inside d738 | `docker exec -i "$OLD" python - < "$E/netns_sockets.py"`, three times 10 s apart | `activeInbound` is `0` every time (TIME_WAIT is a connection already answered and closed) |
| R3 | Host-side listeners | `ss -tanp '( sport = :3005 or sport = :18443 or sport = :18080 )'`, plus every other `HostPort` in `old-inspect.json` | Only `LISTEN` rows, held by `docker-proxy` |
| R4 | Bridge peers | `docker network inspect <each network in old-networks.txt>` | The members list matches what you expect; every member other than `$OLD` is identified as a non-submitter (for example an engine container) |
| R5 | Reference workers and broker, both PCs | `pgrep -af 'reference_worker_daemon\|nas_broker\|reference_joint_driver'` and `systemctl --user list-units --all 'ledgetop*'` on .89 and main | No process; no active unit |
| R6 | Operator and synthetic `/v1/prototype/*` | `pgrep -af 'processingctl\|update_reference_coordinator\|run_coordinator_update_bridge'` | No process |
| R7 | Direct engine submissions | `docker exec -i "$OLD" python -` reading `LOCAL_NODE_URL`/`LOCAL_NODE_TOKEN` and `MAIN_NODE_URL`/`MAIN_NODE_TOKEN` from its own environment, then requesting `/task/list?token=…` and **printing only the counts**. Also `docker ps -a` on both PCs | Both counts are 0. The legacy ClusterODX container is `exited`. No unknown NodeODM-client container exists |
| R8 | Updaters and maintenance | `python3 <installed update_service.py> status --config <installed config>` for each installed updater, and `systemctl --user list-timers --all` on both PCs | No lock or in-flight operation; no timer due within the window; no updater targets `$OLD` |
| R9 | The isolated .88 pilot | The Viewer owner confirms read-only that the pilot's provider origin is not `192.168.50.89` | Confirmed |

**Stop condition:** any route fails, or its evidence cannot be produced.

## Step 2 — stop d738, then fence it with its own state lock

**d738 does not take the new authority lease.** It has no lease code; the
isolated test shows d738 and a leased release running together on one
authority. The lease therefore fences only release instances, and d738 needs
an independent fence: its own `coordinator.lock`.

d738 takes `flock` on `<state>/coordinator.lock` before it writes its database.
It refuses to start ("prototype state already owned") while another process
holds that lock. **One holder container per volume** keeps that lock on every
retained d738 state. Each holder has `--network none`, a read-only mount and
`--restart always`. Holders are only ever **added**, never recreated to change
their volumes, so a fenced volume is never released to make room for another.

```sh
fence() {   # name volume: start a holder for exactly one volume
  docker run -d --name "ltp-d738-fence-$1-$RUN" --network none --init --read-only --user 10001:10001 \
    --cap-drop ALL --security-opt no-new-privileges --restart always \
    --mount "type=volume,source=$2,destination=/fence/state,readonly" \
    --mount "type=bind,source=$E/state_fence.py,destination=/state_fence.py,readonly" \
    --entrypoint python "$IMG_NEW" -B /state_fence.py /fence/state
}
verify_fence() {   # name: /proc/locks proves the CURRENT holder owns the lock now
  docker exec "ltp-d738-fence-$1-$RUN" python -B /state_fence.py --verify /fence/state | grep -q '"verified": true'
}
docker update --restart=no "$OLD"          # BEFORE stop: no restart window
docker stop -t 3600 "$OLD"                 # never the default 10 s
docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.State.FinishedAt}}' "$OLD"
fence original "$VOL"
verify_fence original
docker rm "$OLD"                           # nothing left to restart or reconnect; old-inspect.json + IMG_OLD recreate it
"${ABSENT[@]}" "$OLD"   # confirmed absent on the recorded daemon: census plus explicit not-found
```

**Stop conditions:**

- The status is not `exited`, the exit code is not `0`, or `OOMKilled` is
  `true`.
- The stop took longer than a few seconds. That means a request was in flight,
  contradicting R2.
- `verify_fence` fails. Historical log lines are not accepted as proof.
  `--verify` reads `/proc/locks` and passes only if exactly one live holder
  process owns an exclusive flock on that very file. It fails if the holder
  died, or if someone else, such as a running d738, holds the lock.

Removing the old container means that neither a daemon restart nor a
reconnect has anything to act on. Recreating d738 deliberately, whether by
compose or `docker run`, on any fenced state is refused by d738's own lock
check. In step 3, `VERIFY_VOL` gets its own holder **immediately** after
extraction (`fence verify "$VERIFY_VOL"; verify_fence verify`). The original's
holder is never touched. Step 3 copies through read-only mounts, so the holders
do not block it.

**Unfenced intervals that remain** (d738's container no longer exists in any
of them, so only a d738 created deliberately at that moment could use one):

- from a holder's death until Docker restarts it;
- from `VERIFY_VOL`'s extraction until its holder starts;
- from `NEW_VOL`'s extraction until the candidate takes its own state lock in
  step 5;
- after a real Docker daemon restart, until the holders are running again
  (not tested).

Run `verify_fence` for every holder again before steps 5, 6 and 7.

## Step 3 — verified backup and copy

The archive is the single source for both the verification volume and the copy.
A copy that is extracted from the archive and census-matches it proves the
archive can be restored.

```sh
NEW_VOL=ltp-state-release-$RUN
VERIFY_VOL=ltp-state-verify-$RUN
for v in "$NEW_VOL" "$VERIFY_VOL"; do
  if docker volume inspect "$v" >/dev/null 2>&1; then echo "volume exists: $v"; exit 1; fi
done
python3 "$E/state_census.py" keygen "$E/census.key"      # one key for every census in this run
# Key goes in on stdin: never in argv, the environment or a mounted file. Exit 3 (rejected type) aborts under set -e.
census() { docker run --rm -i --network=none --read-only --user 10001:10001 --entrypoint python \
  -v "$1:/src:ro" -v "$E/state_census.py:/c.py:ro" "$IMG_OLD" -B /c.py census /src < "$E/census.key"; }
same() { python3 "$E/state_census.py" compare "$@" < "$E/census.key" > "$E/compare-$(date -u +%s%N).json"; }
census "$VOL" > "$E/census-original.json"
# archive streamed to a host file: no pipeline, so docker's exit status is the archive's
docker run --rm --network=none --read-only --user 10001:10001 -v "$VOL:/src:ro" --entrypoint tar \
  "$IMG_OLD" --numeric-owner -C /src -cpf - . > "$E/state-original.tar"
python3 -c 'import os,sys;[os.fsync(os.open(p,os.O_RDONLY)) for p in sys.argv[1:]]' "$E/state-original.tar" "$E"
sha256sum "$E/state-original.tar" > "$E/state-original.tar.sha256"
for v in "$VERIFY_VOL" "$NEW_VOL"; do
  docker volume create --label ltp.cutover="$RUN" "$v" >/dev/null
  test "$(census "$v" | python3 -c 'import json,sys;print(json.load(sys.stdin)["tree"]["entries"])')" = 1   # root only
  sha256sum -c "$E/state-original.tar.sha256"
  docker run --rm -i --network=none --user 0:0 -v "$v:/dst" --entrypoint tar "$IMG_OLD" \
    --numeric-owner --same-owner -C /dst -xpf - < "$E/state-original.tar"
  census "$v" > "$E/census-$v.json"
done
census "$VOL" > "$E/census-original-after.json"
for c in "census-$VERIFY_VOL" "census-$NEW_VOL" census-original-after; do
  same "$E/census-original.json" "$E/$c.json"            # no allowances: every file, every field
done
```

`compare` must exit 0 **with no allowances** for the verification volume, the
copy and the original-after census. That means every regular file's contents,
mode, owner, group, size and mtime match, and so does every directory's
metadata, including the volume root. The original census must also show:

- `quickCheck` is `ok`;
- `active.jobs` is 0 and `active.viewerTasks` is 0;
- `incomingPartials` and `spoolEntries` are 0;
- every `rejected` counter is 0;
- `sidecars` is empty.

**Stop conditions:**

- Any `compare` failure.
- `census` exits 3. Symlinks, special files, hard links and unreadable entries
  are rejected outright, not handled.

The identical before/after censuses of the original prove that copying never
wrote to it. This census is also the state-level reconciliation of R1, R5 and
R6 **after** the listeners closed, which matters because exit 0 alone does not
prove a request outcome. Keep `VERIFY_VOL` and the archive until Beau accepts
the release.

## Step 4 — client-side reconciliation and direct-engine recheck

1. Repeat R1. Viewer must still have no active job or attempt. If Viewer
   recorded a request failure during the stop window, the census (zero tasks)
   is the coordinator's side of that request. Record both sides; do not retry.
2. Repeat R7 and R8. R7 has no running coordinator to `exec` into, so run its
   script in a short-lived container: old image, `--rm`, `--read-only`, on the
   engine network only, environment from `$E/env-dry`, regenerated in mode
   `0600` and shredded afterwards.

**Stop condition:** any non-zero count. A task now on an engine came from
outside the coordinator.

## Step 5 — start the candidate with `--network none` and the authority lease

```sh
LEASE_HOST_DIR=$HOME/.local/share/ledgetop-processing-prototype/authority-lease   # one fixed host directory for every release instance
install -d -m 700 "$LEASE_HOST_DIR"; sudo -n chown 10001:10001 "$LEASE_HOST_DIR"   # see U10 if sudo is unavailable
python3 "$E/coordinator_container_spec.py" --container "$E/old-inspect.json" \
  --old-image "$E/old-image.json" --new-image "$E/new-image.json" --mode isolated --network none \
  --volume "$NEW_VOL" --name "ltp-coordinator-candidate-$RUN" --lease-dir "$LEASE_HOST_DIR" \
  --accept-compose-source --env-out "$E/env-candidate" > "$E/candidate-spec.json"
# review candidate-spec.json (key names only), then run its argv exactly:
mapfile -d '' -t ARGV < <(python3 -c 'import json,sys;a=json.load(open(sys.argv[1]))["argv"];assert a[0]=="docker";sys.stdout.write("\0".join(a[1:]))' "$E/candidate-spec.json")
[ "${#ARGV[@]}" -gt 1 ]
docker "${ARGV[@]}"                        # the pinned wrapper, never a bare docker
CAND=ltp-coordinator-candidate-$RUN
docker start "$CAND"
```

What is enforced here, and was shown in the isolated test:

- **No network path to the candidate.** With `--network none` its namespace has
  only `lo`. There is no bridge interface or address, so neither the host, nor
  another container, nor a Viewer retry can reach it. The spec script refuses
  isolated mode on any other network.
- **One leased release instance.** The candidate takes the authority lease
  before writing state, and records in the state that a lease is required.
  Any other release instance, on any copy with this authority and the same
  lease directory, refuses with `authority_lease_held`. Starting this state
  without a lease refuses with `authority_lease_configuration_required`.
- **d738 cannot run beside it.** On the original and verification volumes, the
  step 2 fence refuses it. On `NEW_VOL`, the candidate's own `coordinator.lock`
  refuses it.

Not enforced: anyone with `docker exec` or root on the host can still reach
the candidate or its volume (U2, U7). The candidate cannot reach its engines
while isolated, so `/options` checks wait for promotion (U11).

Candidate health and reachability checks run **inside** the candidate, using
its own environment. GET only; no POST:

```sh
docker exec -i "$CAND" python - <<'EOF'
import json, os, urllib.request
base = 'http://127.0.0.1:' + os.environ.get('PROCESSING_PORT', '8080')
def get(path, token=None):
    request = urllib.request.Request(base + path, headers={'Authorization': 'Bearer ' + token} if token else {})
    with urllib.request.urlopen(request, timeout=10) as response:
        return response.status, json.load(response)
status, health = get('/healthz'); print('healthz', status, health.get('state'))
for role, key in (('production', 'PROCESSING_VIEWER_TOKEN'), ('staging', 'PROCESSING_VIEWER_STAGING_TOKEN')):
    status, options = get('/options', os.environ[key]); print(role, 'options', status, len(options))
status, authority = get('/v1/prototype/authority', os.environ['PROCESSING_PROTOTYPE_OBSERVER_TOKEN'])
print('authority', status, authority.get('authorityId'))
EOF
docker exec -i "$CAND" python - < "$E/netns_sockets.py"   # only this check's own connections
```

Also run R7 through `docker exec -i "$CAND"`. The candidate must have
dispatched nothing.

**Stop conditions:**

- Any isolation proof fails.
- `/healthz` is not ready.
- The option count or hash differs from the engines' current catalog.
- The authority is not `7131882e-…e8`.

Assumptions:

- The headers and authority field above follow the coordinator's documented
  API. Adjust them to the exact route contracts before running.
- The reference TLS listener is only checked for being alive, through
  `/healthz`.

## Step 6 — promotion gate: per-file census comparison

```sh
docker stop -t 3600 "$CAND"; docker inspect -f '{{.State.ExitCode}}' "$CAND"     # must be 0
census "$NEW_VOL" > "$E/census-candidate-isolated.json"
same "$E/census-$NEW_VOL.json" "$E/census-candidate-isolated.json" \
  --allow-path .:mtime --allow-path jobs.sqlite3:bytes,content,mtime \
  --allow-new-table viewer_reservations --allow-new-table viewer_option_evidence \
  --allow-lease-marker \
  --schema-allowlist "$E/startup-schema-allowlist.json"   # schema-only file; the loader refuses anything broader
```

Every other allowance is a fixed literal in this command. Nothing is
shell-substituted into the argv.

The allowances are deliberately narrow:

- **Files:**
  - The volume root may change its **mtime only**.
  - The database file may change only its **size, contents and mtime**. Its
    mode, owner and group must stay the same, and it may not disappear.
  - Every other entry, including every `viewer-tasks` photo and archive, must
    match in contents and every metadata field.
- **Database:**
  - Every schema-object allowance names the **exact** expected SQL hash, as
    `TYPE:NAME=SHA256`. This covers `table:viewer_tasks` after the
    `ALTER TABLE`, the two new tables, and their automatic indexes.
  - These come from `startup-schema-allowlist.json`, the reviewed output of
    the U9 rehearsal, because SQLite rewrites the stored SQL on `ALTER TABLE`.
    The loader accepts only
    `{schemaAllowlistVersion: 1, objects: {TYPE:NAME: SHA256}}` and refuses
    any other key, value or shape. The file therefore cannot add path, table
    or new-table allowances, or any other option.
  - Any other change to those objects, or to any other object, fails.
  - The two new tables must be **empty**; nothing relaxes that.
  - `--allow-lease-marker` permits exactly one more thing: the authority-lease
    marker table `coordinator_authority_lease`. It must appear with its exact
    DDL and exactly its single `(1, 1)` row, checked by keyed digest. The flag
    also **requires** the marker afterwards, so a candidate that did not take
    the lease fails the gate. No other table, row or object is relaxed.
  - In the isolated container test, a real release startup (test image,
    synthetic state, no reference configuration) changed exactly these schema
    objects: `table:viewer_tasks`, `table:viewer_reservations`,
    `table:viewer_option_evidence`,
    `index:sqlite_autoindex_viewer_reservations_1` and
    `index:sqlite_autoindex_viewer_option_evidence_1`. With the marker, it
    passed the gate with exact hashes and changed no file except the database
    and the root mtime. The live configuration, including any reference
    runtime, can write more, and must be rehearsed with the exact image (U9).
  - Every existing table must keep an identical keyed row digest. Whole-row
    digests change when a column is added, so the gate passes only because
    `viewer_tasks` is empty on the live volume (step 1). A migration over
    non-empty rows fails conservatively and needs its own review.
  - quick_check, authority, active counts and sidecars must be unchanged.

**The startup schema allowlist does not exist yet (U9).** It must come from a
reviewed rehearsal:

1. Start `IMG_NEW` isolated on a synthetic copy of this volume shape, then stop
   it and take a census.
2. Run `compare` with only the fixed literal allowances above. Every remaining
   difference must be a schema object the release is expected to create or
   alter.
3. Generate the file from the rehearsal census, naming each such object
   explicitly:

   ```sh
   python3 state_census.py schema-allowlist rehearsal-candidate.json \
     --object table:viewer_tasks --object table:viewer_reservations \
     --object table:viewer_option_evidence --object index:<each automatic index> \
     --output startup-schema-allowlist.json      # mode 0600; refuses to overwrite
   ```

   The generator refuses an object that is missing from the census, named
   twice, or malformed. It re-validates its own output through the same strict
   loader.
4. Review the file, then copy it into `$E` by SHA-256.

If startup also writes **rows** (for example reference recovery records), the
schema allowlist cannot express that, by design. The gate then fails until the
procedure gains a separately reviewed, fixed literal allowance. Until all of
this is done, the gate cannot pass.

**Stop condition:** `compare` exits non-zero. Any unallowed difference means a
client reached the candidate, or startup does more than was reviewed.

This gate detects any unallowed difference that is still present when the
candidate stops. Equal censuses show equality at two moments, **not** the
absence of transient writes: something written and then reverted in between is
invisible. It does not prevent writes (U1, U2), and its limits are listed under
U6.

## Step 7 — promote

```sh
"${ABSENT[@]}" "$OLD"                              # exit 0 only on the recorded daemon, with a successful census AND an explicit
                                                   # "No such container"; a daemon or inspect failure exits 2 and stops here
verify_fence original; verify_fence verify        # current holders own their locks, from /proc/locks
census "$NEW_VOL" > "$E/census-before-promotion.json"
same "$E/census-candidate-isolated.json" "$E/census-before-promotion.json"   # unchanged since the gate, no allowances
docker rm "$CAND"; shred -u "$E/env-candidate"
python3 "$E/coordinator_container_spec.py" --container "$E/old-inspect.json" \
  --old-image "$E/old-image.json" --new-image "$E/new-image.json" --mode promoted \
  --volume "$NEW_VOL" --name "ltp-coordinator-$RUN" --lease-dir "$LEASE_HOST_DIR" \
  --production-admission held --accept-compose-source \
  --env-out "$E/env-promoted" > "$E/promoted-spec.json"
mapfile -d '' -t ARGV < <(python3 -c 'import json,sys;a=json.load(open(sys.argv[1]))["argv"];assert a[0]=="docker";sys.stdout.write("\0".join(a[1:]))' "$E/promoted-spec.json")
[ "${#ARGV[@]}" -gt 1 ]
docker "${ARGV[@]}"                        # the pinned wrapper, never a bare docker
docker start "ltp-coordinator-$RUN"; shred -u "$E/env-promoted"
```

**Promotion is the end of clean rollback.** From here on, any admission — a
staging verification task, or production once opened — exists only on
`NEW_VOL`.

The promoted container starts with **production admission held**:

- Every production Viewer POST is refused with `503 production_admission_held`
  before any body is read or any service is called. That covers init, upload,
  commit, cancel, remove, reserve and reservation cancel.
- Production reads and all staging requests are admitted.

So the staging-only Viewer photo job can run through to archive ingestion
**and derivatives** while production cannot change state.

**The hold is a backstop, not a way to pause production work.** Viewer's
isolated tests (12 cases of 503 or connection reset during init, upload and
commit, with and without a reservation) show what happens when a production
request hits the hold:

- Viewer keeps its source data and a single attempt, with no duplicate and no
  removal.
- But it does **not** resume on its own:
  - an init refused with 503 and then answered with 404 stays
    "observation unavailable";
  - an ambiguous upload needs inspection;
  - a refused commit may stay held.
- A generic 503 or connection reset does not tell the client that no body was
  read. The coordinator's refusal happens before the body is read (unit test),
  but the client cannot verify that.

So the cutover still requires **true quiescence**: R1 shows no active
production Viewer job or attempt, and no production submission happens until
production is opened. Any production job that does hit the hold needs a
**coordinated recovery** with Viewer. Removing the hold does not resume it.

The published host ports also act as a fence against d738: a d738 container
that publishes the same host port cannot start while the release holds it
(isolated test).

The published host ports also act as a fence against d738: a d738 container
that publishes the same host port cannot start while the release holds it
(isolated test).

**Opening production**, once the staging run passes and Beau accepts it:

1. Stop the promoted container (exit 0). Its lease is released.
2. Recreate it with `--production-admission open`, the same lease directory
   and the same volume.
3. Start it.

The lease guarantees that the held and open containers can never run at the
same time.

## Step 8 — rollback

- **Before promotion (any failure in steps 5–6):**
  1. Stop and remove the candidate.
  2. Stop and remove the d738 holder for the original volume
     (`docker rm -f ltp-d738-fence-original-$RUN`); d738 cannot start while it
     holds the lock. Keep the `VERIFY_VOL` holder.
  3. Recreate d738 from `old-inspect.json` and `IMG_OLD` on the original
     volume, with its original networks, ports and restart policy, through the
     same compose definition it came from. Then start it.

  The release's lease file is left in place. d738 ignores it, and the lease
  only matters if a release instance starts again.
  `census-original-after.json` shows the original volume was unchanged when
  copied. Anything written to `NEW_VOL` before promotion is **not** carried
  back. Before rolling back, stop the candidate, take a census of `NEW_VOL`,
  and `compare` it to `census-$NEW_VOL.json` with the step 6 allowances. A
  failure means something was written that rollback would discard: **hold**.
- **After promotion, if `NEW_VOL` holds nothing beyond `census-candidate-isolated.json`:**
  1. stop the promoted container (exit 0);
  2. take a census and `compare` it to `census-candidate-isolated.json`, with
     no allowances;
  3. roll back only if `compare` exits 0; otherwise hold.
- **After promotion, if anything was admitted (including a staging task):**
  this is a **hold, not an automatic rollback**.
  1. Stop admitting work; the coordinator has no in-process fence, so this is
     done at Viewer.
  2. Keep the promoted container and `NEW_VOL` intact.
  3. List the tasks and jobs that `NEW_VOL` holds but the original lacks,
     using a census plus a read-only query of the stopped copy.
  4. Reconcile each one with Viewer.

  Rolling back would lose that coordinator history unless it is preserved and
  reconciled first. Choosing to do that is Beau's decision. Never copy a
  migrated `jobs.sqlite3` under d738.

Keep the following until Beau accepts the release:

- `$VOL` and `$OLD` (still fenced);
- `VERIFY_VOL`, `NEW_VOL` and the archive;
- `$E`.

Cleaning them up is a separate step.

## Unresolved blockers

Each item is a **mandatory stop condition**. The cutover must not start until
the item has real enforcement **and** independent verification. A procedural
check, an observation or a snapshot does not resolve it.

| # | Finding | Why the current draft is insufficient | What would resolve it (unverified proposal) |
|---|---|---|---|
| U1 | **Candidate reachability before promotion: enforced, not proven live.** | The candidate runs with `--network none` (only `lo`; no bridge interface or address), and the spec script refuses isolated mode on any other network. The isolated test confirmed only loopback. `docker exec` and host root can still reach it. | Exact-image run of the isolated test; host root stays a trust boundary (U2, U7). |
| U2 | **Other writers before promotion: network path closed, host access not.** | With `--network none` no client can send a request. Release instances are fenced by the lease. d738 is fenced on retained d738 state by the step 2 holder, and on `NEW_VOL` by the candidate's own state lock. Host root, `docker exec` and direct volume writes are not prevented. | A volume-level writer lock (U7). Otherwise accept host root as a trust boundary. |
| U3 | **In-flight requests during the isolated phase: not applicable with `--network none`.** | Nothing can connect, so the socket snapshot is no longer the only defence. The census gate still shows equality only at two moments. | Exact-image confirmation. |
| U4 | **Single authority: d738 and release fenced independently; not proven live.** | **d738 does not take the new lease** (shown in the isolated test). Fences:<ul><li>the old container is removed after its clean stop, so a daemon restart or reconnect has nothing to act on;</li><li>one `--network none`, `--restart always` holder per retained d738 volume keeps d738's own `coordinator.lock`, so d738 refuses to start on it even when recreated deliberately;</li><li>`--verify` proves from `/proc/locks`, at the time it runs, that the live holder owns the lock (tested, including after the holder was killed and Docker restarted it);</li><li>a running release holds the lock on `NEW_VOL` and its published ports, so d738 cannot start on that copy or those ports (tested);</li><li>release instances exclude one another through the host-wide `flock` lease (tested).</li></ul>Still unfenced:<ul><li>the interval between a holder's death and its restart;</li><li>the interval between a copy's extraction and its holder or candidate taking the lock;</li><li>a real Docker **daemon** restart (not tested);</li><li>d738 on a copy nobody fences;</li><li>the exact images.</li></ul>A d738 created deliberately during one of those intervals could run. | Exact-image run of `ops/cutover_fence_test.sh` on a non-live host, including a real daemon restart. Retain no unfenced d738 state. |
| U5 | **Production hold after promotion: a backstop, not resumable.** | Implemented in source: every production Viewer POST gets 503 before any body read or service call; staging and production reads are admitted (unit and container tests). Viewer's isolated tests (mocked adapter, not a live cutover) show that a production request hitting the hold preserves source data and a single attempt, but does **not** resume automatically, and a generic 503 or reset cannot prove to Viewer that no body was read. So the hold does not replace true quiescence or a coordinated recovery. Prototype operator and reference routes are not covered. | True quiescence (R1) for the whole window. An authenticated, durable no-admission record that Viewer can read is not implemented. Coordinated Viewer recovery for any job that hits the hold. |
| U6 | **Rollback losslessness: detection implemented, not proven live.** | Implemented in source and tested synthetically (steps 3 and 6). Every entry's contents and metadata are compared under keyed IDs. Allowances are scoped to named fields, exact schema hashes and empty new tables. Remaining limits:<ul><li>equal censuses show **observed** equality at two moments, not the absence of transient writes;</li><li>mtime is compared at whole-second granularity;</li><li>xattrs, ACLs, file capabilities and ctime are not compared;</li><li>the database file is compared per table, not byte for byte;</li><li>sizes and row counts are printed in clear;</li><li>a migration over a non-empty table fails conservatively;</li><li>the exact schema hashes depend on the U9 rehearsal;</li><li>it has never run against the real volume or the a042 image.</li></ul> | Independent review of the census. The U9 rehearsal to produce the exact schema allowances. A live read-only run in the cutover window. Transient writes need prevention (U1, U2), which detection cannot provide. |
| U7 | **There is no lock on host writers.** | `fuser -vm` is a point-in-time, filesystem-wide observation. Its exit status is no longer masked, but it does not prevent a writer from appearing later. | A writer lock on the volume (for example a mount that only the candidate can make), verified independently. |
| U8 | **Volume creation is TOCTOU, and emptiness is not ownership.** | `docker volume inspect` followed by `docker volume create` can race, and an empty census does not prove this run created or exclusively owns the volume. | Create the volume once with a unique label, and refuse unless the creation call itself reports a new volume and no other container mounts it. Docker reuses an existing name silently, so this needs a verified mechanism. |
| U9 | **Startup schema allowlist: shape enforced, test-image result known, live config not rehearsed.** | The loader and generator enforce a schema-only file, and the lease marker has its own exact allowance. With the test image and synthetic state, release startup changed exactly five schema objects plus the marker, and passed the gate. Not yet done: the exact image, the live environment (including any reference runtime, which can write more), and a review of the generated file. | Rehearse the exact `IMG_NEW` with the live environment's key names on synthetic state; generate and review `startup-schema-allowlist.json`. |
| U10 | **Host-writer check needs root.** | `sudo -n` may be unavailable. | Beau provides an equivalent root check. |
| U11 | **Engine reachability.** | If `LOCAL_NODE_URL`/`MAIN_NODE_URL` use container names, the candidate cannot reach its engines from its own network. | Read the real `old-inspect.json`. U1's `--network none` sidesteps this during the gated phase. |
| U12 | **Reconciling ambiguous requests.** | Exit 0 proves only that handlers ended, and the coordinator records nothing about requests it refused or dropped. | Reconcile against Viewer's records (step 4), which needs Viewer's confirmed status vocabulary. |
| U13 | **API contracts unconfirmed.** | The headers and response fields in the inside-container checks, and Viewer's status list, are unconfirmed. | Confirm them against the release source and the deployed Viewer. |
| U14 | **Direct NodeODX writers.** | They are observed (R7), not fenced. | Outside this procedure; they remain outside the cooperative gate. |

**Validation status:** none of this has run on a live host. Step 3, minus the
host `fsync`, was rehearsed in bash on a synthetic volume, and all four
censuses matched. The helpers pass their synthetic tests; the container-spec
script has never seen a real `old-inspect.json`.
