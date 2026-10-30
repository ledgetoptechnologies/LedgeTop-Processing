# d738 copy-on-cutover procedure (draft, not executed)

This procedure replaces the live cluster coordinator with a frozen release
image. The live coordinator is:

- source `d738edb2`;
- image `sha256:a04271d0…5837`;
- container `9dcb112d…09ae`.

The release image runs on a **verified copy** of the stopped state volume:

- It starts **isolated**: no published port, on a Docker network it alone
  belongs to. It is reachable from no client until promotion.
- Promotion happens only after a census proves the isolated run wrote nothing
  beyond its startup migration.
- The original volume and container stay untouched, and are fenced so they
  cannot run again. Rollback never starts d738 on a schema that the release
  migrated.

Beau has authorized this work. TrueNAS remains under his control, and nothing
here touches it, the Viewer provider settings, credentials or the engine
containers. This draft has not been run.

Every **stop condition** below is mandatory. Hitting one means: stop, keep
every artifact, and inspect. Never improvise a workaround.

## Why copy, isolate and fence

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
  the copy, and rollback would silently discard those writes. Isolation makes
  that impossible, rather than asking clients not to.
- **The same authority twice:** the copy keeps authority `7131882e-…e8`, and
  `coordinator.lock` lives inside each volume, so the lock cannot prevent two
  coordinators. Host port binding does not either: Docker bridge peers reach
  container ports directly. The old container is therefore fenced before it
  stops (restart policy `no`) and after (disconnected from every network).

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

## Helpers (all read-only, all in this PR)

- [ops/state_census.py](../ops/state_census.py): a census of a **stopped**
  state directory. It records:
  - a tree hash covering modes, owners, file bytes, hard-link counts and
    symlink targets;
  - counts of symlinks and other non-regular files;
  - partial `.incoming-*` uploads and spool entries;
  - SQLite (opened `immutable=1`): quick_check, schema hash, per-table row
    counts and content hashes, active jobs and Viewer tasks, the authority, and
    any sidecar files.

  It prints no names, addresses or row contents.
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
cp state_census.py netns_sockets.py coordinator_container_spec.py "$E"/
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
sudo -n fuser -vm "$MP" 2> "$E/fuser.txt" || true                                 # must show only the $OLD process tree
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
| R1 | Viewer production and staging | The Viewer owner runs, read-only on the Viewer database: `SELECT status,COUNT(*) FROM processing_jobs WHERE status IN ('pending','leased') GROUP BY status;` and `SELECT status,COUNT(*) FROM processing_attempts WHERE status NOT IN ('cancelled','failed','published') GROUP BY status;` | Both return no rows. Pending rows with a future `available_at` count as active |
| R2 | Requests inside d738 | `docker exec -i "$OLD" python - < "$E/netns_sockets.py"`, three times 10 s apart | `inboundByLocalPort` is `{}` every time |
| R3 | Host-side listeners | `ss -tanp '( sport = :3005 or sport = :18443 or sport = :18080 )'`, plus every other `HostPort` in `old-inspect.json` | Only `LISTEN` rows, held by `docker-proxy` |
| R4 | Bridge peers | `docker network inspect <each network in old-networks.txt>` | The members list matches what you expect; every member other than `$OLD` is identified as a non-submitter (for example an engine container) |
| R5 | Reference workers and broker, both PCs | `pgrep -af 'reference_worker_daemon\|nas_broker\|reference_joint_driver'` and `systemctl --user list-units --all 'ledgetop*'` on .89 and main | No process; no active unit |
| R6 | Operator and synthetic `/v1/prototype/*` | `pgrep -af 'processingctl\|update_reference_coordinator\|run_coordinator_update_bridge'` | No process |
| R7 | Direct engine submissions | `docker exec -i "$OLD" python -` reading `LOCAL_NODE_URL`/`LOCAL_NODE_TOKEN` and `MAIN_NODE_URL`/`MAIN_NODE_TOKEN` from its own environment, then requesting `/task/list?token=…` and **printing only the counts**. Also `docker ps -a` on both PCs | Both counts are 0. The legacy ClusterODX container is `exited`. No unknown NodeODM-client container exists |
| R8 | Updaters and maintenance | `python3 <installed update_service.py> status --config <installed config>` for each installed updater, and `systemctl --user list-timers --all` on both PCs | No lock or in-flight operation; no timer due within the window; no updater targets `$OLD` |
| R9 | The isolated .88 pilot | The Viewer owner confirms read-only that the pilot's provider origin is not `192.168.50.89` | Confirmed |

**Stop condition:** any route fails, or its evidence cannot be produced.

## Step 2 — fence and stop d738

```sh
docker update --restart=no "$OLD"          # BEFORE stop: no restart window
docker inspect -f '{{.HostConfig.RestartPolicy.Name}}' "$OLD"        # must print: no
docker stop -t 3600 "$OLD"                 # never the default 10 s
docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.State.FinishedAt}}' "$OLD"
while read -r n; do docker network disconnect "$n" "$OLD"; done < "$E/old-networks.txt"
docker inspect -f '{{len .NetworkSettings.Networks}}' "$OLD"        # must print: 0
```

**Stop conditions:**

- The status is not `exited`, the exit code is not `0`, or `OOMKilled` is
  `true`.
- The stop took longer than a few seconds. That means a request was in flight,
  contradicting R2.

Once disconnected, the old container cannot reach clients or engines even if
something starts it.

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
census() { docker run --rm --network=none --read-only --user 10001:10001 --entrypoint python \
  -v "$1:/src:ro" -v "$E/state_census.py:/c.py:ro" "$IMG_OLD" -B /c.py /src; }
census "$VOL" > "$E/census-original.json"
# archive streamed to a host file: no pipeline, so docker's exit status is the archive's
docker run --rm --network=none --read-only --user 10001:10001 -v "$VOL:/src:ro" --entrypoint tar \
  "$IMG_OLD" --numeric-owner -C /src -cpf - . > "$E/state-original.tar"
python3 -c 'import os,sys;[os.fsync(os.open(p,os.O_RDONLY)) for p in sys.argv[1:]]' "$E/state-original.tar" "$E"
sha256sum "$E/state-original.tar" > "$E/state-original.tar.sha256"
for v in "$VERIFY_VOL" "$NEW_VOL"; do
  docker volume create --label ltp.cutover="$RUN" "$v" >/dev/null
  test "$(census "$v" | python3 -c 'import json,sys;print(json.load(sys.stdin)["tree"]["entries"])')" = 0
  sha256sum -c "$E/state-original.tar.sha256"
  docker run --rm -i --network=none --user 0:0 -v "$v:/dst" --entrypoint tar "$IMG_OLD" \
    --numeric-owner --same-owner -C /dst -xpf - < "$E/state-original.tar"
  census "$v" > "$E/census-$v.json"
done
census "$VOL" > "$E/census-original-after.json"
```

**All four censuses** (original, verify, copy, original after) must be
identical in every field, including `tree.root`. Extraction restores the
volume root's mode and owner. They must also show:

- `quickCheck` is `ok`;
- `active.jobs` is 0 and `active.viewerTasks` is 0;
- `incomingPartials`, `spoolEntries`, `symlinks`, `other` and `hardlinked` are
  all 0;
- `sidecars` is empty.

**Stop conditions:**

- Any difference between the censuses.
- Any non-regular file. Links and special files are rejected outright, not
  handled.

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

## Step 5 — start the candidate isolated

```sh
CAND_NET=ltp-candidate-$RUN
docker network create --driver bridge --label ltp.cutover="$RUN" "$CAND_NET" >/dev/null
python3 "$E/coordinator_container_spec.py" --container "$E/old-inspect.json" \
  --old-image "$E/old-image.json" --new-image "$E/new-image.json" --mode isolated \
  --volume "$NEW_VOL" --name "ltp-coordinator-candidate-$RUN" --network "$CAND_NET" \
  --env-out "$E/env-candidate" > "$E/candidate-spec.json"
# review candidate-spec.json (key names only), then run its argv exactly:
python3 -c 'import json,subprocess,sys;sys.exit(subprocess.run(json.load(open(sys.argv[1]))["argv"]).returncode)' "$E/candidate-spec.json"
CAND=ltp-coordinator-candidate-$RUN
docker start "$CAND"
```

Prove the isolation before any check that talks to the candidate:

- `docker port "$CAND"` prints nothing.
- `docker network inspect "$CAND_NET"` lists exactly one container, `$CAND`.
- From another LAN host (main or .88), connecting to `192.168.50.89` on 3005
  and 18443 is **refused**.
- On .89, nothing listens on 3005, 18443 or 18080 (`ss -ltn`).

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

## Step 6 — promotion gate: prove the isolated run wrote nothing

```sh
docker stop -t 3600 "$CAND"; docker inspect -f '{{.State.ExitCode}}' "$CAND"     # must be 0
census "$NEW_VOL" > "$E/census-candidate-isolated.json"
```

Compare against `census-$NEW_VOL.json`:

- Every table present before has an **identical `tableSha256`**, except tables
  on an explicit startup allowlist.
- New tables are exactly `viewer_reservations` and `viewer_option_evidence`,
  plus any allowlisted startup tables. Both new tables have zero rows.
- `active` is still all zeros, and the tree has no new partial uploads, spool
  entries, links or special files.
- The candidate's only `viewer-tasks` changes are allowlisted.

**The startup allowlist does not exist yet.** It must come from a rehearsal:
start `IMG_NEW` isolated on a synthetic copy of this volume shape, then diff the
before/after censuses. Example: reference recovery startup records. Until that
allowlist is reviewed, this gate cannot pass, and the gap is a blocker.

**Stop condition:** any difference not on the allowlist. Any such difference
means a client reached the candidate, or the startup does more than reviewed.

## Step 7 — promote

```sh
docker inspect -f '{{.State.Status}} {{.HostConfig.RestartPolicy.Name}} {{len .NetworkSettings.Networks}}' "$OLD"   # exited no 0
census "$NEW_VOL" | cmp - "$E/census-candidate-isolated.json"    # unchanged since the gate
docker rm "$CAND"; docker network rm "$CAND_NET"; shred -u "$E/env-candidate"
python3 "$E/coordinator_container_spec.py" --container "$E/old-inspect.json" \
  --old-image "$E/old-image.json" --new-image "$E/new-image.json" --mode promoted \
  --volume "$NEW_VOL" --name "ltp-coordinator-$RUN" --env-out "$E/env-promoted" > "$E/promoted-spec.json"
python3 -c 'import json,subprocess,sys;sys.exit(subprocess.run(json.load(open(sys.argv[1]))["argv"]).returncode)' "$E/promoted-spec.json"
docker start "ltp-coordinator-$RUN"; shred -u "$E/env-promoted"
```

**Promotion is the end of clean rollback.** From here on, any admission —
production or a staging verification task — exists only on `NEW_VOL`.

After promotion, repeat the step 5 checks through the published ports. Then
the staging-only Viewer photo job runs through to archive ingestion **and
derivatives**. Production follows only after that passes and Beau accepts it.

## Step 8 — rollback

- **Before promotion (any failure in steps 5–6):** remove the candidate and its
  network. Reconnect `$OLD` to the networks in `old-networks.txt` (with their
  original aliases), restore its restart policy, and `docker start "$OLD"`. The
  original volume is unchanged, as `census-original-after.json` proves. Nothing
  is lost.
- **After promotion, if `NEW_VOL` holds nothing beyond `census-candidate-isolated.json`:**
  1. stop the promoted container (exit 0);
  2. take a census and confirm it is unchanged;
  3. roll back as before.
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

## Remaining gaps (blockers unless noted)

1. **Startup allowlist** for the step 6 gate: requires a rehearsal of `IMG_NEW`
   on synthetic state. Blocker.
2. **Host-writer proof** needs root (`fuser -vm` on the volume mountpoint). If
   `sudo -n` is unavailable, Beau must provide an equivalent. Blocker until
   then.
3. **Bridge membership:** isolation removes the candidate from shared networks.
   The candidate may need its engines via the old network: check whether
   `LOCAL_NODE_URL`/`MAIN_NODE_URL` use container names. If so, the candidate
   cannot be isolated by its own network alone. Joining the engine network
   instead is acceptable only if R4 identifies every member as a
   non-submitter. Unresolved until `old-inspect.json` is read.
4. **No in-process admission fence after promotion.** Between promotion and
   production acceptance, production admission is held only at Viewer. The
   coordinator cannot accept staging while refusing production unless its
   credential configuration changes, which this procedure does not do.
5. **The client side of an ambiguous request** is reconciled from Viewer's
   records (step 4). The coordinator records nothing about requests it refused
   or dropped.
6. **Exact API contracts** for the in-container checks (headers, response
   fields) must be confirmed against the release source before running.
7. **Direct NodeODX writers** stay outside the cooperative gate. R7 observes
   them; it does not fence them.
8. **Live verification:** none of this has run on a live host. Step 3, minus
   the host `fsync`, was rehearsed in bash with `set -euo pipefail` on a
   synthetic volume, using a cached Python image as `IMG_OLD`. All four
   censuses matched, including the volume root.
   The container-spec script is tested on synthetic inspect JSON only, not on
   the real `old-inspect.json`.
