# d738 copy-on-cutover procedure (draft, not executed, BLOCKED)

> **Status: blocked.** Fourteen review findings are open, listed under
> [Unresolved blockers](#unresolved-blockers). Each one is a mandatory stop
> condition until real enforcement **and** verification exist. Nothing in this
> document proves that the candidate is unreachable, that the old coordinator is
> fenced, or that rollback is lossless.

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

## Step 2 — guard against automatic restart, then stop d738 (not a fence; U4)

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

Once disconnected and set to `--restart=no`, the old container cannot restart
**automatically** with network access. This is not a fence: a Docker
administrator can reconnect and start it (blocker U4).

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

## Step 5 — start the candidate without published ports (not isolation; U1–U3)

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

Exposure checks, to run before any check that talks to the candidate. These are
**observations, not isolation**. The dedicated bridge is not `--internal`, and
the host and any host process can reach the candidate's bridge IP (blockers U1,
U2):

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

## Step 6 — promotion gate: census comparison (not lossless; U6)

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

This gate compares database digests and a few file counters. It does **not**
compare every regular file's contents outside the database, so it cannot
establish a lossless rollback (blocker U6).

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

After promotion, repeat **only** step 5's inside-container checks:
`/healthz`, `/options` for both roles, and the authority. The pre-promotion
no-published-port checks do not apply to a promoted container, whose ports are
published by design. Then the staging-only Viewer photo job runs through to
archive ingestion **and derivatives**. Production follows only after that passes
and Beau accepts it.

Recreating the container with published ports exposes it at once, to
production as well as staging. The only production hold is at Viewer
(blocker U5).

## Step 8 — rollback

- **Before promotion (any failure in steps 5–6):** remove the candidate and its
  network. Reconnect `$OLD` to the networks in `old-networks.txt` (with their
  original aliases), restore its restart policy, and `docker start "$OLD"`.
  `census-original-after.json` shows the original volume was unchanged when
  copied. Anything written to `NEW_VOL` before promotion is **not** carried
  back. Until U1, U2 and U6 are resolved, that loss cannot be ruled out; treat
  any unexplained difference as a hold.
- **After promotion, if `NEW_VOL` holds nothing beyond `census-candidate-isolated.json`:**
  1. stop the promoted container (exit 0);
  2. take a census and confirm it is unchanged;
  3. roll back as before.

  Because of U6, "unchanged" covers database digests and counters only, not
  every file's contents.
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
| U1 | **The candidate is not isolated from the host.** | The dedicated bridge is not `--internal`, and the host and every host process can reach the candidate's bridge IP. "No published port" and "one network member" are observations, not isolation. | Run the candidate with `--network none`, leaving only loopback in its namespace, for the census-gated phase. Engine-dependent checks (`/options`) then move after promotion. This must be shown to boot and must be verified by an independent probe. |
| U2 | **Other writers before promotion are not prevented.** | Checking published ports and network membership neither stops a host process, nor an admin, nor a later-attached container from writing. | U1's `--network none`, plus a volume-level guard: no container except the candidate may mount `NEW_VOL`. That needs enforcement, not inspection. |
| U3 | **Requests during the isolated phase are only snapshotted.** | `netns_sockets.py` gives a point-in-time snapshot. It now fails on a missing `tcp`/`tcp6` table, but it still sees nothing that connects after it runs. | Combine U1 with the step 6 census, and a resolved U6. |
| U4 | **The old coordinator is not fenced against a Docker administrator.** | `--restart=no` and network disconnection stop only automatic restarts. Anyone in the `docker` group can reconnect and start it while the candidate holds the same authority. | A fence that the coordinator itself enforces: refuse to start when another instance holds the authority, or an external authority lease. No such mechanism exists in d738 or the release. |
| U5 | **No production admission hold after promotion, except at Viewer.** | Step 7 recreates the container with published ports, exposing it at once to production and staging alike. | Options, each a decision for Beau: start the promoted container with a production Viewer token nobody holds, a credential configuration change; or a qualified in-process admission mode that separates staging from production. Neither exists or is approved. |
| U6 | **Rollback is not proven lossless.** | Step 6 compares database digests and limited file counts, not every regular file's contents, because the migrated database makes the aggregate `treeSha256` differ. | Extend the census with per-file content hashes, then require every non-database file to be identical before and after the isolated run. Not implemented. |
| U7 | **There is no lock on host writers.** | `fuser -vm` is a point-in-time, filesystem-wide observation. Its exit status is no longer masked, but it does not prevent a writer from appearing later. | A writer lock on the volume (for example a mount that only the candidate can make), verified independently. |
| U8 | **Volume creation is TOCTOU, and emptiness is not ownership.** | `docker volume inspect` followed by `docker volume create` can race, and an empty census does not prove this run created or exclusively owns the volume. | Create the volume once with a unique label, and refuse unless the creation call itself reports a new volume and no other container mounts it. Docker reuses an existing name silently, so this needs a verified mechanism. |
| U9 | **Startup allowlist.** | The step 6 gate depends on a list of tables that startup writes, and that list does not exist. | Rehearse `IMG_NEW` isolated on synthetic state, diff the censuses, then review the result. |
| U10 | **Host-writer check needs root.** | `sudo -n` may be unavailable. | Beau provides an equivalent root check. |
| U11 | **Engine reachability.** | If `LOCAL_NODE_URL`/`MAIN_NODE_URL` use container names, the candidate cannot reach its engines from its own network. | Read the real `old-inspect.json`. U1's `--network none` sidesteps this during the gated phase. |
| U12 | **Reconciling ambiguous requests.** | Exit 0 proves only that handlers ended, and the coordinator records nothing about requests it refused or dropped. | Reconcile against Viewer's records (step 4), which needs Viewer's confirmed status vocabulary. |
| U13 | **API contracts unconfirmed.** | The headers and response fields in the inside-container checks, and Viewer's status list, are unconfirmed. | Confirm them against the release source and the deployed Viewer. |
| U14 | **Direct NodeODX writers.** | They are observed (R7), not fenced. | Outside this procedure; they remain outside the cooperative gate. |

**Validation status:** none of this has run on a live host. Step 3, minus the
host `fsync`, was rehearsed in bash on a synthetic volume, and all four
censuses matched. The helpers pass their synthetic tests; the container-spec
script has never seen a real `old-inspect.json`.
