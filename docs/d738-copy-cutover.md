# d738 copy-on-cutover procedure (draft, not executed)

This replaces the live cluster coordinator, source `d738edb2`, image
`sha256:a04271d0…5837`, container `9dcb112d…09ae`, with a frozen release image.
The release image runs on a **copy** of the stopped state volume. The original
volume and container stay untouched, so rollback never runs d738 against a
schema that the release migrated.

This is a reviewable draft. Nobody has run it. Each live step needs Beau's
approval for that window. It changes no NAS, Viewer provider settings,
credentials or engine containers.

## Why copy instead of in-place

On first start the release `TaskStore` runs these schema changes:

- `ALTER TABLE viewer_tasks ADD COLUMN option_evidence_required`;
- creates `viewer_reservations`;
- creates `viewer_option_evidence`.

[viewer-option-evidence.md](viewer-option-evidence.md) states this is not
qualified for downgrade. If d738 were restarted on a migrated database, it would
ignore reservation capabilities. It would also report `held` as failed
(status 30), which e206125 fixed. Starting the release on a copy avoids that
downgrade entirely.

The cost is that **work admitted after cutover is not in the rollback volume.**
Rollback is only clean while the new coordinator has admitted nothing new
(step 8).

## Facts this procedure relies on

| Fact | Source | Re-verify at |
|---|---|---|
| Live d738 has no Viewer tasks or files; both NodeODM task lists are empty | Codex read-only check, 2026-10-30 | step 1 |
| Cluster Docker filesystem has about 115 GiB free | Codex, 2026-10-30 | step 0 |
| On SIGTERM, d738 stops both listeners, then waits without timeout for in-flight handlers, the dispatcher and the reference runtime. It exits 0 after a clean drain and 2 on any drain failure | `processing/app.py` `CoordinatorService.run` at d738 | step 4 |
| Admission drain is unusable in both d738 and the release (`ADMISSION_DRAIN_QUALIFIED = False`) | `processing/config.py` | — |
| The release adds only `PROCESSING_ADMISSION_DRAIN_ENABLED` (default `0`) to the environment | config diff d738..f445ce1 | step 5 |
| The state directory holds `coordinator.lock`, so **two volumes do not share a lock** | 2026-10-28 cutover census | steps 4, 5, 8 |
| Authority `7131882e-…e8` is stored in `coordinator_identity`; the copy keeps it | cutover evidence | step 3 |

## Step 0 — read-only capture (no approval needed)

Run on 192.168.50.89. Record everything into a private evidence folder, never
into Git.

```sh
OLD=9dcb112da75cd9663f87d63fed422979dd9216f46b7035f809673a2d900d09ae
E=~/.local/share/ledgetop-processing-prototype/cutover-$(date -u +%Y%m%dT%H%M%SZ)
mkdir -m 700 -p "$E"
docker inspect "$OLD" > "$E/old-inspect.json"           # contains env: chmod 600, never share
chmod 600 "$E/old-inspect.json"
docker inspect -f '{{.State.Status}} {{.RestartCount}} {{.HostConfig.RestartPolicy.Name}} {{.Image}}' "$OLD"
docker inspect -f '{{range .Mounts}}{{.Type}} {{.Name}} {{.Destination}} {{.RW}}{{"\n"}}{{end}}' "$OLD"
docker inspect -f '{{json .HostConfig.PortBindings}}' "$OLD"
df -B1 "$(docker info -f '{{.DockerRootDir}}')"
docker ps -a --format '{{.ID}} {{.Names}} {{.Image}} {{.Status}}'
```

Abort unless all of these hold:

- Exactly one volume mount, of type `volume`. Set `VOL` to its name and `DEST`
  to its destination; that should be the `PROCESSING_PROTOTYPE_STATE_DIR` value.
- The image is `sha256:a04271d0…`.
- The ports include 3005 (the native/Viewer listener) and 18443 (reference TLS).
- Free space is at least 3× the volume size plus the new image.
- No other running container publishes 3005 or 18443.

## Step 1 — prove every submission route is quiet

An idle queue alone is not proof. Something could be admitted between the
check and the stop, or could bypass the queue entirely. Close or verify each
route separately, and record what you saw for each one.

| # | Route into work | Proof it is quiet | Evidence |
|---|---|---|---|
| R1 | Viewer production and staging on `:3005` (`/task/new/*`, cancel, remove, download) | Viewer has no non-terminal processing jobs for this provider, **including retries whose `available_at` is in the future**. Beau has declared the freeze window and no human submits during it | Read-only query by the Viewer owner. Record counts only |
| R2 | Requests already inside d738 when it stops | Stopping d738 closes both listeners first. Exit code **0** proves every in-flight handler finished. **137 or 2 means abort** (step 4) | `State.ExitCode`, `State.OOMKilled` |
| R3 | Requests arriving after the stop | The port is closed while stopped, and step 5 starts the new container only after the census | `ss -ltn` shows nothing on 3005/18443 between steps 4 and 5 |
| R4 | Open TCP sessions just before the stop | `ss -tn state established '( sport = :3005 or sport = :18443 )'` shows no peers other than this check | Saved output |
| R5 | Reference `/ltds/v1` and its workers (staging only) | No worker daemon or broker process is running on either PC, and the reference tables hold no active claims | `ps`/`systemctl --user list-units` on both PCs; census row counts |
| R6 | Synthetic and operator `/v1/prototype/*` (`processingctl submit-synthetic`, `dispatch`) | No `processingctl dispatch` process is running, and nobody runs `processingctl` during the window | `ps` on .89 |
| R7 | Direct engine submissions that bypass the coordinator (NodeODX on cluster and main, plus anything holding their tokens) | `/task/list` is empty on both engines **before step 4 and again before step 5**. The legacy ClusterODX container is still stopped. No other container or process on either PC is a NodeODM client | Saved `/task/list` counts; `docker ps -a` on both PCs |
| R8 | Updaters and maintenance acting during the window (updaters on `020c15c`, owned maintenance schedule) | No updater lock or in-flight operation exists; no scheduled maintenance falls inside the window; neither updater targets the coordinator container | The updater's read-only status; `systemctl --user list-timers` |
| R9 | Isolated .88 pilot | The pilot Viewer is not configured to send to `.89` | Read-only config check by the Viewer owner |

R2 and R3 are the hard fences: once d738 has stopped cleanly, it can accept
nothing. The others prove that nothing is left in flight that would fail or be
lost when it stops, and that no writer outside the coordinator acts while it is
stopped.

## Step 2 — stop d738 (approval point A)

```sh
docker stop -t 3600 "$OLD"     # never the default 10 s: SIGKILL would skip the drain
docker inspect -f '{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}} {{.State.FinishedAt}}' "$OLD"
docker update --restart=no "$OLD"   # approval point A: keeps a daemon restart from reviving it
```

Continue only if the status is `exited`, the exit code is `0` and `OOMKilled` is
`false`. If the stop takes longer than a few seconds, a request was still in
flight. That contradicts step 1, so stop and inspect before continuing.

Changing the restart policy is a live configuration change and needs approval.
Without it, a Docker or host restart could start d738 again with `unless-stopped`
semantics while the new container holds the same authority on a different
volume.

## Step 3 — census, then copy

```sh
NEW_VOL=${VOL}-release-$(date -u +%Y%m%d)
IMG_OLD=sha256:a04271d08115481d5dd3a8a171b9b609567b823c7d5ea8af34b56b8915db5837
CENSUS="--network=none --read-only --user 10001:10001 --entrypoint python"   # quoted; expanded unquoted below
# 3a. census of the original, read-only
docker run --rm $CENSUS -v "$VOL:/src:ro" -v "$PWD/state_census.py:/c.py:ro" "$IMG_OLD" /c.py /src > "$E/census-original.json"
# 3b. offline archive, a third independent copy (streamed to stdout so the host folder needs no UID 10001 write access)
docker run --rm --network=none --read-only --user 10001:10001 -v "$VOL:/src:ro" --entrypoint tar "$IMG_OLD" -C /src -cpf - . > "$E/state-original.tar"
sha256sum "$E/state-original.tar" > "$E/state-original.tar.sha256"
# 3c. new volume and a copy that preserves mode and owner
docker volume create "$NEW_VOL"
docker run --rm --network=none --user 0:0 -v "$VOL:/src:ro" -v "$NEW_VOL:/dst" --entrypoint sh "$IMG_OLD" -c 'cd /src && tar -cpf - . | (cd /dst && tar -xpf -)'
# 3d. census of the copy, and of the original again
docker run --rm $CENSUS -v "$NEW_VOL:/src:ro" -v "$PWD/state_census.py:/c.py:ro" "$IMG_OLD" /c.py /src > "$E/census-copy.json"
docker run --rm $CENSUS -v "$VOL:/src:ro" -v "$PWD/state_census.py:/c.py:ro" "$IMG_OLD" /c.py /src > "$E/census-original-after.json"
```

`state_census.py` is [ops/state_census.py](../ops/state_census.py), copied from
the frozen release commit.

Continue only if all three censuses have the same `treeSha256`, `schemaSha256`,
`rowCounts` and `authorityId`, and each also shows:

- `quickCheck` is `ok`;
- `active.jobs` is 0 and `active.viewerTasks` is 0;
- `incomingPartials` is 0 and `spoolEntries` is 0;
- `sidecars` is empty (a clean close leaves no `-wal` or `-shm`).

The identical before/after hashes of the original are the proof that the copy
did not write to it. This census is the second, state-level proof for R1, R5
and R6: zero active rows **after** the listeners closed, not just before.

The tar archive is an offline copy that does not depend on Docker volume
storage.

Assumption: the image provides `tar` and `sh` (the python-slim base does), and
running as UID 0 inside an image with `--network=none` is acceptable for the
copy alone. If not, use `docker run` with a pinned, cached busybox image
instead.

## Step 4 — recheck direct engine routes

Repeat the R7 `/task/list` checks on both engines, and R8. Then confirm nothing
listens on 3005 or 18443. If any engine has a task now, it came from outside
the coordinator. Stop and inspect it.

## Step 5 — start the release on the copy (approval point B)

`IMG_NEW` is the locally built and inspected image for the frozen release
commit, identified by digest. Recreate the old container's `docker run`
configuration from `old-inspect.json`: user, read-only root, CPU/memory limits,
tmpfs, environment file, port bindings, network and restart policy. Make only
these changes:

- the image becomes `IMG_NEW`;
- the volume becomes `-v "$NEW_VOL:$DEST"`;
- the name becomes `ledgetop-processing-coordinator-<release-short-sha>`;
- `PROCESSING_ADMISSION_DRAIN_ENABLED` stays unset or `0`.

Derive the configuration with a reviewed script that diffs the old and new
`docker inspect` output. Do not hand-copy flags. The environment values contain
tokens, so print key names only, never values.

```sh
docker inspect -f '{{.State.Health.Status}} {{.RestartCount}}' "$NEW"   # healthy, 0
```

Never run the census against `NEW_VOL` while `$NEW` is running. The census is
only valid on a stopped volume.

## Step 6 — post-start verification (all read-only)

1. `/healthz` reports ready, and the native and reference listeners are alive.
2. `/info` and `/options` on 3005 (production and staging tokens) return the
   fleet option set. The option count and hash match the engines' current
   catalog.
3. `/v1/prototype/authority` returns `7131882e-…e8`, and the nodes are
   `cluster` and `main`, both active.
4. The jobs history count and status counts match `census-original.json`.
5. `docker diff "$NEW"` shows no writes outside the volume and tmpfs.
6. A staging-only Viewer photo job runs end to end, through archive ingestion
   **and derivatives**. Beau approves this as a separate run. Use the 20-photo
   selection or a synthetic set, not production.

## Step 7 — open production

Production may submit only after step 6 passes and Beau accepts it. That ends
the clean-rollback window: from then on, rollback loses whatever was admitted
(step 8).

## Step 8 — rollback

Use rollback when step 5 or 6 fails, or when Beau asks for it.

```sh
docker stop -t 3600 "$NEW"; docker inspect -f '{{.State.ExitCode}}' "$NEW"   # 0, else inspect first
docker update --restart=no "$NEW"
docker update --restart=unless-stopped "$OLD" && docker start "$OLD"         # untouched original volume
```

Before rolling back after any admission (step 7 or a staging run), take a
census of `NEW_VOL`. Each task or job it holds that the original lacks is work
the rollback would lose, so list them and get Beau's decision. Never copy the
migrated `jobs.sqlite3` back under d738.

Keep `$VOL`, the old container, `NEW_VOL` and the tar archive until Beau
accepts the release. Cleaning them up is a separate, approved step.

## Known limitations

- **No admission fence inside the coordinator.** Quiescence rests on the freeze
  window (R1), d738's clean SIGTERM drain (R2, R3) and the post-stop census.
  A Viewer job submitted during the window is refused at TCP level while the
  coordinator is stopped. How Viewer handles that refusal is not verified here.
- **The same authority on two volumes.** `coordinator.lock` sits in the volume,
  so it cannot stop both coordinators from running. The only fences are:
  - the old container is stopped, with restart policy `no`;
  - the port binding, since one host port cannot be bound twice;
  - this procedure.
- **Engines outside the gate.** Direct NodeODX writers stay outside the
  cooperative gate. R7 observes them; it does not fence them.
- **Release env and reference config.** That the reference config loads under
  the new `reference_runtime` is assumed and must be checked. The local
  equivalent: start `IMG_NEW` against a synthetic state volume with the same
  environment key names, before the window.
- **No live qualification yet.** None of this has run on a live host. Step
  3's commands were not executed against a real a042 image, and `tar` being in
  the image is assumed.
