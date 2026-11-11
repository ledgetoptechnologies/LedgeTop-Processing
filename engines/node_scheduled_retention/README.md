# Scheduled cleanup patches — review candidate, 2026-11-10

These patch files modify installed NodeODX/NodeODM source solely to add an opt-in scheduled cleanup control. Original copyright notices and AGPL-3.0-or-later licensing remain in the upstream files; modifications are marked LedgeTop and dated here. The patches do not contain full upstream source. Source provenance is the exact installed engine container in the LedgeTop processing environment; versions were not independently identified beyond the pinned image IDs and hashes below.

| Variant | Image ID prefix | Source config SHA-256 | Source TaskManager SHA-256 | Patched config SHA-256 | Patched TaskManager SHA-256 |
|---|---|---|---|---|---|
| main NodeODX | a53446d53dee | decadc1370d6286defe89e194dabf8e58711eb8a2cded09fff946de37c2a9ffa | 89ff48a399f87fdcec78ad49fa48fc8f7dd233c5421645bd816e9e0510916bbd | 602666721a385552e89b4c9fd0868c9e370ef534962443e959868abf047090f1 | d7c78e953bf485309647c11ab81278ed12fb8f69013b576bf09c69057955d9a3 |
| small NodeODM | 5ce31be03711 | b2e54a5c9a3ad036ada0167da41146f14fc33ecbc804708822e06a06927f5d61 | 9f43fff4882e7fba113c69f73328daa927ebb7e5716414eea5d9225b15d50efb | cc422dceebea33876082c17084e165e40bd2f389df63f0e3afe3ba37e3edf2a5 | 326639d70c12cf62acf979633937861ff929da7eda9d3805b9bf4f43af2ea2c5 |

Verify source hashes, apply the corresponding patch to a disposable source copy with `git -c core.autocrlf=false apply <patch>` and exact LF bytes, verify resulting hashes, then set `RETENTION_SOURCE_ROOT` to its parent containing both variant folders before running `node --test engines/node_scheduled_retention/test.cjs`. Modified source was tested before being removed from this repo; materializing the patch must be independently verified. Malformed CLI and JSON flag values now fail startup rather than silently enabling deletion. Bare `--disable_scheduled_cleanup` enables it; a boolean JSON `disableScheduledCleanup` also works, with explicit boolean CLI taking precedence.

The flag skips automatic terminal-task, orphan-directory and stale-upload sweeps at startup and hourly; default behavior and explicit operator removal remain. It does not cover request-error deletion: task creation can remove an existing same-ID destination and can remove a destination after initialization failure. No general no-deletion guarantee is claimed. When enabled, automatic scheduled reclamation stops: the operator must monitor space and manually remove only confirmed safe data under the agreed retention policy.

## Qualification — November 11, 2026

The patch bytes and four resulting source hashes above are unchanged at source
commit `984f8bc4bae76b2a1c854f94d4dfa73bb989f250`. Both variants were independently
materialized from their installed original files; all original/patched hashes
matched. The four source mock tests passed again against those exact LF files.

The small Node 14 image passed 12 real-parser/lifecycle cases, followed by actual
server startup and clean restart with three expired terminal tasks and five
retained file sentinels. Its child image is
`sha256:9c9f7ed05e0a8d2e4a370a75dd7b8d8524a0603205e79dfc9046239818237940`.
Both server processes exposed 82 options and stopped with exit 0 and no OOM.

The main NodeODX image passed actual server startup and clean restart on local
Docker Desktop staging, using Linux named volumes, its default root user,
three expired terminal tasks and eight retained file sentinels. Its child is
`sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd`.
Both processes exposed 80 options and stopped with exit 0 and no OOM. This image
also carries the test's owner label; installation must review its exact labels
and preserve the live service settings. The main host was not changed.

Both tests used read-only roots, bounded CPU/memory, no GPU/network/published
ports and only synthetic state. Their containers/state were cleaned; qualified
images remain cached. Machine-readable summaries are in
`qualification-20261111.json`. These cases prove synthetic scheduled-retention
behavior, not real-photo processing, production installation, forced crash,
network recovery or power-loss safety. A clean restart does not prove those
failure modes. The opt-in flag still must be supplied at deployment; building
the image alone does not disable cleanup. Neither production engine has been
changed by this qualification.

## Isolated fixture procedure

The first actual-image parser probe stopped before configuration loading because the pinned engine uses Node14.21.3, which does not provide the `node:assert/strict` builtin subpath. The staging helpers use `require('assert').strict` for that runtime. This is a fixture compatibility correction; no engine source or data changed. Actual parser and lifecycle results must still be recorded after the correction.

Use only image sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6 after independently verifying image/source hashes and both patched hashes above. Freeze scripts and patches to Git LF bytes. No client data or app mounts may be used. Create a unique root-owned root:root 0700 staging root and four disjoint synthetic `on/data`, `on/tmp`, `off/data`, `off/tmp` directories. A host-created `.fixture-marker` in the root and in each directory must contain exactly `node-retention-<nonce>\n` for a generated constant 32-lowercase-hex nonce. Prepare protected inputs and an independently trusted SHA-256 manifest for `overlay/config.js`, `overlay/TaskManager.js`, `runner/fixture.cjs`, `runner/parser.cjs`. Run read-only `staging-host-preflight.py ROOT MANIFEST NONCE`; it rejects aliases, unexpected ownership/mode, mismatched markers or artifact hashes, and overlap with running containers' writable binds. It is point-in-time; verify no privileged host writers, actual container mounts/modes and sufficient disk separately.

Use fresh `--rm` containers, explicit `--entrypoint node --runtime runc --network none --read-only --cpus .25 --memory 192m --memory-swap 192m --pids-limit 32 --cap-drop ALL --security-opt no-new-privileges`, `NVIDIA_VISIBLE_DEVICES=void`, no published ports, GPU, Docker socket or live app data. Mount only respective state at `/var/www/data`, `/var/www/tmp` read-write and validated overlays at `/var/www/config.js`, `/var/www/libs/TaskManager.js`, fixture at `/fixture.cjs` read-only. Working directory `/var/www`. Run `FIXTURE_MODE=on-first` with the flag and nonce, followed by `on-restart` in a new container using the same state and nonce; independently run `off` on empty marked OFF state without the flag. Fixture verifies mount modes, exact nonce-derived sentinels, paths, UUID/status index before initialisation; awaits the scheduled async dump, checks the index, and returns success only on an exact `fixture passed <mode>` line plus exit 0. Explicit removal applies only to its own synthetic canceled task. Deadline 30 seconds.

Separately run `staging-parser.cjs` with real image/minimist: default false, flag true, and JSON true, using a separately mounted immutable JSON config file as needed. Malformed string CLI/JSON values must fail nonzero, never silently choose cleanup behavior. Confirm exact node argument ordering. Process-only time substitution ages upload directories; auxiliary logger, UDP receiver and scheduler are isolated in-process. These tests do not prove real outage handling. Review fixture and host guard before any image transfer or test. Remove only verified task-owned test containers/files after agreed retention; never enable automatic deletion or touch shared processes.
