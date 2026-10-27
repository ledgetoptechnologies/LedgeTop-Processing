# LedgeTop Processing

GitHub checks run on `main` pushes and pull requests targeting `main`; feature
branch pushes do not consume Actions minutes. The guarded manual workflow is
main-only. Ordinary branch iteration belongs in Engineering Hub when its actual
execution path is available. See [repository CI policy](AGENTS.md).

The [package workflow](.github/workflows/processing.yml) checks the existing
Dockerfiles and publishes two private repository-linked images on main:
`ledgetop-processing-orchestrator` and `ledgetop-processing-treedar` under
`ghcr.io/ledgetoptechnologies`. Each run uses a unique build tag and retains a
metadata receipt with verified registry digests. It never deploys a container,
updates a live tag, or gives publishing permissions to processing-node tokens.

Private, self-hosted coordination service for LedgeTop's processing computers.
The cluster PC is the single endpoint reached by the TrueNAS Viewer. TrueNAS is
the durable home for source datasets and completed products. The cluster PC and
processing nodes use temporary working storage and remove it only after TrueNAS
has verified and accepted the result.

## Current status

TreeDar now lives in [`engines/treedar`](engines/treedar) with its original Git
history preserved. It remains a separate Python package, `treedar` command and
worker container. Its tested local processing commands are available today;
coordinator submission, scheduling and Viewer integration for that engine are
still future adapter work.

This repository contains the initial architecture and a Dockerized **routing
probe**. The probe can inspect fixed ODM-family nodes and plan local/main
routing. It does not accept datasets, submit engine tasks, store jobs, or return
results. Keep the existing Viewer processing path in service until a candidate
passes the compatibility and recovery checks in [the implementation
plan](docs/architecture.md).

## Run the probe locally

An opt-in [synthetic coordinator prototype](docs/prototype.md) adds durable
production/staging test jobs, one-slot worker claims, result completion and
restart recovery. It uses `compose.prototype.yaml` as a separate localhost-only
stack. This is a synthetic TreeDar integration trial, not a live Viewer provider
or a replacement for the current photogrammetry router.

A separate opt-in [NodeODX photo trial](docs/photo-smoke.md) has passed a
16-photo alignment test through the coordinator on the cluster PC. All selected
photos registered; test copies and outputs were removed and original hashes
verified unchanged. See the [metadata-only evidence](docs/evidence/odx-photo-smoke-20261023.json).

The [bounded synthetic dispatcher](docs/dispatch.md) can drain queued TreeDar
fixture jobs serially from one host command. It preserves production priority and
stops on uncertain execution. The [engine handoff requirements](docs/engine-handoff.md)
describe the remaining ground/coordinate checks before NodeODX products can
become TreeDar forestry inputs.

[Node maintenance](docs/maintenance.md) adds scheduled draining and a bounded
container update/health/rollback runner. It prevents new claims on a drained node
and preserves uncertain updates for recovery. OS updates and reboots are deferred.

[Node ownership and outage recovery](docs/resilience.md) adds an Engine-bound
host admission guard, isolated candidate testing and a read-only shadow preview.
The synthetic worker durably retries the original completion after outages;
restarts preserve ownership and operator holds. Artifact files remain on the
worker: verified artifact transfer and downstream pipeline admission are still
separate work.

The reviewed prototype is now [installed and tested on the cluster PC](docs/cluster-prototype.md).
Its coordinator is healthy on host-local port 18080; synthetic workers use the
installed bounded command. Existing ODX services remain in place.
The [returning main PC](docs/main-node.md) has also completed a synthetic job
through that same coordinator, followed by staging work on the cluster PC.
The [installed main-node maintenance workflow](docs/main-maintenance.md) now
uses a direct supervised SSH tunnel, a drained ingress and a pinned NodeODX
2.4.0 / ODX 3.8.5 backend. Production-first and paused-node synthetic dispatch
have passed across both PCs. Continuous workers and full Viewer task integration
remain separate work.

[Dynamic engine option discovery](docs/engine-options.md) fetches current NodeODX
flags and metadata per request and reports a conservative shared catalog across
configured engines. This management API does not yet implement the Viewer's
native provider task lifecycle.

1. Copy `.env.example` to `.env`. Replace the example token with a private,
   random value of at least 32 characters. Keep `.env` outside Git.
2. Add the exact local and main processing-node origins when available. A blank
   origin reports `not_configured` and cannot be selected.
3. Start the container with `docker compose up --build -d`. Compose binds the
   API to `127.0.0.1:8080` on the host by default. Do not make it reachable on
   the LAN until its authentication and network boundary are reviewed for that
   deployment.
4. Request `GET /healthz`, then use the token as a Bearer credential for
   `GET /v1/nodes` and `POST /v1/plan`. The planning JSON is:

   ```json
   {"engine":"odm","photoCount":250,"inputBytes":1000000000}
   ```

   The response includes `mode: routing_probe` and `executesWork: false` so a
   caller cannot mistake a route decision for a processing submission. The
   probe reads `/info` and `/options` from each configured node. It requires
   the capability signature and output options currently expected by the
   Viewer; incompatible nodes are marked unavailable.

Run `python -m unittest discover -v` for local checks and `docker compose
config --quiet` to validate Compose. This first image has no external Python
packages or writable data volume. Its node selection is a planning result;
health may change before an eventual real submission.

## First target

1. The Viewer on TrueNAS submits a job to one cluster-PC endpoint and retains
   ownership of the client task, dataset, and published result.
2. The cluster PC durably tracks the execution attempt, selects a healthy
   processing node, and reports progress. The local node is preferred for jobs
   within a configurable photo-count and resource limit. A larger or unsuitable
   job waits for the main processing PC; it is never silently sent to an
   undersized local node.
3. The selected engine works on a temporary copy. The result returns to TrueNAS
   and is checked there before temporary copies are eligible for deletion.
4. NodeODX/ODM is the first coordinated engine. OpenSPLAT and TreeDar follow
   through separate adapters and explicit input/output contracts.

## Repository layout and TreeDar development

```text
processing/          coordinator and routing probe
tests/               coordinator tests
Dockerfile           coordinator image
engines/treedar/     forestry engine, tests, research notes and worker Dockerfile
```

From this repository root:

```powershell
python -m unittest discover -v
docker build --tag ledgetop-processing .
docker build --tag treedar ./engines/treedar
Set-Location ./engines/treedar
./scripts/setup.ps1
```

The setup script creates an engine-local virtual environment using its locked
dependencies and runs the engine tests. Do not install the engine's dependencies
into the coordinator image. Run subsequent TreeDar Python commands from the engine
directory; `treedar` CLI arguments and mounted dataset paths are unchanged.
See [TreeDar's README](engines/treedar/README.md),
[worker instructions](engines/treedar/docs/docker.md) and
[migration record](docs/treedar-migration.md). Source datasets and existing NAS
run folders stay in their current private locations; the migration moves code
and documentation only.

The proposed photo threshold of 800 is a **starting configuration value**, not
a guarantee that the cluster PC can safely process 800 photos. Benchmarks and
free disk/RAM checks must set the actual admission limit.

## Sources

- [Current Viewer processing implementation](https://github.com/ledgetoptechnologies/LedgeTop-Ops/tree/main/apps/viewer)
- [ClusterODX](https://github.com/WebODM/ClusterODX), the NodeODX-compatible
  proxy and node scheduler used as a reference
- [ClusterODM](https://github.com/OpenDroneMap/ClusterODM), the analogous
  NodeODM-compatible scheduler

No ClusterODX or ClusterODM source is copied into this repository. Their API
and runtime compatibility with the current Viewer must be proven with the
actual pinned images before either is placed behind the Viewer.
