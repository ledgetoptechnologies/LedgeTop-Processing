# LedgeTop Processing

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
