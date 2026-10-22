# LedgeTop Processing

Private, self-hosted coordination service for LedgeTop's processing computers.
The cluster PC is the single endpoint reached by the TrueNAS Viewer. TrueNAS is
the durable home for source datasets and completed products. The cluster PC and
processing nodes use temporary working storage and remove it only after TrueNAS
has verified and accepted the result.

## Current status

This repository contains the initial architecture and a Dockerized **routing
probe**. The probe can inspect fixed ODM-family nodes and plan local/main
routing. It does not accept datasets, submit engine tasks, store jobs, or return
results. Keep the existing Viewer processing path in service until a candidate
passes the compatibility and recovery checks in [the implementation
plan](docs/architecture.md).

## Run the probe locally

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
4. NodeODX/ODM is the first engine. OpenSPLAT and TreeDAR follow through separate
   adapters and explicit input/output contracts.

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
