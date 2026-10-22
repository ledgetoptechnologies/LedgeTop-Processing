# LedgeTop Processing

Private, self-hosted coordination service for LedgeTop's processing computers.
The cluster PC is the single endpoint reached by the TrueNAS Viewer. TrueNAS is
the durable home for source datasets and completed products. The cluster PC and
processing nodes use temporary working storage and remove it only after TrueNAS
has verified and accepted the result.

## Current status

This repository contains the initial architecture and integration contract. It
does **not** yet contain a deployable orchestrator. Keep the existing Viewer
processing path in service until a candidate passes the compatibility and
recovery checks in [the implementation plan](docs/architecture.md).

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
