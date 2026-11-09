# Live two-node idle preflight — 2026-11-09

## Scope

Read-only verification of the installed Processing coordinator and both configured
NodeODM-family endpoints. No jobs were submitted, no containers were changed, and
no datasets or results were read or modified.

## Installed baseline

- Cluster coordinator container `ledgetop-processing-prototype-c3fcb3a` is running
  and healthy with zero restarts. Image digest:
  `sha256:a1644da352aadb94f5b1c393ebf68f5ff8bf227f2486dff973a2de3711d553ab`.
- `/healthz` reports Viewer enabled, synthetic prototype enabled, and
  `photoSmokeEnabled=false`.
- The authenticated read-only `/v1/nodes` probe reports both configured nodes
  ready and idle:
  - `local` / cluster: NodeODM-compatible `odm` engine, `maxImages=1000`,
    `taskQueueCount=0`, required Viewer outputs present.
  - `main`: NodeODM-compatible `odx` engine, no application photo cap,
    `taskQueueCount=0`, required Viewer outputs present.
- Configured order is `cluster,main`; `LOCAL_MAX_PHOTOS=1000`.

## Live non-executing routing probe

Sent authenticated `POST /v1/plan` requests for the `odm` contract. The API
reports `executesWork=false` for these planning requests:

| Photo count | Decision | Reason |
| ---: | --- | --- |
| 50 | local | `local_within_photo_limit` |
| 1,000 | local | `local_within_photo_limit` |
| 1,001 | main | `exceeds_local_photo_limit` |
| 5,000 | main | `exceeds_local_photo_limit` |

## Limits and next acceptance

This proves the live coordinator's idle-node health, configured ordering, 1,000
photo boundary, and non-executing plan behavior. It does **not** prove a live
claim under contention, a two-node sequence of real processing jobs, or Viewer
acceptance-ACK/replay. The existing real-photo output and synthetic outage tests
remain separate evidence. The Viewer ACK sender still needs an installable,
reviewed candidate and a fresh post-install job before the first end-to-end
milestone is accepted. Automatic cleanup remains disabled.

The legacy ClusterODX container objects on the cluster PC were left untouched;
the configured local endpoint was independently healthy through the coordinator
probe and must not be replaced by starting an unreviewed legacy container.
