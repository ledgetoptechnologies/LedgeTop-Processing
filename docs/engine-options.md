# Dynamic engine option discovery

Deployed discovery runtime: `a40acf5b32c8ab9c220a201f98774ff4da784514`, image
`sha256:b497c6de3326387225173852998bcab61429aad510c9ac0534449e96fb4e1e90`.
Live authenticated reads returned 82 local definitions, 81 main definitions,
75 identical shared definitions and five conflicting definitions. Existing queue
records, authority, named state volume and ODX/ARM services were preserved; no
processing job or Viewer provider registration was started. The stopped previous
coordinator is retained for rollback. The container's historical name remains
`ledgetop-processing-prototype-c3fcb3a`; its current image/revision is recorded in
the private installation metadata. See [deployment evidence](evidence/dynamic-options-20261026.json).

Authenticated `GET /v1/options` fetches fresh `/info` and `/options` responses from
the fixed configured local/main engines on every request. No engine flag list is
hardcoded. It preserves option names, types, raw defaults, domains, help and
extension metadata. Tokens are supplied privately through `LOCAL_NODE_TOKEN` and
`MAIN_NODE_TOKEN` using NodeODM-family query authentication; redirects and
environment proxies are refused.

The response contains each node's options, engine/API versions and hashes. Its
`commonOptions` includes only definitions present and exactly equal on every
configured node. Different defaults/help/types/domains are reported as conflicts;
flags present on only one node stay in that node's catalog. An unavailable or
malformed configured node closes the shared catalog rather than serving stale
data. An unconfigured role is reported separately and does not restrict an
otherwise valid single-node catalog. This conservative intersection can be
smaller than the set of flags usable when a future job is pinned to one node.

The reference-runtime candidate now admits typed booleans/numbers, bounded string
enums and free-text strings up to 4,000 characters. It validates current choice
arrays and recognized numeric bounds against the fresh definitions before queue
admission. Unknown domain prose is display metadata and is never evaluated;
path/file and URL-like settings remain excluded. This candidate change is covered
by synthetic admission tests; it is not deployed.

The API has `Cache-Control: no-store`; API and optional observer credentials may
read it. It is discovery-only: `executesWork: false`, `viewerProviderReady: false`
and `snapshotConsistency: best_effort_read_only`. Info/options are separate reads,
not an atomic upstream snapshot or job admission. Before future processing, an
attempt must retain its chosen node/catalog/versions and exact submitted options,
then revalidate the selected node before commit. A refresh must not rewrite a
running attempt or let an engine flag override host resource limits.

Each response is bounded and schemas reject invalid/duplicate names, non-finite
JSON and oversized catalogs. Private credentials are not added to results or
failure details; an upstream response reflecting its configured credential is
rejected rather than forwarded or edited into a misleading definition.

The installed private host wrapper supports `processingctl.py options` to print
only a short version/count/hash summary. Full metadata is available through the
authenticated management API.

## What upstream actually does

At reviewed ClusterODX commit `818f50b`, `/options` calls the first online
reference node and applies account limits, then caches by caller token for one
hour. It is not a fleet intersection, and that key does not bind engine/node
identity. See [proxy.js](https://github.com/WebODM/ClusterODX/blob/818f50bbbfcab107c41fcf4a7101e2bfcbce87e5/libs/proxy.js#L79-L157)
and [ValueCache.js](https://github.com/WebODM/ClusterODX/blob/818f50bbbfcab107c41fcf4a7101e2bfcbce87e5/libs/classes/ValueCache.js#L18-L64).
The discovery principle is retained here; that long shared cache is not copied.

NodeODX commit `0229646` derives the option definitions from the actual engine
argument parser but caches them and engine version for its process lifetime.
Our fresh fetch sees a normal container update/restart immediately; it cannot
force a running NodeODX process to discover an engine replaced underneath it.
See [odmInfo.js](https://github.com/WebODM/NodeODX/blob/02296467c0ca2142c2c7771876788bacbed102f6/libs/odmInfo.js#L22-L83)
and [the discovery helper](https://github.com/WebODM/NodeODX/blob/02296467c0ca2142c2c7771876788bacbed102f6/helpers/odmOptionsToJson.py#L38-L80).

## Viewer provider registration gate

Canonical Viewer source was inspected at LedgeTop-Ops `origin/main`
`0cd6ffda3c666d721861dd9221cc8b0c2f9d672a`, using
`apps/viewer/server/nodeOdmProvider.js`, `processingApi.js`, `providerAdmission.js`,
`providerHealth.js` and the task-options UI. The Viewer already discovers flags
and refreshes capabilities before submission, rejecting a changed fingerprint
until settings are reviewed. One registered provider row is displayed as a node;
the orchestrator's downstream PCs stay internal.

The new discovery endpoint does not yet satisfy that native provider contract:

| Viewer requirement | Current Processing state |
| --- | --- |
| `/info`, `/options`, query-token authentication | Management uses Bearer auth and `/v1/options`; native facade is absent |
| Multipart init/upload/commit, `set-uuid`, exact task UUID | Only isolated synthetic/photo-smoke profiles exist; no Viewer data-plane adapter |
| Status, log cursors, cancel/remove, streamed `all.zip` | No complete native task lifecycle/result adapter |
| Production/staging provenance | Viewer currently sends name/options, no scheduling class |
| Compatible type detection | Viewer accepts 2.x NodeODM or 1.x ClusterODM with official resource sentinels; no identity spoof is used |
| Reachable provider origin | Loopback is forbidden by Viewer admission; private management port 18080 is not the registration URL |
| Accepted EPT/textured-model result closure | Requires actual end-to-end processing, ZIP validation and Viewer ingestion |

Existing hosts also differ: main discovery reports API 2.3.1 / ODX 3.8.2,
while cluster's direct NodeODX reports version 3.6.2 / ODM 3.6.2, outside the
current Viewer's 2.x NodeODM signature. Catalog discovery does not silently relax
that compatibility check. The existing ClusterODX facade reports 1.5.3 and remains
the current compatible route.

Before registering the new provider, implement and test the full adapter,
immutable environment propagation, selected-node capability revalidation,
durable upload/commit/restart/cancellation/result recovery and accepted cleanup.
Use an honest supported compatibility contract or explicitly extend Viewer
detection. Prevent direct legacy writers from competing for the same worker.
Only then use the Viewer's authenticated registration API with a stable
idempotency key; it probes automatically and creates a disabled provider.
Enablement follows verified readiness, initially with one admission slot. Do not
insert directly into Viewer SQLite or replace the existing provider URL.

No new live Viewer provider is registered by this discovery increment. Beau's
real-data trial starts after that full readiness and registration step.
