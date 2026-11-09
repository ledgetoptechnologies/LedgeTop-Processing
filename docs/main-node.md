# Returning main processing node

The October 27 [installed maintenance workflow](main-maintenance.md) supersedes
the temporary-transport and old-container observations below. Main now has a
direct supervised coordinator tunnel, a private updated backend and a drained
LAN ingress. This historical October 26 acceptance still describes the original
two-node fixture and its retained failure evidence.

Beau confirmed the main processing PC was back on October 26 and asked Codex and
Hermes to continue testing through the existing Ledge Top Processing topic.
The same topic correlation and owning Codex chat were reused. Fresh substantive
messages automatically reached both selected active chats; no inbox polling or
new conversation was created.

Read-only inventory verified `bkoltz@192.168.50.88`, hostname `node`, Docker Engine
`45bc889d-d645-442b-8fb9-d78b77b14fe9`, Python 3.10.12, Threadripper PRO 3955WX
with 16 cores/32 threads, about 252 GiB RAM and about 1.4 TiB free on the NVMe root
filesystem. Its existing NodeODX, ARM, Ollama and monitoring services were
preserved. The DCGM exporter was already restarting before this work; it was not
repaired as part of the processing test. Legacy NodeODX's authenticated task
census remains unknown, so this evidence does not establish exclusive host
capacity or authorize heavy real-data processing.

## One coordinator, two node identities

The cluster's reviewed `c3fcb3a` runtime now configures
`PROCESSING_PROTOTYPE_NODE_IDS=cluster,main`. The coordinator was recreated only
after verifying its queue and cluster gate were idle. Its existing image, named
state volume and authority `7131882e-38ce-4d3e-99cc-4ba5a94d2ee8` were preserved.
The pre-change stopped coordinator is retained as
`ledgetop-processing-prototype-c3fcb3a-before-main-20261026` for rollback.
No second orchestrator or independent job database runs on the main PC.

For acceptance, the main PC uses its actual canonical Engine-bound gate with
node `main`, generation 1, the same coordinator authority and exact loopback
origin `http://127.0.0.1:18080`. Its private source/work root is:

```text
/home/bkoltz/.local/share/ledgetop-processing-staging/main-staging-20261026-a/
```

The root is mode 0700 and the worker credential file is mode 0600. The exact
existing TreeDar image
`sha256:f22ac13e70dbbabbc7d59f56d4eca8ec04476e711e2dd96354ba273011b3e029`
was streamed from the cluster over SSH and inspected on main. The source archive
of `cc467d1` has SHA256
`6dafbe9f1a9cdc56dc7e6e1b90ae1554cbdb4230edd58318e7219e18dcef40bc`.
Hermes checked all tracked paths: archive representations include CRLF-only
differences from canonical Git blobs; no semantic source changes were made.

## Temporary acceptance transport

Main-to-cluster SSH did not have an existing trusted host-key entry. No trust
checks were disabled and no SSH keys were installed. Codex instead supervised
two temporary, loopback-only SSH forwards using its existing host credentials:

```text
main 127.0.0.1:18080
  -> Windows 127.0.0.1:28081
  -> cluster 127.0.0.1:18080
```

This is test transport dependent on the Windows relay, not permanent fleet
connectivity. The coordinator remains host-local. A durable authenticated node
connection and worker process/boot supervision are separate work.
Both test relays were closed after acceptance and independent readback; the main
PC's test-only localhost listener is no longer present.

## Source-extraction failure

The first main-PC attempt selected the production fixture ahead of earlier
staging work, but stopped before TreeDar could run. Extraction with a private
umask left the two mounted helper files mode 0600; image UID 10001 could not read
`/worker/prototype_synthetic.py`. The stopped container and original claim were
verified and acknowledged as failed. That job remains failed permanently.

Only the two fixed helper files were changed to mode 0644; their contents and
the private parent-directory boundary were preserved. The dispatcher failure
guard was retained as evidence. A fresh production job and a distinct dispatcher
state root were used for the inspected retry. The original failed claim was
never reset, replayed or relabeled as successful.

## Accepted cross-node result

The fresh production job `b05071c5-783a-4ab8-ae4f-330498bbf4c6` succeeded on
`main`, producing the expected three-tree fixture. Its exact completion and
canonical gate release were verified. Codex independently read that result while
the original staging job was still queued, then dispatched once on `cluster`.
Staging job `20425878-e710-45a0-a3b5-821c9feac15e` succeeded with the same expected
three-tree fixture. Both nodes returned to the same coordinator authority.

Output hashes were verified on each host before accepted synthetic artifacts,
success receipts and stopped containers were removed. The original failed job
remains failed, and its private failure receipt/dispatcher guard are retained
for audit rather than relabeled or automatically replayed. No real dataset was
processed, and no changes were made to existing engine/ARM/Ollama services.

This establishes bounded CPU-only plumbing across two physical nodes and
production-before-staging ordering. It does not establish large-dataset capacity,
GPU processing, direct NodeODX integration, permanent node connectivity, automatic
worker boot recovery or TrueNAS artifact-byte delivery. See
[sanitized cross-node evidence](evidence/main-pair-20261026.json).

## November 1 synthetic queue follow-up

Two explicitly synthetic TreeDar jobs were queued to check the current cluster
dispatcher. It correctly refused the TreeDar profile on `cluster` with
`node_ineligible`; no worker container was launched there. The jobs remained in
the shared coordinator queue. After confirming `main` was active, its canonical
gate had no outstanding reservation, and the main NodeODX task list was empty,
the existing main-node dispatcher ran both jobs serially. The production-labeled
synthetic fixture ran before the earlier staging-labeled fixture. Both completed
with the expected three-tree output using the pinned synthetic image, and both
worker containers exited successfully. The NodeODX task census remained empty;
the main gate returned to no reservation and the shared queue has no queued,
running, or held jobs. The synthetic receipts and scratch are retained on main.

This checks queue priority and TreeDar placement on main only. The fixtures do
not contain photos or genuine LiDAR, and they did not exercise the Viewer result
acceptance path. The cluster refusal is expected under the current main-only
TreeDar policy; the cluster CLI still lacks a safe native command for main-node
dispatch, and synthetic queued-work cancellation is not implemented. See
[sanitized follow-up evidence](evidence/main-priority-synthetic-20261101.json).

## November 1 provider readiness observation

Read-only checks over SSH found the coordinator healthy on `.89`, with zero
queued, running or held jobs (17 prior successes and 2 prior failures). Its
options endpoint reported `viewerProviderReady=true`, 74 common options, local
ODM 3.6.2 available, and main ODX 3.8.5 available. The main NodeODX container on
`.88` was running. The old cluster `node-odx-gpu` container was stopped, while
the coordinator's local ODM endpoint remained discoverable and available; do
not infer endpoint health from that old container's name alone. Health reported
the legacy `photoSmokeEnabled` flag false, which is distinct from the Viewer
provider readiness result.

An earlier read-only `/v1/plan` probe included 125,000,000 input bytes and
returned `decision=local` with `executesWork=false`; the local NodeODM
advertised `maxImages=1000`, while main ODX had no photo-count cap. The current
probe contract uses only `engine` and `photoCount`; request bytes no longer
participate in admission. This verifies probe behavior, not an atomic
reservation, queue dispatch or completed image-processing job.

This verifies provider discovery and idle queue state, not a submitted photo
job, small-node-first dispatch, image-cap enforcement during real processing,
or result acceptance by Viewer. These readiness checks submitted or mutated no
jobs.
