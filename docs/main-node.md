# Returning main processing node

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
