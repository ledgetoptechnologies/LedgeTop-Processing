# Direct profile negotiation and transport proposal

**Unwired proposal.** Existing Viewer V2 and direct legacy requests retain
their current interfaces and limits. This does not authorize transfer, grant
custody, add a listener, or establish readiness for client jobs.

1. An authenticated coordinator reads an authenticated worker-instance
   capabilities response pinned to authority, node, worker and instance. It
   requires exact support for `ltds-direct-10k-each-v1`, 10,000 source and
   10,000 derived entries, 16 MiB raw UTF-8 manifest bytes, and an explicitly
   negotiated response limit. A reconnect to a different instance requires
   fresh capabilities; stale or absent capability is not implicit support.
2. Before any source bytes are sent, an authenticated coordinator persists
   and fsyncs the selected profile, worker instance, job/attempt/generation,
   source and derived inventory digests, manifest digest and exact transfer
   intent in its reservation transaction. The worker durably fences its
   capacity under the matching exact scratch identity; a duplicate with any
   changed field is held. No profile change or fallback is permitted after
   reservation without a separately proven reconciliation of all bytes and
   reservations. No automatic legacy fallback while a source might exist.
3. A future distinct direct-profile route accepts a fixed-length raw manifest
   of at most 16 MiB, with bounded nesting and rejection of duplicate JSON keys,
   noncanonical numeric values and unknown fields before trusting the parsed
   representation. Enforce the raw limit while streaming *before* buffering or
   parsing. Independently enforce both counts, per-entry limits and the
   canonical manifest digest. The current Python object validator runs only
   after parsing and is not such a transport defense. Chunked/unknown-length
   requests should be refused until their bounded parser is specifically
   designed and tested. Binary source objects require separately declared size,
   quota, digest, authenticated range/replay and a bounded streaming reader.
4. The worker signs its profile-bound local closure only after verifying each
   object on durable scratch. This local closure is not Viewer source custody.
   Coordinator verifies worker identity, exact profile and digests; its
   separately signed source acceptance must explicitly bind the same profile,
   intent and worker closure. Persist the exact receipt with durable custody
   state before return; only an authenticated replay GET may recover a lost
   response. The current coordinator receipt candidate lacks an explicit
   profile field and must receive a separately reviewed versioned extension.
5. Do not route an 8 MiB receipt through a 4 MiB sender. Before wiring, choose
   and enforce a separately versioned direct response with explicit byte cap
   including envelope and transport framing, or a compact signed commitment
   with immutable, paginated, individually verified inventory. A digest is only
   useful for provenance when the complete exact inventory remains retrievable.
   Apply aggregate page/count/byte limits and fail closed, without truncation.
   Agree the selected contract with Viewer and test worst-case long IDs,
   10,000 photos plus derived entries, oversize responses and replay.

The private Python index is a local implementation detail. Its fields can be
rebound by same-process code; never accept one from the network or consider it
an authorization capability. Authentication, separate-process isolation,
physical mount, quota, freeze and retention rules remain required. Prototype
measurement, even if it shows fast lookup, does not test disks, 10,001 durable
transfers, concurrent nodes, real network backpressure or recovery.

`processing/direct_negotiation_candidate.py` is a separate, unwired selection
experiment: it strictly bounds and canonicalizes small request/capability
records, takes already-authenticated identities from a trusted adapter, and
persists exact request, capability and response bytes for idempotent replay in
FULL synchronous SQLite transactions. It returns `selected_not_reserved` with
transfer and custody false. It does not authenticate a socket or capabilities,
enforce a manifest wire limit, create a real coordinator reservation, bind a
Viewer receipt, or prove power-loss safety of the installed database. It must
not be called with identities accepted from untrusted request headers. The
selection and real reservation must eventually become one crash-consistent
transaction, or have an independently reviewed recovery protocol; this
experiment alone is not that transaction.

## One bounded local measurement

On the current isolated worktree with the snapshot implementation at
`0befdb70`, a Linux x86_64 Python 3.13.17 container with one CPU, 768 MiB
memory limit and no network indexed 10,000 synthetic photo records and one
derived record. The canonical input manifest was 1,690,384 bytes. With
`tracemalloc` enabled, one construction took 0.672 s and 10,001 asserted
lookups took 0.168 s; peak newly traced Python allocations were 5,025,575
bytes. The original inventory was created before tracing, so this is **not**
total process or peak resident memory. Single-run wall times include tracing
overhead and are not a throughput, scalability or installed-system claim.
There were no files, source transfers, network calls, custody or recovery in
this measurement.

Reproduce from this worktree using the already available Linux image:

```powershell
docker run --rm --name ltds-index-measure-c8df --network none --read-only --user 10001:10001 --cpus 1 --memory 768m --pids-limit 96 --tmpfs /tmp:rw,nosuid,nodev,size=64m --mount 'type=bind,source=C:/Projects/processing-direct-coordinator-receipt-vectors-20261106,target=/src,readonly' --workdir /src --entrypoint python sha256:bc48c6a81300a7c9b610c6e0f3f4d7cb8aa599075645b2ff1ac1c891dc0ce8c4 -B -m tests.benchmark_direct_manifest
```
