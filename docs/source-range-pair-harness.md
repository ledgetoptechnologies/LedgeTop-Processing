# Source-range v2 paired harness (synthetic)

`tests/test_source_range_pair_harness.py` runs the Processing source-range client
(PR #342) against the Viewer source-range v2 broker candidate (Ops PR #429). The
broker runs over loopback HTTP from `tests/source_range_pair/broker_host.cjs`.
Nothing here is deployed or wired into a runtime.

```sh
git -C <ops-checkout> archive d7589f2f7c10 apps/viewer/server | tar -x -C /tmp/viewer429
SOURCE_RANGE_VIEWER_SERVER=/tmp/viewer429/apps/viewer/server \
SOURCE_RANGE_SMALL_TMPFS=/small \
python3 -m unittest tests.test_source_range_pair_harness -v
```

It needs Linux and Node 22.5 or later (`node:sqlite`). The ENOSPC test also
needs a private tmpfs of about 24 MiB mounted at `SOURCE_RANGE_SMALL_TMPFS`.
Without the broker path, every paired test skips.

## Budget semantics observed in both implementations

- **`requestBudget` and `byteBudget`** are charged once per unique canonical
  8 MiB range, keyed by its request ID. The client charges them at intent time;
  the broker charges them on the first request for that range. The unique ranges
  always sum to `totalBytes`, so a `byteBudget` above `totalBytes` has no effect.
- **`attemptBudget`** is charged on every HTTP attempt. It is the only retry
  limit on either side. The client charges before it opens a request, so after a
  lost request the client's count can be higher than the broker's, never lower.
- **The broker** accepts any budgets at or above the minimums (base requests,
  `totalBytes`) and has no upper caps. The client's `build_source_transfer_request`
  emits `3x` byte and attempt budgets.

`tests/fixtures/source_range_budget_vectors.json` holds the shared integer
vectors for two profiles:

- **`current-v2-3x`** is current wire behavior.
- **`proposed-range-policy-1.5x`** is a **proposal only**. It needs the Viewer
  owner and Beau to agree before it can become wire behavior. Its caps
  (256 MiB per object, 64 GiB per manifest, 20,000 base requests, no zero-byte
  objects) are not enforced by either implementation today.

## Findings

- **Short write on ENOSPC (client, #342 @ 1c79a8af).** The unbuffered
  `stream.write()` return value is ignored. A short write is fsynced and
  committed with the full chunk hash, and every later resume then holds on
  "committed source bytes are missing or corrupt". Source bytes are
  unaffected. The ENOSPC test is marked `expectedFailure` until the client
  checks the written length before it commits.
- **Whole-object rehash per range (broker, #429 @ d7589f2f).**
  `#openAndVerify` hashes the entire object for every range request, so reads
  grow with object size times ranges per object. A 256 MiB object reads 8 GiB
  to serve its 32 ranges. A 25 MiB photo reads 100 MiB to serve 25 MiB.

## Deliberately not covered

- **Grant renewal.** Neither side has a renewal protocol. The broker re-mints
  in process: it revokes an expired grant and issues a new token under the same
  cumulative usage row. The harness checks only that current behavior.
- **The TrueNAS spool** reservation, capacity ledger and writer fence.
- **Production TLS and proxy mounting.** The host asserts protected transport
  instead of proving it.
- **Storage durability.** Real NAS storage, power loss and SQLite durability on
  the deployed filesystem are not exercised.

### Renewal proposal (not wire behavior)

A renewal would be an idempotent request bound to:

- the immutable job ID;
- the manifest SHA-256 and source version;
- the generation and reservation ID;
- the SHA-256 of the previous grant token;
- a monotonic `renewalSequence`.

The broker would:

- **Replay an identical request:** return the same renewed grant.
- **Refuse other conflicts:** a lower or skipped sequence, a different previous
  digest, or a renewal after the reservation lease expired.
- **Never reset or re-mint budgets:** the cumulative usage row is unchanged.
- **Hold on ambiguous races:** if a renewal and an expiry race and the outcome is
  unclear, hold rather than mint a fresh authority.
