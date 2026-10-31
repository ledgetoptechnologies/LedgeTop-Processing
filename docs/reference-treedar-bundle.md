# TreeDar bundle construction — source-only partial writer

`processing/reference_treedar_bundle.py` matches the standalone Viewer
`ltds-treedar-bundle-v1` draft. Its shared fixture is
`contracts/treedar-bundle-v1.contract.json`, copied without semantic changes from
the reviewed Viewer fixture. Native TreeDar 0.0.1 filenames, roles, formats,
sizes and hashes remain intact. In particular, canopy height and crown labels
are not terrain DEMs. This module selects **no** outer result role and never
maps a whole bundle to `tree_inventory`.

The ZIP contains `treedar-bundle.json` plus exactly the listed
`artifacts/<nativePath>` files. Twelve pipeline files are required; the complete
known 22-entry vocabulary includes optional outputs and `manifest.json` as
`engine_manifest`. The sorted manifest binds the original job, generation,
environment, input manifest and pipeline hashes, original LiDAR object/hash/
bytes/provenance-receipt ID, and engine version. Matching a receipt ID is not
acquisition authentication. Geometry and provenance admission remain outside
this byte/metadata constructor.

## Pure metadata validator

`BundleLimits.from_mapping()` requires all six explicit Viewer draft limits.
`validate_manifest(payload, expected, limits)` accepts an object or bounded
UTF-8 JSON and returns a detached manifest. It verifies closed schemas, original
binding, native semantics, required files, sorted/case-unique known paths, byte
totals, hashes and canonical manifest digest. JSON duplicate/escaped keys,
non-integer numeric tokens, malformed UTF-8, depth/array/body overflow and trailing
data are refused. Pure metadata validation does not open a filesystem, execute
TreeDar or produce an acceptance receipt.

## Required owner-supplied authority and files

`BundlePartialWriter` is Linux-only and is **not a durable publication journal**.
It is a one-shot streaming constructor for an existing host-owned partial file.
Before using it, the integration must already provide:

- Three disjoint, real, existing UID-owned mode-0700 RAW/output/state roots.
  Their entire ancestor publication chain must already be durable. This module
  creates no directory. It checks root identities and refuses symlinks.
- A fixed `BundlePolicy`: root paths, host publication/reservation IDs, the
  actual retained terminal stage-proof hash, the parent's monotonic allocation
  base and remaining reserved bytes, and explicit `BundleLimits`. Submitted
  metadata cannot choose these paths, limits or authority.
- A trusted `PublicationGuard` whose exact binding equals `guard_binding()`.
  Its `check(context, mutation=...)` must independently verify the live
  state-owner lease, original parent/physical fence where required, reservation
  identity and allocation, terminal source proof and one-attempt publication
  authority. It returns `None` or raises. A matching dictionary is not proof of
  a real lease or acquisition. The production guard is not supplied here.
  Once a prepared `manifestSha256` is supplied, derive and compare it from the
  terminal proof's exact original native inventory plus original job binding.
  Hashing a proof file while accepting arbitrary current RAW contents is not
  enough.
- The guard/owner lease must remain held through constructor, RAW hashing,
  copying, verification and subsequent publication. Stage source files must be
  terminal, immutable owned outputs. A later or concurrent engine writer is not
  authorized by this constructor.

The caller exclusively creates `<publication_id>.zip.partial` in the output
root with `O_RDWR|O_CREAT|O_EXCL`, mode 0600. The descriptor must match that exact
regular single-link inode, be empty at offset zero, and have no append flag.
It remains caller-owned and open after the constructor finishes or fails.

Call `prepare()` to read and hash the RAW inventory. It opens originals
read-only with `O_NOFOLLOW`, checks their ownership/single-link identity,
sizes/modes/timestamps and hashes, and permits only known native files. A private
RAW root may contain an owned non-writable `classify/` directory with ordinary
readable permissions; the private ancestor still controls access. Unlisted
directories, special files, hardlinks, symlinks and unknown/empty output closures
are refused. Originals are neither chmodded nor deleted.

Directory names are censused with `scandir` before sorting, bounded by the
configured file count plus the single permitted `classify` directory. Unknown
or excess entries stop the census immediately. Each observed file size consumes
the cumulative RAW allowance before hashing; reads stop at that observed size
plus one overflow byte, so a growing source cannot extend the hash loop to a
moving EOF. Growth or truncation holds construction and retains the inputs.

`intent_for(fd)` returns the exact draft intent for the caller to persist at
`<publication_id>.intent.json` in its state root, using exclusive creation,
mode 0600, FULL/appropriate durable state discipline and the owner lease. The
intent binds `state=streaming`, policy/source-proof hashes, original manifest,
root identities, exact partial inode and reservation snapshot. This helper does
not persist or authorize that intent by itself.

`write_to(fd)` reads back the exact bounded intent and fsyncs its existing file,
state/output roots and parents before the first ZIP write. It rechecks the guard
and source closure, streams a deterministic stored ZIP with fixed timestamps,
checks copied hashes, fsyncs the partial and independently rereads its ZIP/CRC/
file closure without extraction. Memory is bounded by chunk and metadata caps;
there is no in-memory whole-archive constructor. The caller gets a draft
`partial_complete_needs_durable_publication` receipt, never a published result.

## Capacity accounting and failure behavior

The policy's allocation base must already include every original parent input,
prior stage output, RAW TreeDar output and earlier control/proof allocation.
The guard must compare that base and remaining bytes against the actual existing
reservation. The constructor does not reset the base to LiDAR size, probe free
space as a substitute for a reservation, modify a ledger or settle the parent.

Before writing it requires remaining capacity for maximum ZIP bytes, actual
bundle-manifest bytes and the separate control/proof allowance. Its bundle-local
coexistence cap includes retained RAW + new ZIP + manifest/control state. The
returned exact `newBytes` is ZIP size plus the actual intent size (which contains
the manifest once); `total` is the prior allocation base plus those new bytes.
Other parent allocations stay included in that base. The caller still has to
record actual allocations and cover all other simultaneous storage copies.

On errors, originals, caller intent and partial bytes remain. A writer instance
is one-shot; a nonempty partial is never overwritten, truncated, removed or
automatically replayed by a fresh writer. Unknown writes/fsyncs return no positive
receipt. The caller must retain its reservation and inspect the exact partial.
This module never marks the intent complete, renames a file, starts/stops an
engine, acknowledges delivery, releases a gate, deletes data or frees scratch.

**Restart/publication is still a separate implementation gate.** Even an empty
partial with an old `streaming` intent is not automatically permission to rerun
the callback. The integration's durable one-attempt journal must distinguish
new start from read-only reconciliation, validate a completed partial against
the negotiated Viewer contract, durably record completion, publish without
clobbering and reconcile a lost publication acknowledgement. There is no generic
rename/commit helper or claim that these guarantees are already implemented.

## Synthetic and paired qualification

Tests use actual private files, SQLite scratch reservations and an OS owner
lease, with an explicitly synthetic guard and native byte bodies. They check
shared canonical metadata, exact descriptors, intent-before-write fsync barriers,
RAW retention, parent allocation preservation, bounded partial failures,
missing/changed intents, authority loss, unknown output flushes, links/FIFOs,
fork refusal and capacity errors. They do not qualify genuine acquisition or a
production parent guard.

For an explicit local synthetic handoff, use the optional test emitter:

```text
python -B -m tests.emit_reference_treedar_bundle --handoff /handoff --name python-native-bundle
```

It exports only fixture bytes/metadata into new files, using the actual writer
API. With retained synthetic CLI artifacts, additionally supply `--native-root`,
`--native-input`, `--expected` and `--submission`. It checks the independent
original submission/LiDAR hashes, copies RAW and LAS into owned temporary paths
without changing retained evidence, and charges the whole parent input byte
count. Its `.part` artifact can then be read by the frozen standalone Viewer
bundle validator. This is fixture emission, not a worker publication or job.

Outer transport/version agreement, real guard/producer publication recovery,
actual intermediate adapters, original-source retention and Viewer ingestion/
durable acceptance remain unfinished. TreeDar/combined capability advertisement
and runtime deployment are unchanged by this source.
