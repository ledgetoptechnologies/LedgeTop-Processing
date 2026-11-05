# Bounded predecessor diagnostic and metadata-budget followup

This followup fixes the independent review's budget finding before the closed
predecessor component is used on installed state. It adds a diagnostic seam; it
still does not wire or activate an update controller.

The full census now checks a coarse physical envelope **before SQLite connect**:
`8 * maxMetadataBytes + 100000 * 1024 + 1 MiB`. Its existing final page-derived
bound still applies. The known-schema object count and byte size are checked
before schema objects are retained. SQLite's own LENGTH limit is installed before
schema/metadata reads. A scalar row-count/raw-cell-byte pass runs before
`quick_check`, foreign-key checking, or full row materialization. Full rows are
then fetched individually, and their exact ASCII JSON size is counted with bounded
string chunks before encoded copies or retained rows are added. Unsupported
binary metadata, oversized cells, excessive rows, or budget exhaustion hold.
The SQLite LENGTH API is required; the installed predecessor image uses Python
3.13. An older Python lacking it fails closed instead of running an unbounded
reader.

`metadata_probe` requires reviewed schema bytes in memory and validates their
canonical hash against the same frozen artifact pin. It checks schema, row and
maintenance bindings, archive receipt identity, all expected paths/owners/modes and
sizes. It does **not** open photos or ZIPs for bulk content reads and does not
claim their digest matches. Its result has:

```
profile = installed-693cb7e-metadata-diagnostic-v1
validationScope = metadata-paths-receipts-only
retainedFilesValidated = false
writerExclusionProven = false
signedViewerAcceptanceProven = false
```

Full `census` retains the original full hashing behavior, returns the separate
closed profile and `retainedFilesValidated=true`, and still does not establish
writer exclusion or signed Viewer acceptance.

Diagnostic defaults are 5 seconds, 1 MiB of **direct retained-file reads** and an
8 MiB SQLite file cap. The direct-read counter covers the header/receipt reads;
SQLite's metadata page reads are separately constrained by the physical file cap,
row/cell/LENGTH budgets and progress-handler duration checks. These counters are
not presented as an exact count of kernel page-cache I/O. Full hashing accepts
optional cooperative duration/direct-read/database-size bounds. Bounds are checked
before/after direct reads and between metadata operations; no timeout thread is
created. A blocked kernel read/stat cannot be forcibly preempted by this library,
so the invoking process should also have an outer deadline.

`serializable_probe_source(request)` returns a standard-library-only script for
`python -` over stdin. The closed request carries `schemaBase64`, the private
census key, reviewed identity/count/budget settings and the logical root. No tool,
package or contract file is written to the target. The script validates the
schema pin, prints only bounded reason codes or aggregate/keyed result hashes,
and replaces the per-file map with its aggregate inventory digest/count. Keep the
generated script private because it embeds the ephemeral census key. Source and
request contents must not be printed in tool output or review records.

Validation:

```
python -m unittest tests.test_coordinator_predecessor_census tests.test_coordinator_optimized_safety -v
```

34/34 Linux tests passed, no skips, in 1.165 seconds: 31 census/probe cases and
3 optimized-Python safety cases. One CPU, 768 MiB RAM, no network, read-only
source/root and a 192 MiB temporary filesystem. The task container was removed and
filtered inventory was empty.

Specific new proof includes refusal of a sparse oversized SQLite file before
`sqlite3.connect`, refusal of a giant same-schema cell before any full jobs-row
fetch or `quick_check` on both paths, exact escaped-Unicode canonical-size bounds,
no bulk-content opening in the metadata probe, explicit non-detection/non-acceptance
of same-size content corruption by that diagnostic, full-census detection of that
corruption, cooperative time/direct-read/database caps, and an actual isolated
`python -I -` subprocess running the serialized script without the repository or
contract file present. Wrong in-memory schema bytes fail closed with sanitized
output. This remains synthetic qualification; installed state has not been read
through this component by this agent.
