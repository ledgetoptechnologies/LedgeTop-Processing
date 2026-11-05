# Source operation fence qualification

Scope: prerequisite only, based on Processing `c217d98`. No deletion executor,
source-release API, NAS change, Viewer edit, or client data operation is included.
The design document records Beau's verified-ready plus 24-hour source cleanup
policy; this patch does not implement that timer or grant deletion authority.

Source PUT, receipt verification, completion and job reconciliation now use the
existing coordinator-owned SourceReaders registry and Linux process lease. Pins
are durable and quota-accounted; they drain under the same permanent per-job/
generation source fence. An additive operation-kind column defaults old pins to
read. Audits atomically exclude concurrent source I/O, including reads, and busy
audits do not remove active upload partials or overwrite release custody. Only
an actual same-thread nested operation may reuse its audit/PUT custody for receipt
adoption. Ordinary read streams remain concurrent and separate jobs stay eligible.

Completion captures immutable manifests/receipts inside the queue transaction,
hashes outside the queue lifecycle lock, and compares the exact captured bindings
again before committing. Full source hashes require stable inode/owner/link/mode/
time metadata. Explicit uncertain owned-descriptor close outcomes are not retried
or normalized to ordinary hash failures: pins and coordinator authority remain
held until actual owner-process retirement. A fresh process must acquire the exact
existing lease before retiring old registered pins. Ordinary verified-closed
source errors drain pins and preserve intended API retry/backpressure status:
capacity 507, changed/unavailable mount 503, object conflict 409, oversized 413.

Final bounded Linux run: **53/53 passed, no skips, 11.967 seconds**:

```
python -B -m unittest tests.test_reference_source_operations \
  tests.test_reference_source_readers tests.test_reference_source_intake \
  tests.test_source_spool
```

Runner: task-owned `ltds-source-operations-20261105`, 1 CPU / 1 GiB, network none,
read-only source mount, existing local Processing qualification image. Its child
process tests are synthetic, bounded and joined; the container auto-removed and
its absence was checked. A follow-up activity index was added after that run;
its fresh-schema and additive-schema DDL were independently checked using SQLite.
The parent combined run must qualify the final integrated revision.

Coverage includes real API streaming PUT blocked mid-upload by a fence; active
pin/refused shutdown until stream exit; no durable upload ACK after fence; rejected
subsequent PUT before source I/O; completion fence race outside the DB lock; exact
receipt drift; metadata quota before effects; audit versus active I/O; rejection
of another thread claiming audit scope; no custody overwrite during a fenced
audit; stable hash identity; ordinary spool error status; and an actual API
ambiguous-close response followed by process-death/reopen retirement.

Limits: synthetic local Linux storage is not NAS power-loss qualification or ACL
proof. The approved 24-hour window, authenticated release-role owner binding,
qualified physical deletion/tombstone recovery, and Viewer operational hold
retirement remain separate implementation slices. Source results and originals
are not released by this prerequisite.
