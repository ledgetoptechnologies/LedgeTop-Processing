# Legacy read-only delivery candidate

Base `01a85ef1382f0e3b1af70f8c747ef20d05a2e044`. This default-disabled
adapter preserves authorized access to retained terminal legacy Viewer metadata
and cached archives beside the reference scheduler. It is not accepted-copy
evidence, an engine cleanup permission, or installed cutover qualification.

Explicit configuration is `PROCESSING_LEGACY_VIEWER_READ_ONLY=1`, with existing
separate `PROCESSING_VIEWER_TOKEN` and optional staging token. It requires a
reference configuration and persistent state; `PROCESSING_VIEWER_ENABLED` must
remain zero. Ordinary existing behavior is unchanged when the new flag is absent.
The normal application constructs one Queue and its original enrolled leases.
It does not construct ViewerService, TaskStore or a legacy dispatcher. Fresh jobs
continue through the reference API; every legacy POST returns 405 before reading
the body. `/info` and `/options` return 405, so this delivery-only adapter does not
advertise fresh native admission or invent a stale engine catalog.

Read routes are `/task/list` and `/task/{uuid}/info`, `/output`, and
`/download/all.zip`, using the existing query-token environment authentication.
They retain the existing application freeze operation boundary; frozen reads
return maintenance 503. Reads do not change rows, generate receipts, repair a
cache, recover a journal or contact an engine. Wrong-environment or removed tasks
are not exposed. Terminal tasks without exact job/request/generation/node/claim
bindings remain unavailable. Failed tasks that never established those bindings
are intentionally not promoted into trusted retained executions.

A download requires the exact frozen693 receipt shape and identity, private
single-link owner-controlled receipt/archive/input files under descriptor-opened
nofollow directories, exact retained file inventory and input sizes, matching
archive byte count and SHA-256, and a bounded safe ZIP namespace. It does not
rehash photos or claim their historical receipt establishes Viewer acceptance.
Missing cached ZIPs or receipts return explicit retained-data-unavailable 503;
there is no engine fallback. Archives are read in at most 1 MiB chunks, bounded by
the receipt and configured `viewer_max_archive_bytes`. ZIP central directories
are preflighted before ZipFile allocation (8 MiB / 10,000 entries), and declared
expanded bytes cannot exceed that configured archive ceiling. No extraction
occurs. Metadata uses SQLite LENGTH limits and incremental input metadata bounds.
These checks assume the owning service's ordinary original-state lease and no
competing legacy writer; they do not defend against a trusted host administrator
mutating bytes after validation. Controller writer exclusion remains necessary.

Initial bounded Linux qualification passed 7/7 tests in 5.548 seconds, no skips:
actual normal application child processes, original synthetic 693 table/files
and enrolled lease layout, real native HTTP, private file/hash/receipt/ZIP checks,
authentication/environment separation, mutation refusal, and real operator
intent/drain/seal blocking delivery. Original table projections, input/archive/
receipt bytes and original lock/lease modes and bytes remain unchanged after
graceful child shutdown. The existing dependency image
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`
ran with source read-only, UID/GID10001, network none, one CPU, 768 MiB memory and
a 192 MiB /tmp tmpfs. The owned container auto-removed and its exact filtered
inventory was empty. No live PC, NAS, real photo job or deployment was changed.

Final affected-suite qualification passed 48/48 Linux tests in 18.279 seconds,
no skips: eight new legacy cases plus existing node-role, ordinary service,
Viewer HTTP and prototype HTTP compatibility. This includes explicit missing-ZIP
and unknown-partial refusal without cache repair. The owned final container also
auto-removed and its exact all-container inventory was empty. Local compile and
diff checks passed. A native Windows compatibility attempt could not initialize
seven Queue cases because that host lacks rfc8785; the complete affected suite
was therefore qualified on the bounded Linux runner rather than called green
from Windows. Actual installed receipt inventories, public Viewer behavior, backup,
Docker recreation, live processing and first-cutover activation remain separate
unqualified boundaries.

## Descriptor-owner review followup

Independent review of the initial candidate found that a bare Python archive
file could lose its only HTTP pin after an uncertain close, because the generic
Viewer facade catches OSError on close. The followup replaces that file with an
exact typed owner registered in the existing central Gate. It reserves a separate
durable `legacy-archive` operation pin before opening any validation descriptor,
tracks non-inheritable descriptor identities and the bounded directory iterator,
and retains the actual owner object until each descriptor is closed once and
EBADF is verified. Pin retirement requires the original typed registered owner,
original process/thread, closed status, empty descriptor/iterator sets and exact
original epoch/kind. No new database table, lease, execution authority, receipt or
accepted-copy proof is added.

A failed close or a false success with a still-live descriptor becomes typed
uncertainty. The owner and pin remain; no timed expiry, blind FD close retry,
cleanup, or fallback occurs. An unknown pin-retirement commit also retains the
in-memory owner. Ordinary HTTP pin retirement cannot remove this separate pin.
The final normal CoordinatorService shutdown waits for these owners after native
HTTP drain and before runtime/Queue/external-lease teardown. It intentionally
continues to own the process and original leases if close remains uncertain.
The generic Viewer facade is unchanged; only this adapter handles the typed
exception by closing the HTTP connection without writing a second response.
Bytes already delivered before a close failure do not establish any new custody
acknowledgement. Retained journal/descriptor uncertainty requires inspection.

Directory inventory uses incremental scandir and refuses the first unknown or
excess entry, rather than materializing an unbounded list. A failed iterator
close also retains its actual iterator object and owner/pin.

Initial lifecycle qualification passed 10/10 Linux tests in 9.584 seconds,
including an actual blocked HTTP archive read plus SIGTERM retaining both main
and external enrolled flocks until unblock and verified close. An injected
unknown archive close retained its durable uncertain pin and owning process
through SIGTERM without retrying. Only the fault-injected owned child was killed
for test cleanup after proving that negative; the uncertain journal remained.
The owned runner auto-removed and its exact filtered inventory was empty.

The complete affected lifecycle suite subsequently passed 112/112 Linux tests,
no skips, in 32.391 seconds. It includes the twelve legacy tests, coordinator
freeze and Reference runtime, ordinary service/native HTTP/node-role compatibility,
normal quarantined constructor/restart, and the actual source-release service
SIGTERM/blocked-I/O lease test. New negatives prove that a close returning success
while its FD remains live cannot retire the owner, and that directory inventory
refuses its first unknown entry without consuming an unbounded tail. The runner
auto-removed and its exact filtered inventory was empty. Test-only readback
connections were then changed to explicit `contextlib.closing` after Python's
ResourceWarning exposed SQLite context managers that did not close connections;
production read-only connections already close explicitly.

Final qualification passed 113/113 affected Linux tests in 31.331 seconds, no
skips or ResourceWarnings, with the same bounded runner settings. It includes
thirteen legacy cases and all preceding compatibility/lifecycle scopes. A real
SQLite metadata invariant additionally proves that the legacy pin's longest
`uncertain` state is prepaid in both total and CONTROL usage; reporting an
unknown close cannot consume new capacity. Other operation kinds retain their
existing accounting. The exact final owned container auto-removed and its
all-container inventory was empty. Compile and diff checks also passed.
