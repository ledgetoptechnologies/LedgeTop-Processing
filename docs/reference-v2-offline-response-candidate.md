# Disabled offline v2 response candidate

This isolated candidate checks a tentative whole/empty response status and
header shape, writes a durable started-stream record, and stages bytes in an
exclusive no-follow task-owned file. It verifies exact length and whole-object
SHA-256, synchronizes file and directory, then marks the local record verified.
Partial files and started records remain held; the same delivery cannot be
replayed. A started record blocks local synthetic adoption and reconciliation,
so SQLite transactions need not remain open during file I/O.

The parser/header names are provisional. A 200 response, metadata match or local
verified file does not prove server authorization, accepted custody, budget or
remote fencing. `OFFLINE_SYNTHETIC_QUALIFIED` and all relevant client and proof
qualification flags remain false; there is no HTTP client or listener. The
cooperative deadline checks operate only between iterator and write calls;
blocking iterators, filesystem operations and OS scheduling cannot be forcibly
bounded. Same-UID external writers, path/mount replacement, database rollback,
old binaries, power failure and automatic recovery are not qualified.

Synthetic tests cover duplicate and wrong headers, ambiguous framing, redirects,
truncated/trailing/changed bytes, empty object, deadline, local adoption fence,
and an owned child dying after its first partial byte. That child was reaped;
its partial file and started intent survived and the same delivery was held on
reopen. This does not establish real Viewer response behavior or proxy safety.
Viewer’s isolated delivery-ledger fixture is not an authorization service; do not
wire it to this candidate. Current-authority proof, signed canonical observation,
ledger epoch/watermark, adoption and predecessor fencing remain open.
