# Source reader custody prerequisite

This candidate adds source-specific reader ownership to the existing coordinator
queue. It does not expose a source-release endpoint or delete any source file.
The authenticated release request, deletion journal/executor, retained-source
policy, Viewer hold retirement and installed NAS qualification remain unfinished.

The Linux-only registry uses a private `source-readers/lease` below the local
queue state directory, never below the NAS spool. Its exact identity, marker,
local root, database inode and queue authority are durably bound. A new process
can supersede old registered reader pins only after it acquires that same
exclusive flock and validates the persisted binding. Time, PID liveness and a
generic daemon lock do not authorize retirement. Unknown epoch rows remain held.

Actual full-object and range broker reads reserve a pin before opening bytes,
then recheck the grant, job state and per-job/generation fence after opening.
Full-object hashing occurs outside the queue transaction. The managed reader
has no raw file-descriptor method; callers must not bypass its private internals
or duplicate the underlying descriptor. EOF, read failure and HTTP abort close
the known descriptor before releasing its journal pin. Duplicate attachment,
uncertain close/commit, changed lease/root or unknown ownership retain the pin
and lease. Queue shutdown refuses to release its authority while such handles
remain. Shutdown joins HTTP requests before closing this reader session.

The internal per-job fence blocks new reads, initial/replayed source grants,
renewal and successor acknowledgement. Existing registered streams can drain;
other jobs and nodes do not require a global idle boundary. Fences are persistent
and exact-replay-bound, and they grant no permission to delete. Reader sessions,
pins and fences count against the existing metadata quota, reserving terminal
state space so closing a reader does not need an extra quota allocation.

Qualification uses synthetic source bytes in a temporary local spool with an
explicitly injected mount probe. It exercises real Linux flock/descriptor and
SQLite lifecycle behavior, full/range EOF and abort handling, fence/open races,
grant changes/expiry/clock rollback, blocked NAS-hash simulation without blocking
the queue, quotas, process-death recovery, and refusal to unlock live or uncertain
ownership. This is not NAS durability, physical power-loss, installed runtime or
real photo processing evidence. Existing worker-result ScratchLedger readers
and the scheduler are unchanged.

Final isolated Linux suite: **93/93 tests passed, no skips**, in 9.143 seconds
across `test_reference_source_readers` (14 custody cases),
`test_reference_source_intake`, `test_reference_source_range_broker`,
`test_reference_runtime` and `test_reference_http`. Tests ran with one CPU,
1 GiB RAM, read-only source/root, disabled external networking and a private
192 MiB temporary filesystem in image
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
The task container exited and was removed. Fork coverage includes inheriting a
locked Python mutex and refusing source operations before touching that mutex.
