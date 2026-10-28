# Result-reader ownership and restart recovery

Result readers protect local objects from cleanup while a file is being hashed
or streamed. Viewer acceptance does not permit deletion while any reader is
active. Graceful shutdown stops admission, joins handlers, and closes their file
descriptors before releasing pins. A lost TCP connection follows the same path.

An anonymous legacy pin carries no proof of which process owned it. Process
age, a PID lookup, an expired timer or an empty queue cannot authorize releasing
that pin. Such pins continue to require explicit reconciliation.

The owned-reader implementation uses a dedicated OS-exclusive lease next to the
exact scratch ledger. Its persistent binding includes the lease file's identity
and the ledger/storage identities. A new owner must acquire that same lease
before it can retire an older registered ownership generation. Replacing the
lease file or changing a binding fails closed. The existing daemon state lock
remains an outer guard; the reader lease also protects independently constructed
worker factories pointing at the same ledger.

Starting an ownership generation and retiring its predecessors is one durable
SQLite transaction. Retirement applies only to exact registered read-pin
ownership rows, under the same lease domain. Anonymous, foreign, worker and
downstream pins are not adopted or released. It changes pin ownership state,
not file contents, result acceptance, reservation completion or cleanup grace.
Normal cleanup admission must still pass every existing check.

Each stream obtains a handle before opening its result descriptor. It attaches
the descriptor before validation or hashing. Releasing the handle requires the
descriptor to be closed. The handle closes its attached descriptor itself and
records success before a later request can reuse that descriptor number. If
outside code closes it instead, release requires a closed-FD observation; a
reused number then causes a conservative hold.
Failed or uncertain close/release retains the handle, pin and lease. A session
cannot close while such a handle remains. Handles and session operations reject
fork-inherited ownership. There is no TTL or PID-based release path.

The worker service owns one session for both ODM and TreeDar results. It creates
the session before starting its result listener, then closes it only after the
listener's handlers have joined successfully. A factory or drain failure never
becomes permission to release a lease protecting a live reader. When the entire
owning process dies, the OS releases its lease; a subsequent owner may then
retire only its explicitly recorded old-generation read pins. This proof assumes
the service retains exclusive control of its lease and does not transfer result
descriptors to unmanaged processes.

This recovery does not delete data or establish Viewer acceptance. Missing
acceptance, unknown legacy pins, a changed lease identity, or uncertain ownership
still blocks cleanup. Runtime activation and qualification remain separate from
source qualification.

The source checkpoint passed all 663 isolated Linux tests with no skips. Core
ownership tests include real subprocess death and restart, live flock exclusion,
forking while a parent mutex is held, lost commit responses, first-bootstrap
crashes, replaced lease identities and mixed anonymous/foreign pins. Consumer
tests cover actual result-open process death, successful normal cleanup admission
after acceptance/grace, descriptor reuse, uncertain attachment/close/release,
and HTTP HEAD, abort and drain. Independent source review found no blocking
issue. Evidence is recorded in `evidence/reference-readers-20261028.json`.
These checks do not activate a worker or qualify the NAS-backed live pipeline.
