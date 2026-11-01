# Inactive engine mutation admission candidate

`processing/engine_ingress_guard.py` is a **source-only, unqualified prototype**.
Nothing imports it into the running coordinator or nginx ingress.
It changes no live route, network, credential, TrueNAS setting or engine data.
Do not enable it for real photographs. The socket-backed streaming method is
unwired, has at most a ten-second deadline and has no production download bridge.

The experimental upload method accepts a dedicated unbuffered source socket and
an exact multipart body length and SHA-256. It holds the existing host gate and
in-process Queue lifecycle lock throughout, commits a per-upload `started`
journal row before forwarding, and commits `complete` only after the exact body
hash and a bounded framed successful response. Partial sends, lost replies,
errors and timeout retain `started`; later cooperating mutations for that job
hold. A previously used upload ID cannot be retried, including after success.
After the intent commit the guard reacquires the SQLite write transaction and
revalidates the exact claim, task phase, node and gate before sending; it holds
that transaction throughout forwarding. A process writing directly during the
small transaction gap can cause a hold, and external physical writer fencing
remains unqualified. The hash is verified only after forwarding the bytes: a
mismatch is an ambiguous upstream side effect and retains the hold.
After the journal exists, the older control-body upload path is refused. The
buffer is bounded to 64 KiB and the same monotonic deadline covers socket I/O;
filesystem stalls and OS scheduling do not have a hard deadline.

This journal is in the coordinator DB, with no migration, old-binary exclusion,
independent writer fence or qualified startup/repair workflow. A crash releases
the process-held flock; the durable node reservation and pending row do not
prove the engine stopped. The expected digest is caller-supplied over the whole
multipart body; it is not yet bound to the coordinator's `viewer_files` record.
The hard-disabled internal file-bound candidate adds a trusted TaskStore intent
for an exact task generation, job, claim and existing `viewer_files` record.
The guard revalidates that record, constructs the filename and multipart framing
itself, and hashes the raw file socket as it forwards. It scans incoming file
bytes for the exact boundary token across chunks before sending each chunk; a
collision aborts and leaves an uncertain hold. Hash mismatch is detected only
after bytes may have reached the backend, never a pre-send integrity proof.
TaskStore and guard share one SQLite database, but physical source inode
identity, old-writer exclusion, journal migration and recovery are unqualified.
Once its intent table exists the old unbound upload path is refused. The current
coordinator sender does not call this method; there is no public route.

Synthetic tests of the earlier unbound whole-multipart method killed the actual guard process before forwarding, after a fake
backend received a body byte, and after the fake backend's full response. In all
three, the started journal and gate reservation persisted. Queue restart held the
job and a manual guarded attempt refused before source consumption; that restart
refusal was due to the held job. A separate same-process test demonstrates that
the pending journal blocks retry while the job remains running. No real engine,
dispatcher restart, physical engine quiescence or power loss was tested.
No upload route, end-to-end physical isolation, engine storage reservation,
automatic recovery, or readiness for large photographs is established.

## Current bypass and boundary

Read-only November 1 inspection found cluster ingress on 3001 permitting loopback,
the cluster address and the entire default Docker bridge CIDR. Main ingress on
3004 permits loopback and cluster. Both proxy all methods/paths from permitted
clients. Raw backends also publish loopback ports 13001/13004; cluster retains
a backend credential, whereas main's inspected backend credential is empty.
An IP allowlist and cooperative host NodeGate are not job admission enforcement.
Local processes/shared-network containers or existing direct writers may bypass
the coordinator. Docker/root administrators remain outside this software boundary.

## What this candidate actually enforces

The trusted sole-owner coordinator supplies the actual `Queue`, an installation
`OwnerPin`, and fixed node/Engine/generation/backend binding. `OwnerPin` captures
the canonical database path and inode, actual coordinator lock descriptor inode
and startup epoch under the Queue lifecycle lock. A cloned database with the
same authority UUID is not equivalent. This pin must come from the authenticated
owner's qualified installation, never from HTTP caller fields or an arbitrary
caller-selected database; capture itself does not establish remote authority.
Descriptor/inode identity does not independently prove the flock is still held
or establish which database was canonical initially. The authentic Queue owner
must retain its OS lease and qualified selection; those remain external trust
assumptions. This module is not a remote authority enrollment protocol.

Mutations require a distinct coordinator credential plus the original opaque
claim, which is checked against the durable claim hash and private native-task
row. The current global active job, exact node, private engine UUID, worker and
durable phase must agree. An actual local Engine probe must match the fixed pin.
The canonical NodeGate reservation is created durably before init forwarding;
another job's reservation is never replaced. Restart-held/operator-held work,
stale generations, changed Engine identity and unknown routes fail closed.

Both the NodeGate OS lock and SQLite `BEGIN IMMEDIATE` stay owned across one
fixed-loopback HTTP operation. Returning an authorization boolean and forwarding
after releasing these locks would lose this property. The Queue lifecycle lock
also retains its original coordinator lease during the transaction. A nonblocking
socket uses one absolute deadline (at most ten seconds), bounded request/response
and header sizes, no redirects, no environment proxies and no retries. A trickling
peer cannot reset the deadline. Contended SQLite writers use the remaining budget.
The Queue lifecycle mutex is also acquired with the remaining absolute deadline
before the NodeGate, avoiding an unbounded mutex wait while holding the host gate.
This blocks other queue writes for that short control operation; it is unsuitable
for large streaming uploads without a separately qualified lifetime protocol.

Recognized mutation operations are exactly:

- `POST /task/new/init` with the actual assigned `Set-UUID`.
- `POST /task/new/upload/<exact-engine-uuid>` with a bounded fixture/control body.
- `POST /task/new/commit/<exact-engine-uuid>`.

Cancel is deliberately refused until exact body-target binding is qualified.
Remove, restart, DELETE/PUT/PATCH/HEAD/OPTIONS, unknown routes, query aliases and
duplicate headers are refused. A separate management credential permits only
GET `/info`, `/options`, `/task/list` and exact-task `/info` or `/output`; it
does not acquire the mutation locks and cannot write. Large result downloads
are not supported by this candidate.

An uncertain/failed/cancelled exchange leaves the durable reservation in place.
Even a matching successful database result cannot automatically release it:
`completed()` validates the claim/result and then refuses because independent
engine-process quiescence and exclusive network-writer fencing are not qualified.
No caller flag can substitute for those facts. Operator recovery still requires
the existing independently inspected reconciliation procedure.

## Remaining production gates

1. Preserve the current result/task while Viewer acceptance is still pending.
2. Close raw backend bypasses using a reviewed dedicated topology and private
   nonempty engine credentials. Scope coordinator mutation identity independently
   from management reads; possession of a permitted IP is insufficient.
3. Implement bounded streaming and an authenticated cross-host owner/lease
   protocol. Main does not share cluster's Queue filesystem: this in-process
   model cannot simply be mounted there or replaced by a cached snapshot.
4. Independently attest process quiescence before any successful reservation
   release, and qualify recovery/expiry behavior without freeing unknown work.
5. Integrate native client headers, management, cancel, retained result reads,
   maintenance draining and updates. Existing clients are incompatible today.
6. Qualify actual network fencing and concurrent/uncertain operations in isolated
   fixtures, then review a separate explicit idle cutover and rollback plan.

Focused tests use real Queue/TaskStore/NodeGate state and actual bounded synthetic
HTTP peers. They cover unauthorized writes, actual claim binding, held/draining
states, both locks during forwarding, stale/copied authority, fixed Engine
identity, management separation, unsupported streaming, unknown routes, retained
uncertainty and database success that cannot falsely prove engine quiescence.
