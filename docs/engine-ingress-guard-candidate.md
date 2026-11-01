# Inactive engine mutation admission candidate

`processing/engine_ingress_guard.py` is a **source-only, unqualified control-body
prototype**. Nothing imports it into the running coordinator or nginx ingress.
It changes no live route, network, credential, TrueNAS setting or engine data.
Do not enable it for real photographs: its 64 KiB body bound deliberately refuses
normal image uploads, and it has no production streaming/download bridge.

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
