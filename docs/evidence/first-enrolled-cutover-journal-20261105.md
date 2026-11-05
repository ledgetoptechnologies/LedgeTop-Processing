# First enrolled cutover journal checkpoint

This is the explicit `enrolled-693-first-v2-cutover-v1` update mode's private
phase/journal engine. It does not modify the historical updater ABI, process
queue, app/config constructor, or installed service. No CLI/default driver is
provided yet; concrete transport/helper ownership and strict transform verifier
are separate required boundaries being implemented in parallel.

The mode pins the actual enrolled693 container/image/source and original
restart-policy capture. It binds reviewed target image/source, both ordinary
configuration/recipe hashes, declared transform policy, original physical/env/
recipe/census-key bindings and NAS/pilot/legacy-delivery/worker qualification
artifact hashes. Missing prerequisites block forward mutation.

Every effect has a durable exact intent before the boundary executes. Unknown
responses leave that intent pending; advance cannot reissue it. Inspection uses
only the driver's read-only observe/verify path. Receipt observations bind the
exact intent digest; journal/history/proof/index/admission projections are checked
on reopen. File ownership/mode/single-link rules, fsync+atomic replace and a local
flock protect the private journal. The concrete driver must also reuse the
existing host-wide update lock and durable active-operation binding across receipt
directories; the local journal lock is not claimed as that global proof.

Forward phases explicitly cover original restart disable, graceful stop, exact
original leases, full custody/backup, same-image isolated bootstrap, central Gate
freeze, clean stop, declared frozen transform, final recreation/readiness,
same-plan resume and final restart policy. There is no database-restore action.
Rollback scopes distinguish a still-running original needing only restart-policy
restoration, a stopped original before candidate startup, and a candidate that may
have added declared V2 metadata. New NAS prerequisites do not block compensation
that restores original availability. Unknown effects and admission opening forbid
old-image rollback; rolled-back operations require a fresh rebaseline.

Validation: 19/19 Linux journal/fault tests passed, no skips, in 0.074 seconds.
These use real private/fsynced/flocked files with a **synthetic effect boundary**;
they prove ordering, lost-response inspection without replay, process reopen,
rollback refusal/compensation and corruption/config/transform holds. They do not
prove actual Docker, lease helper, custody copy, image/config recipes or live
handover. One CPU, 512 MiB, UID/GID10001, no network, read-only source/image and a
32 MiB task filesystem. Task container auto-removed; filtered inventory empty.

The normal application constructor/frozen restart prerequisite is separately
qualified in `first-coordinator-quarantine-feasibility-20261105.md`. The concrete
driver and strict declared-transform/rollback verifier must be integrated and
qualified before this mode can activate anything. Gate RPC and helper lifecycle
steps are being refined to keep each mutation independently journaled; no hidden
second controller or generic startup bypass is authorized.


The refined driver contract journals lease-holder creation separately from
starting it, and journals bootstrap Gate intent, drain and seal as three separate
RPC mutations. Early rollback includes its own known-recipe holder-create phase
because the original may have stopped before forward holder creation.

The driver's observe/verify path is read-only with respect to deployment,
processing/source data, leases and APIs. One narrow metadata exception is allowed:
an already owned private operation-receipt directory may receive a bounded,
0600, O_EXCL, fsynced proof artifact bound to the pending intent. Existing artifacts
must be byte-identical and descriptor/hash verified; they are never overwritten.
An ambiguous local artifact creation is inspected by exact path/identity/bytes.
No container creation/start, authority lease acquisition, RPC POST, source mutation
or repair can occur in observation. The main journal stores only its proof digest.


Refinement validation: 21/21 journal cases passed within the combined77-case
Linux run in3.976 seconds. Independent freeze-RPC loss/inspection and holder
creation ordering are now included. The first refined run found one stale test
index after the new rollback-holder phase; the test now advances by exact action.
No production behavior was weakened. The task container was removed and its exact
label inventory was empty.


## Supported not-applied recovery

The journal writes a durable dispatch marker after the pending intent and before
any possible external dispatch. A crash before that marker is now recoverable
through explicit `reconcile_not_applied(disposition="rollback"|"rearm")`, requiring
a fresh complete before-state proof bound to the exact pending intent. A durable
no-effect history entry is recorded before clearing pending. Rollback disposition
blocks further forward advance; rearm is an explicit operator/controller action
and keeps the same intent/request identity. Inspection alone never replays.

After dispatch is marked, absent Docker objects or unchanged state cannot prove
that no daemon RPC remains in flight. Those outcomes stay held. Only a concrete
transport's exact authenticated, code-bound nonmutating rejection can support the
other accepted proof kind. Generic digests/absence do not constitute this proof.
The driver must implement `observe_not_applied` and `verify_not_applied`; unknown
or unsupported evidence keeps pending. Fresh guard checks run before and after
the complete read-only proof. Retained proof history includes the original
marker, intent and closed proof kind and is validated on reopen.

Rollback separately journals retaining the exact final candidate's name before
restoring the original predecessor name. No second Docker rename is hidden in
one mutation and no candidate is deleted.

Validation:30/30 Linux journal tests passed, no skips, in0.131 seconds. Two real
forked children call `os._exit(71)` immediately after the fsynced pending write
versus fsynced dispatch marker. Parent waitpid proves their death; fresh journal
owners recover only the pre-dispatch case, while the dispatched case stays held
even though the synthetic effect is absent. Authenticated-rejection rearm uses
same intent, forged/wrong-intent proofs refuse, and guard/complete-proof failures
retain pending. These tests use a synthetic transport boundary; concrete daemon
and authenticated-rejection qualification remains transport-owned. The bounded
one-CPU/512MiB network-none container auto-removed; exact label inventory empty.


### Distinct dispatch attempts

A logical intent/request remains stable for backend idempotency, but every
possible dispatch now has a separate durable global attempt ordinal and UUID5
`dispatchId`. Completed events, no-effect receipts and a pending unknown outcome
retain their exact marker. Reopen validates the complete contiguous attempt
sequence, marker/intent binding and unique deterministic ID. No-effect proof must
include its exact current dispatch ID (null only before dispatch). A retained
attempt1 rejection therefore cannot clear rearmed attempt2 after a lost response.
History capacity is checked before dispatch or receipt allocation.

The followup suite passed31/31 journal cases within the51/51 combined Linux run
in2.751 seconds, no skips. It explicitly proves valid rejection/rearm followed by
unknown second dispatch refuses the old first-dispatch proof while preserving
the same logical request. The two real child-death cases remain green. Concrete
transport proof production still needs independent review/qualification. The
one-CPU/768MiB network-none runner exited0 and was auto-removed; exact task-label
inventory was empty.


A final pre-write guard requires a valid non-null dispatch marker before any
mutating intent can be completed. Existing desired state does not prove that a
pre-dispatch intent ran. The regression crashes before dispatch, supplies an
already-matching after-state, and verifies inspection refuses without changing a
single journal byte. Explicit complete no-effect evidence still supports normal
rollback. Read-only observation phases remain completable without dispatch.
32/32 Linux journal cases passed, no skips, in0.176 seconds; owned bounded
network-none container exited0 and auto-removed, exact task-label inventory empty.
