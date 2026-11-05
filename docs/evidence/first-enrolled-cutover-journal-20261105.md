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
