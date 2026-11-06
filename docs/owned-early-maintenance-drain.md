# Owned drain before container qualification

This extends the existing updater and coordinator node plan. It does not create
another controller, enable a timer, perform OS updates, or activate maintenance
on an installed host.

The default four-field schedule window keeps the existing next-idle behavior.
An explicitly reviewed window may additionally contain:

```json
{
  "prepare_before_qualification": true,
  "after_job_id": "00000000-0000-4000-8000-000000000001",
  "drain_at": null,
  "not_before": null
}
```

The original four offset/wait/drain fields remain required. The example job is
a placeholder for the exact currently running job on the chosen configured node.
Alternatively, supply both absolute UTC `drain_at` and `not_before` timestamps.
An exact job or absolute boundary requires this early preparation mode; it is
never replaced with whichever job happens to be current after qualification.

## Existing ownership path

1. Discovery retains the immutable registry candidate and current host baseline.
   Before any pull or startup qualification, the supervisor journals a drain
   intent. The adapter journals one exact operator request and submits it through
   the existing `schedule-owned` endpoint.
2. New owned schedule schema 2 explicitly declares `qualificationPending:true`
   and a null target engine version. Its target image is the discovered immutable
   OCI config digest. The coordinator atomically validates the exact running job
   or time boundary and installs its ordinary scheduled/draining node plan.
   Queue claims stop at the boundary while existing work retains ownership.
3. Running or held work, an occupied physical Gate, or a non-idle backend blocks
   staging and qualification. Both remain idle-only. The readiness check must
   prove this exact owned drain is still current; another plan or an operator
   cancellation cannot serve as preparation authority.
4. Once idle, the existing staging and startup/API qualification run. The staged
   image must equal the originally declared config digest. A different runtime
   image representation holds instead of changing the target. This early mode
   does not silently reinterpret a manifest-store identity as a config image ID.
5. The qualified receipt supplies the exact engine version. New owned binding
   schema 3 references the original drain request ID/hash, plan, authority and
   fresh node revision. A single coordinator CAS binds that version and records
   an immutable receipt. It cannot change the job, times, target or plan ID.
6. The existing qualified run, worker version handover, health checks, supported
   rollback and reopen path continues using that same plan. Until binding,
   begin-update is refused for this explicitly pending plan.

The owned schedule table retains both immutable request/receipt pairs. Historical
schema-1 schedules and unbound versions retain their previous meaning and hashes;
they cannot acquire binding authority by being old, null-version plans. Existing
read-only receipt lookup reconciles lost prepare/bind replies. An unknown issued
mutation is not automatically posted again. Candidate baseline and generation
remain bound through every step.

Qualification failure or an uncertain outcome leaves the node drained and the
durable intent intact. Existing exact-plan cancellation is available before the
update starts; the updater does not broadly cancel, delete journals or restart
work. An operator must account for any uncertain qualifier effects before
reopening. No SQLite restore or source/result cleanup is part of this workflow.

## Qualification boundary

Prepared tests cover real coordinator transactions, exact after-job draining,
concurrent binding/replay, unchanged legacy receipts, current-plan checks and
the real updater/HTTP/managed path with synthetic Docker transports. They do not
prove installed systemd operation, real container replacement or production
worker handover on this revision. Keep update timers disabled until those
installed checks pass. Dynamic-node host/render bindings are described in
`configured-updater-inventory.md`.
