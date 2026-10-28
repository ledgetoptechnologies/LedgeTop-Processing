# Managed update finalization

This describes the source integration in `processing/container_update.py` and
`ops/node_maintenance.py`. It does not install a detector, supervisor, schedule,
image pull, or runtime update.

## Explicit backend binding

`ComposeNode` accepts `expected_engine='odx'|'odm'`; the compatibility default is
`odx`. It never chooses an engine from the response to `/info`. Both dialects
require the exact configured Compose project/service, Docker Engine identity,
and private container-to-loopback port binding.

ODM additionally requires these operator settings in `maintenance.json`:

```json
{
  "expectedEngine": "odm",
  "uploadLayout": "var-www-tmp-v1",
  "processLayout": "node-var-www-v1"
}
```

The configured ingress `backendEngine` must agree. The layout selectors permit
only the verified `/var/www/tmp` staging layout and `/var/www/index.js` server
process allowlist. A single regular one-byte `.gitignore` containing `*` is
allowed; other uploads or processing children block replacement. Unknown layout
names, engine responses, public bindings, and remote Docker contexts fail closed.

Managed arguments use the ingress's actual `backendPort`, `backendToken`, and
`backendEngine`; they do not assume main's backend port. No credential is copied
into public results. Resolved Compose settings are preserved: this integration
adds no CPU, RAM, or PID limits to main and does not remove cluster settings.

## Success ordering

`execute(..., defer_finish=True)` leaves definite replacement success in private
`update.json` at `finalization_pending`. The coordinator node remains `updating`;
the update token stays only in that private receipt. The default standalone
`execute` completion behavior remains compatible; managed `run` explicitly uses
deferred completion.

Under the existing ingress and physical host-gate locks, managed `run` then:

1. Verifies the frozen operation and unchanged Compose configuration.
2. Writes the target image pin and fsyncs the authoritative Compose file and
   directory.
3. Verifies the exact replacement image, expected engine and idle health.
4. Opens only its recorded ingress, verifies old proxy workers drained, and
   checks public health against the pinned backend.
5. Rechecks and fsyncs the pin and verifies backend health again.
6. Calls `finalize_deferred` for the exact plan/token and verifies the resulting
   node identity, target, revision and completion result before marking receipts
   completed.

Only the final coordinator finish reopens node capacity. No successful Viewer
receipt, engine task deletion, or artifact cleanup is part of this operation.

## Recovery boundaries

`deferred_receipt` accepts only a bound, private receipt of definite successful
replacement. `finish_matches` recognizes the exact completed plan/revision and
result. `finalize_deferred` reconciles or retries only that final finish; it
contains no Docker replacement operation.

Managed `run` can reconcile a lost pin checkpoint or a proven completed ingress
open, then finish without replacing the container again. A lost finish ACK is
checked through coordinator readback. Already completed finish readback does not
require the node to be idle again: new jobs may legitimately have started.

Unknown begin/replacement outcomes, unproved ingress opening or worker drain,
changed settings/pins, changed plans/revisions, and held coordinator state require
inspection. They do not authorize another replacement, nginx reload, peer-plan
cancellation, or automatic activation. Known failed replacement retains the
existing failed-node/closed-ingress inspection behavior, including any verified
rollback to the old image. A crash-retained updater lock also requires inspection;
these recovery helpers do not remove it.

Private receipts and credentials must remain private. Public results contain
bounded status, immutable image identities and finalization facts, never the
update token or backend credential.
