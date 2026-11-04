# Scoped cleanup after the exploratory photo pilot

This procedure is for the one host-owned pilot produced by the pinned launcher.
It is not a general cleanup method and does not authorize removal of any source
photographs, unrelated Viewer jobs, or other containers. No cleanup occurs as
part of launching or monitoring the pilot.

1. Require `pilot-complete.json` and `pilot-receipt.json` to agree on owner,
   attempt and provider-task IDs, and check the Viewer database and model are
   readable. Recompute the recorded archive-member and generated tileset
   hashes. An incomplete, failed, missing or ambiguous result is a hold: retain
   the source copies, database, artifacts, registry markers and backend task.
2. Preserve the final result and its proof in the intended durable destination
   before removing the isolated Viewer storage. The pilot database alone is not
   proof that the result is available in the production Viewer. Verify the
   destination copy and its hashes separately. Never delete the original photo
   directory, and do not remove the last verified result copy.
3. Read `launcher.json`, `launch-intent.json`, `container.json`,
   `pilot-receipt.json` and `pilot-complete.json` from the fixed private
   registry. Reconcile their owner and task IDs and confirm that the one-shot
   registry marker belongs to this pilot. A missing, malformed or mismatched
   record requires inspection; do not infer ownership from a name prefix.
4. Inspect the recorded Docker ID. Remove only that exact pilot container after
   verifying its full ID, owner label, expected name, image and mounted pilot
   storage, and verifying that it is stopped and cannot restart. If identity,
   state or writer activity is uncertain, preserve it and hold cleanup. Never
   stop or remove the shared coordinator or NodeODM containers.
5. Reconcile the recorded staging task with the existing coordinator and
   confirmed result custody before any backend removal. A timeout or a stopped
   local Viewer process does not mean the node has stopped. Do not invoke
   generic cleanup or remove other jobs, archives, logs or node storage.
6. Only after the result has an independently verified retained copy and all
   owned writers are stopped may the owned local input copies and temporary
   pilot data be removed. Recheck the resolved exact target is the recorded pilot
   directory beneath the fixed registry; never follow links or sweep sibling
   paths. Preserve the registry and an audit record. Clear the one-shot markers
   only after reconciliation so another pilot cannot be submitted during an
   ambiguous cleanup.

Do not touch the separately retained synthetic scratch directory whose cleanup
was rejected by automatic review. TrueNAS remains under the owner's control.
