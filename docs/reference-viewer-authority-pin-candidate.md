# Inactive Viewer coordinator authority pin

The candidate v3 API requires an independently provisioned canonical UUID
`expected_authority_id` for the coordinator installation. Authenticated v3
prepare, recovery, status, result and activation transactions compare it to
both the running Queue authority and the one durable `coordinator_identity`
row; missing or changed identity returns 503. Successful responses add
`authorityId` and Viewer must compare it to its independently trusted,
immutable per-intent pin before accepting the binding. The legacy v2 wire
contract is unchanged. The existing Queue creates its installation UUID, but
the pin must be provisioned independently and retained across restarts; never
learn the pin solely from an untrusted response, URL or the current database.

A disk clone or backup restore can duplicate the identity. An older database
snapshot may retain the same identity while losing accepted work, so this
comparison alone cannot detect rollback or concurrent clones. Production needs
an external installation-unique authority registry or hardware-bound lease,
rollback monotonicity, clone fencing and a documented promotion/rekey process;
none is implemented. A cloned database must not be brought online as the same
authority, and ambiguous identity/rollback must hold rather than resubmit. The
candidate remains unmounted, and source, activation, worker and cleanup holds
remain. Internal store methods accept an optional pin for synthetic testing;
the candidate v3 API requires the pin explicitly.
