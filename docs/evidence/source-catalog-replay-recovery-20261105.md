# Exact source-admission replay across catalog outages

Processing `1025e44` with follow-up `3346383` adds a read-only V2 replay path.
Previously fresh fleet capability validation ran before the existing request
lookup. A lost admission reply followed by an unrelated node outage could make
the unchanged request fail as stale even though it was durably admitted.

The protected, authenticated POST route now looks up only the existing
submission identity before discovery. Unknown requests continue through normal
fresh catalog validation. Existing requests require their exact canonical digest,
the reconstructed persisted request, current source signer/principal trust,
environment, original queue authority and manifest/pipeline/attestation bindings.
Expensive validation runs outside the Queue transaction; a final read transaction
rechecks the exact saved binding before returning current public metadata.
Changed requests, revoked keys or changed/corrupt bindings cannot use this path.
It creates no new queue job, byte reservation, source receipt, claim or execution.

## Evidence

- Ten focused native tests passed, including actual loopback TLS with catalog
  discovery unavailable, changed catalog definitions, current key revocation,
  wrong transport/environment/authority, persisted corruption and a binding
  mutation during signature verification.
- Disabling the new replay hook reproduces two targeted regression failures;
  enabling it passes those cases. No production guard is patched by the passing
  qualification.
- Integrated Linux selection at `3346383`: **104/104 passed, no skips**, in
  10.890 seconds across source replay, runtime, source intake, Reference API and
  HTTP modules. This includes the existing 10,000-object metadata admission case.
  The initial selection exposed an overly broad replay authority check on an
  unknown new request; it was moved after the existing-admission lookup so the
  ordinary new-admission validator remains unchanged. The corrected full
  selection passed without changing that existing test.
- One CPU/2 GiB, external networking disabled, read-only source/dependency mounts
  and an owned disposable volume; container and volume were removed. Log run:
  `ltds-combined-qualification-d395e7dc9ae1`.

The matching Viewer fix is separately committed as
`5caf5d88886e0db0b9e6e0139f3dabd4231e59e2`; its exact LF source export SHA-256 is
`f7747efa3cb4c6279d0f779ea72b6bebe0530b73ef95facbbf0ede4653ff0ba0`.
Paired qualification of that new Viewer export remains pending. This backend
result alone does not prove Viewer outbox recovery, real engines, installed
services, NAS durability or large photo transfer/processing.
