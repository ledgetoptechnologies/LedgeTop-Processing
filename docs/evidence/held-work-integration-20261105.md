# Held-work recovery integration

The independently reviewed original held-work commits `cb5082b`, `2817960`,
`6433613`, `531797c`, `825397f`, `c8443a2` and `86bb95f` were cherry-picked
individually as `7af6e9b` through `9126aa2`. No merge of the older branch tree
replaced the integration branch's newer freeze, source-release or lease work.

At code head `9126aa2`, **332/332 selected Processing Linux tests passed in
239.595 seconds**, no skips. The subsequent focused Viewer selection passed
**26/26 tests in 962.829 milliseconds**, no skips. This expands the prior
178-test selection with worker holds, worker API, recovery, engine worker,
Driver and routing modules. Existing actual Viewer pairings, signed cleanup,
GCP preservation, normal rerun/terminal hold repair, legacy read-only lifecycle
and frozen restart remained in the selection. The one-CPU/2 GiB owned runner
and scratch volume exited 0 and were verified absent.

The changes distinguish recoverable observation from sticky unsafe-init or
physical-reservation holds. A returning exact engine task can resume its normal
outbox path. Coordinator restart recovery retains its original authority.
Missing tasks require a corroborating task-list census. Overtaken staging or
lost claim replies reconcile through an exact durable withdrawal, so a delayed
original POST cannot later admit a duplicate. A local intent retires only on
the closed own terminal claim-state response and proven absence of local
effects; claim-shaped malformed replies remain retained.

The tests use synthetic engines and controlled local services. No processing
PC, live provider, NAS, package tag or installed container was changed. Claude's
scratch fence, its accounting-version rollback latch, settlement refinements
and expired source-grant renewal are separate pending work and are not covered
by this passing integration claim.

The later operator-only custody observation is separately qualified by its
owner's four actual-service tests; it supplements physical lease and projection
proofs and does not itself authorize a cutover.
