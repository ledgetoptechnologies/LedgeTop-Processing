# Combined foundation and Viewer checkpoint

Exact Processing `751aced554038f1af67025db4e04d1197dd87704` and Viewer
`ae43303b1954f7fca079cce42c19f64482061739` passed the sequential bounded check:

- **148/148 Linux Python tests**, no skips, in 13.834 seconds. Selection:
  predecessor census, candidate projection, enrolled cutover journal, startup
  policy, first quarantine, runtime custody, authority lease, coordinator freeze
  and enrolled transport modules.
- **46/46 Linux Node tests**, no skips, in 9.699 seconds. Selection: five
  source-release suites, GCP API/auto-import/ODM/processing and V2 intake metadata
  bounds.

The one-CPU/768 MiB/128-PID runner used read-only exact sources, the existing
Linux dependencies/Node binary, no external network and a private 128 MiB tmpfs.
Handle 53539 exited zero; its exact task-labelled container was absent afterward.
These selections are not added to older totals or described as full discover.

At Processing `ef1fddc` with the same exact Viewer candidate, **three actual
Viewer/Python paired cases passed** in 37.474 seconds: whole-object intake and
result copy with lost acknowledgement/worker cleanup; V2 range intake and result
copy; licensed controlled conversion with verified derivative publication. All
three use no GCP. The one-CPU/2 GiB runner and its ownership-checked temporary
volume were removed; handle 90054 exited zero and both inventories were empty.

The pair uses real Viewer/storage/transfer/acceptance code and the qualified
licensed converter, with synthetic source bytes and simulated photogrammetry.
It is not an installed node job, real orthophoto reconstruction, NAS durability
test or ten-thousand-photo qualification. The source-renewal changes integrated
after this checkpoint require their own combined qualification.

Viewer `ae43303b` is a fast-forward of the owned paired branch through `36f26839`
from the previous `3c90cebc`. It preserves GCP review changes without modifying
the Viewer owner's unrelated worktree. The Operations production build was
reported green by its owner; full TypeScript checking still has unrelated
workspace/type diagnostics. New explicit height-source/provenance work is not
included in this checkpoint. No deployment, package publication, grant or
TrueNAS change occurred.
