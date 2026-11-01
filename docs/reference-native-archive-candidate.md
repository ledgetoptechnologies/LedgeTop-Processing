# Reference-compatible native archive candidate

This isolated main-based candidate builds on the coordinator-only review
boundary in `coordinator-release-boundary-evaluation.md`, with the shared
admission, concurrent native archive capture, completion wake, and killed-child
inspection changes layered on top. It retains the reference listener and its
configuration path rather than silently disabling it. It does not qualify
optional worker or combined pipeline runtimes omitted by that boundary.

Archive capture is opt-in. A recorded archive path does not prove current byte
integrity or Viewer custody; uncertain partials require inspection. Engine
retention can still expire during outages, and there is no atomic physical
space reservation across all writers. Small jobs wait for the small node, and
route planning no longer applies main's former local byte ceiling; the Viewer
input limit remains separate. These changes need capacity review.

This is not a live upgrade plan. Enabling archive status adds schema that older
strict census/update paths may reject. The actual nonempty installed state,
backup and restore, old-writer exclusion, and rollback after candidate writes
must each be qualified against the exact installed versions before any cutover.
Rollback must preserve all newly captured bytes and task state; it must not
assume the predecessor understands new archive metadata. Synthetic tests do not
prove power-loss durability, actual Viewer acceptance, or safe engine cleanup.
Archive registry and closed-start admission remain disabled and unqualified.

## Synthetic predecessor rehearsal

The running container image was independently identified read-only as
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
with OCI revision `693cb7eca32295da959a4f42399f317577ef8a8a`. An isolated
Linux test exported the predecessor's source at that revision, created only
synthetic nonempty task state with the predecessor, and opened it with this
candidate. A succeeded task, an in-progress task that became held on restart,
synthetic original bytes and result ZIP survived. Enabling archive status added
`viewer_tasks.archive_state` and `viewer_tasks.archive_reason`. A valid
synthetic candidate-era receipt survived restart; reference-configured handler
construction also passed with synthetic config and mocked TLS setup.

The test inspected the resulting database read-only and never ran the old
coordinator against candidate-written state. It therefore does not qualify
rollback. SQLite's added columns remain in the database, and this candidate
offers no qualified reverse migration; enabling the optional archive registry
can additionally create `viewer_archive_registry` but was not enabled here.
The predecessor cannot be assumed to preserve candidate-only receipt or partial
semantics. No live data or real certificates were used in this rehearsal.
