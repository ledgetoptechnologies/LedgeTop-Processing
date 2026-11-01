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
