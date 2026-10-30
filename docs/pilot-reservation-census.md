# Pilot reservation migration census

The optional pilot census and serialized backup profile recognize the exact
empty `viewer_reservations` table created by the 7df TaskStore. The comparison
allows that one schema delta while checking the remaining logical rows, schema,
identity, metadata and non-database file hashes. Any reservation row is held.
The actual backup program copies and checks the full offline state tree.

This is preparation only. Historical updater activation remains enabled for
its previously qualified exact pairs; d738 to 7df is not in that allowlist.
No live activation or rollback calls the new pilot profile. An empty census is
not an admission fence or permission to start old code, and does not prove
there are no active writers or other new-version-only effects. A real update
requires a verified admission fence, exclusive volume ownership, exact image
and configuration identities, a durable verified backup, and a version-aware
operation with crash recovery before this profile can participate in cutover.
