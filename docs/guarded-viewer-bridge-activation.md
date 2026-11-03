# Synthetic guarded Viewer dispatch: activation boundary

The `viewer_require_guard` flag and injected `guard_probe` on this branch are an
**isolated scheduler experiment**, not a production safety control. Leave this
flag disabled. The current application does not configure or authenticate a
node-local readiness reader, nor select a guarded engine transport.

A readiness observation can become stale before dispatch. A Boolean callback
can also return `True` while ViewerService uses an ordinary NativeEngine and
sends unguarded mutations. The tests explicitly demonstrate both cases. In the
race fixture the actual node-local guard rejects a competing mutation and the
Viewer job remains held; an unguarded engine would lack that protection.

Before activation, the existing Viewer queue needs an authenticated, node-local
mutation transport with independently verified node/authority/job/engine-task
generation binding, durable reservation and journal before every mutation, and
protected private upload handling. The physical engine must be unreachable to
other submission routes, including obsolete coordinator versions; rollout and
rollback must enforce that boundary. Readiness is advisory only, never a
substitute for that atomic reservation.

Completion currently frees the Viewer queue slot on native status 40 even if
the host guard still holds its reservation. The experimental readiness check
prevents selecting that host while the guard reports occupied, but no live
reader or safely acknowledged release exists here. Failed, cancelled and
ambiguous jobs must retain their data and ownership until termination and
handoff are independently confirmed. The test-only in-process bridge copies
synthetic files locally and supplies a fixed Boolean transport verdict; it is
not a real TLS path, process-isolation proof, or a verified Viewer acceptance.
