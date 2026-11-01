# Disabled owner-controlled job binding experiment

This experiment creates a reference job and a Viewer attempt binding in one
Queue-owned SQLite transaction. It returns no database connection and accepts
no callback. An exact retry under the same binding, attempt, synthetic
principal, submission, environment and input/pipeline identity returns the
existing job. An existing unbound v1 job is held rather than retroactively
assigned a principal. Binding or attempt reuse and a changed pipeline profile
hold; failed binding insertion rolls back the new job. The unchanged public
v1 submission method retains its existing transaction and validation path.

`QUALIFIED` remains false. The empty-only installer and immutable singleton
pipeline profile are synthetic test scaffolding, not trusted provisioning.
The principal is supplied by the caller and is **not authenticated**. There is
no issuer, credential revision/revocation, anti-rollback anchor, nonempty
migration, Viewer active-attempt check, v1 endpoint bypass prevention, or
physical writer fence. Legacy binaries and administrators can bypass these
tests. Do not enable, install or deploy this candidate. It does not establish
result custody, receipt acceptance, publication, or cleanup authority.

Windows synthetic tests in this branch cover failed binding insertion, exact
retry, changed binding/attempt/principal/environment and profile, legacy
unbound jobs, and same-owner serialization. In one loopback rehearsal, two
separate clients raced identical requests into a synthetic handler using one
Queue owner: client processes 113004 and 139300 exited 0, one created a job
and the other returned it. This is not the production HTTP listener or an
authentication or multi-owner fence test. In another test process 150472
exited 75 after a successful committed submit before sending a response; a
reopened Queue returned the same job on retry. That is process termination,
not power loss or a production network failure. The server and tracked
handler threads exited, and no child required a kill.

Those test roots are retained at
`C:/Users/fstor/AppData/Local/Temp/lt-owned-binding-lva_m95s` and
`C:/Users/fstor/AppData/Local/Temp/lt-owned-binding-rws5_xbj` respectively.
Other synthetic `lt-owned-binding-*` test roots are also retained; no cleanup
of earlier approval-blocked resources was attempted. The Windows focused
suite ran 11 tests successfully. Prior API/store/protocol suite runs passed
55 tests before the two process tests were added. No live data was touched.
