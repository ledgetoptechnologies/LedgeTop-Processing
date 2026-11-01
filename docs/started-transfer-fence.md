# Inactive started-transfer fence and lifetime pin

This is an offline migration and guarded-upload prerequisite experiment. It is
not enabled or wired into production. The migration refuses an existing upload
journal or an active job, installs the known upload schema and four attested
SQLite triggers, and protects updates/deletes of the matching job and Viewer
task while an upload journal row is `started`. This conservatively blocks
operator hold and progress updates too. A killed upload leaves `started`, so a
cooperating old transaction cannot accidentally terminalize the job. The
migration requires an independent externally fenced idle coordinator; checking
SQLite alone is not an external fence.

`Queue.pin_transfer` retains the current process's coordinator lease while
allowing other threads to acquire the lifecycle mutex. The bound upload path
uses the pin and checks migration attestation, but still holds the old Queue
lifecycle mutex and SQLite transaction for the actual network stream. No longer
upload deadline is enabled by this change. Releasing those locks requires
pre/post transaction refactoring and independent tests before it is safe.

The fence is not protection against a writer able to update/delete the upload
journal, alter/drop triggers, replace the database or access the engine
directly. Existing legacy unbound code and older executables are not qualified.
Neither a timeout nor process death proves a NodeODM task stopped. The worker
must preserve the started journal and physical gate until separately reconciled;
there is no automatic retry, expiry, reset or operator bypass here. Schema
installation and the tests make no change to live nodes.
