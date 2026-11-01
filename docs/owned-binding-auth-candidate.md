# Disabled synthetic server-owned credential experiment

This branch extends the owner-controlled binding experiment. The test-only
registry resolves a 32–128-byte credential to a principal and environment,
checks both principal and credential status, verifies the pipeline profile,
and creates or replays the job binding in the same `BEGIN IMMEDIATE` Queue
transaction. Only the digest of the supplied secret is stored. Issuance of
high-entropy secrets must be performed by a trusted provisioning mechanism;
the length check cannot establish entropy. First credential revision is kept
in the immutable binding; a later active revision of the same principal can
replay the same binding without changing the first revision.

The synthetic administrator functions allow an overlapping next revision,
credential revocation and principal revocation. Database triggers prevent
ordinary reassignment, deletion, replacement and un-revocation, including
`INSERT OR REPLACE` with recursive triggers disabled. The test profile allows
only the initially provisioned principal; it is not a general operator API.
The binding result is separated into immutable binding and mutable current job
status. The candidate remains disabled and has no network route.

These tests establish neither production authentication nor physical writer
authority. The installer and administrator calls have no authenticated caller,
the server registry is not externally protected, and SHA-256 cannot protect
low-entropy secrets from offline guessing. Old v1 endpoints and old/direct SQL
writers remain outside the experiment. A compromised administrator can alter
or remove triggers, and restoration of an older database can roll back
revisions or revocations. No protected issuer, anti-rollback anchor, audited
recovery, Viewer active-attempt recheck, nonempty migration, or v1 bypass gate
is implemented. Do not deploy or enable this candidate.

The in-process create-versus-revoke test uses the same Queue owner and its
lifecycle lock; it is not an independent-process database-ownership or host
fencing test. Synthetic client process tests use a loopback fixture rather
than the production authenticated HTTP listener. No receipt acceptance,
downstream custody, publication or cleanup authority is established.

The SQL guard checks include explicit `NOT NULL` primary keys and null-safe
comparisons. Tests reject a revocation combined with nulling the principal,
environment, credential digest or revision, with recursive triggers both off
and on. This is a check inside the untrusted synthetic database, not protection
against an administrator changing the schema or restoring old state.

The combined candidate, reference API/store/protocol and Queue suites passed
79 tests on Windows and 79 as an unprivileged WSL Ubuntu user. In the latter
run the owned post-commit child exited 75 (PID 401) and concurrent loopback
clients exited 0 (PIDs 405 and 406); server and handler threads joined. The
respective synthetic process-test roots remain at
`/tmp/lt-owned-binding-_z029vex` and `/tmp/lt-owned-binding-b9_jlccc`.
Additional synthetic roots were printed by the test run and retained. No
production system was changed, and no rejected cleanup was retried.
