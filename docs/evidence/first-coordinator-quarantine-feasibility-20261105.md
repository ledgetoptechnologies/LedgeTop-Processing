# First enrolled coordinator quarantine: normal-constructor feasibility

Base: `b128636325982c3a452e0feb75d5ab5576f34970`, the integrated root containing
SourceReader typed shutdown and the shared service/source shutdown barrier. This
is a synthetic prerequisite proof for the first-cutover controller, not activation
of that controller or a live replacement.

The local Docker runner was inspected before start:

```
network=none
published={}
image=sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8
```

It ran as UID/GID 10001 with one CPU, 768 MiB RAM, read-only image/source,
`--init`, and a task-owned 192 MiB `/tmp` tmpfs. `/sys/class/net` contained only
loopback. The image is the existing dependency image with the exact source
worktree mounted read-only; this is not qualification of a newly packaged
production image.

The fixture uses exact frozen693 19-table DDL, 22 terminal jobs, 7 complete Viewer
tasks, 155 inputs, option evidence and archive receipts, 16 retained owned node
receipts plus distinct closed direct-plan metadata. Original state permissions
are 0755/0600/0644 and the existing external authority lease is 0711/0600. TLS uses
real generated certificate/key material. Both normal child processes execute
`python -m processing.app`, with ordinary private config and real original main
and enrolled authority flocks. No discover callback, server factory, SSL loader,
mount check or backend is mocked.

1. Bootstrap uses ordinary schema2 config, native Viewer mutation facade disabled,
   sourceSpool/push/release services absent, and unbound real loopback backend
   origins. The normal application starts native HTTP and real TLS successfully
   without working backend discovery. A V2 source submission fails closed; no
   source/queue job is created.
2. The controller-side fixture uses real operator HTTP to seal the **same** central
   Gate through intent/drain/seal under one plan/owner. Native jobs, V2 source and
   worker claims are rejected with 503/Retry-After 2 before a declared 100 MB body
   is sent.
3. SIGTERM drains and stops the normal application. Every original column of all
   19 tables, including callback/log/result/credential metadata, matches exactly;
   every legacy input/archive/receipt and original lock/lease byte and permission
   matches. The only tolerated old-table addition is nodes.target_engine_version,
   explicitly required to be NULL.
4. The same image/source process boundary restarts with ordinary schema6 config,
   real SourceSpool settings on the task's real `/tmp` mount, pinned synthetic
   source/Viewer public keys, and empty push targets. The persisted Gate remains
   frozen under the same plan with a new startup epoch. Native and source requests
   still reject before large body reads. Clean shutdown succeeds; original table
   projections/files remain unchanged, spool remains empty, job count stays22,
   new reference/source jobs and active pins stay0, and exactly3 freeze receipts
   are retained.

The first trial on the old18e-based child branch reached the frozen second startup
but failed its shutdown because that branch lacked SourceAgent's typed
SourceReaders.close hook. No production fix was made there. The test was moved to
fresh integrated b128636 and passed with the already-integrated hook.

Validation: `python -m unittest tests.test_first_coordinator_quarantine -v` passed
1/1 in 1.577 seconds. Both owned child identities exited. Container exit0 was
verified, then the exact named container was removed and its filtered inventory
was empty. No live PC, NAS, client data, image publication or deployment changed.

Remaining boundaries: this is process restart in one isolated test container,
not a Docker public-port recreation. The actual first-cutover controller must
still own predecessor restart disabling/restoration, stopped-writer exclusion,
original leases, frozen full custody census and backup, exact image/config/mount
recipes and declared additive transforms, final backend/worker/NAS readiness,
unknown Docker-outcome holds, safe rollback without restoring over new writes,
and same-plan/owner resume. Historical legacy read-only delivery remains held
until accepted-copy evidence or a qualified read-only adapter exists. Opaque
pilot-journal closure is also unresolved. Hot-journal and generic future-V2 update
features remain separate open work.
