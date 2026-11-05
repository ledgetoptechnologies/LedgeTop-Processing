# Integrated first-cutover primitives

At exact code `2778a8953412d26200844c7979f79e8b4a6dc401`,
the selected original commits `57514af`, `f98c693`, `7843b39`, `2228337`,
`f626e10`, `109e6c8`, `594c8cb` and `2673549` are integrated individually.
The older branch tree and duplicate custody dependency `d25b41e` were not
merged. Existing held-work recovery, legacy read-only delivery and source
release code remain present.

Independent read-only review found and closed three journal issues before this
qualification: permanently stranded pre-dispatch intents, rejection replay
across distinct dispatch attempts, and mutation completion without a dispatch
marker writing history that later reopen would refuse. The tests include real
forked child deaths immediately after durable pending versus dispatch writes,
same-intent rearm with distinct dispatch identity, stale rejection refusal and
byte-identical retention when existing after-state cannot prove dispatch.

The startup policy derives expected schemas from pinned predecessor DDL and
hash-checked source literals in private in-memory SQLite. Configuration hashes
and independently observed runtime custody/physical identity provide dynamic
bindings; the candidate database does not supply its own allowlist. Historical
rows, files and modes remain baseline-bound. This first profile refuses enabled
SourceRelease; controlled enablement and installed cleanup acceptance remain
required subsequent work, rather than a permanent scope reduction.

**121/121 selected Linux tests passed in 10.762 seconds, no skips.** Modules:

- `tests.test_coordinator_predecessor_census`
- `tests.test_coordinator_candidate_projection`
- `tests.test_enrolled_coordinator_cutover`
- `tests.test_coordinator_startup_policy`
- `tests.test_first_coordinator_quarantine`
- `tests.test_runtime_source_custody`
- `tests.test_authority_lease_current`
- `tests.test_coordinator_freeze`

Runner `ltds-cutover-integrated-2778a89` used the existing Python dependency
image `sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`,
one CPU, 768 MiB memory/swap, 128 PIDs, network none, read-only root/source,
UID/GID 10001 and private 128 MiB `/tmp`. Managed execution handle 94349
completed with exit zero. The auto-removed container's exact task-label
inventory was verified empty afterward.

These tests qualify the combined journal, comparator, source-derived policy,
actual normal application/TLS child phases and custody primitives. The concrete
Docker transport/helper remains a separate candidate. No installed container
was stopped, renamed or recreated; no NAS setting, credential, permission,
engine job or client source was changed. This result does not establish live
NAS durability, actual worker maintenance, real photogrammetry, or the complete
Viewer/source-release pipeline on installed hosts.
