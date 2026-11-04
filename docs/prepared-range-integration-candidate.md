# Prepared envelope and source-range integration candidate

This is synthetic integration work, not a release or deployment. The prepared
v2 activation and activation-bound source grant remain unavailable; worker
fetch/launch and cleanup holds remain in effect. No principal-authenticated
Viewer grant or acceptance replacement route is mounted.

Focused unittest run: 131 tests, Windows: 2 failures, 32 skipped; Linux as root:
2 failures, 4 errors, 1 skipped. The two failures exercise legacy Viewer
bearer HTTP grant/acceptance routes, which return 404 for principal-owned jobs;
the replacement authenticated route does not exist. They remain failing and
must not be treated as successful end-to-end tests. Four Linux errors concern
TreeDar daemon composition, which intentionally refuses root; rerunning daemon,
private TLS and scratch cleanup suites unprivileged passed 32 tests. The Linux
skip is the paired Viewer test, which requires LEDGETOP_VIEWER_SOURCE_DIR.
Windows skips Linux-only cases. Tests of post-activation mechanics use explicit
synthetic patches for a future bound-grant implementation; they do not prove a
real authenticated grant, activation, data transfer, or Viewer result handoff.
