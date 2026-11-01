# Native Viewer cross-repository rehearsal draft

This is an unrun fixture draft, not evidence of compatibility. Processing is
based on commit `42da801be55b686e43a78e35b88d32de89f8c5b4` (PR 242). The
Viewer source pin is pending: PR 402's initial head
`f9288330d53f56b44e7505cc7e6d8d9be36c0d93` predates active owner changes and
must not be treated as the approved input. Obtain the owner's stable commit,
export exact Git bytes read-only and verify them before the first run.

The draft aims to use Viewer `processOne`, SQLite and normal result ingestion
against the native coordinator HTTP server and synthetic HTTP engines. It
intends to test one running staging attempt, two queued attempts with
production priority, an offline coordinator and graceful in-process restart,
and one Viewer model-output row per attempt. Its synthetic ZIP contains
placeholder geometry and does not prove a renderable model, real ODM output,
physical storage fencing, Viewer custody, crash recovery, or client readiness.

The fixture and its launcher are not yet qualified for execution. The launcher
refuses to run until a reviewed Viewer commit pin is set. It exports Git bytes,
uses a pinned local dependency image, starts a uniquely labeled container with
`--network none`, limits CPU, memory, PIDs and wall time, and rechecks ownership
and daemon identity before cleanup. Uncertain resources are retained and make
the launcher fail. Evidence and synthetic Git archives remain in a new private
temporary directory; inspect them before any later cleanup. Git, Docker and
filesystem timeouts are individual limits, not a global hard deadline.
The temporary files and fixture control server are synthetic only. Do not point
the fixture at live services, client data, or existing shared storage; do not
run it against the Viewer's uncommitted worktree. No remote cleanup or Viewer
acceptance acknowledgement is part of the intended test.
