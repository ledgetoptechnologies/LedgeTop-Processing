# Disabled bound-job v1 exclusion experiment

This candidate prevents the current reference v1 Viewer and worker code from
acting on a job in the synthetic owned-binding registry. The v1 submission,
grant and acceptance checks run in their SQLite transactions; status and
receipt relay checks use the same read snapshot as their data read. A bound
queue head holds worker v1 discovery and claim, preserving order rather than
skipping the head. An invalid or partial candidate schema holds v1 operations
that would otherwise touch it. Existing unbound v1 requests remain supported.

This is **not deployable**. The binding experiment is disabled, and there is
no protected issuer, credential lifecycle, anti-rollback anchor, physical
writer fence, nonempty migration or qualified v2 worker. Old binaries, direct
database access, unrestricted host administrators and Queue mutation APIs
remain outside these guards. If an older writer drops every candidate schema
object, current v1 code cannot infer that the binding once existed. Some v1
delivery stages use separate transactions; safety of their preliminary check
depends on the candidate's rule that existing unbound v1 work is never
retroactively bound and on excluding older or privileged writers. No output or
cleanup authorization follows from this experiment.

Loopback tests exercise the real v1 listener with a candidate-bound job,
blocking Viewer submission, status, result and acceptance routes and worker
discovery, claim, result, failure, phase, reservation, recovery and receipt
relay routes. A deliberately synthetic preexisting valid claim gives successful
worker receipt and claim-state responses with the guard patched off, and 409
with it on. A separately prepared ready result and valid acceptance likewise
succeeds only with the guard patched off. These patches exist only in isolated
tests. Worker terminal-acceptance and transfer-grant HTTP tests do not yet have
otherwise-ready positive controls; they must not be read as proof of those
complete data paths. The concurrent create-vs-v1 test uses one Queue owner and
does not prove independent process or host exclusion.

The scoped candidate, HTTP, reference protocol/store/worker/delivery/failure/
recovery and Queue suites ran 156 tests on Windows (136 passed, 20 platform
skips) and 156 tests as an unprivileged WSL Ubuntu user (all passed). In the
WSL run, owned synthetic child PID 376 exited 75 after commit and clients 380
and 381 exited 0; owned threads were joined. Their retained synthetic roots
are `/tmp/lt-owned-binding-2gh67c1y` and `/tmp/lt-owned-binding-37j3ht90`.
Other synthetic roots were printed and retained. No production system was
changed and no previously rejected cleanup was retried.
