# Accepted-copy crash and updater census candidate

This inactive candidate combines the corrected coordinator census and updater
guards with the existing synthetic worker-to-Viewer crash-recovery harness.
The harness uses the actual Processing worker, sender, durable stores and
cleanup executor, plus a pinned Viewer acceptance server. Its engine, photo,
credentials and TLS keys are synthetic. It exercises lost commit response,
worker and coordinator process death, and cleanup intent, quarantine and
unlink recovery; it preserves the accepted Viewer copy throughout.

The harness is opt-in and requires an explicitly pinned, clean Viewer tree,
Linux Node and fixture dependencies. A Linux container run with the pinned
Viewer at `4df1a77bc4a4070558a52177275ac7fdb942ebaa` and a non-root process
passed 100 combined updater, installation, TreeDar runtime, transfer and crash
tests. The Viewer identity and clean state were checked on the Windows host
before mounting that worktree read-only; its Windows-specific worktree path
could not be resolved by Linux Git, so the test used a shim for those two
already-verified Git queries. This is not an in-container Git identity check.

The tests do not exercise production credentials, live data, NAS custody,
runtime installation, automatic cleanup activation or the newer guarded node
path. Nothing here authorizes enabling transfer or cleanup.

Discovery now scans a bounded keyset page and moves past a full page of
unrepresentable results, retaining them for later reconciliation. The cursor
is in memory and resets on worker restart: repeated restarts before the worker
reaches a later page could still delay those jobs. It is not a durable
quarantine or a replacement for operator-visible reconciliation.

Reference submissions now persist whether their configured environment
requires a signed Viewer receipt. That policy survives a later configuration
change, and only signed accepted objects count towards completion when the
policy or an existing Viewer transfer requires it. Legacy jobs with no policy
remain ambiguous and are held for reconciliation; a transfer marked complete
without a signed Viewer receipt is also surfaced as needing reconciliation.
The migration does not establish the history of old records, fence older
binaries, or reconcile a pre-existing unsigned completion automatically.
Full-transfer coverage is recorded only in the same transaction as the exact
durable request and its verified signed receipt. A prior lone signed partial
receipt does not prove that coverage. Pre-existing completed transfer rows
without that new evidence are held for reconciliation; a later environment
configuration change does not silently convert an explicitly non-Viewer job.
