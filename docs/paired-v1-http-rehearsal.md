# Paired Viewer v1 acceptance rehearsal

This is a synthetic test of the actual Viewer `ReferenceProvider` client
against the actual Processing reference HTTP listener and store. It does not
connect to a live service or use a production credential. An explicit fixture
fetch adapter permits only two paths at the fixed synthetic origin and routes
them to the owned `127.0.0.1` listener; transport is plaintext loopback, not
the production TLS path.

The Processing runtime source is pinned to commit
`3e4c5eacfd4df22169b9b08ae5e4b90c9e7da57a` and the Viewer source to
`7c526b49d78423372c29eac98b95a16a270c2bd3`. The exact shared golden SHA-256 is
`5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41`;
the client acceptance digest is
`5f2056f679638551e0f00034485bb2600f9d12c4cb5049ff7ea5e081e440b504`.
The Windows Node executable SHA-256 for this run was
`b3094d0b49f9ad602262a9921551737bb97637c05dd357a06ae98188d7290aa3`.
The test checks the tracked source state and selected fixture before use and
rechecks Viewer state and the executable afterward; this is not a system-wide
writer fence against concurrent privileged changes.

Run from the Processing worktree root with the clean Viewer commit checked out:

```powershell
$env:LT_VIEWER_PAIRED_DIR = 'C:\Projects\viewer-v2-delivery-ledger'
$env:LT_VIEWER_PAIRED_COMMIT = '7c526b49d78423372c29eac98b95a16a270c2bd3'
python -m tests.paired_v1_outer_watchdog
```

On 2026-11-02 UTC the run exited 0: one test passed in 0.495 seconds. The Viewer
client fetched the synthetic manifest, sent the receipt, discarded the fully
received committed response to simulate uncertain delivery, and retried the
identical request. The actual Processing response had `created: false` and the
expected receipt ID and digest; exactly one acceptance was stored. The owned
listener was closed and its thread joined; successful outer Job completion
includes the watchdog's no-active-descendant check. Retained synthetic root:
`C:\Users\fstor\AppData\Local\Temp\lt-paired-v1-http-kg5vyjh6`.

The first response was received then intentionally discarded: this does not
prove behavior for a real network interruption, process or host power loss, or
Viewer restart. It does not execute Viewer ingestion/bridge publication,
verify genuine Viewer custody, qualify authentication or writer fencing, or
authorize cleanup. The immutable committed receipt is still metadata only.
