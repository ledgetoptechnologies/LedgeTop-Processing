# Future-source canonical ingress bridge

Source candidate only. No tests, host actions, existing operation edits, ingress
changes or maintenance replay were performed by this source author.

The original retention helper changed nginx bytes and drained workers without
writing canonical node_ingress/state.json. Cleanup correctly refused open state.
Its substring replacement also retained proxy directives that differ from the
canonical closed configuration. This successor resolves both source mismatches.

Runtime.change_ingress uses supported Ingress.change with an explicit metadata-
preserving writer. Existing ingress.lock is opened without creation, checked and
held across the transition. Gateway/container/master/ports, canonical source,
state and lock identities are rechecked. Existing owner/group/mode are preserved
and file bytes/metadata plus parent directories are fsynced. Closing/opening intent
is durable before config/HUP; only worker-drain and HTTP/backend proof attests
closed/open. Canonical state carries the same request ID and gateway master.

Recover-close may re-attest only already-owned closing/closed evidence with the
same request/master, exact canonical closed bytes, drained original workers and
HTTP503. It never sends another HUP. Open-state/closed-config, foreign requests
and uncertain proofs hold; this is not adoption of an old operation's mismatch.
The recreation guard now checks the same exact canonical closed bytes.

The combined phase journal/CLI retain only known exception classes and fixed
underscore failure codes. Raw paths, credentials and exception text are redacted.
Synthetic source regressions cover metadata preservation, close/open receipts,
the actual open-state/closed-config mismatch, lost HUP without signal replay,
foreign evidence and failure-code custody. They have not run yet.

The already-held e8 operation remains frozen. A separately reviewed recovery
continuation must preserve its original prepared/state/TMP bytes, use a new
recovery request linked to the original owner and explicitly qualify any new
source. This future helper does not reset stages, replay BEGIN, reopen admission
or authorize cache deletion.
