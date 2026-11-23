# Future-source canonical ingress bridge

Source candidate only. No tests or host actions for this bridge, existing
operation edits, ingress changes or maintenance replay were performed by this
source author. Parent qualification is recorded separately below.

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
The lock profile is sealed in a NEW preparation: UID/GID1000, supported mode600
or mode664 within an owner1000/mode700 nonaliased ingress root. Mode664 comes
from the supported user's CLI/umask002, not a permission exception for public
parents. Every use rechecks exact root/lock identities and recorded mode; drift,
world-writable locks or a nonprivate parent hold. No lock chmod is performed.

Recover-close may re-attest only already-owned closing/closed evidence with the
same request/master, exact canonical closed bytes, drained original workers and
HTTP503. It never sends another HUP. Open-state/closed-config, foreign requests
and uncertain proofs hold; this is not adoption of an old operation's mismatch.
The recreation guard now checks the same exact canonical closed bytes.

The combined phase journal/CLI retain only known exception classes and fixed
underscore failure codes. Raw paths, credentials and exception text are redacted.
This includes the actual CleanupHeld ingress_readers_not_drained class/code.
Synthetic source regressions cover metadata preservation, close/open receipts,
the actual open-state/closed-config mismatch, lost HUP without signal replay,
foreign evidence and failure-code custody.

The already-held e8 operation remains frozen. A separately reviewed recovery
continuation must preserve its original prepared/state/TMP bytes, use a new
recovery request linked to the original owner and explicitly qualify any new
source. This future helper does not reset stages, replay BEGIN, reopen admission
or authorize cache deletion.
The supported canonical CLI's current four-key closed receipt has no retention
tags. It must not be relabeled as a receipt produced by this future helper.
Recovery needs its own independently bound adoption evidence: original closed
receipt bytes/hash, current master/worker/config proof, original e8 plan/owner
and new API recovery receipt/token/revision. That continuation is not implemented
by this source change and is not accepted merely by inserting fields into e8.

## Parent qualification of frozen1161 only

Root independently reviewed1161 and ran its exact canonical Git archive on the
main PC:153 cases,148pass/5expected skips,2.031s suite time, exit0/noOOM. Output
SHA25681addabccf1d607cafbf48150abc1958db8b803d2e3bec8abbb0236189d19b8d.
All248 regular archive members matched Git blobs, with no CRLF-only or other
differences. Root reported the exact owned container/source/scratch removed.
The redacted evidence JSON records full hashes and observer scope. This does
not qualify the separate recovery continuation, authorize adoption of old e8,
or establish a live installation. No approved private manifest is published.
