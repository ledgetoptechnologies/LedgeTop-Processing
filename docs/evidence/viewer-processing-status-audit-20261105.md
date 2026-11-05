# Viewer processing status and log audit

Read-only trace of Processing `f7059fb` and the frozen Viewer `7598858b` tree
(identical Viewer content in PR446 `e00d477f`). This is code-path evidence, not
rendered or installed acceptance. No tests or live jobs were run for this audit.

## Premature processing label

`ReferenceDriver` submits internal phase `processing` after input fetch, before
`adapter.start` initializes, uploads and commits the engine task. That phase
flows through `ReferenceStore._public` into Viewer `processingWorker.js`, which
maps it immediately to local `running`. The engine wrapper also marks its row
processing after commit; its poll path treats native queued10 and processing20
as nonterminal without propagating that distinction to Viewer. A claim or
finished upload therefore does not prove actual engine processing.

Keep the internal phase/custody protocol intact. Claude and the Viewer owner
are coordinating a bounded authenticated presentation-state/start-witness
contract. User-facing queued/preparing/running must reflect actual progress,
including initialization, uploads and native queued tasks. Do not merely
relabel every processing task queued or trust status20/elapsed time alone as
proof of a child start. Actual wrapper output/start evidence and physical/task
identity remain necessary. No contract change is qualified by this audit.

## Stale transient transfer warning

The driver re-posts a watch while its recorded stage matches the current stage.
A successful individual fetch saves stage `fetching` without clearing the
earlier `source_transfer_unavailable` watch. Thus a recovered large transfer
can retain that warning until all objects finish and the phase changes. Claude
accepted the fix: clear only the transient watch after proved recovery/progress;
irreversible holds and failure evidence must remain. Positive and refusal-path
tests are required before integration.

## Existing log bridge and remaining display gaps

ODM logs already travel through the actual V2 source-intake client, environment/
generation-bound coordinator API, node TLS/result-reader proxy and authenticated
worker API to the original engine's bounded paged output. Viewer stores its
cursor and polls; the workspace displays logs and assigned node. Before an
execution row exists, assigned/fetching log requests can return503.

The workspace's generic `processing` label currently returns before parsing
detailed ODM stage lines, masking those stage labels even though raw logs are
available. The Viewer owner has this finding. TreeDar/combined log requests are
explicitly unsupported in the reviewed worker path; do not claim their live log
integration is complete. These findings do not qualify node activation, real
processing, NAS custody, output delivery or cleanup.
