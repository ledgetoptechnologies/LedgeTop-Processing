# Exact legacy archive-intent retention

Frozen693 ViewerService.download publishes `all.zip.intent` with the exact
_archive_identity before copying the result archive, and retains that intent
after successful ZIP/receipt publication. The actual read-only diagnostic found
seven such private files under seven already indexed Viewer task directories.
The prior census correctly held unknown paths but lacked this known retained
producer artifact.

The census now accepts only `all.zip.intent` under an already bound current or
historical Viewer directory. It securely reads at most 64 KiB, requires owned
single-link private mode 0600, rejects duplicate JSON keys and extra fields, and
checks exactly version/task/generation/job/node/engine with type and value equality
to the indexed incarnation. Archive receipts now receive the same private-mode
check. Every intent's existing bytes are preserved in the keyed file inventory;
nothing is repaired, rewritten or removed.

`archiveIntents` is a separate aggregate count. An intent is not proof a ZIP
completed or Viewer accepted its copy; `signedViewerAcceptanceProven` stays false.
An absent intent is not synthesized. Metadata diagnostics still do not hash ZIP
or photo bodies. No partial, restore, temporary-link or wildcard cache paths are
accepted by this extension.

Validation: 44/44 Linux tests passed, no skips, in 1.788 seconds (41 census/probe
and 3 optimized-safety cases). New tests retain seven exact intents, reject
engine/type/extra-field/private-mode mismatches, and keep partial/restore/temp
paths held. One CPU, 768 MiB, UID/GID 10001, no network, read-only image/source,
192 MiB task temporary filesystem. Task container auto-removed and filtered
inventory empty. This is synthetic qualification. The parent owns the next
actual bounded read-only probe; no installed/NAS/client data changed here.
