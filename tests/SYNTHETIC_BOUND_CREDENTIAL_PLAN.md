# Pending synthetic bound-credential offline retry rehearsal

This unexecuted test extends the reviewed three-container offline retry
fixture; independent review of the exact changes is required before container
operations. It pins Viewer commit `3f150734aa5f5359f004a3984d1df806113ddbf6`
and uses its actual `nativeReferenceBoundSenderCandidate.sendBound`,
`nativeReferenceProviderAuthCandidate.withCredential`, and `sendHeld` code.

Only synthetic injected authority callbacks are used. Configuration has a
fixed staging origin and synthetic secret reference. On the offline sender
run, configuration loading and secret resolution supply revision 1 for the
first attempt and revision 2 for the second. The fixture asserts four
configuration loads, two resolutions with revisions `[1, 2]`, and retry
exhaustion while Processing is stopped. Both attempts must use the same held
SQLite payload. After Processing cleanly restarts, a new Viewer process
resolves revision 2 and obtains a matching actual HTTPS replay response. The
Processing test API accepts only two fixed synthetic test credentials and logs
only the revision labels; the test asserts that the original online request
used revision 1 and the replay used revision 2.

These callbacks and synthetic tokens are not a real secret manager or a
production authorization boundary. This shows the wrapper requests refreshed
configuration and credentials for each retry and that its held receipt can
replay against the actual Processing HTTPS/store; it does not prove production
credential provisioning, rotation safety across processes, partner tenancy,
or physical network/power failure behavior.

The existing outer Windows Job Object watchdog and exact-ID, matching-label
three-container plan are unchanged. Fixtures, snapshots, SQLite databases and
containers are retained; no recursive cleanup. No container starts, stops,
signals, or fixture execution have occurred for this version.

## Synthetic results (November 1, 2026)

The corrected reviewed run passed with Processing snapshot
`b54dbd64476e86e6e97008b75568134e52db8e84` and Viewer
`3f150734aa5f5359f004a3984d1df806113ddbf6`. The real bound wrapper loaded
configuration four times and resolved revisions 1 and 2 for two retries while
Processing was stopped. After clean Processing and Viewer restarts, the same
held receipt was replayed with the synthetic revision-2 credential. Processing
observed v1 and v2 on the two online requests, returned created=true then
created=false, and retained one receipt.

Sender fixture SHA-256:
`75b1eeb05ae6dbe06a6860e0cea764d7029ce9f70cce036107edd915bf91cafe`.
Server fixture SHA-256:
`a7d53a7e2f26f0802dc3305a7e6943c5a6228d51ba4acee02afb75c8707bc99a`.
Retained Viewer container:
`d3b45644dcfc6663f69b2f34299de415a5379b0b5dcf84e5307d20430fc66e59` (exit 0).
Retained Processing container:
`90610c2be9e06381e0dda12a4542641c76bcba87b10763b0bda230363828143f` (exit 0).
Retained namespace holder:
`2d0debc49414025203521f67a4c3f5ecb6671e686745860fe2e3cf27d5a32187` (exit 137
after verified owned stop). Outer job active processes: 0; uncertain names: 0.
Retained root: `C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-jjovrtif`.

An earlier run failed the test assertion because the synthetic revision logger
looked up a case-sensitive header name while Node sent lowercase `authorization`.
Its actual sender and store operations completed but it did not establish the
revision-label assertion. Its retained Viewer and Processing containers were
`e5de330c31c982f2784e98955f2a90199632aeb851f266f28dd0ba33cbfe45a9` and
`ca5f6d10296b1c065ffbbeba39b04805a200bf9119f3694277c3b2b9895867df` (exit 0);
its namespace holder `eba30a80381c76389043301a78ff63b0f37fb1c6583332e7012fa0e676be04e3`
exited 137 after owned stop. Outer job active processes: 0. Its retained root is
`C:/Users/fstor/AppData/Local/Temp/ledgetop-acceptance-outer-wklyk_av`.
The fixture was corrected to compare header names case-insensitively before the
passing run. No test artifacts were removed. The scope limitations above apply.
