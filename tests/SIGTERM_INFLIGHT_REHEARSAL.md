# Synthetic coordinator shutdown rehearsal

On the `19111a5` source (based on installed revision `693cb7e`), the
POSIX-only `test_sigterm_inflight_owned` passed under unprivileged WSL
Ubuntu 24.04. The test starts an owned child with the real `make_handler`,
native HTTP listener, and `CoordinatorService`; it mirrors the application's
SIGTERM handler but does not invoke the unmodified application entrypoint.

The child owns its ephemeral port and reports entry to an actual request
handler before SIGTERM. A partial HTTP submission remains in flight. The
coordinator and its Queue lock stay alive until the remaining request body is
sent and a 201 response is received; the process then exits zero and a new
Queue opens the retained state with the one committed job. The only child was
reaped. The synthetic state root `/tmp/lt-sigterm-inflight-mizht2as` remains
retained. No production resources were accessed or modified.

This does not exercise the Viewer facade, reference TLS listener, Docker stop
timeout, old writers, physical network fencing, backup, rollback, or a real
cutover. It does not qualify admission drain or prove power-loss durability.

An additional independently lifecycle-reviewed `test_sigterm_viewer_owned`
passed under the same unprivileged WSL environment. It enables the real
Viewer facade, starts a partial authenticated reservation POST, sends SIGTERM,
and verifies that the Queue lock stays held until the request returns 200 and
the child exits zero. A reopened Queue contains the reserved UUID. Synthetic
root `/tmp/lt-viewer-sigterm-fq5bxkgz` remains retained and the owned child
was reaped. The entry marker precedes route and gate acquisition; this tests
the in-flight reservation only and does not establish the admission barrier,
uploads, downloads, reference listener, or any external writer fencing.

The independently lifecycle-reviewed `test_sigterm_main_owned` also passed
under unprivileged WSL. It invokes the real `processing.app.main()` signal
handler and service; a test-only `ThreadingHTTPServer` subclass binds and
reports an owned ephemeral loopback port and marks accepted handler entry.
An in-flight synthetic prototype submission completes with 201 after SIGTERM,
the Queue lock remains held until completion, and the committed job remains
present on reopen. The child exited zero and was reaped; synthetic state at
`/tmp/lt-main-sigterm-a_c_3ynn` remains retained. The marker precedes parsing
and admission, and this covers no Viewer/reference listener, Docker shutdown,
external fence, power loss, or deployment.

The independently lifecycle-reviewed `test_sigterm_reference_tls_owned` passed
under unprivileged WSL. It invokes `app.main()` with the actual ReferenceRuntime
TLS listener and a synthetic certificate/configuration. Test-only wrappers
select owned ephemeral ports and mark handler entry. A partial authenticated
TLS POST to an unknown reference route remains in flight across SIGTERM, while
the Queue lock remains held. Completing the body produces the expected 404;
the process exits zero and Queue opens again. The owned child was reaped and
synthetic state/certificates remain at `/tmp/lt-reference-tls-sigterm-giy3u8hu`.
This is not evidence of a successful reference mutation, a streamed transfer,
client-certificate authentication, admission fencing, Docker shutdown,
external writer exclusion, or power-loss durability.

The independently lifecycle-reviewed `test_sigterm_reference_claim_owned`
rehearses an actual authenticated reference-worker claim over TLS. A
synthetic reference job is persisted before the owned `app.main()` process
starts. A partial claim POST enters its real listener; after SIGTERM the Queue
lock stays held until the request completes with 200, and the process exits
zero. On Queue reopen, the claim row is intact and the job is held with
`coordinator_restart`. This is a fail-closed hold, not automatic resumption.

The first run at `/tmp/lt-reference-claim-sigterm-chq1sp4h` failed because
the test incorrectly expected a running job after reopen. Read-only inspection
confirmed the expected restart hold and intact claim. After correcting that
assertion, the test passed 1/1 under unprivileged WSL; its retained root is
`/tmp/lt-reference-claim-sigterm-hzj3nxuv`. Both synthetic roots include
private synthetic credentials/claim tokens and must remain private. Owned
processes exited and were reaped. Test-only listener wrappers select owned
ephemeral ports and mark request entry; that marker precedes admission.
There is no claim of real processing, Viewer delivery, full stream handling,
admission closure, Docker behavior, external fencing, or power-loss safety.
