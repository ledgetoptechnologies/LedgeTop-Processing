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
