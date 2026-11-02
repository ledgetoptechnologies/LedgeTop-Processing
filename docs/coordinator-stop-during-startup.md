# Coordinator stop during startup candidate

The coordinator installs its stop signal handlers before loading configuration.
If stop has been observed, `CoordinatorService` skips each Viewer dispatcher,
reference listener and native listener that has not yet started, and closes
owned resources. Prior signal handlers are restored when `main()` returns or
configuration fails. A synthetic child self-SIGTERM test exercises `main`, and
two owned real-Queue/ViewerService/listener tests exercise observed stop before
startup and immediately after Viewer start.

The stop check is not atomic with a component's individual start call; a signal
inside that call can still race with initialization or a dispatcher turn. The
existing case where handler construction succeeds but listener construction
fails also requires separate resource-cleanup work. These tests do not fence
host restart, direct engine access or other writers and do not authorize a live
coordinator replacement. The admission-drain startup gate remains disabled.
