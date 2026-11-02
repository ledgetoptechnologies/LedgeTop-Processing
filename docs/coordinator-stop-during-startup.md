# Coordinator stop during startup candidate

The coordinator installs its stop signal handlers before loading configuration.
If stop has been observed, `CoordinatorService` skips each Viewer dispatcher,
reference listener and native listener that has not yet started, and closes
owned resources. Prior signal handlers are restored when `main()` returns or
configuration fails. A synthetic child self-SIGTERM test exercises `main`, and
two owned real-Queue/ViewerService/listener tests exercise observed stop before
startup and immediately after Viewer start.

The dispatcher shares the coordinator's stop event and checks it before each
turn; a synthetic stop during its actual start prevents its first tick. This
check is not atomic with a tick, so a signal immediately after the check can
still race with work already entering the turn.

If native listener creation fails after handler construction, the entrypoint
attempts to close the unstarted reference runtime, Viewer service, queue and
authority lease. Cleanup errors are reported, and a synthetic queued job is
preserved in the bind-failure test. If `make_handler` fails after a component
was returned, it also attempts cleanup of the known components, Queue and
authority lease. A constructor that acquires a resource and then raises before
returning its object still requires its own cleanup; this helper cannot see that
resource. Synthetic tests cover failure after ViewerService construction and a
cleanup failure, with Queue and authority lease reacquired afterward.
Queue now closes its own coordinator lock if initialization raises after opening
it. AuthorityLease closes its own descriptor if flock is interrupted or the
diagnostic write/sync fails; the diagnostic file can remain truncated and is not
the authority. Synthetic tests inject those failures, immediately reacquire the
owned lock/lease, and check a queued job. Other constructors and ambiguous OS
close failures have not been comprehensively qualified.
Reference listener startup now closes a returned server if thread setup fails
before the serving thread starts. A synthetic ephemeral-listener test injects
thread construction failure and checks that the listener closes and the queued
job remains. A server factory that raises before returning must clean up its own
resources.
Native thread construction is also covered by the service cleanup path. A
synthetic failure closes the already-bound listener and releases the Queue,
whose queued job remains available to a new owner.
If starting the Viewer dispatcher thread fails before the thread starts, cleanup
does not join that unstarted thread; a synthetic failure checks listener closure,
Queue reacquisition and preservation of the queued job.
A real loopback Viewer multipart upload test with default settings pauses in the server's body read,
then requests graceful stop. Shutdown waits until the client either completes
the body or closes its write side; the former receives 200 and retains exact
input bytes, and the latter receives 400 with no input file. A separate queued
job survives both cases. Additional client-reset and 300 ms stall-then-reset
cases leave no input file and preserve the queued job; shutdown remains waiting
during the observed stall. This is an in-process fixture, not a deadline for
stalled clients, a process-crash test, or host ingress fencing. Socket inactivity
timeouts do not impose an absolute deadline on a client that keeps sending.
In a synthetic two-file request, the first file commits before a reset during
the second; the first survives reopening, an identical direct store retry is
accepted, and different bytes under that name conflict. This does not establish
that a real Viewer automatically retries an ambiguous response.

An experimental `Config.stop_upload_reads` option (default false, with no
environment switch) registers native Viewer upload sockets and interrupts their
read side when shutdown begins. A loopback client dripping a second file was
interrupted while a previously committed file and a queued job survived; an
identical direct store retry succeeded and different bytes conflicted. The
client may lose the response and must reconcile or retry; actual Viewer retry is
not proven. Default shutdown retains its prior behavior. Interruption covers
only registered native Viewer upload socket reads; buffered data, filesystem
sync, SQLite, other handlers and thread joins can still delay shutdown. It is
not a hard drain deadline or a host/network fence.
These tests do not fence
host restart, direct engine access or other writers and do not authorize a live
coordinator replacement. The admission-drain startup gate remains disabled.
