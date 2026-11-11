# Main-based native photo candidate — review only

This local draft starts at `origin/main` `7200fbf49908820f6b76d77a164fd23e191655ae`
and carries the native photo queue, Viewer facade, retained ACK store, TLS
listener and their current reference-startup dependency closure from Processing
`ad7f0f57ee9c8e0803430ea817a1349e12c94e01`. It also carries the separately
tested synthetic paired Viewer lost-ACK fixture and its source-content pin.
GitHub workflows were not changed; the existing main-only CI policy remains.

This is a source extraction, not an installed-image rebuild or a deployment.
The photo components share queue/config/maintenance modules with prototype and
reference callers. Viewer TLS can use the existing reference listener's context
or an explicitly configured standalone certificate and private key. The
separate reference listener and its startup code remain present when their
existing configuration is supplied. This draft does not include a deployment
compose override or claim optional TreeDar/reference workflows are complete. Do
not enable additional reference paths merely because their startup dependencies
are present.

For standalone Viewer TLS, set `PROCESSING_VIEWER_TLS_CERTIFICATE` and
`PROCESSING_VIEWER_TLS_PRIVATE_KEY` to absolute file paths, alongside an enabled
Viewer and `PROCESSING_VIEWER_TLS_PORT`. Both paths are required. The key must
be owner-private on POSIX; paths cannot contain symlinks and files must be
single-link regular files. TLS 1.2 is the minimum. On Linux, OpenSSL loads from
the checked file descriptors when procfs is available; other platforms use
path identity checks before and after loading. Encrypted keys that need an
interactive password are rejected. Startup validates the context before opening
the queue. When neither standalone path is supplied, the existing reference
context fallback is unchanged. If both standalone paths and reference config
are supplied, standalone applies only to the Viewer listener; the reference
runtime remains independently configured.

Seven bounded Windows tests passed: production-priority per-node scheduling,
busy-small-node fallback, large-job waiting, durable ACK replay, HTTPS-only ACK
handling, TLS startup guard and dynamic node order. Viewer and reference runtime
modules imported successfully. These tests use synthetic data and do not prove
Linux behavior, live state migration, process exclusivity, package publication,
or rollback. The pinned Viewer paired fixture has not been rerun on this exact
main-based candidate.

Before treating this as a release: independently review all imported code and
main compatibility; run the Linux Viewer, queue, TLS/reference, maintenance and
paired tests against this exact head; compare the installed image, config,
ports, worker ownership, persisted schema and jobs; prove predecessor upgrade
and rollback while excluding old concurrent writers; and verify that image
publication and deployment are separate. In particular, the historical compose
example does not expose the additional Viewer TLS port and is not included as a
deployment recipe here. No client data or running services were changed.

Disposable state qualification at source base `91701e58` uses exact
Git-archive sources from compared main `7200fbf4` and deployed-source
predecessor `ad7f0f57`. Four low-priority Linux tests passed: legacy
queue-column migration; main-origin queued, running and terminal retention;
predecessor Viewer task, input, archive and one ACK receipt retention; and
old-writer locking. Running records become held at startup. The fixture checks
SQLite integrity and uses synthetic bytes.

Both actual old-main and ad7 source writers were blocked while the candidate
owned its lock, but both reopened and mutated the upgraded state once that lock
was released. This is a rollback gap: the lock alone does not exclude an older
writer during handoff. An independent supervisor and reviewed state/census
procedure must prevent an old writer starting until the handoff is safe. These
tests do not prove live-image or dependency identity, real power-loss migration,
or rollback of running engine work.
