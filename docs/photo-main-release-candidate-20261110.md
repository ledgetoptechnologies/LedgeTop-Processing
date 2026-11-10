# Main-based native photo candidate — review only

This local draft starts at `origin/main` `7200fbf49908820f6b76d77a164fd23e191655ae`
and carries the native photo queue, Viewer facade, retained ACK store, TLS
listener and their current reference-startup dependency closure from Processing
`ad7f0f57ee9c8e0803430ea817a1349e12c94e01`. It also carries the separately
tested synthetic paired Viewer lost-ACK fixture and its source-content pin.
GitHub workflows were not changed; the existing main-only CI policy remains.

This is a source extraction, not an installed-image rebuild or a deployment.
The photo components share queue/config/maintenance modules with prototype and
reference callers. Viewer TLS still uses the reference listener's context and
the existing reference configuration; the separate reference listener and
its startup code remain present. This draft does not decouple TLS, include a
deployment compose override, or claim optional TreeDar/reference workflows
are complete. Do not enable additional reference paths merely because their
startup dependencies are present.

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
