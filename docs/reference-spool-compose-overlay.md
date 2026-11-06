# Local queued-input mount overlay

`compose.reference-spool.yaml` adds one explicit bind mount to the existing
coordinator service. Use it together with the reviewed existing coordinator
compose configuration; do not create another coordinator or another state
volume. This file alone does not enable reference intake, start a container,
change the Viewer, or prove a live worker is ready.

Before use, an operator must provision a separate, private host directory on
the intended cluster disk, ensure coordinator UID 10001 can use it, install the
identity marker and qualify mount, storage capacity and restart behavior. The
overlay declares `create_host_path: false`; actual host and Docker behavior must
be tested before use. In a disposable Windows Docker Desktop Compose probe, an
absent Windows bind source was nevertheless created. Do not rely on this flag
to protect an absent or incorrectly selected path. The private reference config
must use `/processing-source-spool` as both `sourceSpool.root` and
`sourceSpool.mountPath`, with the corresponding separately provisioned marker
and its ID, and appropriate limits and disk safety floor. Do not put client
data in an unqualified mount. Existing source and result retention safeguards
remain required; leave automatic cleanup disabled until acceptance and the
approved retention period are qualified.

`PROCESSING_SOURCE_SPOOL_HOST_PATH` identifies the already provisioned host
directory. Protect the runtime config, signer pins, and credentials using the
existing enrollment procedure. A successful Compose configuration check is
only syntax validation; it does not establish installed host path identity,
filesystem durability, job execution or Viewer result delivery.
