# Staged reference deployment configuration check

Run `python -m ops.reference_deployment_preflight` against explicitly staged
owner-private coordinator, cluster-worker and main-worker JSON files. This tool
calls the actual coordinator and worker configuration loaders, then compares
their identity and authentication bindings. It does not create credentials,
state, gates, reservations, Docker resources or network connections.

All referenced TLS paths must resolve on the staging filesystem where the check
runs. The coordinator's ordinary environment configuration must describe the
intended persistent state, configured nodes and native engine tokens. Do not
copy live private settings into reports or put credentials in CLI arguments.

Required arguments are `--coordinator-config`, `--cluster-config`,
`--main-config`, `--authority-id`, `--coordinator-origin` and `--broker-origin`.
The authority must come from the existing coordinator's independently verified
identity. Providing it to this check does not create or prove that authority.
Use explicit intended HTTPS origins; the check does not discover a NAS broker.

For the current small-photo pilot composition, both workers must use the
ODM-only service profile, original option evidence and the v3 three-retained
input-accounting profile. Worker and result-reader credentials must match the
coordinator's corresponding private roles, native engine tokens must match its
native roles, worker identity and optional routing/recovery pins must agree,
and configured environments and result/initial-TTL budgets must be compatible.
Cleanup remains disabled. Configuration with legacy or v4 worker profiles holds
rather than being adopted. This tool neither bootstraps nor migrates them.

Success prints a secret-free `staged-configuration-only` checklist, never a
credential hash or a deployment approval. Failure prints one fixed diagnostic.
The existing configuration loaders verify private regular files and referenced
TLS file structure; this is not proof of actual TLS trust/hostname/key matching,
port mapping, installed image bytes, physical gate ownership, fresh bootstrap,
worker availability/capacity, Viewer bearer issuance/recovery/source custody or
durable Viewer result publication. Those remain explicit deployment gates.

The old fixed `install_reference_coordinator.py` cutover must not be replayed
for a new source/image/configuration. Prepare a new reviewed operation with
actual image/source inventories and rollback evidence after these bindings pass.
The current pilot should start with a separately authorized small dataset;
neither the independent 10k manifest bound nor the legacy upload-size cap changes.
