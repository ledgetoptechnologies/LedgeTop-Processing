# Node gateway authentication candidate

This is an inactive, unqualified candidate. `GATEWAY_CLIENT_QUALIFIED` and
`GATEWAY_AUTH_QUALIFIED` are false. Do not enable or deploy this profile.
Those flags guard the included runtime entry points; direct lower-level
`Client`, `NativeEngine`, `ViewerService`, or `ReferenceRuntime` construction
is still callable and is not a universal authorization or deletion fence.

The optional gateway credential is separate from the existing native NodeODM
query token. Client calls use an HTTP header and reject non-loopback plaintext
origins. Non-loopback use requires an HTTPS origin trusted by the client's
normal certificate store. Secrets must be independently generated with a
cryptographically secure random generator; the base64url syntax and length
check do not establish entropy. The credential must not be reused across
coordinator, gateway, node, worker, Viewer, or operator roles.

The legacy NodeODM token remains in the query string because that is the native
engine protocol. Disabling gateway access logging reduces one exposure but does
not prove the engine, upstream proxy, diagnostics, or host logs never record the
query. JSON reflection checks are not a confidentiality boundary against a
malicious engine or arbitrary binary result. Docker administrators and host
root can still inspect credentials, attach networks, or contact the engine.

This branch has not qualified a real Nginx gateway, its non-root UID 101 file
ownership, trust provisioning, TLS hostname/SAN configuration, startup or
restart. Current ingress may restart from an open configuration while its
durable state says closing or opening; the journal alone does not prevent that.
The inspected live gateway runs as UID/GID 101 while its current host config
is owned by UID 1000. An include or key created as UID 1000 with mode 0600
would be unreadable by that gateway; provisioning and verification of the
actual identity and permissions are prerequisites, not solved by this draft.
An owner-controlled startup boundary and failure/restart tests are required.
Header authentication alone is not an engine process fence, capacity guarantee,
safe cleanup authority, or permission to bypass existing maintenance gates.

Any rollout needs a separately reviewed maintenance plan: inventory exact
services and active jobs, preserve the current config and certificates, provision
distinct high-entropy credentials and correct file ownership, verify trusted
TLS and coordinator/maintenance health paths, drain before switching, and
retain a tested rollback that does not lose newly accepted work. A serving
Nginx test must exercise missing/wrong/correct credentials, header stripping,
TLS certificate/hostname rejection, streaming uploads, both open and closed
states, crash/restart windows, log redaction, and unrelated clients. Until then
the existing ACL and runtime remain untouched.

## Synthetic checks

On Windows the focused suite ran 202 tests with 31 platform skips. On
unprivileged WSL the targeted gateway, worker, updater, and ingress suites ran
66 tests successfully. The isolated WSL TLS read test used a synthetic
certificate: a trusted hostname succeeded and a certificate-name mismatch
failed before the HTTP header reached the synthetic server. It does not test
the streaming `http.client` TLS path, a real Nginx gateway, or deployment.
The synthetic TLS roots `/tmp/lt-gateway-tls-test-5wt5tv76` and
`/tmp/lt-gateway-tls-test-guq58kd3` were retained. No live services changed.
