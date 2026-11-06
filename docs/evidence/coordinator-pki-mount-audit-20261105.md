# Coordinator PKI ownership and candidate mount audit

Read-only audit on November 5, 2026. No CA issuance, key/configuration-content
read, permission change, container/service change, or cutover was performed.

## Installed material

The original coordinator container
`525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977`
uses image
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`
and user `10001:10001`. Its existing read-only bind maps
`/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848/coordinator-tls`
to `/run/processing-reference`.

An explicit UID/GID-10001 read-only metadata probe in that exact container found
the directory owned by 10001:10001, mode 0700, and `ca.crt`, `coordinator.crt`,
`coordinator.key`, and `runtime.json` owned by 10001:10001, mode 0600. Each file
was a regular single-link file. Opening each with `O_RDONLY|O_NOFOLLOW` and
checking its descriptor identity succeeded; the private key and runtime JSON
contents were not read. The configured reference path is
`/run/processing-reference/runtime.json`.

Public certificate PEM hashes agree with the separate UID-1000 author copies:

| File | SHA-256 |
| --- | --- |
| `ca.crt` | `d35b9882b6f7244ecc09ff934ddb524a27fb2080ac4350087c5b96529a636318` |
| `coordinator.crt` | `13f855d1578cc5830f5e1345cbec7a3555ef9fd7a9d07df0cdc0dbddedfe4f82` |

Thus existing coordinator-owned material already exists. The UID-1000 worker
copy need not and must not be substituted for it. A loopback TLS connection to
127.0.0.1:18443 returned connection-refused (111), so this audit proves staged
ownership/readability, not a current listener or successful private-key TLS
handshake. Every foreground SSH probe exited.

## Candidate recipe proposal

Let `TLS` be the existing host `coordinator-tls` directory above and `OP` the
already-reviewed cutover operation UUID. Use the existing ConfigStage writer
only for its new operation directory; preserve all original files and modes.

| Host source | Candidate destination | Access |
| --- | --- | --- |
| `TLS/first-cutover-OP` | `/run/processing-reference` | read-only |
| `TLS` | `/run/processing-tls` | read-only |

Bootstrap uses `PROCESSING_REFERENCE_CONFIG=/run/processing-reference/bootstrap.json`;
final uses `/run/processing-reference/final.json`. Both new private configurations
use these listener fields:

```json
{
  "certificate": "/run/processing-tls/coordinator.crt",
  "privateKey": "/run/processing-tls/coordinator.key",
  "clientCA": "/run/processing-tls/ca.crt"
}
```

These are proposed new configuration bytes, not an edit to the installed
runtime. Their configuration/environment/recipe/policy/deployment hashes must
be recomputed and pinned together before any candidate is created.

## Existing contract support and limits

The audited integration code already supports this mapping:

- `ops/enrolled_coordinator_deployment.py:62`: configuration container paths
  must be direct children of `/run/processing-reference`. Mounting only the
  original parent and using a nested container path would fail this rule.
- `validate_environment` binds each exact configured path through one read-only
  bind to the exact host artifact path.
- `read_config_from_predecessor` permits precisely the owned
  `first-cutover-OP/{bootstrap,final}.json` pair and cold-reads it through the
  original container's unchanged parent bind. It verifies both reviewed hashes.
- `ops/enrolled_coordinator_transport.py` rejects overlapping destinations and
  source/loader-control destinations. The two proposed `/run` destinations are
  disjoint and require no validator exception.
- `ops/qualify_enrolled_phase_transport.py` already separates its operation
  config bind from its TLS-parent bind (`/run/phase-tls`) in the full fixture.

No new PKI writer or CA is required. The actual operation's private candidate
configuration pair, pinned bundle/recipe/policy, and later connected listener
and readiness verification remain distinct prerequisites. This document does
not authorize activation or claim those prerequisites are already satisfied.
