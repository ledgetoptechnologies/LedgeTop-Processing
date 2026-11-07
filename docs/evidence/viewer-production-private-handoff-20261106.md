# Private Viewer production handoff — 2026-11-06

These artifacts are inactive preparation, not authorization to modify TrueNAS
or evidence that the installed Viewer has been configured. Beau controls all
TrueNAS administration. Do not enable these inputs before a coordinated
coordinator/Viewer cutover and rollback plan is ready.

On cluster PC `.89`, the task-owned private directory
`/home/bkoltz/.local/share/ledgetop-processing-reference/photo-pilot-viewer-transfer-20261106/`
is owned by `bkoltz` and mode `0700`. It contains two files owned by `bkoltz`,
both mode `0600`:

| File | SHA-256 |
| --- | --- |
| `viewer-production.env` | `2798c38bd3d2462e669947513166b7e7a318d714c3a52c4d85e6c596df530b57` |
| `processing-ca.crt` | `bda132617eba65dc23683ba8b6890597faaba2bcbf3f02b12207877d1a7c46db` |

The env file contains secret bearer and private signing keys. Do not print it,
paste it in a chat, put it in shell history, or commit it. The source JSON and
signing keys remain unchanged. A read-only comparison confirmed the prepared
source-attestation signer, receipt signer, snapshot key pair and scoped bearer
match the prepared dual coordinator settings. The public CA matches the
coordinator CA and validates its candidate certificate for `192.168.50.89`.
The Viewer settings pin production, provider ID
`f4ad3d2f-d067-48b7-924c-f1c843a1092c`, coordinator origin
`https://192.168.50.89:18443`, and CA path
`/app/storage/config/processing-ca.crt`.

On `.89`, `docker compose config --format json` was run on an isolated temporary
test definition referring to this file; its complete output stayed in process
memory and all 22 parsed values matched the source JSON. On `.90`, an isolated
network-disabled container using synthetic values confirmed that a Compose
service `env_file` injects single-quoted multiline and literal dollar-sign
values intact. A separate synthetic check confirmed `environment` values
override the same keys from `env_file`. These tests did not run against the
installed TrueNAS app. Never display Compose's resolved configuration when it
contains real secrets; privileged Docker inspection can also expose them.

An SSH ED25519 host-key fingerprint observed from the existing authorized
connection to `.89` was
`SHA256:y/s8mVpo03ezMWKDSDmie/zYKUCX7B9GEKTKeLPkf+Q`. Beau must independently
verify the target host key before the first TrueNAS-to-cluster transfer. A
possible user-run transfer uses an `umask 077` private temporary directory and
`scp` for the two files, followed by SHA-256 verification. NAS-to-cluster SSH
credentials and the user's selected persistent secrets directory are not yet
verified. A persistent env file needs a verified root-only ACL, not an assumed
mode alone; its directory should not be mounted into Viewer containers. The CA
is public, but its eventual storage path must be readable by Viewer UID 568.

Before the user applies an app change, verify the actual TrueNAS app deployment
mode supports the intended absolute service `env_file`, identify overlapping
keys in both `viewer-api` and `viewer-worker` environment maps, and confirm the
live provider ID, endpoint, coordinator TLS and exact image. For a Compose app,
both services need the same protected env file and conflicting entries removed
from their environment maps; keeping a conflicting `false` value there overrides
the file. `NODE_EXTRA_CA_CERTS` is read at process start, so both services need
the correctly mounted CA before coordinated restart. Do not assume a TrueNAS UI
save or a Compose example proves either host-path acceptance or installed
configuration. Preserve the existing Viewer and coordinator rollback state and
perform small installed acceptance afterward.

References: [Docker Compose env-file behavior](https://docs.docker.com/reference/compose-file/services/#env_file)
and [TrueNAS custom app settings](https://www.truenas.com/docs/scale/apps/installcustomappscreens/).
