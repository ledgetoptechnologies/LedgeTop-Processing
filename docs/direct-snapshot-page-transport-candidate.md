# Private Viewer snapshot page intake candidate

This is explicit opt-in and is not wired into an installed coordinator or host
service. `create_snapshot_server` requires a server TLS context with
`CERT_REQUIRED` and a separately supplied leaf-certificate fingerprint to
`{principal, environment}` mapping. The peer scope comes from the verified
socket, not a header or caller-provided identity. A missing backend returns 503.
No trust store, client enrollment, secret lifecycle, certificates, firewall or
host startup are provisioned by this code.
The separate `create_snapshot_owner` defaults to disabled and reuses the
existing owned listener lifecycle. Closing first closes admission and waits for
active handlers; a timeout leaves the owner draining rather than terminating a
writer. It is not wired into coordinator startup or an installed updater.

The private POST paths are
`/internal/v2/reservations/{reservationId}/source-snapshots/{snapshotId}/begin`,
`.../pages/{index}` and `.../complete`. Begin has exactly canonical unpadded
base64url `intentBase64url` and `signatureBase64url`; intent is at most 1 MiB,
signature exactly 64 bytes. Each source page is raw canonical JSON without a
trailing newline, at most 256 KiB; up to 64 pages and 12 MiB total are accepted.
Completion has an empty body and reconstructs the Viewer LF-framed stream.
Every operation rechecks the mTLS-derived owner/environment against the
current direct reservation; begin verifies the signer pin, signed intent digest
and snapshot ID against that reservation. Pages are immutable, hash-bound and
idempotent. Completion calls the existing source-page inspector and returns a
non-custodial result, never an ACK or permission to release originals.

Journal state is FULL-synchronous SQLite under coordinator authority, with
closed schemas, immutable triggers, count markers and exact replay. Each new
session reserves the signed frame-byte total plus the exact intent bytes and
512 KiB logical overhead against the shared metadata quota. A maximum of 128
sessions is retained. Neither sessions nor pages have a qualified retirement
protocol. At quota or session capacity, later requests hold without evicting
records; this is unsuitable for sustained operation. Version 2 of this journal
refuses version 1 state until a separately reviewed migration; older binaries
likewise refuse the new marker. Logical
reservation does not prove a physical SQLite/WAL space bound. An enrolled
older writer, restored database snapshot, external source writer, actual disk
failure, and online upgrade/rollback are not fenced or qualified.

Only source pages are inspected; selected pages, GCP evidence and raw GCP source
are not supported and must remain held. Snapshot-signing key configuration is
independent of the client certificate registry. No worker custody, content
availability or Viewer original-release claim is made. The end-to-end synthetic
test uses a real direct reservation, source attestation and inspector, mTLS
listener and SQLite journal. Its Viewer data and certificates are synthetic;
it is not an actual Viewer integration.
