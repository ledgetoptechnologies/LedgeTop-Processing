# Disabled local TreeDar spatial receipt candidate

This source-only candidate never starts TreeDar, registers a result, sends a
Viewer receipt, releases the parent gate or deletes files. It requires the
actual issued combined TreeDar intent, exact original parent/generation,
manifest/pipeline, source object and retained reservation, a concrete production
parent guard and prior ODM verifier, and the exact configured
`LidarProvenanceClient.verify` bound to the stage journal. It performs fresh
issuer reads before and after LAS validation, rejects any receipt drift or
revocation, checks the source file hash again, and writes an immutable local
SQLite receipt tied to the original intent, prior ODM proof and admission
policy. It remains false-qualified. A real trusted NAS issuer, its authority,
audience, signing, revocation and deployment are not established here.

LAS validation checks bounded original bytes, a projected metre CRS and its
area of use, point layout/count, class-2 ground density, non-collinearity and
coverage. The optional detailed report returns the complete horizontal WKT from
the same validation pass and its SHA-256. Vertical metre units and datum are
only the issuer's authenticated declaration; no vertical conversion or
independent datum/acquisition observation is claimed. A custom projected CRS
without an authority label currently holds. WKT is not a complete verified
vertical-datum definition and does not establish scientific suitability.

The proof is a local observation, not a remotely authenticated result or custody
acknowledgment. Parent guard checks bracket I/O; they do not continuously fence
remote revocation. The file descriptor used by validation is not held until a
later engine stage, and external writers, mounts, DB rollback and old binaries
are not fenced. Any later consumer needs fresh validation under its own held
authority and physical fence. Combined processing remains held; original inputs
and outputs remain retained.

A nonroot, network-none synthetic container test exercised actual parent
journal/guard/ledger and the configured provenance client with a mocked issuer
and mocked LAS validator. It checked repeated exact issuer reads, durable receipt
idempotence, changed WKT/file, revocation during validation and no additional
engine actions. This did not validate actual LAS science or a genuine issuer.
Eight selected synthetic tests passed in a nonroot, network-none Linux Python
3.12 container using laspy 2.7.0, numpy 2.5.4 and pyproj 3.8.0. They included
the local receipt test, generated LAS/WKT validation, exact source binding,
vertical declarations, invalid CRS and insufficient ground rejection. The
receipt test still mocks LAS validation and issuer responses; the generated LAS
does not demonstrate genuine acquisition or production image qualification.
No production or client data was used and no endpoint was enabled.
