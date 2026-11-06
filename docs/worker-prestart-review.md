# Worker pre-start configuration review

`workerQualificationSha256` keeps its existing plan/bundle field name, but its
artifact is a closed `ltds-worker-prestart-review-v1` configuration review. It
does not prove installation, current physical custody, service start or readiness.
The operator's reviewed plan pin approves the exact artifact, rather than an
artifact's own boolean asserting approval.

The artifact contains exactly `schemaVersion:1`, `protocol`,
`scope:configuration-review`, `operationId`, `authorityId`, `candidate` (the
unchanged complete candidate pin object), and `workers` in exact configured node
order. No plan or deployment digest is embedded, avoiding a hash fixed point.
Each worker entry contains `nodeId`, `workerReferenceSha256`, `manifest`,
`preparationReceipt`, and `enrollment`. Artifact references are closed
`{path,sha256}` private-file descriptors. The two referenced public-metadata
documents must be a completed installer-valid enrollment manifest and the exact
existing worker preparer's receipt. The config hash must be real, never the
zero-hash/one-byte pre-composition intent.

`enrollment` is one of:

- `{kind:install-intent,operationId,manifestSha256}`: reviewed future install.
- `{kind:enrolled-static,operationId,manifestSha256,receipt}`: an exact historical
  install-only receipt including its installed-tree digest. This does not assert
  that the installed tree still exists or matches today.

The manifest binds source archive/footprint/requirements, dependency archive and
interpreter hash/version, derived unit, real config hash, original host/service,
native engine and canonical Gate. The validator compares config/source/unit,
service UID, engine/generation and installer-derived source/interpreter/config/unit
paths to existing bundle expectations. It never opens the worker private config,
native credential, source/dependency archive or TLS material. Worker-side fresh
checks remain authoritative for those actual files and identities.

Prepare with `python -B -m ops.worker_prestart_review --spec /private/spec.json`.
The closed private spec is `{bundlePath,planPath,entries,outputPath}`; `entries`
is the worker-entry list above. A planning plan may still have an unset worker
gate while composing. The producer reads all referenced metadata first, then
writes one new mode-0600 file through an anchored mode-0700 parent, with exclusive
nofollow creation, fsync and readback. Existing output is never overwritten;
partial outcomes are retained. Standard output contains only protocol/scope,
hash and size. Set the resulting hash in the existing plan gate and descriptor
in the bundle, then calculate the final deployment digest and review the plan.

The concrete driver's global-lock entry validates the artifact before obtaining
the host operation lock or writing the cutover journal. Existing full private
verification validates it again before dispatch. Original authority, schema-2
bootstrap, schema-6 final and independent startup projection policy stay intact.

After the coordinator reaches its authenticated healthy frozen bootstrap, the
existing first-worker activation boundary separately requires fresh installed
tree/config/unit/interpreter checks, original lease/Gate/native/ingress custody,
then its existing journaled start and authenticated readiness. Its actual
post-bootstrap activation receipt remains required before final cutover/resume.
There is no new controller, installer or journal.

Old full-phase synthetic `fixtureOnly` / `installedQualified:false` marker bytes
now fail this gate. Their prior source-specific forward/rollback results are
historical; adapting that fixture requires real pure-valid completed preparation
metadata and installer-derived layout. The focused tests use the existing actual
pure worker composer and loader with synthetic private inputs. They provide no
installed SSH/systemd/native or live processing qualification.
