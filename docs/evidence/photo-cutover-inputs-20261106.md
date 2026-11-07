# Photo queue cutover inputs — staging preparation only

These observations are not an executable deployment bundle, worker readiness,
or authority to replay an old cutover journal. Refresh the live observations at
execution and retain the existing predecessor and its complete state.

## Observed on cluster

- Existing coordinator: container `525ca6e24ad3b9e57e2a3be36567471c10745a4aaa8ebc03c7f2d2f215dfb977`,
  image `sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
  start `2026-10-31T09:11:38.823054427Z`, restart `unless-stopped`. Its ID,
  image and authority agree with the existing first-cutover ABI. It retains the
  original `/prototype-state` volume, private reference bind and external lease
  bind. These mount names do not prove exclusive live custody.
- Readback of the predecessor binds the state volume
  `ledgetop-processing-prototype-c3fcb3a-state` to `/prototype-state` and the
  lease directory `cutover-pr92-20261031-01/lease` below the reference root to
  `/run/ledgetop-authority-lease`. Its bridge network publishes 8080 on
  `127.0.0.1:18080` and `192.168.50.89:3005`, and 8443 on
  `192.168.50.89:18443`. These are observations, not permission to replace it.
- Inactive source-verified candidate: commit
  `4705ba3fad2d70f01507aa089aa1f5d2eee4e062`, image
  `sha256:98c37f4b7bcf2ac5626b9368852e8c1d2e9c2553d5606b26b26d3e37dccc7fdb`.
  It was built locally on the cluster from the existing 63c8049b dependency
  image plus exact 4705 Processing source. No installed runtime is implied.
- Prepared staging bootstrap/final JSON SHA-256:
  `4e057983f19d6a0936aee6165348f6f681fd361e3d6225967417eb609e06df17` /
  `4f1318141f5c2f76a46ada07bbd742e49900868836f7b80b214f8b67ccc1f94f`.
  Both are service-private preparation inputs; they have not been staged into
  the original service's protected config mount.
- Local cluster spool bind is prepared but not attached. Its marker SHA-256
  `196741f82a7a6b76ffe523f76f91b0eaa8247e955d86e293cc24334b0a4fc2b4`
  matches its separately captured private identity file. A disposable candidate
  container opened the mount and performed a capacity check; that is not a
  full restart/durability or independent identity qualification.
- A later, separately captured limited observation used the independently pinned
  private marker in the exact candidate image and the actual spool bind, as
  uid 10001. It checked the 10 GiB safety floor plus 1 GiB of available capacity,
  and fsynced and read back a 4096-byte exclusive temporary file before removing
  it and fsyncing the containing directory. The private artifact is
  `photo-pilot-runtime-inputs-20261106/local-spool-observation-20261106.json`,
  SHA-256 `de3a11cc89b389683535a437dc2912f22bafdc0339ee029a5df8838053b43509`.
  Its observed available space was 120,738,426,880 bytes. This is a point-in-time
  filesystem check, not a power-loss or reboot test, a complete qualification,
  or activation-time revalidation.
- Corrected inactive coordinator certificate SHA-256
  `ecb0aa0a308d7e24437560de93252c3b839294f1df460842a92804fc7747c62e`.
  The image recipe must map the corresponding private CA/key and corrected
  certificate to `/run/processing-tls`; no such candidate recipe is yet pinned.
- Cluster worker is enrolled but inactive; protected installer inspect output
  was captured with SHA-256
  `2dae6d607dcb907d47144fba41c4c24c01dce2935e86c272c956700ba338aab2`.
  Its installed-tree digest was independently read back. This is historical
  install-only evidence, not worker readiness. The main worker is not enrolled.

## Missing executable inputs

The existing `enrolled-693-first-v2-cutover-v1` driver requires a new operation
plan and private deployment bundle, pinned bootstrap/final/holder recipes and
environment files, exact config-stage receipts, original physical state/lease
and writer census bindings, reviewed counts/census key, independent startup
policy/transform, and four qualification references. It explicitly requires
both configured workers and fresh authenticated readiness before final resume.
The `nasQualificationSha256` field is a historical ABI name: do not assume an
old NAS receipt qualifies this local SSD spool, or create a NAS mount to satisfy
the name. Determine and test an exact local-source qualification compatible with
the driver before providing a value. Other named qualifications also require
real applicable evidence, not placeholder hashes.

The cluster's original worker-preparation receipt was not retained. A reviewed
source candidate permits an explicit `not-retained` marker only with the exact
separately captured enrolled-static installer receipt; its downstream activation
and fresh readiness checks remain mandatory. This source change is not deployed
or a complete cutover bundle. The main worker is still an install intent without
its original preparation receipt; do not represent it as enrolled or invent a
receipt. Viewer staging source-intake and accepted-copy settings remain unset.

## Rollback boundary

The supported journal's rollback preserves original files and predecessor
container, reacquires original leases, verifies the appropriate projection,
restores the predecessor's name/restart policy and checks readiness. It does not
restore SQLite, delete files, requeue a job, or retry unknown effects. This is
the intended rollback design, not a verified live rollback for this candidate.
Before any cutover, prepare and independently review exact predecessor/candidate
recipes and state custody, verify backup/space and both workers, and bind the
same operation to its immutable plan. A lost or ambiguous action is inspection
only. After admission opens or its outcome is uncertain, the journal must not
pretend rollback is automatically safe.

No coordinator stop/start, job submission, worker activation, TrueNAS mutation,
credential disclosure or existing-journal modification was performed while
collecting these inputs.
