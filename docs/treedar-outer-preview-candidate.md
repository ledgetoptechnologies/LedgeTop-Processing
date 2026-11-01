# TreeDar outer preview candidate

This is a local, inactive preview of a freshly qualified native publication.
It does not define an accepted transport role, publish or acknowledge a result,
settle a job, prove remote custody, or authorize cleanup. No endpoint calls it.
Mixed ODM/TreeDar completion remains unqualified. HTML is untrusted and any
future Viewer must sandbox it or offer it only as a download.

`NativeBundlePublication.preview_outer_manifest(job)` holds the existing local
publication/daemon authority, requalifies the original source and proof,
verifies the exact published ZIP using the existing bounded archive verifier,
and rechecks the live source. It does not change the existing inner v1 manifest,
publication journal schema, or local publication receipt. In this prototype,
loss of authority, source changes, missing published ZIP or mismatched receipt
holds instead of returning a preview.

The outer object's `archive` size/hash refer to the full ZIP. `inner` fields
`zipEntryByteSize` and `zipEntrySha256` refer to the exact stored canonical
`treedar-bundle.json` ZIP entry; `manifestSha256WithoutDigestField` is instead
the inner manifest's semantic digest excluding its own digest field. The inner
validator closes names, paths, formats, roles, file sizes and hashes, requires
each of its twelve native outputs once, permits each of ten optional outputs at
most once, checks sorted unique paths and byte totals, and retains native roles.
No whole TreeDar ZIP is relabeled as an ODM archive or terrain model.

Original binding includes job/generation/environment/input manifest and
pipeline digests and exact original LiDAR object, size, hash and provenance
receipt ID. Native proof bytes and stage-proof digest are local evidence only;
matching these hashes does not authenticate the producer to a remote Viewer.
The Docker image ID identifies the observed local container configuration, not
an authenticated registry or producer. The provenance receipt is locally
checked by the configured verifier, not proven remotely by this preview.

The CRS definition is digest-only with null WKT; consumers needing a complete
authoritative definition must hold. Horizontal metre units reflect the pinned
bootstrap's check of reported axes of the projected LAS CRS. Vertical metre
units and datum are the locally authenticated source declaration, with no datum
conversion or independent measurement claim. Shared geometry is explicitly
scoped to the pinned bootstrap's same shape, affine transform, CRS and finite
block-window checks for `dtm.tif`, `chm.tif`, `chm_raw.tif`, and `crowns.tif`;
it is not a CRS or geometry guarantee for every artifact.

The pure comparison helper re-derives the closed preview from fresh local
evidence and refuses changes; it is not a general remote/untrusted parser or
cross-language authentication protocol. A Viewer must independently validate
the archive and every artifact, enforce decompression and resource budgets,
verify authenticated evidence and exact provenance, and establish durable
custody and publication authority. An eventual wire schema and error/recovery
behavior require joint review. Until then TreeDar and mixed capability remain
disabled and original inputs and outputs remain retained.

## Isolated test evidence

The two actual-publisher fixture tests passed in an owned network-none Linux
container based on image
`sha256:5be5aaecb962a41567bc8041f1e9e3432cb085b037fe057f9a562630b2e4b370`.
They compared the preview with the exact ZIP member bytes, changed binding,
role, hash and spatial fields, checked original retention and refused a changed
native proof. The test uses synthetic evidence, a fake Docker controller and a
synthetic provenance verifier, not a genuine acquisition or remote authority.

Run label `d7eb9f2805f54ddd8edc3fcf8aeed9ba`; exact container ID
`ed1df977a0329869c66612da1eecf8b89c68932612e08e2a5586dfae8a3dd506` exited 0;
the test runner found no remaining owned threads. Synthetic roots
`/tmp/tmp_249newi`, `/tmp/tmps7s5ssao`, and `/tmp/tmp0m05_3pe` remain in the
retained container. The 1 CPU, 1 GiB, 128 PID run had no external network,
mounted source read-only and did not enable a service. Host crash, real
producer authentication and filesystem/power-loss durability were not tested.
