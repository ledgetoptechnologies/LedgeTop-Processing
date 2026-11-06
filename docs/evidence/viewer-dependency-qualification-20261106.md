# Dependency-backed Viewer GCP and measurement qualification

The coordinated Viewer owner ran its prepared exact source
`76f7c11258e5a0fe0660a17e89a102bd8ed2348d` from Ops PR446. Viewer subtree:
`de1a9e8a3b0b120c50a820dd73fb50419787b4d6`; exported archive SHA256:
`f5c06d202b37f3e0138571cef37c09fef4113dff20b83ed83b44d7d87b996f90`.

The existing dependency cache's package.json and lockfile matched the frozen
source; lockfile SHA256:
`a0eaead118642843905756fcfe3f53b5336b5a437e22a2f80677d6f06087fb12`.
Verified Node24.22.2 resolved both CommonJS and ESM dependencies. No package was
installed or changed. This clears the earlier load-only gaps caused by absent
express/unzipper/three/geotiff in the ordinary Windows worktree.

One serial `node --test --test-concurrency=1` process ran the owner's explicitly
selected34 GCP and measurement files. **294/294 tests passed in43.556777055
seconds**, zero failures, cancellations, skips or TODOs. Root independently
read the final test counts and verified the full log SHA256:
`577a64f0d12e890687cb7507fe7319e577ebe37ff19ec057245a50fc03f5f33e`.
Local log: `ltds-viewer-qa-76f7c112-20261106.log` under the temporary artifacts.

The run used one CPU/768MiB, network none, no Docker socket or client data,
read-only source/dependencies/root and a task-owned scratch volume. The owned
runner `ltds-viewer-qa-76f7c112-20261106` auto-removed; its exact task-labelled
scratch volume was verified and removed. Shared BuildKit was preserved. Claude's
current runner finished before this window; the resource slot was explicitly
released after the Viewer result.

The tests cover Emlid auto-import and immutable raw-source custody, bounded GPS
photo ranking with explicit correspondences, deterministic ODM snapshot output,
cut/fill against a custom plane, cross-sections/profiles, source/vertical-unit
evidence, calculation/server jobs, workspace authorization and draft recovery.

No browser test was included. File names containing `rendered` or `ui` do not
make their Node unit checks rendered visual proof. This does not establish live
admin/client sign-in, a deployed PR446 image, NAS crash durability, survey datum
accuracy, a real processing return or client model quality. Rendered/admin-client
and installed end-to-end acceptance remain required.
