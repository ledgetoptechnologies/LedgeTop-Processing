# PR197 synthetic predecessor compatibility rehearsal

This is isolated test evidence, not a release, live cutover, physical writer
fence, or rollback qualification. The candidate was built from the clean Git
archive of `4d7c3c7f08eaf965b84098fbb69ee407c2335bcf`, archive SHA-256
`e71afb4014e244762c7b94a20f66dd15f2f7f87141cf87993b4a2f6f494a88db`, and
the pinned Dockerfile Python base. Candidate image ID is
`sha256:df741c6ad340ebc32dc3a4c9caf4b6468260cd9cf42523c6f6406e7e05bbbdee`;
its revision label matches the source. Build steps used network none, but
BuildKit contacted the registry for metadata; this is not an offline build.

The exact installed predecessor image used here is
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
revision `693cb7eca32295da959a4f42399f317577ef8a8a`. The isolated test created
one synthetic Viewer task and tiny photo, plus 16 inert synthetic maintenance
schedule rows inserted directly into the predecessor schema. Their contents
do not model or reproduce actual production schedules. With the old process
stopped, it made an independent backup and copied the synthetic state. The
candidate started with archive capture explicitly disabled, served the same
task metadata and retained the photo bytes on disk; the census reported matching task/file/schedule counts and
digests. A separate restore of the original backup ran under the predecessor.
This does not roll back or preserve writes made later by the candidate.

The old updater census rejected the nonempty candidate. The assertion checks
only that it rejected the state: it does **not** establish whether the
schedule rows, Viewer rows/files, another constraint, or more than one caused
the rejection. A separately qualified live census and writer fence are still
required. The candidate's source defaults archive capture off; this rehearsal
also sets its environment explicitly to `0`. It does not qualify capture.

The exact test passed once in 10.164 seconds on 2026-10-31 with synthetic
owner label `codex-viewer-e2e-cd4b29eaa3eb4354b27d81ad9ee63f95`. A subsequent
read-only label census found no remaining containers or volumes for this run.
Earlier failed rehearsal resources and any previously rejected cleanup targets
were not touched. No real Viewer, engine, user data, or NAS was involved.
