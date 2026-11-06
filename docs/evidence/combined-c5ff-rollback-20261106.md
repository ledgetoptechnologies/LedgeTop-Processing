# Combined candidate: local synthetic rollback qualification

Fresh operation `928ea653-471a-4b07-86e4-d668bb652e97` completed all
**30 forward-and-rollback phases**, ending at
`rolled-back-rebaseline-required` with admission closed. A separate
post-restart stopped-writer custody audit passed; it did not qualify continuous
availability or a live SQLite audit.

The local derived candidate image was
`sha256:bc48c6a81300a7c9b610c6e0f3f4d7cb8aa599075645b2ff1ac1c891dc0ce8c4`,
with runtime revision `c87d6ec1935da6a58d3c39c92228ff1171da8f34` and separately
mounted worker/controller fixture source
`c5ff250ce51926c0c5ff47afd5f9b422091aed55`. The fresh layout SHA256 was
`82065447b9b1e04f617acc38a63b66fcf228cdafda6b15f6b73e8848f4b2894b`.
The test did not reuse the forward run's operation, seed or journals.

Outer container `6ef62ebae91c81059147e5a66d13c358c0581fa300c57a5dd94b2fead02fa41f`
ran from `2026-11-06T13:43:18.698713104Z` through
`2026-11-06T13:47:46.220976751Z`; it exited 0, PID 0, with no OOM. The
qualification-only retention wrapper preserved the inner resources. A fresh
operation-label inventory showed all five synthetic inner containers already
exited 0; all task volumes and evidence remained retained.

The cold export contains 432 files and 1,538,526 bytes, with manifest SHA256
`6b561cd05806312f285cadfbd13a4be95d4db53f4283b239b24544c563022be9`.
Captured console text SHA256 is
`f958699703c299a07aced291ab0028e320b7d309c9cab994ad3a474d47bc38e8`.
These identify retained evidence and do not authorize replay or restore.

This local test uses synthetic activation, native engines and enrollment seams.
It does not qualify installed hosts, SSH, systemd, a real Viewer job or any
production deployment.
