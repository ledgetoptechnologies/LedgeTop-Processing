# Combined candidate: local synthetic forward qualification

On November 6, 2026, operation `b1657639-c536-4712-b6fe-54d6e7eb2de2`
completed all **22 forward phases** through `complete`, with admission open.
The qualification exercised actual local Docker phase transitions, custody
backup, application gate, the real worker factory and TLS readiness. After
resume, a live catalog refresh observed both synthetic workers: the cluster
worker advertised a 1,000-image maximum and the main worker no maximum.

The candidate was local derived image
`sha256:bc48c6a81300a7c9b610c6e0f3f4d7cb8aa599075645b2ff1ac1c891dc0ce8c4`,
whose runtime source revision is `c87d6ec1935da6a58d3c39c92228ff1171da8f34`.
Its packaged Processing source was checked against that revision. The separately
mounted controller and worker fixture source was
`c5ff250ce51926c0c5ff47afd5f9b422091aed55`, frozen archive SHA256
`ce3ac1ae849720fec6c7fc90a59e85667604017e7eb6efef4fe6212c738c41e6`.
That source difference is explicit: the local candidate was not a canonical
Dockerfile rebuild, published package or production deployment.

Before the forward run, **166/166 focused Linux tests passed, with zero skips**
on the frozen controller source and candidate image. The two bounded selections
covered Viewer acceptance and the legacy-push fence, fixture and production
activation-journal tests, configuration and prestart review, strict TLS,
retention, worker preparation and fixture profiles. These focused tests are not
a replacement for the forward run, rollback qualification or live acceptance.

The outer controller was
`6d067c55e05b8290128fc737271fbd747f5a757b089e62be392a35a673cb9ec4`.
It ran from `2026-11-06T13:34:57.505036764Z` to
`2026-11-06T13:39:11.601115759Z`, and exited 0 with PID 0 and no OOM. It used
one CPU, 768 MiB, a 900-second outer limit and a new owned artifact volume.
Its seed layout SHA256 was
`070bbeb7b4a2d1117a072f84e528dd45128574232e7d76993b64359952704bc5`.
An explicitly qualification-only wrapper retained inner resources after
success; the synthetic final container was subsequently identified by exact
ID, operation label, image and restart policy and stopped cleanly, with exit 0
and PID 0. Inner containers, volumes and evidence were not deleted.

The private cold artifact export contains 396 files and 1,500,500 bytes;
its manifest SHA256 is
`e7ec45b326490401cddbab5a876764a2be1a1aebf7f1ea12cffca24427f367a0`.
The captured console text SHA256 is
`5d444bf22b34723b3bef8c43dd062172dea4397b92964db10f77b7377afae7a7`.
These hashes identify retained evidence, not permission to replay or restore it.

The activation boundary and backend are explicitly synthetic. This run does
not qualify installed worker enrollment, SSH, systemd, TrueNAS configuration,
real photo processing, Viewer result acceptance, uninterrupted availability,
rollback, or production readiness. A first real workload and a separate rollback
and recovery qualification remain outstanding.
