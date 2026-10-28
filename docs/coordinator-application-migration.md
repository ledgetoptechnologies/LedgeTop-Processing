# Actual-image schema1 application lifecycle qualification

`tests.test_coordinator_application_migration` is a separate, explicit local
Docker oracle. It runs the actual predecessor image
`sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249`
and candidate image
`sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b`.
It creates one uniquely named disposable local volume, seeds synthetic state
using the predecessor application, and performs three complete cold lifecycles:
predecessor671, candidate6bb, predecessor671 again. No rebuilt predecessor or
Queue-only fixture substitutes for either application.

Each image runs as its actual10001:10001 user, networknone, read-only root,
2CPU/2GiB/128PID test limits, dropped capabilities and a bounded private `/tmp`.
No ports are published. A synthetic idle backend, native listener and actual TLS
reference listener communicate exclusively over that container's loopback.
Only the newly created test volume is writable/persistent. Its initializer
changes only the new empty root's owner/mode, without recursive operations.
The test removes that exact volume after its containers finish.

Before importing any Processing application module, every `/app/processing`
Python file is verified against an exact raw-image map. A second independently
Git-derived map checks LF equivalence; original exports have CRLF differences.
Both checks are mandatory, and every loaded Processing module must come from
that image's `/app/processing`, never the mounted review checkout.

| Source | Files | Raw image map SHA256 | LF-equivalent Git map SHA256 |
| --- | ---: | --- | --- |
| 671fd2a6bde40088bdfad91ccf67eab45ee3d8d7 |49|c5d141da519e4787e26fb2c01dfc6506d4834f730885c802a940c36d87ddbb77|a0dc9950d3d4dc33aa513524cb04ced518df7efb4f67b7c55a5566becb122e12|
| 6bbd468e1177746cb695e43195e75355b0681f23 |63|04a14d73f7f1652b82a61428be51f9dd751b9bc957094500f8eb776458387add|06905adbf065ef2f4d3d3e027aa6ba90a49c99c806f4b15d629cdff7cfe73109|

The seeded database contains12 terminal synthetic jobs (10 succeeded,2 failed),
two historical owned schedules produced/cancelled through the real maintenance
API, and empty Viewer/reference tables. The synthetic schema1 private config
uses newly generated test TLS material and separate synthetic credentials.
No real configuration, credentials, certificates, source datasets or live state
are exported into this oracle.

Every lifecycle constructs the real `make_handler`, ViewerService,
ReferenceRuntime and CoordinatorService. It starts both actual listeners and
the empty dispatcher, checks native health/authority/history, validates TLS with
the synthetic CA, checks authenticated dynamic capabilities over actual HTTP
backend discovery, rejects unauthorized/wrong-role credentials, and proves a
second Queue cannot take the live OS state lock. Graceful shutdown joins native,
TLS and dispatcher threads, closes both sockets and releases the actual lock.
Full logical schema and all table rows, authority and exact config bytes must
remain unchanged through every image transition. TreeDar and Gaussian splats
remain unavailable, the optional recovery service remains absent, and no
recovery/control/pipeline tables appear. Backend requests must be read-only
`/info` or `/options`; a backend POST is a test failure.

Run explicitly on a host with the two exact cached local images and Docker:

```powershell
$env:MIGRATION_IMAGE_ORACLE = '1'
python -B -W error::ResourceWarning -m unittest -v tests.test_coordinator_application_migration
Remove-Item Env:MIGRATION_IMAGE_ORACLE
```

Ordinary runs skip this one Docker oracle explicitly. Opting in requires the
exact images; it never pulls a replacement. Qualification receipts must record
the actual run separately from default-suite coverage.

This proves synthetic full-application schema1 cold lifecycle and rollback
compatibility. It does not qualify actual private-config/state-copy migration,
an updater executor, current gate leases, backend replacement, scientific
processing, combined execution, runtime activation or production acceptance.

## Separate remaining gate on the cluster

No remote gate is executed by this component. A reviewed follow-up can keep all
real private material on the cluster and use the following bounded sequence:

1. Capture a consistent qualified copy of the real state into a new disposable
   directory/volume, using the reviewed stopped/offline-backup or SQLite snapshot
   procedure. Never mount the live state writable in the qualifier and never
   restore the copy over it. Verify exact history/schema/owned rows and numeric
   ownership; reject active work, nonempty Viewer/reference state or sidecars
   outside the qualified copy procedure. An inconsistent recursive live copy
   is insufficient.
2. Keep existing schema1 config, certificate/key/CA and required environment
   material on the cluster. Mount the existing private config/TLS paths read-only
   at their exact expected container paths, and only the disposable state copy
   writable at `/prototype-state`. Hash them locally before/after, reporting only
   approved digests and boolean comparisons. Do not export their bytes, tokens
   or environment values to this computer or Hub records.
3. Run both actual images sequentially against the disposable copy with
   networknone, no host port publication, read-only root and bounded **qualifier**
   resources. These are not changes to main-node processing limits. Preserve
   actual config and credentials; leave schema2 recovery and worker/combined
   activation absent. Verify there are no queued/active jobs for the existing
   Viewer dispatcher to execute. Network isolation prevents backend access, but
   does not replace the state census.
4. First verify the configured listener bind address is usable without changing
   it. Wildcard/loopback bindings work inside networknone; a specific host LAN IP
   generally cannot bind there and is an explicit gate requiring a reviewed
   isolated network/address plan. Do not quietly rewrite the private config.
   Validate certificate SANs and perform a loopback TLS connection with the
   configured certified identity as SNI/hostname; do not disable TLS validation.
5. Exercise native/reference health, exact shared authority, role isolation and
   read-only status routes over loopback. With backend networking intentionally
   unavailable, dynamic discovery may fail closed; that is not backend readiness
   or end-to-end processing proof. Join all listeners/dispatcher before reopening
   the same copy under the next image, and compare all logical rows, config/TLS
   digests and optional-feature absence after each cold lifecycle.
6. Retain the qualifier's bounded evidence, then remove only its specifically
   identified disposable state after review. Live container/config/volume/gates
   stay unchanged throughout. Remote activation remains a separate reviewed
   updater operation with fresh physical/history/ownership checks.
