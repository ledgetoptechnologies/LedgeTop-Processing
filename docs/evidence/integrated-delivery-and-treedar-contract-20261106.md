# Delivery process death and TreeDar result contract — November 6

## Selected changes

Claude's tests-only `899b8f3e` and correction `79c82e42` are selected at
`31cb6e45`/`649b8c0e`. The fixture uses real separate worker/coordinator processes,
ReferenceSender, WorkerOutbox, SQLite and authenticated loopback TLS, with
synthetic engine bytes and a synthetic Viewer signer. SIGKILL occurs at named
delivery/acceptance hooks. This is not actual Viewer ingestion or power-loss
qualification, nor death inside a SQLite transaction/HTTP write.

The initial branch run had26 cases,15 passing and11 failing, including the
no-death control. The hand-built driver journal lacked production's
`background_checked` column, so background acceptance/cleanup never ran. Those
initial no-cleanup observations were vacuous. Correction adds the column,
records background exceptions for refusal, verifies background progress during
retention windows, bounds child replies and checks `/proc` reap census. It does
not change production code or loosen retention assertions.

Corrected isolated branch `79c82e42` passed26/26 in66.068seconds, no skips.
Root inspected its complete terminal log and reap receipts: delivery class35/35
children reaped, process-restart class59/59, no children remaining. Original
and corrected logs remain in Claude's exact session scratchpad:

- `skd_slot1.log`: SHA-256
  `0a36424d56e83e6cbfd0d50c948ee2e2883f32bb02168a1c6c66d8c21b598069`.
- `skd_slot2.log`: SHA-256
  `500929d7b16c0136f9092f543a8fe9819df99ae5b54e1381c52e11e25ca4faa7`.

The report multiplicity correction `33736daa` is selected at `4cddb612`.
It matches canonical Viewer `ca5eedf3`: combined results allow one or two reports;
TreeDar-only allows exactly one. Every other required native role remains
exactly one, with unchanged identity, order, digest, object-count and acceptance
closure. TreeDar capabilities remain disabled; this does not finish typed
registration or combined execution.

## Exact combined-source checks

Both root runs freeze source `649b8c0e18af67fea06ba1c527d579715b8d5494`, canonical
LF archive SHA-256
`284a2bf3efd7543b1f7f0cb821301883002872edd14df3b056504460ac939df8`.

The first selected run reports43 cases in73.562seconds: **42 passed and one
loader error**, no skips. The error is root's nonexistent module name
`tests.test_reference_treedar_delivery`. The26 delivery/source-process cases and
all16 Viewer manifest/acceptance-transfer cases passed. Both reap censuses again
show35/35 and59/59 with no remaining children. Do not describe this mixed run as
a green43-case suite. Its log SHA-256 is
`686b002ee1061ed69f926efe2cf7d74799a30be9b85103015a20c08fc6038812`.

A targeted attempt with the correct `tests.test_treedar_delivery` name then
errored in all18 setups because root reused a UID0 runner and TreeDar requires a
nonroot host. Its log remains SHA-256
`293b90cdc34411cb94212587113526176db2b48d952e848fd7b1a58605c9b302`.
Production's nonroot guard was unchanged. The corrected dedicated UID10001
runner supplied its explicit passwd/group fixture and passed **18/18 in0.987s,
no skips**. Already passing process tests were not repeated for those corrections.

The same corrected runner executed **28 actual Python/Node V2 role vectors**
against exact canonical Viewer `ca5eedf3549a8d3c1463f9c3cede7c3f696d41a3`.
The frozen `server/referenceProtocol.js` SHA-256 is
`09d835cab64ce5d4d31498fa707f14ad511804c7ff4ce4e57131bc6493edccf3`.
Both validators agreed on valid combined1/2 and standalone1 reports, refusal of
standalone2/combined0/3, and every missing/duplicate non-report required role.
Its complete test/pair log SHA-256 is
`4f856b73f41eed3c416e10d21b8d097b81e1464700fa1d1fb58f7ea68968cbe0`.

## Runner and resource scope

Artifacts/receipts are under `C:/Users/fstor/AppData/Local/Temp/` directories
`ltds-delivery-integrated-649b-20261106`, `ltds-treedar-pair-649b-20261106` and
`ltds-treedar-pair-nonroot-649b-20261106`. All use cached image
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
one CPU,768MiB memory/swap ceiling,96PIDs, network none, readonly source/root,
private192MiB tmpfs, no capabilities, no-new-privileges, no host ports/socket or
Docker volumes. The existing readonly Node24.22.2 binary was hash/version checked
before pairing. A240second alarm/270second controller bound applied. Each
task-labelled container inventory was empty at its terminal return.

No installed engine, service, NAS setting or client dataset was changed. These
separate checks are not a single60-case green run and are not installed delivery
acceptance. Full constructor/factory coverage remains separate because the
process fixture builds ReferenceDriver without `__init__`.
