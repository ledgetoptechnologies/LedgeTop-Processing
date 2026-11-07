# Full-input validation draft — 2026-11-07

This is a private-input validation, not an installed cutover or readiness
claim. The running coordinator, canonical worker configuration and unit,
and TrueNAS Viewer were not changed. No new worker was enrolled or started.

## Candidate and controller

The runtime candidate is the independently checked e277 Processing source
(`e277691844c2ec8440fb81a9dc9b66d7520a1954`) in image
`sha256:1a0432ef13cf133db42fd47fd787cadea7603b391f82624baf9b945d130718cc`.
The external controller source is the LF export of commit
`b5ae2f464f20fc424c1cca74905cc357d9427bf3`, staged privately at
`photo-pilot-coordinator-inputs-20261106/controller-exec-b5ae2f46/` on the
cluster. Its launcher-compatible archive SHA-256 is
`869ebcdd276b2fc619aae521ac5bd65be4bcb4e599318df1e31914395442b481`;
the launcher SHA-256 is
`b34cb3d20345f2defd078293a83f2af83a8e122d80a84fff340d460c42b081e0`.
Using the private Python environment, the launcher checked that archive and
source tree and invoked `--help` for each of its two controller modules. This
did not execute a cutover action. A prior CRLF-helper legacy capture was
rejected and retained; it was not edited to force acceptance.

New candidate operation `27cd35b5-3ea9-4779-af8b-384bd995e102` has a startup
policy SHA-256 `ccd5d7efdc06ce33de94cc61dae2f88f63500b20a39103e12576447cb1881f6d`.
It binds 16 reviewed runtime sources, including the fixed legacy reader. The
exact e277 image independently regenerated that policy byte-for-byte with
networking disabled and read-only inputs. Its bootstrap and final recipe
digests are `c2942d5d85479453ac84e4dbb058b95e4f10e75a1c1c0b32df3e49ca8ad991d6`
and `8e006419a8c50218c5b142b87125a96a67d0c2f3b6fe0191cfbbb48c806d823b`.

## Observations and draft validation

The new final recipe's actual spool bind was checked using the e277 image,
the b5ae controller code, UID 10001 and a private network-disabled test.
The typed record at `candidate-e277-b5ae-27cd35b5-3ea9-4779-af8b-384bd995e102/spool-observation-d72a7e62f166.json`
has SHA-256 `32c036356d8ab96e9bb1b7ddd50717899eedd51ede70971a0aa5c3693142701b`.
It records current identity, marker, capacity, and a bounded fsync/readback;
it does not prove host reboot or power-loss behavior. The test's own containers
and volume were removed. The controller must recheck the current bind and
capacity before admission.

The separately captured LF-helper legacy receipt
`legacy-qualification-observation-20261107/capture-v3-lf.json`, SHA-256
`6adca93191bc3a20676e7fd885b408c7bd744346a0de980ae9b5bf715dc834f1`,
validates its MAC, candidate, exact helper hashes and retained archive metadata.
It is a read-only observation, not a frozen-baseline or live HTTP proof. The
historical pilot input remains a static candidate that must be checked again
after predecessor shutdown. The predecessor table counts matched the earlier
count input in a read-only observation; they must be rechecked after drain.

The private `candidate-e277-b5ae-27cd35b5-3ea9-4779-af8b-384bd995e102/full-input-validation-draft/`
contains a populated plan with SHA-256
`d39fc02e30429a7c75244f96b857e885b4816d2836e6ae3bb2b70b5415925687`
and bundle with SHA-256
`786660b3076658c5c53b2f42131960adf2666e2a39d4ae2c246f5f75d5c1a7c9`.
The b5ae controller's structural and private-artifact validation accepted all
four input gates, including the genuine worker preparation review, spool record
and LF-matching legacy capture. The config-reader callback deliberately read
the prepared private input files, not service-owned staged configs. Therefore
this is not an actual service mount readback and the plan is not ready to run.

## Cluster worker bootstrap only

The separately prepared cluster worker operation
`339dda71-7d6a-488c-84c8-7155fbbd89c2` remains pinned to the 4705 worker
source. Root-only installer bootstrap files were extracted to
`/opt/ltds-photo-bootstrap-339dda71-20261107` after validating the host, boot,
engine, gate and genuine preparation receipt. The root manifest readback SHA
is `8d51902ea89019239d3b3d57cabe214086e9e67c57825b96e8f7fe7bdd2411df`.
Before/after identities and hashes of the existing canonical worker unit and
configuration matched, and the existing unit remained loaded, disabled,
inactive and PID 0. The new worker installation root is absent. Crucially,
the existing installer refuses `prepare` while that old unit/config exists;
only the separate bootstrap extraction ran. No installer prepare, enrollment
or service start is implied.

Before any cutover, establish the actual closed admission/ingress boundary,
fresh frozen full-custody backup and counts, compare legacy and pilot evidence
against that baseline, safely retire/re-enroll the old cluster worker, install
and verify the main worker, stage/read back service-owned config, coordinate
the user-controlled Viewer change and verify rollback. Receipt expiry or any
changed candidate/source/recipe/bind requires a fresh observation. Preserve all
existing data and do not execute this draft as a shortcut around those checks.
