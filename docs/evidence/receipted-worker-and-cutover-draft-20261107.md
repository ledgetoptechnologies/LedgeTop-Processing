# Receipted worker preparation and cutover draft — 2026-11-07

These artifacts are preparation only. No worker was enrolled or started, no
coordinator was stopped, and no TrueNAS configuration was changed. The draft
is deliberately missing qualification gates and cannot be executed.

## Worker preparation

The pinned 4705 private preparer ran again on each actual host with a fresh
operation ID and a new private output directory. Its genuine returned
preparation receipt was saved with the manifest and config, all mode 0600.
The original candidate artifacts remain intact. Each receipt's config, manifest
and unit digests were checked against the generated files.

| Node | Operation | Manifest SHA-256 | Preparation receipt SHA-256 |
| --- | --- | --- | --- |
| cluster | `339dda71-7d6a-488c-84c8-7155fbbd89c2` | `8d51902ea89019239d3b3d57cabe214086e9e67c57825b96e8f7fe7bdd2411df` | `818236cec9432c1d1477ebc57b1b1c8422ec48a8a1901a95212a4bfa079d038b` |
| main | `97bf2bec-c5e4-4826-8396-2ffc09b8ce10` | `bcee244c06c0eac207a983769ab2bbc19531e9c675f8e9bfc69d4dc554e1d128` | `decc6480063e95b40b534dad0a1e52d52772992492c138acc3d5cd0296b2aa55` |

The new main prepare-only wrapper is
`photo-pilot-worker-prep-20261106/prepare-main-receipted-97bf2bec.sh` under
the private reference root on `.88`, SHA-256
`d2788b6e0b45a94eb324c008a687f9cd3a61623e697233fd861d350a6712ff6a`.
It supersedes the previously posted `8fa19b66` wrapper; it has not been run.
It checks the receipt/config/manifest hashes, verifies root bootstrap targets
are absent, and prepares/inspects only. It does not enroll, start or enable a
worker. A fresh readback confirmed matching host boot ID, Docker engine and
image identities, gate, source/dependency hashes, service UID, absent targets
and absent main unit. Those observations must be rechecked at execution.

## Scoped SSH for worker probes

The cutover driver uses strict existing-key SSH from `.89` to both worker
hosts; authenticated SSH from the operator workstation did not satisfy it.
The `.89` service account had neither a client key nor known-hosts entries for
these hosts. The actual ED25519 host fingerprints were read over independently
authenticated workstation connections and matched fresh scans.

A new task-specific Ed25519 key was generated on `.89`; its private key remains
there. Two alias-only entries in the newly created user SSH config map
`ltds-photo-cluster` and `ltds-photo-main` to the respective IP addresses with
`IdentitiesOnly`, strict host-key checking and a task-specific known-hosts file.
Other hostnames are not matched. The task key's public fingerprint is
`SHA256:4RrtAhXGLTCwWIzsrIKaNqRh9dd0xwk3qnXJDeI+pKM`. Exactly one
`ltds-photo-cutover-20261107` public-key entry was appended to each existing
authorized-keys file; existing bytes, inode and private modes were preserved.
Each entry uses `restrict` and forces its node's exact new operation's worker
Python executable with `-I -B -`. Python stdin still executes code with the
service user's authority; this is not a read-only OS privilege boundary. Rotate
the authorized entry when the installed operation changes.

An isolated, task-owned loopback sshd with disposable keys verified the forced
command and request override; the server exited and its files were removed.
Actual alias connections subsequently verified host keys and authenticated by
public key on both hosts. Both returned exit 127 because the forced worker
interpreter is intentionally absent until enrollment; no real task ran.
Post-enrollment, verify the real read-only worker probe and readiness separately.

The actual manifest/receipt pairs were copied privately to `.89` for review.
`worker-review-inputs-alias.json` under
`photo-pilot-coordinator-inputs-20261106/worker-review-receipted-20261107/`
has SHA-256 `06086c7dc116af230076dc27dce2c2f47590093e3749acaad68aafeb2f1c4489`.
The existing 4705 worker-reference and preparation-review checks passed for
both ordered nodes and distinct engine identities. These are install-intent
references, not installer receipts or readiness evidence.

## Draft cutover inputs

The 4705 typed worker prestart configuration review was generated from those
actual inputs, SHA-256
`262498867c38e629878856f980aaae8f1fe0488703d86d4d9ddbf5e20fa3016f`.
A canonical plan and bundle using real observed/static bindings and this
review passed schema and binding validation, with unresolved gates explicitly
null. After validating the actual historical receipt and matching its opaque
pilot inventory against a fresh read-only running-predecessor observation, it
was included as a *static candidate* in
`photo-pilot-coordinator-inputs-20261106/pilot-bound-draft-20261107/`.
The draft plan SHA-256 is
`8ef55f3c02dc81df0d9bc0b6f4d7d254b08944b953df52fd2dc251a4f5f27601`;
the draft bundle SHA-256 is
`e0e48c37ee368b5ff99bc5dd0e3eb29837377433bb50e58aa9cbbd0b02fd3811`.
The final pilot observer must still verify the stopped predecessor namespace
and compare the retained inventory to the fresh custody baseline.

The historical `nasQualificationSha256` and `legacyDeliverySha256` gates stay
null: the existing code only checks their artifact hashes and has no typed
producer or semantic validation. The 4 KiB local-spool fsync observation and
legacy synthetic tests are partial evidence, not full qualification. A fresh
drained-state census/counts, complete backup, service-owned config stage,
enrolled and ready workers, production Viewer cutover and rollback all remain
necessary. Do not use the draft as an executable cutover.

An additional bounded observation used the actual cluster spool bind, its pinned
marker and the exact 4705 image as uid 10001. One isolated container checked the
10 GiB reserve, wrote and fsynced 7,602,176 synthetic bytes and directory
metadata, and exited. A separate fresh container read the same bytes and
verified SHA-256
`d94b08e12f8095d50e367133b936c89e679ac1f75cddf8d6dedffcbd1c7370e4`,
then removed only its own synthetic file and directory. Both task-owned
containers exited. This verifies persistence across container replacement on
the current mount, not a host reboot, power loss, installed pipeline or full
local-spool qualification; the historical NAS gate remains null.
