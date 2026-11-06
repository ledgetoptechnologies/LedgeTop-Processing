# Proposed local synthetic rollback qualification

The `b1657639` forward operation is complete and retained. Never replay it or
reuse its journals, runtime volumes, seed layout, or completion records. A
rollback test needs a separate fresh operation and new owned resources.

Use the same verified candidate image and frozen fixture source as the forward
receipt, unless either changes; record both immutable identities. Create a
fresh, labeled seed volume with the reviewed nonroot seed helper and verify the
layout, extracted source and dependency evidence before constructing the
controller command. The new seed and cutover must share their new operation
UUID because versioned worker roots and layout validation bind to it.

Construct a fresh one-shot rollback launcher after the actual layout digest and
worker paths are known. Verify the source archive and complete manifest,
candidate and predecessor images, daemon and CLI identities, seed helper and
volume, a closed named/bind-alias census and absent task namespace. Preserve the
qualification-only success-retention wrapper and independently review the
launcher before running it. The wrapper retains resources; later cleanup needs
separate exact-identity and alias checks. A Docker socket grants broad daemon
access, so labels are ownership evidence, not a security boundary.

The local controller is bounded to one CPU, 768 MiB and a 900-second alarm,
with private task volumes and no host-published ports. Explicit `--rollback`
must start a new forward path through `final-ready`, request rollback, and
require `rolled-back-rebaseline-required`, admission closed, original image and
restart policy restored, plus the post-restart stopped-writer custody audit.
Check the exact container exit status separately from the launcher exit; a
successful shell must not mask a nonzero Docker container exit. Retain all
resources and diagnose the same operation on any uncertainty; never retry a
consumed marker or replay its effects.

This is a plan, not a completed run. It does not authorize host enrollment,
deployment, NAS modification, client processing, or claims about live SSH and
systemd. Production and Viewer release gates remain separate.
