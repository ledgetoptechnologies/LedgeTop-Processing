# Processing aeff: synthetic rollback held

Fresh local synthetic rollback operation
`75fbe729-7174-4f31-b826-f20674788e1a` did **not** pass. It reached
`bootstrap-frozen`, then the post-activation fixture readiness probe failed with
`command_read_unavailable`. The `BoundedSubprocess` wrapper intentionally maps
multiple failure types, including timeout and child failure, to that message.
The retained probe diagnostics do not establish which occurred. Do not infer a
specific cause, replay the operation, or apply the earlier bc48 rollback result
to this aeff candidate.

The local derived candidate image was
`sha256:aeff9d1b2cb90f6fe5762a77bcd04e04c9c67e5ee5cee7af2213ae5837761dad`,
runtime revision `ac0a25b57be1d1716983a666654d4fdbe80d5bb3`. The separately
mounted fixture source was `5c75dbcd489fab76aab5a6d33ebf1f8b48a64cc4`;
its seed layout SHA256 was
`71f9ccd3388bd841ab5873a7b6aaa2bc8655dfd55544eaa823a504a181c11c96`.

Outer container `ddea1d6c3d4db9a6e00434d4472cd303c73feb41b5c5852976b5c2c3c6de60c9`
ran from `2026-11-06T14:25:19.349333860Z` to
`2026-11-06T14:28:06.778993214Z`, exiting 1 with PID 0 and no OOM. Both
fixture worker exit records show reaped exit 0. Diagnostic capture reported
four containers, no incomplete captures, and no unknown Docker mutation
outcome. After cold export, the task-owned bootstrap was inspected by exact ID,
image, label and restart policy, stopped and verified exited 0/PID 0. All
task containers, volumes and evidence remain retained.

The private cold export contains 261 files and 1,440,795 bytes, manifest SHA256
`a8dabcc6871df898e00db6bfca3ae68531c652bc8e9eaa02e6e4b39082dab216`.
Captured console text SHA256 is
`31c6ef065f4272bee7fed6a5b00e8087069935083e22b35119f81b1349f1f394`.
These hashes identify evidence, not permission to replay or restore it.

The separate synthetic forward run passed, but **aeff rollback remains
unqualified**. Neither result establishes installed-host, SSH, systemd,
TrueNAS, actual Viewer processing, or production readiness.
