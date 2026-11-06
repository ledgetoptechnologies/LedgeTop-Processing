# Processing aeff: local synthetic forward qualification

Fresh operation `8d36c766-8de6-4dcf-b33c-d004f305f393` completed all **22
forward phases** through `complete`, with admission open. The isolated test
exercised Docker phase transitions, custody backup, the application gate and
actual fixture worker factory/TLS readiness. Post-resume catalog refresh saw
two available synthetic nodes: cluster maximum 1,000 images, main unlimited.

The local derived image was
`sha256:aeff9d1b2cb90f6fe5762a77bcd04e04c9c67e5ee5cee7af2213ae5837761dad`,
runtime revision `ac0a25b57be1d1716983a666654d4fdbe80d5bb3`. The separately
mounted controller and worker fixture source was
`5c75dbcd489fab76aab5a6d33ebf1f8b48a64cc4`, frozen archive SHA256
`91d7253b963e9a7529289a5718316a93d26a7f293b83b58cce7b7b13c15157d3`.
The image is a derived local candidate, not a canonical Dockerfile rebuild,
published package or deployed release. The seed layout SHA256 was
`52a8bd97d9ab16a994955b3e0f4cb3002350d5ea382200f1b7a41ac47714b5d4`.

Outer container `f99bf6e4fd762b38223eef4aabd96a25e9b72d7cdf63b2871b75801207f8ef02`
ran from `2026-11-06T14:17:46.662949957Z` to
`2026-11-06T14:22:22.197595143Z`, exiting 0 with PID 0 and no OOM. It had a
one-CPU, 768 MiB and 900-second qualification cap. A qualification-only
retention wrapper preserved task resources. The final synthetic container was
subsequently inspected by exact ID, image, task label and restart policy,
stopped and verified exited 0 with PID 0; task volumes and evidence remain.

The private cold export contains 406 files and 1,500,933 bytes, manifest SHA256
`cba06829289dce8be1bd592d29e5af026b33d964fd73bee1077b7e4ba456378c`.
Captured console text SHA256 is
`8cb5acb5af7b53ea716950be07e484ca09ed97df64a825b18c6b77ca3f0fdf24`.
These hashes identify evidence and do not authorize replay or restoration.

Worker activation, native engines and enrollment used explicitly synthetic
fixture seams. This result does not establish installed host, SSH, systemd,
TrueNAS, real photo processing, Viewer acceptance, TreeDar readiness,
continuous availability, rollback or production deployment. The earlier
combined regression selections and their overlap are documented separately.
