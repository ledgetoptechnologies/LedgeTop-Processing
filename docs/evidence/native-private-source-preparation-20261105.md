# Native inactive source qualification

Processing `e11231a510642b516abda7c3a15049c8488e440e` is privately staged on
cluster `192.168.50.89` and main `192.168.50.88` beneath
`~/.local/share/ledgetop-processing-reference/dependency-candidates/e11231a`.
Source archive SHA256:
`eacda078ac98c276dcadcfc67fcfed78eeeb5678399e0af50b20cf69d8a82101`,
5,120,000 bytes. This is not an activation or deployment.

Both hosts verified local ext4 staging, owned private paths, archive hash and
safe archive entries. Sources were extracted with private modes, parsed and
imported using the existing dependency environment. Each passed **130/130 tests,
no skips**: option policy, protocol, source-reader ownership, receipt-bound ODM
GCP inputs, worker acceptance-observation, scratch policy and engine worker.

| Host | Python | Parsed processing/ops modules | Test duration |
| --- | --- | --- | --- |
| Cluster | 3.14.4 | 107 | 23.278 seconds |
| Main | 3.10.12 | 107 | 6.791 seconds |

The preceding cluster `89c3dc4` preparation ran 64 tests with two errors.
Its shell umask is `0002`; `mkdir(parents=True, mode=0o700)` left the intermediate
`jobs` directory group-writable, so scratch cleanup correctly refused it.
The failed inactive source remains retained without a successful preparation
receipt. It was not activated or silently overwritten.

The fix in `e11231a` creates every job-directory component explicitly with mode
0700, fsyncs newly created parent entries and uses descriptor-relative nofollow
traversal on POSIX. Existing wrong-owner, symlinked or other-writable directories
are refused, without chmod or deletion. Driver input fetch and ODM archive
collection use this shared ScratchLedger method. Tests exercise permissive umask,
unsafe existing directories, symlinks and unreserved paths. The existing worker
systemd unit already specifies `UMask=0077`; it was unchanged.

Both preparation receipts record `workerStarted:false` and
`activationChanged:false`. Test children completed and temporary fixture data
was removed by their owned contexts. No current-source link, credentials, unit,
container, engine image, hardware job, NAS directory/mount/ACL or client data was
changed. These are native-host synthetic code checks, not installed worker,
photogrammetry, licensed converter or NAS durability acceptance.

The full cross-language checkpoint remains `89c3dc4`: 542/542 tests with frozen
Viewer and simulated photogrammetry. The new directory fix is qualified above;
do not relabel the earlier combined run as having executed the newer source.
