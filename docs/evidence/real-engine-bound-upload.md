# Synthetic bound-upload image rehearsal

This records an isolated, non-production rehearsal on November 1, 2026. It
does not qualify the gateway, physical writer fencing, storage capacity, crash
recovery, or an actual photo job. It did not submit an engine commit or start
photogrammetry. The input was a 31-byte synthetic, invalid image payload.

The owned engine container used exact local image ID
`sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6`.
The owned Python helper used image ID
`sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530`.
The source was the `processing` archive of commit
`f0646ab2e1b92e079234f1ff960ca82dd80ca0f7`, SHA-256
`a92e9ee7130d6ca2e0218698ae82d5ba4cdebfdc728f2f2e76fcf77da6168993`.
All 65 regular archive files were independently compared with the commit's Git
blobs and executable bits. The decoded synthetic probe SHA-256 was
`63b033dd9f48152cd3959e01f2b252c5c82b87f4f8750d1a851e694c88fb304e`.
These are local source-consistency checks, not signed image build provenance.

The host wrapper in `tests/real_engine_bound_host.py` validates exact image,
daemon, nonce labels, container IDs, network mode, tmpfs, user, resource limits,
read-only root, no host binds/devices/ports and fixed launch command. It uses a
network-none engine and an unprivileged helper sharing that network namespace;
only the engine's own bounded tmpfs paths were writable. A fixed synthetic
config sent logs to its tmpfs and launched the image's NodeODM entrypoint. No
test-mode flag, host data mount, GPU or engine commit was used. The probe is
stored as inert base64 and is executed only by the verified fixture wrapper.

The real NodeODM API accepted init and the guard's file-bound multipart upload.
TaskStore refused a second preparation of the same file/generation. A separate
partial upload failed and both associated journal states remained `started`;
an attempted retry was refused while the synthetic NodeGate reservation remained.
The probe overrides the gate path and engine identity check; it does not verify a
real Docker engine ID or acquire the host's physical node gate.
The image's `/bin/tar` produced a bounded in-memory archive from the exact
synthetic task's temporary directory. Strict parsing found exactly one
`synthetic.jpg` or NodeODM underscore-duplicate variant; the one regular
`synthetic.jpg` was 31 bytes with SHA-256
`dbf597c06188325763901c9687203c7662967feb9599ae116dd1fc284da18b4b`,
matching the synthetic source. This is a point-in-time observation; it is not a
durability, inode-fencing or later availability guarantee.

The successful wrapper reported both owned containers removed after verifying
identity and daemon at each cleanup step. A separate successful Docker label
census was empty. The wrapper and these observations are recorded narratively;
no signed external execution or cleanup attestation is retained. No automatic
approval rejection was encountered or retried in these runs.

Earlier isolated attempts are not successes: the first engine exited before
readiness with no retained error category; the next reported `EROFS`. Static
read-only source inspection showed default logging to `/var/www`, consistent
with but not proving that error. After the synthetic logger config, the probe's
API assertions passed, but a Node-based stored-object check produced two stdout
lines and was rejected by strict parsing; the extra line was not identified.
`docker cp` then failed on the tmpfs path; Docker documents tmpfs as a `cp`
limitation. The final `/bin/tar` check above avoided both problems without
stripping untrusted output. Each completed attempt's task-owned containers were
removed; the final independent label census was empty.

Thirteen local POSIX wrapper safety tests passed, covering bounded command
output and timeouts, creation uncertainty, strict parsing, pinned source/probe,
safe archive extraction and tar identity/duplicate rejection. The fixture's
default-off qualification patch, synthetic backend probe and temporary
filesystem do not establish production authorization or cross-host isolation.
