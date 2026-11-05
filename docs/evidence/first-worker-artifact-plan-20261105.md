# First-worker artifact preparation checkpoint

This is a preparation plan, not an installation or activation receipt. The
existing `e11231a` source preparations and `39e0825` environments remain untouched.

## Confirmed privilege boundary

On November 5, the existing authenticated main-PC SSH connection successfully
ran the exact read-only capability probe:

```text
sudo -n /usr/bin/python3 -I -B -c 'import os; print(os.geteuid())'
```

It exited 1, reporting that a password is required. No prompt was attempted.
The previously observed unrestricted cluster permission is distinct from this
main-PC result. Do not change sudo policy to work around this boundary. Once
artifacts and the exact execution bundle are reviewed, an administrator can run
the single reviewed root installation invocation on main. No administrator
action is required merely to prepare unprivileged artifacts.

## Frozen source artifact

An explicit local `git -c core.autocrlf=false archive --format=tar` of reviewed
Processing commit `99578a6896dfb53d11295f2582bfc4c4b4c039fa` produced:

- Plain archive: 8,642,560 bytes; SHA-256
  `da2670855b91df079beaf9ece8994fe66821e0d19e5e0b8c92b4cc274ae4b778`.
- Python footprint: 136 `processing`/`ops` files; sorted compact filename-to-SHA
  map digest `10053da402978b4c92e917c224ab5c8754f784f84f62a2004c2f678d6cca78fc`.
- Exact `requirements.txt`: 442 bytes; SHA-256
  `fa52c01bb21c89778ad44a8a43966ce5a688e105cc4cabbe229845e78c154357`.
- All 708 archive entries are ordinary files or directories; expanded regular
  contents total 8,107,442 bytes. No archive extraction or host install occurred.

The task-owned archive is retained at
`C:/Users/fstor/AppData/Local/Temp/ltds-first-worker-artifacts-20261105/source-99578a6896df.tar`
for the next reviewed preparation. It contains repository source, no private
configuration or credentials. A later source selection requires a new archive,
footprint, unit/config pins and review; do not silently relabel this artifact.

## Per-host dependencies and remaining artifact work

Read-only inspection of each existing venv's resolved interpreter yielded:

| Host | Python | Interpreter bytes | Interpreter SHA-256 |
| --- | --- | ---: | --- |
| Main .88 | 3.10.12 | 5,941,864 | `a2f33a6e006989270f4340528eb61f8f97366e00a5d1b602ac8672ea44fc56ae` |
| Cluster .89 | 3.14.4 | 7,477,160 | `52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41` |

These executable hashes are actual observed bytes, not complete dependency
artifact hashes or proof of OS-library portability. The old venvs contain four
and five symlinks respectively and cannot be used as the installer's closed TAR
inputs. Do not automatically dereference or adopt their mutable package trees.

The next bounded preparation, one host at a time, is:

1. Allocate a new operation-owned unprivileged artifact directory, preserving
   old candidates. Capture exact actual interpreter/OS profile. Download and
   retain reviewed wheels for the existing requirements: cryptography 50.0.2,
   cffi 2.1.1, rfc8785 0.1.4, and pycparser 2.23 on main / 3.12 on cluster.
   Record real wheel filename, size and SHA before installation; no invented
   package/archive hash is acceptable.
2. Build a separate copied venv for that host from the reviewed local wheel set
   with network disabled and user site disabled. Produce an explicitly copied,
   regular-file-only artifact, including the interpreter, rather than passing
   standard venv symlinks through the installer. Reject unexpected links and
   files, verify `include-system-site-packages=false`, actual distributions and
   interpreter bytes, then hash the completed plain TAR.
3. Have the existing private-config owner generate a new operation-specific
   config/TLS artifact with exact source footprint, version, original authority,
   node/engine identity, new state/scratch paths, maintenance readiness and no
   borrowed activation. Retain only its descriptor/hash in review records.
4. Refresh the actual host boot/Docker/native catalog/original Gate metadata
   immediately before sealing each closed manifest. Render and hash the unit
   from that exact manifest. Final dependency TAR hashes, private config hash,
   original Gate path and complete manifest are pending, not guessed here.
5. Review the execution bundle and manifest, then install-only `prepare`,
   `inspect`, `enroll`. The main root invocation needs administrator execution.
   Activation is separately journaled at the same coordinator's authenticated
   bootstrap-frozen boundary, after independent complete idle/custody proof.

No remote build, installation, service start/enable, engine mutation, permission
change, lock creation or NAS operation was performed for this checkpoint.
