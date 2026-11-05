# Local image and actual Docker transport qualification

The coordinator image was built from exact source
`d0c84fef4c6144f6e846b77c0705b4bf76ff72a8`, using a restricted git archive of
the Dockerfile, ignore rules, pinned requirements and Processing package.
Archive SHA256: `36a94a6c99c73129ae837200fd3fd96c17c50035a02b54195575d42b0a38472f`.

Local tag: `ledgetop-processing:qualification-d0c84fe`.
Runnable image/manifest ID:
`sha256:2e5675f85b2d8abfeec712dbe4055c497ded63c2a49480aac08ea1bd51552751`.
Revision and repository-source labels were verified. Build handle 84556 exited
zero. The existing exact task-owned builder retained its one-CPU/2 GiB bounds
and was stopped and verified stopped after export. No registry publication or
installed runtime update occurred. This image predates the later renewal fixes;
it must not be described as the latest runtime package.

The reusable opt-in helper is `ops/qualify_enrolled_docker_transport.py`.
Against transport source `751aced554038f1af67025db4e04d1197dd87704`, its source
SHA256 was `fef279c4c911ba1d5224028cc2540f9485ce5844b83cd40d5129dcf8f47d2b8c`.
**Twelve actual local Docker checks passed in 5.312 seconds:**

- Synthetic fixture files and volumes owned by UID/GID 10001.
- Closed normal-application recipe creates a never-started exact container.
- Exact private-config bytes can be read through Docker CP before startup.
- Public-mode files, symbolic links and oversized files are refused.
- Readback leaves the container in its original created state.
- An actual 17 MiB source object streams through `archive_to`; callbacks are
  at most 64 KiB. Clean stream completion, TAR SHA/byte count, member UID/mode,
  inventory and full member content digest all match.
- The packaged application returns health OK as UID/GID 10001; its source is
  read-only and the imported custody module has the expected `/app` origin.
- Exact PID/start identity of the completed probe is no longer live.
- Non-escalating SIGTERM stop exits zero.
- Private readback still works while stopped, and an exact retained rename
  preserves the immutable image/environment/mount recipe.

The count includes the three separate negative readback checks. This is not a
unit-test count. The test uses disposable synthetic volumes, no user data or
network ports, and no image pulls. Controller bounds: one CPU, 768 MiB, 128
PIDs, read-only source/root, UID10001 plus the daemon socket group. The daemon
ID is explicitly supplied and compared before mutation. Candidate bounds are
one-quarter CPU, 256 MiB and 64 PIDs. The installed native Docker CLI and daemon
both reported 29.9.2. The native CLI was copied read-only from the already
running Desktop WSL CLI-tools installation (SHA256
`6c71cfe71faff949a602946a4df4799b563232e79d0b8b6fd343d0ef081c9621`).

An initial probe mistakenly imported Processing under `python -I` without the
serialized helper's existing fixed `/app` bootstrap. It failed; this was a
qualification-script issue. The corrected probe uses that fixed path and
checks the exact imported module origin. Earlier successful eleven-check runs
excluded the subsequent large stream check.

Every successful run removed its exact labelled containers and volumes; final
filtered inventories were empty. The helper now conservatively retains all
resources and reports the operation ID if a Docker mutation has an unknown
outcome, rather than treating an empty scan as proof no daemon request remains
in flight. That failure policy has read-only review; this actual run did not
inject a daemon mutation timeout.

This qualifies the normal recipe, CP/readback, streaming and packaged startup
seams. It does **not** qualify the non-init PID1 lease holder, complete tracked
observer lifecycle under Docker, full original-state backup/projection,
installed host handover, NAS durability, real engine execution or automatic
updates. No shared container, processing PC service, TrueNAS setting, credential
or permission was changed.
