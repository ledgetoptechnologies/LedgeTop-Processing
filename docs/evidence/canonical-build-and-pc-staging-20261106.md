# Canonical build and two-PC artifact staging — 2026-11-06

## Candidate image and source

The local coordinator image is `ledgetop-processing:qualification-63c8049b-lf-20261106`, immutable image ID `sha256:7adf6e6ece0b1a5a6246cc5af4d8707d146b577b58b9df3ac6d27148032a20b4`, revision label `63c8049b74e4e179e812f67b489f8f67f7d9d8b1`. Docker inspected the same ID after loading the archive on the actual cluster PC. No latest tag, registry publication or runtime replacement was performed.

This is a frozen earlier candidate. Later combined source changes, including
maintenance selected at162f489c, are not present in this image or source TAR.
Their release requires new immutable artifacts and corresponding qualification;
the existing staged files and receipts are not overwritten or relabelled.

Both build context and worker source archive use explicit `git -c core.autocrlf=false archive`. All 99 image Python files match the canonical reviewed source exactly. Their sorted compact relative-path-to-SHA map hashes to `654fceb89d4dc114434053f14bbb503a53763401f458e1a628e2724979e9989d`. The image runs UID10001; installed distributions checked are cryptography50.0.2 and rfc8785 0.1.4.

| Artifact | Bytes | SHA-256 |
| --- | ---: | --- |
| Image context archive | — | `fe4ba7d09e778a954820e04698b767a07437af61101165a766793126fb4ad6d1` |
| Source archive `source-63c8049b.tar` | 9,584,640 | `86f37100155af8031b2dad62e6fb8419995fb021e4afd722fa6af0ffe9fd2355` |
| Saved image `coordinator-image.tar` | 50,282,496 | `be0d0cbb929ddfe334d2f1ec7bd96c39d1d907418c881d1aa5d8c8e0c3a47e5a` |

The source archive has 776 ordinary file/directory entries, 8,994,373 regular-file bytes, and 144 Processing/ops Python files. Its Python footprint is `ec4a819043fb9ada4eaea0ae5d18d234129729c67f9fcb4c64b969bd6786565b`. requirements.txt remains 442 bytes / `fa52c01bb21c89778ad44a8a43966ce5a688e105cc4cabbe229845e78c154357`. Later preparation-tool changes are versioned separately; never silently relabel this frozen source archive.

Local artifacts remain under `C:/Users/fstor/AppData/Local/Temp/ltds-processing-canonical-63c8049b-20261106`. Build log SHA-256: `861d624fac204814c4a51e70f5e236058bd9ac34108690d252079e53dd68acd0`.

## Build/probe history and scope

An initial network-none build missed the Docker network-mode cache key and failed resolving pinned pip dependencies. The ordinary network-mode retry used the existing cached dependency layer; the build log reports CACHED. No package version changed. An initial ordinary Windows archive materialized CRLF; that local image is superseded by the canonical LF image above and must not be selected.

Two early health attempts omitted required PROCESSING_API_TOKEN. The second captured application exit1/noOOM with the expected configuration error; docker exec returned137 as its container exited. This was not a positive health result or proof of insufficient RAM. After supplying a synthetic test-only API credential, isolated HTTP health returned200/statusok. The final canonical image was independently source-checked and health-probed in one bounded container, whose application child was terminated/waited and whose container auto-removed. No host port was published. This is basic image startup, not reference TLS/worker/NAS readiness.

The exact canonical image subsequently passed the actual local Docker forward and separate rollback/audit qualifications recorded in `enrolled-canonical-final-forward-20261106.md` and `enrolled-canonical-final-rollback-20261106.md`. Their explicit fixture and installed-proof limits remain applicable.

## Actual PC preparation

Fresh authenticated SSH reached cluster192.168.50.89 and main192.168.50.88 as UID1000. Both were lightly loaded. Cluster mounted root had112GiB free; main had1.4TiB free. No claim about unallocated physical disk capacity is made.

A NEW UID1000/mode0700 directory was created on each:
`/home/bkoltz/.local/share/ledgetop-processing-reference/release-candidates/release-eac1db21-9469-4d39-985c-61480d26c674`.

- Both received the exact source archive, verified against its expected byte count and SHA through a nofollow single-link descriptor, then fsynced with mode0600.
- Cluster also received the exact saved image with the same verification. It was loaded into Docker's image cache and an isolated readonly/no-network UID10001 container verified all99 source hashes and dependency versions. That container auto-removed; its exact task-label inventory was empty.
- Main received source only. No second orchestrator was launched.
- Existing coordinator, native engines and ingress containers retained their existing names and uptimes. No worker unit, timer, live config, native task, old Gate or NAS setting changed.

## Captured original binding inputs

Both original Gate files were independently opened readonly/nofollow, UID1000/mode0600/single-link, size-bounded and descriptor-identity checked. Their existing reservation was empty; no Gate constructor, writer or lease was opened. New private copies and typed host/service/Gate preparation bindings were fsynced under `node-binding-capture` inside each new release directory.

| Node | Original Gate bytes/SHA-256 | Binding bytes/SHA-256 |
| --- | --- | --- |
| cluster | 590 / `5966c9cfbe78abd5f544a93fde4d86a62d69549e244297211a8bf35df05d5a05` | 640 / `93db338a77abcb3ba099979fb739693eb1454896bc3887c4fff3d19b8464f3ab` |
| main | 603 / `34c3744d14137f388793d25836c7737e713fdf485e01fc119d82106284dbf1f4` | 634 / `e79f20bb9d041b94045df5fa6551fb520eca08a9417d06c5c2fe436d4c955bb4` |

Both declared authority `7131882e-38ce-4d3e-99cc-4ba5a94d2ee8`, generation1. Main's two private copies were relayed over authenticated SSH to cluster `main-node-binding-import` and rechecked mode0600/UID1000/hash, without printing their contents. These are captured preparation inputs, not continuing idle/physical-custody authority. Installation/activation must freshly validate original identities and current state.

Private Viewer signer/provider configuration, the NAS mount/marker/durability boundary, actual generated bootstrap/final and worker config artifacts, main's privileged installer invocation and connected acceptance remain outstanding. Beau controls every TrueNAS administrative change. The read-only Viewer inventory request is pending; do not guess existing signing-key availability or replace keys on that assumption.
