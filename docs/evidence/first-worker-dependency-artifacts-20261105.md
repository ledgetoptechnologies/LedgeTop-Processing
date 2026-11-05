# Real per-host dependency artifact receipt

On November 5, new unprivileged operation directories were prepared sequentially
over the existing authenticated SSH connections. All binary downloads and pip
children ran under a foreground supervisor with a one-CPU affinity, finite
timeouts, a private process group, parent-death signal and explicit wait/reap.
No child remained after terminal checks. No native image, existing environment,
service, system Python, permissions outside the new artifacts, or NAS data was
changed. Nothing was installed as a service or started.

## Actual artifact descriptors

All paths below are beneath
`/home/bkoltz/.local/share/ledgetop-processing-reference/dependency-candidates/`.

| Field | Main .88 | Cluster .89 |
| --- | --- | --- |
| New directory | `worker-artifacts-e34cf17c-fd11-4712-a590-7a03adfdb45f` | `worker-artifacts-29abdcda-6840-4fa9-8015-43e210e0c389` |
| Python | 3.10.12 | 3.14.4 |
| Interpreter SHA-256 | `a2f33a6e006989270f4340528eb61f8f97366e00a5d1b602ac8672ea44fc56ae` | `52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41` |
| Dependency TAR | `dependencies.tar` | `dependencies.tar` |
| TAR bytes | 34,437,120 | 46,110,720 |
| TAR SHA-256 | `be2b9ffba448d629ea82d231468eb97a308ff42cebc95936bac6a6a338d815ff` | `75e27f7afb3aae42c8a827f0522d14b07a3fdb4b2291be832f7db389607aadb1` |
| Environment regular files | 198 | 190 |
| Expanded regular bytes | 34,263,695 | 45,939,550 |
| Inventory SHA-256 | `82e8bc0e29a2d3251b088e4e6d0a7f4cbf7f2dcc1538f7a87743d62086cc7c65` | `673bcf3bf420cc091444e101ae2ab87b34ac43edce45de3f2c290d01847e03a0` |

The archive and `inventory.json` are mode 0600, owned by UID 1000. The TARs are
plain USTAR, exact regular-file/directory namespaces with no duplicate/traversal
entries, symbolic links, hard links or special files. `pyvenv.cfg` disables
system-site packages. Version and import checks succeeded for all four packages;
a synthetic Ed25519 sign/verify passed on each actual interpreter.

The existing pip 25.3 was used only as an installer tool, without changing its
environment. A NEW `venv --copies --without-pip` was created per host. Its one
standard `lib64 -> lib` directory alias was checked and removed before assembly.
Packages were downloaded with `--only-binary=:all: --no-deps`, then installed
into the new environment with `--no-index --no-deps --no-compile` using only the
four downloaded wheels. There was no source build or package-version upgrade.

## Exact downloaded wheel inputs

| Host | Wheel | Bytes | SHA-256 |
| --- | --- | ---: | --- |
| Main | `cffi-2.1.1-cp310-cp310-manylinux2014_x86_64.manylinux_2_17_x86_64.whl` | 218,742 | `194cffa889098ced9976c3fc6340305e43f6303657d298da55366907c05c22d6` |
| Main | `cryptography-50.0.2-cp39-abi3-manylinux_2_34_x86_64.whl` | 4,780,973 | `1981f1db4630889b9ef7803fadef12b056f428cb6b85c27ba57b774793b6093c` |
| Main | `pycparser-2.23-py3-none-any.whl` | 118,140 | `e5c6e8d3fbad53479cab09ac03729e0a9faf2bee3db8208a550daf5af81a5934` |
| Cluster | `cffi-2.1.1-cp314-cp314-manylinux2014_x86_64.manylinux_2_17_x86_64.whl` | 221,525 | `b0431303acaea1089ad4b3e9ce4e6518193def1118d4073ca848635ee4ea2e96` |
| Cluster | `cryptography-50.0.2-cp311-abi3-manylinux_2_34_x86_64.whl` | 4,752,576 | `9dab55f57c74c3cad24c323bacbbd04be4705ba6eb0d92e920b1fc4837ed5079` |
| Cluster | `pycparser-3.12-py3-none-any.whl` | 62,600 | `a076f3f489b39b43ed13563a1dafe42447445bec2e294154ec5c57fcdda4c85e` |
| Both | `rfc8785-0.1.4-py3-none-any.whl` | 9,240 | `520d690b448ecf0703691c76e1a34a24ddcd4fc5bc41d589cb7c58ec651bcd48` |

These are actual downloaded wheel identities, not a claim of independently
audited upstream provenance. Their review and approval remain required before
privileged enrollment.

## External interpreter assumptions and retained evidence

The copied environments still depend on each host's existing OS interpreter
stdlib/shared-library installation; they are not cross-host portable runtime
images. Inventories record actual system stdlib files, sizes, hashes and modes:
main 695 files / 13,989,565 bytes, digest
`a1b2a001ee7ae5c92b958344ebff7a1c4d14b3d1ba9db1e18633e72842d51582`;
cluster 673 files / 15,528,163 bytes, digest
`3c277067d678e776c779669788d2853928090701d45cb318c33155bc34bec2b2`.
Files and resolved targets/ancestors were checked non-writable by service UID.
Main's three OS stdlib aliases were recorded as external resolved dependencies,
not included in the symlink-free TAR. Initial assembly held on those aliases;
after explicit read-only diagnosis, assembly resumed in the same owned directory
without repeating downloads or adopting foreign files. This does not constitute
a complete native shared-library inventory or future OS update guarantee.

Each new directory also contains the exact `source-99578a6896df.tar` described in
[the source checkpoint](first-worker-artifact-plan-20261105.md), SHA
`da2670855b91df079beaf9ece8994fe66821e0d19e5e0b8c92b4cc274ae4b778`,
8,642,560 bytes, UID 1000 / mode 0600. Both copies were hash-verified after
transfer. The task-owned local source archive and safe inventory copies remain
under `C:/Users/fstor/AppData/Local/Temp/ltds-first-worker-artifacts-20261105/`.

The next missing manifest inputs are private config/TLS descriptor, fresh
host/engine/Gate identities, complete approved closed manifest and rendered unit
hash. Final service-UID read-only validation follows privileged installation in
its separately reviewed step. Main's exact noninteractive root installer probe
still requires administrator execution; no sudo/ACL workaround was applied.
