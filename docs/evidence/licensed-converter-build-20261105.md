# Licensed converter candidate — November 5, 2026

## Exact source and build

Viewer source: `6718d1beca0f7fbf8cbaae2bbec14326e1e053bb`.
The converter-only Docker target completed successfully using the existing
Dockerfile, its locked dependencies and a private BuildKit license secret.
The task-owned builder was limited to one CPU and 2 GiB and stopped afterward.
No image was published or deployed.

The initial export failed the Dockerfile's checksum check before compilation.
On this Windows host, `git archive` applied `core.autocrlf` and changed
`third_party/obj2tiles/locks/SHA256SUMS` to CRLF. Its exported digest was
`c25743b024571273a8187f4dba1b3e044095dc747e16e0705e3ff0fe443e1717`.
The corrected archive used `git -c core.autocrlf=false archive`; both the
Dockerfile and checksum file were compared byte-for-byte with committed blobs.
No checksum was changed or bypassed. The corrected source archive SHA-256 is
`e74dfde26e17d409c6f7a94694623880bf254696745ad0e91ae8e08bc9ea488b`.

Verified build-info fields:

| Field | Value |
| --- | --- |
| Schema | 2 |
| Obj2Tiles | v1.6.2 |
| Source SHA-256 | `79093e12f6eab2cfcd522aebe670892c5d8874e160956b84f3e55c77b94ac0b5` |
| Patch SHA-256 | `fb35e1f99e76d6af99748681b65e08c3e0072da23cb7d61098c4bea1e45e604c` |
| ImageSharp | 3.2.0 |
| Lock set SHA-256 | `b30f6a37f1d3cc542ea10a484672750d93af70d76b57257dce2ff47abab64d98` |
| License-notice manifest SHA-256 | `86768eb4bb03feacad31ba6ec90c003abafdebeb5257a1c3eeb1b4f348530a4e` |
| Executable SHA-256 | `a68fd91118342b106e521d064a11066611aadf089b79e67a16503fac082bb155` |

The executable is 77,008,939 bytes. Its actual digest matched build-info.
The complete 11-file export was scanned for `.lic` files and the private
license's exact bytes; neither was present. The export includes third-party
license notices. Private license material and raw build output are excluded
from this evidence record and the repository.

## Qualification boundary

Successful compilation and digest verification do not prove conversion,
publication, installed Viewer behavior, photogrammetry or NAS durability.
The paired qualification completed **6/6 tests, no skips, in 67.585 seconds**:
`tests.test_viewer_result_v2_roundtrip` and
`tests.test_viewer_provider_reference_pairing`. It paired this exact executable
with Viewer 6718 and Processing source `ad9e9d3` (integration HEAD `fc72a2e`,
documentation-only changes after that source).

The five round-trip cases exercise whole/range source intake, derived GCP input
without forwarding the raw CSV, lost acknowledgement and worker cleanup, and
the actual controlled converter, derivative verification and publication routes.
The sixth case exercises actual Viewer provider admission over loopback TLS.
NodeODM remains a synthetic engine fixture; this does not prove real mapping,
installed containers, rendered browser behavior, TrueNAS durability or client
dataset processing. The earlier 682-test run used a different converter and
Viewer base and remains separate evidence.

The runner used one CPU, 2 GiB, read-only source/artifact mounts, external
networking disabled, and an owned disposable disk volume. Its container exited
and its ownership-verified volume was removed. Qualification log identifier:
`ltds-combined-qualification-7e17a75621ec`.
