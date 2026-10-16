# Dependency licenses

Reviewed 2026-10-16 against upstream license files and official PyPI release metadata. These are the dependencies for the local structural and classification prototypes and synthetic tests. TreeDar's own project license remains unassigned; dependency licenses do not assign one.

## Code

| Dependency | Candidate version | Purpose | Upstream license evidence |
| --- | --- | --- | --- |
| laspy | 2.7.0 | LAS/LAZ input and synthetic fixture output | [BSD-2-Clause](https://github.com/laspy/laspy/blob/2.7.0/LICENSE.txt) |
| lazrs | 0.8.2 | LAZ compression backend | [MIT Python bindings](https://github.com/laz-rs/laz-rs-python/blob/0.8.2/LICENSE.txt); underlying [laz-rs uses Apache-2.0](https://github.com/laz-rs/laz-rs/blob/master/LICENSE.txt) |
| NumPy | 2.5.4 | Numeric arrays | [BSD-3-Clause core](https://github.com/numpy/numpy/blob/v2.5.4/LICENSE.txt); release [package metadata](https://github.com/numpy/numpy/blob/v2.5.4/pyproject.toml) also lists 0BSD, MIT, Zlib and CC0-1.0 for included code |
| SciPy | 1.18.1 | Interpolation and spatial/numeric operations | [BSD-3-Clause core](https://github.com/scipy/scipy/blob/v1.18.1/LICENSE.txt); additional [bundled licenses](https://github.com/scipy/scipy/blob/v1.18.1/LICENSES_bundled.txt) apply |
| scikit-image | 0.26.0 | Canopy segmentation | [BSD-3-Clause with listed BSD-2-Clause and MIT files](https://github.com/scikit-image/scikit-image/blob/v0.26.0/LICENSE.txt) |
| scikit-learn | 1.9.1 | Local random-forest classification and probability calibration | [BSD-3-Clause](https://github.com/scikit-learn/scikit-learn/blob/1.9.1/COPYING) |
| pytest | 9.1.1 | Tests only | [MIT](https://github.com/pytest-dev/pytest/blob/9.1.1/LICENSE) |
| pyproj | 3.8.0 | CRS parsing/validation if used | [MIT](https://github.com/pyproj4/pyproj/blob/3.8.0/LICENSE); underlying [PROJ license](https://github.com/OSGeo/PROJ/blob/master/COPYING) is MIT style |
| rasterio | 1.5.2 | GeoTIFF output | [BSD-3-Clause](https://github.com/rasterio/rasterio/blob/1.5.2/LICENSE.txt); bundled GDAL has [MIT style general terms and component notices](https://github.com/OSGeo/gdal/blob/master/LICENSE.TXT) |
| affine | 2.4.0 | Raster coordinate transforms | [BSD-3-Clause](https://github.com/rasterio/affine/blob/2.4.0/LICENSE.txt) |
| Matplotlib | 3.11.2 | Local area and tree-map PNG previews using the Agg backend | [Matplotlib license agreement, PSF-based](https://github.com/matplotlib/matplotlib/blob/v3.11.2/LICENSE/LICENSE); preserve [bundled component notices](https://github.com/matplotlib/matplotlib/tree/v3.11.2/LICENSE) |

Retain copyright, license and applicable attribution notices when redistributing dependencies. Apache-2.0 also carries its own redistribution and modification-notice requirements. The table summarizes direct dependencies, not every bundled library or transitive package. In particular, SciPy's bundled notices include BSD, MIT, Boost, Qhull and Apache/LLVM terms; binary wheels may include additional native runtime notices. Preserve the actual installed distributions' license files with any redistributed environment and review the resolved dependency set before distribution.

The classification dependency adds the following exact transitive versions to `requirements-test.lock`. These licenses cover their upstream code; preserve any bundled notices with redistributed wheels.

| Dependency | Locked version | Purpose | Upstream license evidence |
| --- | --- | --- | --- |
| joblib | 1.6.0 | scikit-learn parallel execution and local model serialization | [BSD-3-Clause](https://github.com/joblib/joblib/blob/1.6.0/LICENSE.txt) |
| threadpoolctl | 3.7.0 | scikit-learn native thread-pool control | [BSD-3-Clause](https://github.com/joblib/threadpoolctl/blob/3.7.0/LICENSE) |
| narwhals | 2.27.1 | scikit-learn dataframe interoperability | [MIT](https://github.com/narwhals-dev/narwhals/blob/v2.27.1/LICENSE.md) |
| cloudpickle | 3.1.2 | joblib serialization dependency | [BSD-3-Clause](https://github.com/cloudpipe/cloudpickle/blob/v3.1.2/LICENSE) |

Map previews add these exact transitive versions, retaining all existing lock pins:

| Dependency | Locked version | Purpose | Upstream license evidence |
| --- | --- | --- | --- |
| ContourPy | 1.4.0 | Matplotlib contour generation | [BSD-3-Clause](https://github.com/contourpy/contourpy/blob/v1.4.0/LICENSE) |
| cycler | 0.12.1 | Matplotlib property cycles | [BSD-3-Clause](https://github.com/matplotlib/cycler/blob/v0.12.1/LICENSE) |
| fonttools | 4.67.0 | Matplotlib font handling | [MIT](https://github.com/fonttools/fonttools/blob/4.67.0/LICENSE) |
| kiwisolver | 1.5.1 | Matplotlib layout constraints | [BSD-3-Clause](https://github.com/nucleic/kiwi/blob/1.5.1/LICENSE) |
| python-dateutil | 2.9.0.post0 | Matplotlib date handling | [BSD-3-Clause applies to all code; specified contributions also Apache-2.0](https://github.com/dateutil/dateutil/blob/2.9.0.post0/LICENSE) |
| six | 1.17.0 | python-dateutil compatibility dependency | [MIT](https://github.com/benjaminp/six/blob/1.17.0/LICENSE) |

The Matplotlib agreement requires retaining its copyright and license, and a brief summary of changes when distributing a modified Matplotlib derivative. Wheel redistribution must preserve its component notices, including font and native-library notices.

## Python 3.13 feasibility

The official PyPI metadata for [laspy 2.7.0](https://pypi.org/pypi/laspy/2.7.0/json), [lazrs 0.8.2](https://pypi.org/pypi/lazrs/0.8.2/json), [NumPy 2.5.4](https://pypi.org/pypi/numpy/2.5.4/json), [SciPy 1.18.1](https://pypi.org/pypi/scipy/1.18.1/json), [scikit-image 0.26.0](https://pypi.org/pypi/scikit-image/0.26.0/json), [pytest 9.1.1](https://pypi.org/pypi/pytest/9.1.1/json) and [pyproj 3.8.0](https://pypi.org/pypi/pyproj/3.8.0/json) lists universal wheels or Windows x86-64 CPython 3.13 wheels. Declared Python requirements permit 3.13, except lazrs does not declare a Python minimum in that metadata; its CPython 3.13 wheel establishes availability for this interpreter.

Declared direct constraints are compatible: laspy's lazrs extra requires `>=0.8.0,<0.9.0`; SciPy requires NumPy `>=2.0.0,<2.8`; scikit-image requires NumPy `>=1.24` and SciPy `>=1.11.4`. These candidate versions are metadata checks, not an installed-environment lock or evidence that end-to-end tests passed. Packaging and tested version records should reflect the environment actually validated.

[Rasterio 1.5.2](https://pypi.org/pypi/rasterio/1.5.2/json) requires Python `>=3.12` and provides a Windows x86-64 CPython 3.13 wheel. [Affine 2.4.0](https://pypi.org/pypi/affine/2.4.0/json) requires Python `>=3.7` and provides a universal wheel. The intended reproducible lock covers Windows x86-64 Python 3.13.14 only. Broad project dependency ranges may resolve older releases for Python 3.11+, but those environments are untested; the exact candidates above are not a Python 3.11 lock.

[Scikit-learn 1.9.1 metadata](https://pypi.org/pypi/scikit-learn/1.9.1/json) requires Python `>=3.11`, NumPy `>=1.24.1`, SciPy `>=1.10.0`, joblib `>=1.4.0`, narwhals `>=2.0.1`, and threadpoolctl `>=3.5.0`. It provides a Windows x86-64 CPython 3.13 wheel. The existing NumPy 2.5.4 and SciPy 1.18.1 pins satisfy those requirements. [Joblib 1.6.0](https://pypi.org/pypi/joblib/1.6.0/json) requires Python `>=3.10` and cloudpickle `>=3.0`; [threadpoolctl 3.7.0](https://pypi.org/pypi/threadpoolctl/3.7.0/json), [narwhals 2.27.1](https://pypi.org/pypi/narwhals/2.27.1/json), and [cloudpickle 3.1.2](https://pypi.org/pypi/cloudpickle/3.1.2/json) require Python `>=3.9`, `>=3.10`, and `>=3.8` respectively. All four transitive packages provide universal Python 3 wheels. A pip dry run resolved these five additions with the unchanged existing lock on Windows Python 3.13.14; this metadata check does not establish classifier accuracy or test results.

## Map preview dependency feasibility

[Matplotlib 3.11.2 metadata](https://pypi.org/pypi/matplotlib/3.11.2/json) requires Python `>=3.11` and lists ContourPy `>=1.0.1`, cycler `>=0.10`, fonttools `>=4.28.2`, kiwisolver `>=1.3.1`, NumPy `>=1.25`, packaging `>=20.0`, Pillow `>=9`, pyparsing `>=3`, and python-dateutil `>=2.7`. Existing NumPy 2.5.4, packaging 26.3, Pillow 12.3.0, and pyparsing 3.3.3 pins satisfy those constraints. [ContourPy 1.4.0](https://pypi.org/pypi/contourpy/1.4.0/json) requires Python `>=3.12` and NumPy `>=2.0`; [cycler 0.12.1](https://pypi.org/pypi/cycler/0.12.1/json) requires Python `>=3.8`; [fonttools 4.67.0](https://pypi.org/pypi/fonttools/4.67.0/json) requires Python `>=3.11`; [kiwisolver 1.5.1](https://pypi.org/pypi/kiwisolver/1.5.1/json) requires Python `>=3.10`. [Python-dateutil 2.9.0.post0](https://pypi.org/pypi/python-dateutil/2.9.0.post0/json) requires six `>=1.5`; it and [six 1.17.0](https://pypi.org/pypi/six/1.17.0/json) permit Python 3.13. Matplotlib, ContourPy, fonttools, and kiwisolver provide Windows x86-64 CPython 3.13 wheels; the remaining additions provide universal wheels. A pip dry run resolved all seven additions against the existing lock without changing earlier pins. This exact lock remains scoped to Windows Python 3.13; other environments are unverified.

## Container base and Linux wheel availability

Reviewed 2026-10-16 using public Docker Hub registry manifests and official upstream metadata, without pulling image layers or downloading wheels. The official [`python`](https://hub.docker.com/_/python) image tag `3.13.16-slim-trixie` resolves to OCI index digest `sha256:70729b46c69b4f1e97c4822c1af3df53a1476cf5ddc6c087c0c10bc3a5678c2f`. The registry's `Docker-Content-Digest` matched a locally computed SHA-256 of the manifest response. Its Linux amd64 child manifest is `sha256:8fb4cfa1a2616d7b8e0c2175cc6ad68f5729c34ea8488c0b360d2934b7be9024`. The index supports multiple architectures, but this dependency check covers only Linux x86-64 CPython 3.13.

The image annotations identify [source revision `d2a898cb88e46203577eabc62cdcf6e92aaee176`](https://github.com/docker-library/python/blob/d2a898cb88e46203577eabc62cdcf6e92aaee176/3.13/slim-trixie/Dockerfile), whose Dockerfile builds CPython 3.13.16 on `debian:trixie-slim`. The amd64 annotation records Debian base digest `sha256:918311b7b6c4c6f68b232ba516584925f6c78ad82b6fd534b98979df6438e483`. The current [official-image tag definition](https://github.com/docker-library/official-images/blob/master/library/python) identifies 3.13.16 as the patch release behind the moving `3.13-slim-trixie` alias at review time. The digest freezes that image and must be deliberately updated for later Python or Debian fixes.

| Component | Version/source | License evidence and notice handling |
| --- | --- | --- |
| CPython | 3.13.16 | [PSF License Version 2 and retained historical/component notices](https://github.com/python/cpython/blob/v3.13.16/LICENSE); preserve the installed Python license and applicable component licenses |
| Official image build scripts | Source revision above | [MIT](https://github.com/docker-library/python/blob/d2a898cb88e46203577eabc62cdcf6e92aaee176/LICENSE); this covers image build code, not every package in the image |
| Debian runtime packages | Debian 13 (trixie) slim base | [Package-specific licenses](https://www.debian.org/legal/licenses/), including copyleft terms where applicable; retain `/usr/share/doc/*/copyright` and `/usr/share/common-licenses` as described by [Debian Policy 12.5](https://www.debian.org/doc/debian-policy/ch-docs.html#copyright-information) |

Before redistributing an image, retain Python distribution notices, Python wheels' `.dist-info` license files and bundled native-library/font notices, and Debian package notices. Review the actual installed Debian package inventory and corresponding source obligations for any GPL/LGPL components; this metadata review does not inventory the image filesystem or assign one license to the complete container. TreeDar's own license remains unassigned.

All 37 exact entries in `requirements-test.lock` have a universal wheel or a standard CPython 3.13 Linux x86-64 manylinux wheel in their version-specific official PyPI release metadata. No missing-wheel blocker was found. The native packages checked are [ContourPy 1.4.0](https://pypi.org/pypi/contourpy/1.4.0/json), [fonttools 4.67.0](https://pypi.org/pypi/fonttools/4.67.0/json), [kiwisolver 1.5.1](https://pypi.org/pypi/kiwisolver/1.5.1/json), [lazrs 0.8.2](https://pypi.org/pypi/lazrs/0.8.2/json), [Matplotlib 3.11.2](https://pypi.org/pypi/matplotlib/3.11.2/json), [NumPy 2.5.4](https://pypi.org/pypi/numpy/2.5.4/json), [Pillow 12.3.0](https://pypi.org/pypi/pillow/12.3.0/json), [pyproj 3.8.0](https://pypi.org/pypi/pyproj/3.8.0/json), [Rasterio 1.5.2](https://pypi.org/pypi/rasterio/1.5.2/json), [scikit-image 0.26.0](https://pypi.org/pypi/scikit-image/0.26.0/json), [scikit-learn 1.9.1](https://pypi.org/pypi/scikit-learn/1.9.1/json), and [SciPy 1.18.1](https://pypi.org/pypi/scipy/1.18.1/json). Their selected manylinux tags require glibc versions no newer than 2.28; [Debian trixie ships glibc 2.41](https://packages.debian.org/trixie/libc6), which meets that platform baseline. Wheel availability establishes installation feasibility only. It does not establish a successful image build, working native-library imports, passing Linux tests, or support for other architectures.

## Model weights and datasets

Synthetic tests use no pretrained model weights or external datasets. Synthetic point clouds and classification fixtures are generated locally by project code; any classifier fitted to those fixtures establishes software behavior only, not accuracy on real forests. Dependency packages may ship examples or reference databases; those are separate from external forest observations. Do not add downloaded imagery, point clouds, training labels, pretrained weights, or PROJ transformation grids without a separate source and license record. Client and hunting-land data remain outside Git history.
