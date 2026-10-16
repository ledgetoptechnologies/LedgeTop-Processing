# Dependency licenses

Reviewed 2026-10-16 against upstream license files and official PyPI release metadata. These are the proposed direct dependencies for the local structural prototype and synthetic tests. TreeDar's own project license remains unassigned; dependency licenses do not assign one.

## Code

| Dependency | Candidate version | Purpose | Upstream license evidence |
| --- | --- | --- | --- |
| laspy | 2.7.0 | LAS/LAZ input and synthetic fixture output | [BSD-2-Clause](https://github.com/laspy/laspy/blob/2.7.0/LICENSE.txt) |
| lazrs | 0.8.2 | LAZ compression backend | [MIT Python bindings](https://github.com/laz-rs/laz-rs-python/blob/0.8.2/LICENSE.txt); underlying [laz-rs uses Apache-2.0](https://github.com/laz-rs/laz-rs/blob/master/LICENSE.txt) |
| NumPy | 2.5.4 | Numeric arrays | [BSD-3-Clause core](https://github.com/numpy/numpy/blob/v2.5.4/LICENSE.txt); release [package metadata](https://github.com/numpy/numpy/blob/v2.5.4/pyproject.toml) also lists 0BSD, MIT, Zlib and CC0-1.0 for included code |
| SciPy | 1.18.1 | Interpolation and spatial/numeric operations | [BSD-3-Clause core](https://github.com/scipy/scipy/blob/v1.18.1/LICENSE.txt); additional [bundled licenses](https://github.com/scipy/scipy/blob/v1.18.1/LICENSES_bundled.txt) apply |
| scikit-image | 0.26.0 | Canopy segmentation | [BSD-3-Clause with listed BSD-2-Clause and MIT files](https://github.com/scikit-image/scikit-image/blob/v0.26.0/LICENSE.txt) |
| pytest | 9.1.1 | Tests only | [MIT](https://github.com/pytest-dev/pytest/blob/9.1.1/LICENSE) |
| pyproj | 3.8.0 | CRS parsing/validation if used | [MIT](https://github.com/pyproj4/pyproj/blob/3.8.0/LICENSE); underlying [PROJ license](https://github.com/OSGeo/PROJ/blob/master/COPYING) is MIT style |
| rasterio | 1.5.2 | GeoTIFF output | [BSD-3-Clause](https://github.com/rasterio/rasterio/blob/1.5.2/LICENSE.txt); bundled GDAL has [MIT style general terms and component notices](https://github.com/OSGeo/gdal/blob/master/LICENSE.TXT) |
| affine | 2.4.0 | Raster coordinate transforms | [BSD-3-Clause](https://github.com/rasterio/affine/blob/2.4.0/LICENSE.txt) |

Retain copyright, license and applicable attribution notices when redistributing dependencies. Apache-2.0 also carries its own redistribution and modification-notice requirements. The table summarizes direct dependencies, not every bundled library or transitive package. In particular, SciPy's bundled notices include BSD, MIT, Boost, Qhull and Apache/LLVM terms; binary wheels may include additional native runtime notices. Preserve the actual installed distributions' license files with any redistributed environment and review the resolved dependency set before distribution.

## Python 3.13 feasibility

The official PyPI metadata for [laspy 2.7.0](https://pypi.org/pypi/laspy/2.7.0/json), [lazrs 0.8.2](https://pypi.org/pypi/lazrs/0.8.2/json), [NumPy 2.5.4](https://pypi.org/pypi/numpy/2.5.4/json), [SciPy 1.18.1](https://pypi.org/pypi/scipy/1.18.1/json), [scikit-image 0.26.0](https://pypi.org/pypi/scikit-image/0.26.0/json), [pytest 9.1.1](https://pypi.org/pypi/pytest/9.1.1/json) and [pyproj 3.8.0](https://pypi.org/pypi/pyproj/3.8.0/json) lists universal wheels or Windows x86-64 CPython 3.13 wheels. Declared Python requirements permit 3.13, except lazrs does not declare a Python minimum in that metadata; its CPython 3.13 wheel establishes availability for this interpreter.

Declared direct constraints are compatible: laspy's lazrs extra requires `>=0.8.0,<0.9.0`; SciPy requires NumPy `>=2.0.0,<2.8`; scikit-image requires NumPy `>=1.24` and SciPy `>=1.11.4`. These candidate versions are metadata checks, not an installed-environment lock or evidence that end-to-end tests passed. Packaging and tested version records should reflect the environment actually validated.

[Rasterio 1.5.2](https://pypi.org/pypi/rasterio/1.5.2/json) requires Python `>=3.12` and provides a Windows x86-64 CPython 3.13 wheel. [Affine 2.4.0](https://pypi.org/pypi/affine/2.4.0/json) requires Python `>=3.7` and provides a universal wheel. The intended reproducible lock covers Windows x86-64 Python 3.13.14 only. Broad project dependency ranges may resolve older releases for Python 3.11+, but those environments are untested; the exact candidates above are not a Python 3.11 lock.

## Model weights and datasets

This prototype and its synthetic tests use no model weights or external datasets. Synthetic point clouds are generated locally by project code. They establish software behavior only, not accuracy on real forests. Dependency packages may ship examples or reference databases; those are separate from external forest observations. Do not add downloaded imagery, point clouds, training labels, pretrained weights, or PROJ transformation grids without a separate source and license record. Client and hunting-land data remain outside Git history.
