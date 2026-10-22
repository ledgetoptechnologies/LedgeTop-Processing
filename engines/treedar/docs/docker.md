# Local Docker processing

Build from `engines/treedar` in PowerShell, with Docker Desktop's Linux engine running:

```powershell
Set-Location C:\Projects\LedgeTop-Processing\engines\treedar
docker build --tag treedar .
docker run --rm treedar --help
```

The image uses the digest-pinned official Python 3.13.16 Debian Trixie slim base, installs exact versions from `requirements-test.lock`, and runs as user/group 10001. The build context includes only packaging files and `src/`; datasets, outputs, credentials, and local environments are excluded. The lock was originally validated on Windows; the Docker integration test validates its Linux installation and execution when Docker is available. See [dependency licenses](dependency-licenses.md) for base and package notices.

## Process a mounted dataset

Create an output folder on local disk and mount input read-only:

```powershell
New-Item -ItemType Directory -Force 'C:\TreeDar\Runs' | Out-Null
docker run --rm --network none `
  -v 'C:\TreeDar\Datasets:/data:ro' `
  -v 'C:\TreeDar\Runs:/runs' `
  treedar pipeline '/data/example.laz' --out '/runs/pilot-01'
```

Replace `example.laz` with the actual relative input path. Paths with spaces are supported by keeping each mount and container path quoted. The container reads the mounted original; it does not import another permanent raw copy into its image. Generated results persist under `C:\TreeDar\Runs\pilot-01` after the container exits. Use a separate output folder for each run. These run folders are intended for later import into the 3D Viewer.

For a first check, select an approved small area using `--bbox minx,miny,maxx,maxy` in the input CRS. Use `--no-qc` to skip the whole-file QC pass for repeated subsets. Avoid `--rm` only when deliberately retaining a stopped container for diagnosis; input and output files belong on the mounts either way.

Image building downloads software. The shown processing command has networking disabled; it does not call Jev or other remote inference services. Do not bake data or credentials into images. The same image and mount layout can run on the separate processing PC later.

## Memory and permissions

The existing 200-million-point flight was reported to peak around 24 GB of memory. This is a measurement for that dataset and processing version, not a guaranteed maximum. Docker Desktop's available memory must exceed the job's peak with headroom for the engine and other processes; 32 GB available to Docker is a reasonable initial budget for this job when the host has enough RAM. Larger clouds may need spatial subsets or tiling. Do not start a full flight under the synthetic test's 2 GB limit. Change Docker Desktop resource settings manually only after agreeing on the host's budget.

Windows bind mounts use Docker Desktop's host-file permissions. On a Linux processing host, make the output directory writable by UID/GID 10001, or run the image with an explicitly chosen host user via `--user`. Keep the input mount read-only. No privileged mode, Docker socket mount, or host-network mode is required.

## Container end-to-end check

```powershell
./.venv/Scripts/python.exe -m pytest tests/test_docker_pipeline.py -v
```

The test automatically skips if the Docker CLI or reachable Linux engine is unavailable. Otherwise it builds a uniquely tagged image, checks the non-root runtime and read-only input mount, then runs the actual pipeline on a generated synthetic LAZ. It checks tree counts, raster alignment, output images, and the absence of baked-in datasets. The test removes its own container and image afterward; it does not start Docker Desktop or change its settings. Building may take several minutes on the first run and requires access to the image/package registries.
