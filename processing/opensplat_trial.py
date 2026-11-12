"""Offline inspection of OpenSplat Gaussian PLYs; never trains or publishes.

This deliberately has no coordinator, Viewer, Docker or cleanup integration.
The format profile follows OpenSplat Model::savePly at 62fd86fe (v1.2.2).
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import struct
import sys


PROFILE = "opensplat-gaussian-ply-float32-v1"
MAX_BYTES = 512 * 1024 * 1024
MAX_VERTICES = 2_000_000
MAX_HEADER_BYTES = 16 * 1024
PREFIX = ["x", "y", "z", "nx", "ny", "nz", "f_dc_0", "f_dc_1", "f_dc_2"]
SUFFIX = ["opacity", "scale_0", "scale_1", "scale_2", "rot_0", "rot_1", "rot_2", "rot_3"]


class TrialError(ValueError):
    """A stable, data-free validation failure code."""


def _require(ok, code):
    if not ok:
        raise TrialError(code)


def _identity(stat):
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _unlinked_path(path):
    path = Path(path).absolute()
    # Refuse links/junctions through every existing component. This is a local
    # offline utility, not a hostile concurrent filesystem admission boundary.
    for part in (path, *path.parents):
        _require(not part.is_symlink() and not getattr(part, "is_junction", lambda: False)(),
                 "linked_path")
    return path


def _plain_file(path):
    path = _unlinked_path(path)
    _require(path.is_file(), "regular_file_required")
    return path


def inspect_ply(path):
    """Stream every float, hash the exact bytes and reject malformed scene data.

    Fixed caps bound memory and I/O. Validation is structural and numeric only;
    it does not establish visual quality, source provenance or a usable CRS.
    """
    path = _plain_file(path)
    before = path.stat()
    _require(0 < before.st_size <= MAX_BYTES, "ply_byte_limit")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        header = bytearray()
        while True:
            line = stream.readline(MAX_HEADER_BYTES + 1 - len(header))
            header.extend(line)
            _require(line and len(header) <= MAX_HEADER_BYTES, "ply_header_limit")
            if line == b"end_header\n":
                break
        digest.update(header)
        try:
            lines = header.decode("ascii").splitlines()
        except UnicodeDecodeError as exc:
            raise TrialError("ply_header_ascii") from exc
        _require(lines[:2] == ["ply", "format binary_little_endian 1.0"], "ply_encoding")
        body = [line for line in lines[2:-1] if not line.startswith("comment ")]
        _require(body and body[0].startswith("element vertex "), "ply_vertex_element")
        try:
            count = int(body[0].removeprefix("element vertex "))
        except ValueError as exc:
            raise TrialError("ply_vertex_count") from exc
        _require(0 < count <= MAX_VERTICES, "ply_vertex_limit")
        properties = body[1:]
        degree = None
        names = None
        for candidate in range(4):
            rest = 3 * ((candidate + 1) ** 2 - 1)
            expected = PREFIX + [f"f_rest_{i}" for i in range(rest)] + SUFFIX
            if properties == [f"property float {name}" for name in expected]:
                degree, names = candidate, expected
                break
        _require(names is not None, "ply_gaussian_properties")
        row = struct.Struct("<" + "f" * len(names))
        _require(before.st_size == len(header) + count * row.size, "ply_payload_size")
        bounds_min, bounds_max = [math.inf] * 3, [-math.inf] * 3
        remaining = count
        while remaining:
            batch = min(remaining, 1024)
            payload = stream.read(batch * row.size)
            _require(len(payload) == batch * row.size, "ply_truncated")
            digest.update(payload)
            for values in row.iter_unpack(payload):
                _require(all(math.isfinite(value) for value in values), "ply_nonfinite")
                # OpenSplat writes unnormalized quaternion parameters. A zero
                # quaternion still cannot describe an orientation.
                _require(any(value != 0 for value in values[-4:]), "ply_zero_rotation")
                for axis in range(3):
                    bounds_min[axis] = min(bounds_min[axis], values[axis])
                    bounds_max[axis] = max(bounds_max[axis], values[axis])
            remaining -= batch
        _require(not stream.read(1), "ply_trailing_bytes")
    _require(_identity(before) == _identity(path.stat()), "file_changed")
    return dict(format=PROFILE, plyVersion="1.0", encoding="binary_little_endian",
                byteSize=before.st_size, sha256=digest.hexdigest(), gaussianCount=count,
                sphericalHarmonicsDegree=degree, properties=names,
                bounds=dict(min=bounds_min, max=bounds_max),
                validation=dict(structural=True, allFloatsFinite=True,
                                visualQuality="unverified", coordinateReference="unverified"))


def inspect_trial(path):
    """Return a versioned private inspection report, never an acceptance receipt."""
    return dict(schemaVersion=1, kind="opensplat-offline-inspection",
                publicationEligible=False, viewerReady=False,
                provenance=dict(trainingRun="unverified", toolBuild="unverified",
                                inputIdentity="unverified"),
                coordinates=dict(reference=None, units=None, transform=None,
                                 status="unknown"),
                artifact=inspect_ply(path), validationErrors=[])


def plan_command(project, output_dir, *, iterations=2000, downscale=2):
    """Generate an argv plan for a prepared COLMAP project, without execution.

    Presence checks are not a COLMAP parser or proof of matching images/poses.
    Run only on isolated scratch copies after node/resource ownership is agreed.
    """
    project = _unlinked_path(project).resolve()
    output_dir = _unlinked_path(output_dir).resolve()
    _require(type(iterations) is int and 1 <= iterations <= 30000, "iterations_limit")
    _require(type(downscale) is int and 1 <= downscale <= 8, "downscale_limit")
    _require(project.is_dir() and (project / "images").is_dir(), "colmap_images_required")
    _require(not (project / "transforms.json").exists(), "ambiguous_input_format")
    # Match v1.2.2 colmap.cpp exactly: root camera file wins; otherwise
    # sparse/0 is used. A direct sparse/*.bin export needs preparation first.
    sparse = project if (project / "cameras.bin").exists() else project / "sparse" / "0"
    for name in ("cameras.bin", "images.bin", "points3D.bin"):
        model = _plain_file(sparse / name)
        _require(model.stat().st_size > 8, "colmap_model_empty")
    _require(output_dir != project and project not in output_dir.parents
             and output_dir not in project.parents, "separate_output_required")
    _require(not output_dir.exists(), "fresh_output_required")
    argv = ["opensplat", str(project), "--num-iters", str(iterations),
            "--downscale-factor", str(downscale), "--save-every", "-1",
            "--output", str(output_dir / "scene.ply")]
    return dict(schemaVersion=1, kind="opensplat-command-plan", executesWork=False,
                inputValidation="presence-only", argv=argv,
                coordinatePolicy="preserve-input-no-center-flag",
                prerequisites=["isolated scratch input and working directory",
                               "verified engine build and CLI compatibility",
                               "exclusive physical-node resource admission",
                               "create fresh output directory before execution"],
                viewerReady=False, publicationEligible=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("ply")
    plan = commands.add_parser("plan")
    plan.add_argument("project")
    plan.add_argument("output_dir")
    plan.add_argument("--iterations", type=int, default=2000)
    plan.add_argument("--downscale", type=int, default=2)
    args = parser.parse_args(argv)
    try:
        result = (inspect_trial(args.ply) if args.command == "inspect" else
                  plan_command(args.project, args.output_dir,
                               iterations=args.iterations, downscale=args.downscale))
    except (TrialError, OSError) as exc:
        code = str(exc) if isinstance(exc, TrialError) else "local_io_error"
        print(json.dumps(dict(schemaVersion=1, viewerReady=False,
                              publicationEligible=False, validationErrors=[code])))
        return 2
    print(json.dumps(result, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
