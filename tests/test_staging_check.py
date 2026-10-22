"""Staging acceptance against disposable Git checkouts and synthetic data only."""

import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
from uuid import uuid4

import pytest

from synthetic_forest import write_forest


def invoke(command, *, cwd, env=None, timeout=60):
    return subprocess.run(command, cwd=cwd, env=env, capture_output=True,
                          text=True, encoding="utf-8", errors="replace", timeout=timeout)


@pytest.fixture
def staging_checkout(tmp_path):
    git = shutil.which("git")
    bash = (Path("C:/Program Files/Git/bin/bash.exe") if os.name == "nt"
            else shutil.which("bash"))
    if not git or not bash or not Path(bash).is_file():
        pytest.skip("Git and Bash are required for the staging shell script")
    project = Path(__file__).resolve().parents[1]
    repo = tmp_path / "isolated checkout"
    repo.mkdir()
    for name in ("Dockerfile", ".dockerignore", "requirements-test.lock", "pyproject.toml", "README.md"):
        shutil.copy2(project / name, repo / name)
    shutil.copytree(project / "src", repo / "src", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    (repo / "scripts").mkdir()
    script = repo / "scripts" / "staging-check.sh"
    script.write_text((project / "scripts" / script.name).read_text(), newline="\n")
    # The script tags by commit: make our image unique even in simultaneous runs.
    (repo / "fixture-id").write_text(uuid4().hex)
    for args in (["init", "--quiet"], ["add", "."],
                 ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                  "commit", "--quiet", "-m", "Synthetic staging fixture"]):
        result = invoke([git, *args], cwd=repo)
        assert result.returncode == 0, result.stdout + result.stderr
    commit = invoke([git, "rev-parse", "--short=12", "HEAD"], cwd=repo).stdout.strip()
    root = tmp_path / "synthetic staging share"
    (root / "Datasets").mkdir(parents=True)
    scratch = root / "Scratch"
    scratch.mkdir(mode=0o755)
    scratch.chmod(0o755)
    bins = tmp_path / "tool shims"
    bins.mkdir()
    # Do not rely on Windows' python3 Store alias or a different Python install.
    (bins / "python3").write_text(
        "#!/usr/bin/env bash\nexec " + shlex.quote(Path(sys.executable).as_posix()) + ' "$@"\n',
        newline="\n")
    (bins / "python3").chmod(0o755)
    env = dict(os.environ, MSYS_NO_PATHCONV="1", MSYS2_ARG_CONV_EXCL="*")
    env["PATH"] = str(bins) + os.pathsep + env["PATH"]
    if os.name == "nt":
        env["PATH"] = str(bins) + os.pathsep + "C:/Program Files/Git/usr/bin" + os.pathsep + os.environ["PATH"]
    cfg = {"input": "synthetic.laz", "bbox": [0, 0, 32, 32], "args": ["--no-qc"],
           "limits": {"memory": "2g", "cpus": 2, "max_seconds": 120},
           "expect": {"n_trees": 3, "n_trees_tolerance": 0,
                      "height_median_m": 15, "height_tolerance_m": 1}}
    return {"repo": repo, "root": root, "script": script, "bash": str(bash),
            "env": env, "commit": commit, "config": cfg}


def run_check(fixture):
    return invoke([fixture["bash"], fixture["script"].as_posix(), fixture["root"].as_posix()],
                  cwd=fixture["repo"], env=fixture["env"], timeout=1200)


def write_config(fixture):
    (fixture["root"] / "staging-check.json").write_text(json.dumps(fixture["config"]))


def test_staging_check_missing_config(staging_checkout):
    result = run_check(staging_checkout)
    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stdout.strip() == (
        f"TREEDAR STAGING CHECK FAIL commit={staging_checkout['commit']} reason=missing-config")
    assert not list((staging_checkout["root"] / "Scratch").iterdir())


def test_staging_check_rejects_dirty_tracked_files(staging_checkout):
    if not shutil.which("docker"):
        pytest.skip("The script checks Docker CLI presence before dirty checkout detection")
    write_config(staging_checkout)
    # This is our disposable checkout, never the developer's source tree.
    (staging_checkout["repo"] / "fixture-id").write_text("dirty")
    result = run_check(staging_checkout)
    assert result.returncode == 1, result.stdout + result.stderr
    assert result.stdout.strip() == (
        f"TREEDAR STAGING CHECK FAIL commit={staging_checkout['commit']} reason=uncommitted-changes")
    assert not list((staging_checkout["root"] / "Scratch").iterdir())


def test_staging_check_real_docker_pass_and_wrong_expectation(staging_checkout):
    docker = shutil.which("docker")
    if not docker:
        pytest.skip("Docker CLI is unavailable")
    try:
        engine = invoke([docker, "info", "--format", "{{.OSType}}"],
                        cwd=staging_checkout["repo"], timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        pytest.skip("Docker engine is unavailable")
    if engine.returncode or engine.stdout.strip() != "linux":
        pytest.skip("A running Linux Docker engine is required")
    source = staging_checkout["root"] / "Datasets" / "synthetic.laz"
    write_forest(source)
    original = source.read_bytes()
    write_config(staging_checkout)
    image = "treedar:staging-check-" + staging_checkout["commit"]
    try:
        passed = run_check(staging_checkout)
        assert passed.returncode == 0, passed.stdout + passed.stderr
        assert passed.stdout.strip().startswith(
            f"TREEDAR STAGING CHECK PASS commit={staging_checkout['commit']} seconds=")
        assert "n_trees=3" in passed.stdout
        output = staging_checkout["root"] / "Scratch" / ("staging-check-" + staging_checkout["commit"])
        assert json.loads((output / "summary.json").read_text())["n_trees"] == 3
        if os.name != "nt":
            assert (output / "summary.json").stat().st_uid == os.getuid()
        staging_checkout["config"]["expect"]["n_trees"] = 999
        write_config(staging_checkout)
        failed = run_check(staging_checkout)
        assert failed.returncode == 1, failed.stdout + failed.stderr
        assert failed.stdout.strip().startswith("TREEDAR STAGING CHECK FAIL commit=")
        assert "n_trees=3(expected 999)" in failed.stdout
        assert source.read_bytes() == original
        assert list(source.parent.iterdir()) == [source]
    finally:
        # Unique fixture commit means this tag belongs exclusively to this test.
        if os.name != "nt":
            # UID 10001 creates nested directories; allow pytest's host UID to
            # remove its own temporary outputs after validation.
            permissions = (
                "from pathlib import Path; import os; "
                "[(p.chmod(0o777 if p.is_dir() else 0o666)) "
                "for p in Path('/runs').rglob('*') "
                "if not p.is_symlink() and p.stat().st_uid == os.getuid()]"
            )
            invoke([docker, "run", "--rm", "--network", "none", "--mount",
                    f"type=bind,source={staging_checkout['root'] / 'Scratch'},target=/runs",
                    "--entrypoint", "python", image, "-c", permissions],
                   cwd=staging_checkout["repo"])
        invoke([docker, "image", "rm", "--force", image], cwd=staging_checkout["repo"])


def test_staging_timeout_removes_running_container(staging_checkout):
    docker = shutil.which("docker")
    if not docker:
        pytest.skip("Docker CLI is unavailable")
    engine = invoke([docker, "info", "--format", "{{.OSType}}"],
                    cwd=staging_checkout["repo"], timeout=15)
    if engine.returncode or engine.stdout.strip() != "linux":
        pytest.skip("A running Linux Docker engine is required")
    repo = staging_checkout["repo"]
    # A disposable image with a deliberately long-running process makes the
    # timeout deterministic and proves cleanup stops a real running container.
    (repo / "src" / "treedar" / "__main__.py").write_text(
        "import time\nprint('FIXTURE STARTED', flush=True)\ntime.sleep(30)\n")
    # The installed console script calls cli.main, rather than __main__.
    with (repo / "src" / "treedar" / "cli.py").open("a") as stream:
        stream.write("\ndef main(argv=None):\n    import time\n    print('FIXTURE STARTED', flush=True)\n    time.sleep(30)\n")
    git = shutil.which("git")
    for args in (["add", "."], ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                                "commit", "--quiet", "-m", "Slow synthetic timeout fixture"]):
        result = invoke([git, *args], cwd=repo)
        assert result.returncode == 0, result.stdout + result.stderr
    commit = invoke([git, "rev-parse", "--short=12", "HEAD"], cwd=repo).stdout.strip()
    staging_checkout["config"]["limits"]["max_seconds"] = 5
    write_config(staging_checkout)
    prefix = "treedar-staging-check-" + commit
    try:
        result = run_check(staging_checkout)
        assert result.returncode == 1, result.stdout + result.stderr
        assert "reason=pipeline-run" in result.stdout
        log = staging_checkout["root"] / "Scratch" / f"staging-check-{commit}.log"
        assert "FIXTURE STARTED" in log.read_text()
        remaining = invoke([docker, "ps", "--all", "--quiet", "--filter", f"name={prefix}"], cwd=repo)
        assert remaining.returncode == 0 and not remaining.stdout.strip()
    finally:
        own = invoke([docker, "ps", "--all", "--quiet", "--filter", f"name={prefix}"], cwd=repo)
        for identifier in own.stdout.split():
            invoke([docker, "rm", "--force", identifier], cwd=repo)
        invoke([docker, "image", "rm", "--force", "treedar:staging-check-" + commit], cwd=repo)
