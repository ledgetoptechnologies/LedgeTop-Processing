"""End-to-end synthetic queue -> bounded Docker TreeDar -> durable result."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import Request, urlopen
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    project = "processing-engine-test-" + uuid.uuid4().hex[:10]
    scratch = Path(tempfile.mkdtemp(prefix="processing-synthetic-"))
    scratch.chmod(0o700)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = str(listener.getsockname()[1])
    env = {**os.environ, "PROCESSING_API_TOKEN": secrets.token_urlsafe(36),
           "PROCESSING_PROTOTYPE_OPERATOR_TOKEN": secrets.token_urlsafe(36), "PROTOTYPE_PORT": port}
    compose = ["docker", "compose", "-p", project, "-f", str(root / "compose.prototype.yaml")]

    def command(args, timeout=300):
        result = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            # Worker only prints safe categories/metrics; never dump credentials or receipts.
            raise RuntimeError("operation failed: " + args[0] + " " + result.stderr[-500:])
        return result.stdout

    origin = "http://127.0.0.1:" + port
    def request(path, payload=None):
        req = Request(origin + path, data=json.dumps(payload).encode() if payload is not None else None,
                      headers={"Authorization": "Bearer " + env["PROCESSING_API_TOKEN"],
                               "Content-Type": "application/json"})
        with urlopen(req, timeout=2) as response:
            return json.load(response)

    try:
        command(["docker", "build", "--tag", "treedar:processing-prototype", str(root / "engines/treedar")], 1200)
        image = command(["docker", "image", "inspect", "--format", "{{.Id}}", "treedar:processing-prototype"]).strip()
        command([*compose, "up", "--build", "--detach"])
        for _ in range(30):
            try:
                request("/v1/prototype/jobs")
                break
            except (URLError, OSError):
                time.sleep(0.2)
        else:
            raise RuntimeError("coordinator did not become ready")
        jobs = []
        for name, environment in (("staging-first", "staging"), ("production-second", "production")):
            jobs.append(request("/v1/prototype/jobs", {"requestId": name,
                         "environment": environment, "profile": "treedar-synthetic-v1"})["job"])
        worker = [sys.executable, str(root / "engines/treedar/scripts/prototype_worker.py"),
                  "--coordinator", origin, "--image", image, "--scratch", str(scratch),
                  "--worker-id", "docker-smoke", "--timeout", "240"]
        for expected in (jobs[1], jobs[0]):
            command(worker, 300)
            actual = request("/v1/prototype/jobs/" + expected["id"])["job"]
            assert actual["status"] == "succeeded", actual["status"]
            assert actual["result"]["n_trees"] == 3
            assert actual["result"]["environment"] == expected["environment"]
            assert actual["result"]["image_id"] == image
            assert (scratch / expected["environment"] / expected["id"] / "synthetic/report.html").is_file()
            if expected["id"] == jobs[1]["id"]:
                assert request("/v1/prototype/jobs/" + jobs[0]["id"])["job"]["status"] == "queued"
        print(json.dumps({"status": "PASS", "checks": "real TreeDar, production-before-staging, artifacts, persisted completion",
                          "trees_per_job": 3, "image_id": image, "scratch": str(scratch)}))
    finally:
        command([*compose, "down", "--volumes"])
        # Stopped worker containers and private receipts/artifacts are retained for
        # operator inspection. The worker never receives the operator credential.


if __name__ == "__main__":
    main()
