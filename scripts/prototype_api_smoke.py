"""Real Docker HTTP/persistence test; uses only a new, isolated Compose project."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid


def main():
    root = Path(__file__).resolve().parents[1]
    project = "processing-api-test-" + uuid.uuid4().hex[:10]
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        test_port = str(listener.getsockname()[1])
    env = {**os.environ, "PROCESSING_API_TOKEN": secrets.token_urlsafe(36),
           "PROCESSING_PROTOTYPE_OPERATOR_TOKEN": secrets.token_urlsafe(36),
           "PROTOTYPE_PORT": os.environ.get("PROTOTYPE_PORT", test_port)}
    compose = ["docker", "compose", "--project-name", project,
               "--file", str(root / "compose.prototype.yaml")]
    origin = "http://127.0.0.1:" + env["PROTOTYPE_PORT"]

    def command(*args):
        result = subprocess.run([*compose, *args], cwd=root, env=env, capture_output=True, text=True, timeout=240)
        if result.returncode:
            raise RuntimeError("compose operation failed: " + args[0])

    def request(path, payload=None, operator=False, timeout=5):
        key = "PROCESSING_PROTOTYPE_OPERATOR_TOKEN" if operator else "PROCESSING_API_TOKEN"
        req = Request(origin + path, data=json.dumps(payload).encode() if payload is not None else None,
                      headers={"Authorization": "Bearer " + env[key], "Content-Type": "application/json"})
        with urlopen(req, timeout=timeout) as response:
            return json.load(response)

    def ready():
        for _ in range(30):
            try:
                assert request("/healthz", timeout=1)["syntheticPrototypeEnabled"]
                request("/v1/prototype/jobs", timeout=1)
                return
            except (URLError, OSError):
                time.sleep(0.2)
        raise RuntimeError("coordinator did not become healthy")

    try:
        command("up", "--build", "--detach")
        ready()
        def submit(identifier, environment):
            return request("/v1/prototype/jobs", {"requestId": identifier, "environment": environment,
                                                 "profile": "treedar-synthetic-v1"})["job"]
        staging = submit("staging-first", "staging")
        production = submit("production-second", "production")
        assert submit("staging-first", "staging")["id"] == staging["id"]
        job = request("/v1/prototype/claim", {"workerId": "smoke"})["job"]
        assert job["id"] == production["id"]
        assert request("/v1/prototype/claim", {"workerId": "second"})["job"] is None
        command("restart", "coordinator")
        ready()
        assert request("/v1/prototype/jobs/" + job["id"])["job"]["status"] == "held"
        assert request("/v1/prototype/claim", {"workerId": "second"})["job"] is None
        resolve_path = "/v1/prototype/jobs/" + job["id"] + "/resolve"
        payload = {"note": "API-only smoke: no engine was launched; old claim stopped", "workerStopped": True}
        try:
            request(resolve_path, payload)
            raise AssertionError("worker token resolved operator hold")
        except HTTPError as exc:
            assert exc.code == 401
        request(resolve_path, payload, operator=True)
        resumed = request("/v1/prototype/claim", {"workerId": "smoke"})["job"]
        assert resumed["id"] == staging["id"]
        result = {"environment": "staging", "apiSmokeOnly": True}
        completion = {"claimToken": resumed["claimToken"], "status": "succeeded", "result": result}
        request("/v1/prototype/jobs/" + resumed["id"] + "/complete", completion)
        request("/v1/prototype/jobs/" + resumed["id"] + "/complete", completion)
        assert len(request("/v1/prototype/jobs")["jobs"]) == 2
        print("PASS Docker API: authentication, priority, idempotency, restart hold, operator recovery, completion")
    finally:
        # Only this invocation's unique synthetic project and volume are removed.
        command("down", "--volumes")


if __name__ == "__main__":
    main()
