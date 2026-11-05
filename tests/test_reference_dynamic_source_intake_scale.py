"""Scale-boundary qualification for real, metadata-only V2 coordinator admission.

This launches the real HTTP handler and `_DynamicAPI` in a child process so its
peak RSS excludes the synthetic Viewer-side producer/signing work. The only
source payload is signed metadata declaring 10,000 photos / 50 GB; no photo
bytes are created, uploaded, or processed. The temporary source spool disables
the physical mount requirement and injects a 60 GiB capacity answer.
"""
from __future__ import annotations

import copy
import http.client
import ipaddress
import json
import os
import queue as queue_module
from pathlib import Path
try:
    import resource
except ImportError:  # pragma: no cover - benchmark runs in a Linux container
    resource = None
import select
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.x509.oid import NameOID

from processing import reference_protocol as protocol
from processing.reference_limits import V2_SOURCE_INTAKE_BYTES
from tests.test_reference_source_intake import ReferenceSourceIntakeTests


VIEWER_BEARER = "synthetic-metadata-scale-viewer-token-" + "v" * 32
WORKER_BEARER = "synthetic-metadata-scale-worker-token-" + "w" * 32
MAX_NAMES = 10_000
PHOTO_BYTES = 5_000_000
DECLARED_PHOTO_BYTES = MAX_NAMES * PHOTO_BYTES
SYNTHETIC_AVAILABLE_BYTES = 60 * 1024**3
ACTIVE_NODE_RAM_LIMIT_BYTES = 256 * 1024**2


def _private_pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode("ascii")


def _public_pem(key):
    return key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def _write_tls_material(directory: Path):
    key = generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)
            - timedelta(minutes=1)).not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256()))
    key_path, cert_path = directory / "scale-key.pem", directory / "scale-cert.pem"
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if os.name != "nt":
        key_path.chmod(0o600)
        cert_path.chmod(0o644)
    return key_path, cert_path


def _server_main(config_path: Path):
    """Run the actual dynamic API in a dedicated child and report its RSS."""
    from processing.config import ProcessingNode
    from processing.prototype import Queue
    from processing.reference_control import ReferenceControlAPI
    from processing.reference_http import create_reference_http_server
    from processing.reference_runtime import _DynamicAPI, discovered_policy
    from processing.reference_source_intake import SOURCE_PREFIX
    from processing.reference_store import PROFILE, ReferenceStore
    from processing.source_spool import SourceSpool

    config_data = json.loads(config_path.read_text(encoding="utf-8"))
    state_root = Path(config_data["stateRoot"])
    queue_root = state_root / "queue"
    spool_root = state_root / "spool"
    queue_root.mkdir(mode=0o700, parents=True)
    spool_root.mkdir(mode=0o700, parents=True)
    marker = spool_root / ".ltds-source-spool-id"
    marker.write_text(config_data["mountId"], encoding="ascii")
    if os.name != "nt":
        marker.chmod(0o600)

    queue = Queue(queue_root, node_ids=("cluster", "main"), reference_enabled=True)
    queue.allowed_profiles.add(PROFILE)
    spool = SourceSpool(spool_root, mount_id=config_data["mountId"], safety_floor_bytes=0,
        max_object_bytes=20 * 1024**3, chunk_bytes=1024**2, require_mount=False)
    capacity_checks = []

    def synthetic_capacity_probe(required_bytes, *, reserved_bytes=0):
        capacity_checks.append({"requiredBytes": required_bytes, "reservedBytes": reserved_bytes,
            "syntheticAvailableBytes": SYNTHETIC_AVAILABLE_BYTES})
        if required_bytes < 0 or reserved_bytes + required_bytes > SYNTHETIC_AVAILABLE_BYTES:
            raise ValueError("synthetic source capacity exceeded")

    spool.check_capacity = synthetic_capacity_probe
    snapshot_options = []
    snapshot = {"ready": True, "commonOptions": snapshot_options, "nodes": [
        {"role": node, "available": True, "configured": True,
         "capabilitiesSha256": ("a" if node == "cluster" else "b") * 64,
         "engine": "odm", "engineVersion": "synthetic", "options": snapshot_options}
        for node in ("cluster", "main")]}
    config = SimpleNamespace(local_max_photos=MAX_NAMES,
        local_max_input_bytes=SYNTHETIC_AVAILABLE_BYTES,
        prototype_node_ids=("cluster", "main"),
        node_endpoints=(ProcessingNode("cluster", "http://127.0.0.1:19001", "synthetic"),
                        ProcessingNode("main", "http://127.0.0.1:19002", "synthetic")))
    capability_fingerprint, options_fingerprint, allowed_options = discovered_policy(snapshot, config)
    settings = SimpleNamespace(viewer_bearers={VIEWER_BEARER: "staging"},
        viewer_receipt_keys={}, source_spool=spool,
        source_attestation_keys=config_data["sourceKeys"])
    control = ReferenceControlAPI(ReferenceStore(queue, local_max_photos=MAX_NAMES,
        local_max_input_bytes=SYNTHETIC_AVAILABLE_BYTES),
        viewer_bearers=settings.viewer_bearers,
        worker_bearers={WORKER_BEARER: {"node_id": "cluster", "worker_id": "scale-worker",
            "worker_instance_id": "scale-instance"}},
        capability_fingerprint=capability_fingerprint, options_fingerprint=options_fingerprint,
        allowed_odm_options=allowed_options, source_spool=spool,
        source_attestation_keys=settings.source_attestation_keys, authority_id=queue.authority_id)
    api = _DynamicAPI(control, config, settings, lambda: snapshot,
        lambda: {node: {"ready": True, "maxImages": MAX_NAMES,
            "maxInputBytes": SYNTHETIC_AVAILABLE_BYTES, "taskQueueCount": 0}
            for node in ("cluster", "main")})
    requests = []
    original_handle = api.handle

    def observed_handle(method, path, headers, body=b"", **kwargs):
        started = time.perf_counter()
        response = original_handle(method, path, headers, body, **kwargs)
        if method == "POST" and path in (SOURCE_PREFIX + "/jobs", protocol.PREFIX + "/jobs"):
            requests.append({"method": method, "path": path, "bodyBytes": len(body),
                "status": response.status, "error": response.body.get("error")
                    if type(response.body) is dict else None,
                "handlerMilliseconds": round((time.perf_counter() - started) * 1000, 3)})
        return response

    api.handle = observed_handle
    streamed_puts = []
    original_stream = api.stream_source_object

    def observed_stream(path, *args, **kwargs):
        streamed_puts.append(path)
        return original_stream(path, *args, **kwargs)

    api.stream_source_object = observed_stream
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain(config_data["certificate"], config_data["privateKey"])
    cgroup_root = Path("/sys/fs/cgroup")
    def read_cgroup(name):
        try:
            return (cgroup_root / name).read_text(encoding="ascii").strip()
        except OSError:
            return None
    server = create_reference_http_server(api, host=config_data.get("listenHost", "127.0.0.1"),
        port=int(config_data.get("listenPort", 0)), ssl_context=tls,
        request_timeout=float(config_data.get("requestTimeout", 2)))
    thread = threading.Thread(target=server.serve_forever,
        kwargs={"poll_interval": 0.05}, daemon=True, name="metadata-scale-http")
    thread.start()
    public_info = {"port": server.server_address[1], "authorityId": queue.authority_id,
        "capabilityFingerprint": capability_fingerprint,
        "optionsFingerprint": options_fingerprint,
        "cgroup": {"memoryMax": read_cgroup("memory.max"), "cpuMax": read_cgroup("cpu.max"),
            "memoryEventsAtStart": read_cgroup("memory.events")}}
    print(json.dumps({"ready": public_info}), flush=True)
    try:
        for line in sys.stdin:
            command = line.strip()
            if command == "METRICS":
                row = None
                with queue._db() as db:
                    row = db.execute("""SELECT j.status AS job_status,r.phase,s.source_state,s.derived_state,
                        s.reserved_bytes,r.input_json,s.source_manifest_json
                        FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
                        JOIN reference_source_intakes s ON s.job_id=r.job_id
                        ORDER BY r.created_at DESC LIMIT 1""").fetchone()
                    queued_jobs = db.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0]
                    receipt_rows = db.execute("SELECT COUNT(*) FROM reference_source_object_receipts").fetchone()[0]
                source_count = input_count = None
                if row:
                    source_count = len(json.loads(row["source_manifest_json"])["objects"])
                    input_count = len(json.loads(row["input_json"])["manifest"]["objects"])
                marker_size = marker.stat().st_size
                spool_bytes = sum(path.stat().st_size for path in spool_root.rglob("*") if path.is_file())
                rss_current = None
                try:
                    for status_line in Path("/proc/self/status").read_text().splitlines():
                        if status_line.startswith("VmRSS:"):
                            rss_current = int(status_line.split()[1]) * 1024
                            break
                except OSError:
                    pass
                result = {"serverPid": os.getpid(),
                    "serverPeakRssBytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                    "serverCurrentRssBytes": rss_current,
                    "cgroup": {"memoryMax": read_cgroup("memory.max"),
                        "memoryPeak": read_cgroup("memory.peak"),
                        "memoryCurrent": read_cgroup("memory.current"),
                        "memoryEvents": read_cgroup("memory.events"),
                        "cpuMax": read_cgroup("cpu.max")},
                    "requestHandlers": requests, "streamedPutCount": len(streamed_puts),
                    "capacityChecks": capacity_checks, "queueRows": queued_jobs,
                    "phase": row["phase"] if row else None,
                    "queueStatus": row["job_status"] if row else None,
                    "sourceState": row["source_state"] if row else None,
                    "derivedState": row["derived_state"] if row else None,
                    "reservedBytes": row["reserved_bytes"] if row else None,
                    "sourceManifestObjectCount": source_count,
                    "engineManifestObjectCount": input_count,
                    "objectReceiptRows": receipt_rows,
                    "spoolBytesIncludingMarker": spool_bytes,
                    "markerBytes": marker_size}
                print(json.dumps({"metrics": result}), flush=True)
            elif command == "SLOT":
                print(json.dumps({"sourceSlotOccupied":
                    server.source_submit_semaphore._value == 0}), flush=True)
            elif command == "STOP":
                break
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        queue.close()
    return 0


def _json_response(connection, method, path, body=None, *, bearer=None):
    headers = {"Content-Type": "application/json"}
    if bearer:
        headers["Authorization"] = "Bearer " + bearer
    start = time.perf_counter()
    connection.request(method, path, body=body, headers=headers)
    response = connection.getresponse()
    response_body = response.read(1024 * 1024 + 1)
    if len(response_body) > 1024 * 1024:
        raise AssertionError("bounded metadata response exceeded one MiB")
    return response.status, response_body, (time.perf_counter() - start) * 1000


@unittest.skipUnless(os.name == "posix", "server child RSS measurement uses Linux /proc and ru_maxrss")
class DynamicSourceIntakeScaleTests(unittest.TestCase):
    def _new_payload(self, helper, *, authority_id, capability_fingerprint,
                     options_fingerprint, names):
        payload = helper._large_dji_payload(MAX_NAMES)
        helper.authority_id = authority_id
        payload["submissionId"] = str(uuid.uuid4())
        payload["providerTaskId"] = str(uuid.uuid4())
        payload["viewerTaskId"] = str(uuid.uuid4())
        payload["sourceManifest"]["id"] = str(uuid.uuid4())
        payload["derivedManifest"]["attemptId"] = payload["submissionId"]
        payload["sourceManifest"]["datasetManifestSha256"] = "d" * 64
        payload["capabilityFingerprint"] = capability_fingerprint
        payload["pipeline"]["odm"]["options"] = {}
        payload["pipeline"]["odm"]["optionsFingerprint"] = options_fingerprint
        by_id = {item["id"]: item for item in payload["manifest"]["objects"]}
        for index, source_item in enumerate(payload["sourceManifest"]["objects"]):
            name = names(index)
            source_item.update(name=name, byteSize=PHOTO_BYTES)
            pipeline_item = by_id[source_item["id"]]
            pipeline_item.update(name=name, byteSize=PHOTO_BYTES)
        payload["sourceManifest"]["totalBytes"] = DECLARED_PHOTO_BYTES
        payload["manifest"]["totalBytes"] = DECLARED_PHOTO_BYTES
        helper.payload = payload
        helper.rehash()
        signed = helper.validate(payload, capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint, allowed_odm_options=set(),
            authority_id=authority_id)
        request = copy.deepcopy(payload)
        request["sourceAttestation"] = signed["sourceAttestation"]
        return request

    def test_dynamic_http_v2_10k_metadata_limit_and_server_rss(self):
        helper = ReferenceSourceIntakeTests()
        helper.setUp()
        with tempfile.TemporaryDirectory(prefix="reference-source-scale-") as directory:
            root = Path(directory)
            private_key_path, certificate_path = _write_tls_material(root)
            config_path = root / "server.json"
            server_config = {"stateRoot": str(root / "server-state"),
                "certificate": str(certificate_path), "privateKey": str(private_key_path),
                "mountId": "e" * 64, "sourceKeys": helper.trusted_keys}
            config_path.write_text(json.dumps(server_config), encoding="utf-8")
            if os.name != "nt":
                config_path.chmod(0o600)
            env = os.environ.copy()
            source_root = str(Path(__file__).resolve().parents[1])
            env["PYTHONPATH"] = source_root + os.pathsep + env.get("PYTHONPATH", "")
            process = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                "--server", str(config_path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, bufsize=1, env=env)
            server_thread_stopped = False
            slow_thread = None
            try:
                ready_line = self._read_line(process, timeout=20)
                ready = json.loads(ready_line)["ready"]
                context = ssl.create_default_context(cafile=str(certificate_path))
                def connect():
                    return http.client.HTTPSConnection("127.0.0.1", ready["port"],
                        context=context, timeout=20)

                connection = connect()
                unauthorized_status, _, _ = _json_response(connection, "POST", "/ltds/v2/jobs",
                    body=b"not-json")
                self.assertEqual(unauthorized_status, 401,
                    "viewer authentication must run before V2 body decoding")
                connection.close()

                payload = self._new_payload(helper, authority_id=ready["authorityId"],
                    capability_fingerprint=ready["capabilityFingerprint"],
                    options_fingerprint=ready["optionsFingerprint"],
                    names=lambda i: f"DJI_{i:05d}_" + "X" * 230 + ".JPG")
                signed_body = protocol.canonical_json(payload)
                self.assertEqual(payload["sourceManifest"]["totalBytes"], DECLARED_PHOTO_BYTES)
                self.assertEqual(payload["sourceManifest"]["objectCount"], MAX_NAMES)
                self.assertGreater(len(signed_body), protocol.MAX_BODY_BYTES)
                self.assertLessEqual(len(signed_body), V2_SOURCE_INTAKE_BYTES)

                legacy = {key: payload[key] for key in (
                    "schemaVersion", "submissionId", "manifest", "pipeline", "capabilityFingerprint")}
                legacy_body = protocol.canonical_json(legacy)
                self.assertGreater(len(legacy_body), protocol.MAX_BODY_BYTES)
                self.assertLess(len(legacy_body), V2_SOURCE_INTAKE_BYTES)
                connection = connect()
                legacy_status = self._content_length_only(connection, "/ltds/v1/jobs", len(legacy_body))
                self.assertEqual(legacy_status, 413,
                    "legacy V1 HTTP metadata must keep its four MiB limit")
                connection.close()

                slow_outcome = []
                def slow_incomplete_source_post():
                    slow_connection = connect()
                    try:
                        slow_connection.putrequest("POST", "/ltds/v2/jobs")
                        slow_connection.putheader("Content-Type", "application/json")
                        slow_connection.putheader("Authorization", "Bearer " + VIEWER_BEARER)
                        slow_connection.putheader("Content-Length", str(len(signed_body)))
                        slow_connection.endheaders()
                        slow_connection.send(b"{")
                        result = slow_connection.getresponse()
                        slow_outcome.append((result.status, result.read(1024)))
                    except Exception as exc:
                        slow_outcome.append(exc)
                    finally:
                        slow_connection.close()

                slow_thread = threading.Thread(target=slow_incomplete_source_post,
                    name="incomplete-source-post")
                slow_thread.start()
                deadline = time.monotonic() + 3
                occupied = False
                while time.monotonic() < deadline:
                    process.stdin.write("SLOT\n")
                    process.stdin.flush()
                    occupied = json.loads(self._read_line(process, timeout=3))["sourceSlotOccupied"]
                    if occupied:
                        break
                    time.sleep(0.02)
                self.assertTrue(occupied, "first source POST did not acquire the intake slot")

                connection = connect()
                connection.putrequest("POST", "/ltds/v2/jobs")
                connection.putheader("Content-Type", "application/json")
                connection.putheader("Authorization", "Bearer " + VIEWER_BEARER)
                connection.putheader("Content-Length", str(len(signed_body)))
                connection.endheaders()
                busy = connection.getresponse()
                busy_status, retry_after, connection_close = (busy.status,
                    busy.getheader("Retry-After"), busy.getheader("Connection"))
                busy.read(1024)
                connection.close()
                self.assertEqual((busy_status, retry_after, connection_close), (429, "2", "close"))

                connection = connect()
                capability_status, _, _ = _json_response(connection, "GET", "/ltds/v2/capabilities",
                    bearer=VIEWER_BEARER)
                self.assertEqual(capability_status, 200,
                    "source intake backpressure must not block ordinary GET requests")
                connection.close()

                slow_thread.join(timeout=5)
                self.assertFalse(slow_thread.is_alive(), "incomplete-body request did not time out")
                self.assertEqual(len(slow_outcome), 1)
                self.assertIsInstance(slow_outcome[0], tuple, repr(slow_outcome[0]))
                self.assertEqual(slow_outcome[0][0], 408,
                    "incomplete source body must time out and release the intake slot")

                connection = connect()
                invalid_status, _, _ = _json_response(connection, "POST", "/ltds/v2/jobs",
                    body=b"not-json", bearer=VIEWER_BEARER)
                self.assertEqual(invalid_status, 400,
                    "API validation errors must release the intake slot")
                connection.close()

                connection = connect()
                status, response_body, client_ms = _json_response(connection, "POST", "/ltds/v2/jobs",
                    body=signed_body, bearer=VIEWER_BEARER)
                response = json.loads(response_body)
                print("SOURCE_INTAKE_SCALE_PROBE " + json.dumps({
                    "bodyBytes": len(signed_body), "clientRoundTripMilliseconds": round(client_ms, 3),
                    "responseStatus": status,
                    "responseError": response.get("error") if type(response) is dict else None},
                    sort_keys=True), flush=True)
                self.assertEqual(status, 201, response)
                self.assertEqual(response["phase"], "source_receiving")
                self.assertEqual(response["status"], "queued")
                connection.close()

                connection = connect()
                replay_status, replay_body, _ = _json_response(connection, "POST", "/ltds/v2/jobs",
                    body=signed_body, bearer=VIEWER_BEARER)
                replay = json.loads(replay_body)
                self.assertEqual(replay_status, 200)
                self.assertEqual(replay["jobId"], response["jobId"])
                connection.close()

                connection = connect()
                too_large_status = self._content_length_only(connection, "/ltds/v2/jobs",
                    V2_SOURCE_INTAKE_BYTES + 1, bearer=VIEWER_BEARER)
                self.assertEqual(too_large_status, 413,
                    "V2 request bodies over 16 MiB must be rejected by the HTTP limit")
                connection.close()

                process.stdin.write("METRICS\n")
                process.stdin.flush()
                metrics = json.loads(self._read_line(process, timeout=10))["metrics"]
                self.assertEqual([(item["path"], item["status"]) for item in metrics["requestHandlers"]],
                    [("/ltds/v2/jobs", 401), ("/ltds/v2/jobs", 400),
                     ("/ltds/v2/jobs", 201), ("/ltds/v2/jobs", 200)])
                admitted_request = metrics["requestHandlers"][2]
                self.assertEqual((admitted_request["bodyBytes"], admitted_request["error"]),
                    (len(signed_body), None))
                self.assertEqual(metrics["streamedPutCount"], 0)
                self.assertEqual(metrics["capacityChecks"], [{"requiredBytes": DECLARED_PHOTO_BYTES,
                    "reservedBytes": 0, "syntheticAvailableBytes": SYNTHETIC_AVAILABLE_BYTES}])
                self.assertEqual((metrics["queueRows"], metrics["queueStatus"], metrics["phase"],
                    metrics["sourceState"], metrics["derivedState"]), (1, "queued", "queued", "receiving", "receiving"))
                self.assertEqual((metrics["sourceManifestObjectCount"], metrics["engineManifestObjectCount"],
                    metrics["reservedBytes"], metrics["objectReceiptRows"]),
                    (MAX_NAMES, MAX_NAMES, DECLARED_PHOTO_BYTES, 0))
                self.assertEqual(metrics["spoolBytesIncludingMarker"], metrics["markerBytes"],
                    "metadata-only admission must not write any source objects")
                peak = metrics["serverPeakRssBytes"]
                margin = ACTIVE_NODE_RAM_LIMIT_BYTES - peak
                print("SOURCE_INTAKE_SCALE " + json.dumps({
                    "metadataOnly": True, "sourceBytesCreatedOrUploaded": 0,
                    "declaredPhotoCount": MAX_NAMES, "declaredPhotoBytes": DECLARED_PHOTO_BYTES,
                    "bodyBytes": len(signed_body), "clientRoundTripMilliseconds": round(client_ms, 3),
                    "serverApiMilliseconds": metrics["requestHandlers"][2]["handlerMilliseconds"],
                    "serverPeakRssBytes": peak, "serverCurrentRssBytes": metrics["serverCurrentRssBytes"],
                    "activeNodeRamLimitBytes": ACTIVE_NODE_RAM_LIMIT_BYTES,
                    "estimatedMarginAgainstNodeLimitBytes": margin,
                    "containerMemoryLimitBytes": (int(metrics["cgroup"]["memoryMax"])
                        if str(metrics["cgroup"]["memoryMax"]).isdigit() else None),
                    "qualificationCaveat": "server-only RSS; use the separate external-producer cgroup profile for a 256 MiB OOM qualification",
                    "syntheticCapacityBytes": SYNTHETIC_AVAILABLE_BYTES,
                    "physicalMountProbe": "disabled for temporary test spool; no NAS access",
                    "queueState": "queued/source_receiving; not claimable or processing"}, sort_keys=True),
                    flush=True)
            finally:
                if slow_thread is not None and slow_thread.is_alive():
                    slow_thread.join(timeout=5)
                if slow_thread is not None:
                    self.assertFalse(slow_thread.is_alive(), "task-owned slow request thread did not exit")
                try:
                    process.stdin.write("STOP\n")
                    process.stdin.flush()
                    process.wait(timeout=10)
                except (BrokenPipeError, OSError, subprocess.TimeoutExpired):
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)
                if process.stdout:
                    process.stdout.close()
                if process.stdin:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass
                if process.stderr:
                    error_text = process.stderr.read()
                    process.stderr.close()
                    self.assertEqual(process.returncode, 0, error_text[-2000:])
                server_thread_stopped = process.poll() is not None
                self.assertTrue(server_thread_stopped, "benchmark server child process did not exit")

    @staticmethod
    def _read_line(process, *, timeout):
        ready, _, _ = select.select([process.stdout], [], [], timeout)
        if not ready:
            if process.poll() is not None:
                detail = process.stderr.read() if process.stderr else ""
                raise RuntimeError("benchmark server exited before returning metrics: " + detail[-2000:])
            raise TimeoutError("benchmark server child did not report before timeout")
        line = process.stdout.readline()
        if not line:
            detail = process.stderr.read() if process.stderr else ""
            raise RuntimeError("benchmark server exited before returning metrics: " + detail[-2000:])
        return line

    @staticmethod
    def _content_length_only(connection, path, content_length, *, bearer=None):
        connection.putrequest("POST", path)
        connection.putheader("Content-Type", "application/json")
        connection.putheader("Content-Length", str(content_length))
        if bearer:
            connection.putheader("Authorization", "Bearer " + bearer)
        connection.endheaders()
        response = connection.getresponse()
        response.read(1024)
        return response.status


def _external_cgroup_profile():
    """Keep the synthetic producer on the host and constrain only the API container."""
    helper = ReferenceSourceIntakeTests()
    helper.setUp()
    source_root = Path(__file__).resolve().parents[1]
    image = os.environ.get("LTDS_PROCESSING_TEST_IMAGE",
        "sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8")
    port_probe = socket.socket()
    port_probe.bind(("127.0.0.1", 0))
    port = port_probe.getsockname()[1]
    port_probe.close()
    run_id = uuid.uuid4().hex
    container_name = "ltds-source-intake-cgroup-" + run_id[:12]
    owner_label = "codex-reference-source-intake-cgroup-20261105-" + run_id
    owner_label_key = "com.ledgetop.codex-task"
    process = None
    output_queue = queue_module.Queue()
    output_reader = None
    slow_thread = None
    slow_outcome = []
    try:
        with tempfile.TemporaryDirectory(prefix="ltds-source-cgroup-host-") as directory:
            root = Path(directory)
            private_key_path, certificate_path = _write_tls_material(root)
            config_path = root / "server.json"
            server_config = {"stateRoot": "/tmp/reference-source-cgroup-state",
                "certificate": "/fixture/scale-cert.pem", "privateKey": "/fixture/scale-key.pem",
                "mountId": "f" * 64, "sourceKeys": helper.trusted_keys,
                "listenHost": "0.0.0.0", "listenPort": port, "requestTimeout": 2}
            config_path.write_text(json.dumps(server_config), encoding="utf-8")
            if os.name != "nt":
                config_path.chmod(0o600)
            fixture_mount = ("type=bind,source=" + root.as_posix() + ",target=/fixture,readonly")
            source_mount = ("type=bind,source=" + source_root.as_posix() + ",target=/processing,readonly")
            command = ["docker", "run", "--rm", "-i", "--read-only", "--name", container_name,
                "--label", f"{owner_label_key}={owner_label}",
                "--cpus=0.5", "--memory=256m", "--memory-swap=256m", "--pids-limit=32",
                "--tmpfs", "/tmp:rw,size=64m,noexec,nosuid", "-p",
                f"127.0.0.1:{port}:{port}", "--mount", source_mount, "--mount", fixture_mount,
                "-w", "/processing", "-e", "PYTHONPATH=/processing",
                "-e", "PYTHONDONTWRITEBYTECODE=1", image, "python",
                "tests/test_reference_dynamic_source_intake_scale.py", "--server", "/fixture/server.json"]
            process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True, bufsize=1)

            def collect_output():
                for line in process.stdout:
                    output_queue.put(line)
                output_queue.put(None)

            output_reader = threading.Thread(target=collect_output, name="cgroup-docker-output")
            output_reader.start()

            def next_json_line(timeout=15):
                try:
                    line = output_queue.get(timeout=timeout)
                except queue_module.Empty:
                    raise TimeoutError("cgroup server did not report before deadline") from None
                if line is None:
                    errors = process.stderr.read() if process.stderr else ""
                    raise RuntimeError("cgroup server exited early: " + errors[-2000:])
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    raise RuntimeError("unexpected cgroup server output") from None

            ready_message = next_json_line()
            ready = ready_message["ready"]
            if int(ready["port"]) != port:
                raise AssertionError("server listened on an unexpected port")
            if ready["cgroup"]["memoryMax"] != str(256 * 1024**2):
                raise AssertionError("server container does not have the requested 256 MiB cgroup")
            if ready["cgroup"]["cpuMax"] != "50000 100000":
                raise AssertionError("server container does not have the requested 0.5 CPU quota")

            tls_context = ssl.create_default_context(cafile=str(certificate_path))
            def connect():
                return http.client.HTTPSConnection("127.0.0.1", port,
                    context=tls_context, timeout=20)

            unauthenticated = connect()
            unauthorized_status, _, _ = _json_response(unauthenticated, "POST", "/ltds/v2/jobs",
                body=b"not-json")
            unauthenticated.close()
            if unauthorized_status != 401:
                raise AssertionError("unauthenticated V2 request was not rejected before parsing")

            fixture = DynamicSourceIntakeScaleTests()
            payload = fixture._new_payload(helper, authority_id=ready["authorityId"],
                capability_fingerprint=ready["capabilityFingerprint"],
                options_fingerprint=ready["optionsFingerprint"],
                names=lambda i: f"DJI_{i:05d}_" + "X" * 230 + ".JPG")
            signed_body = protocol.canonical_json(payload)
            if len(signed_body) <= protocol.MAX_BODY_BYTES or len(signed_body) > V2_SOURCE_INTAKE_BYTES:
                raise AssertionError("synthetic signed body is outside the configured V2 budget")
            if payload["sourceManifest"]["totalBytes"] != DECLARED_PHOTO_BYTES:
                raise AssertionError("synthetic payload does not declare the expected 50 GB")

            legacy = {key: payload[key] for key in (
                "schemaVersion", "submissionId", "manifest", "pipeline", "capabilityFingerprint")}
            legacy_connection = connect()
            legacy_status = DynamicSourceIntakeScaleTests._content_length_only(
                legacy_connection, "/ltds/v1/jobs", len(protocol.canonical_json(legacy)), bearer=VIEWER_BEARER)
            legacy_connection.close()
            if legacy_status != 413:
                raise AssertionError("legacy V1 path did not keep its four MiB cap")

            def slow_incomplete_request():
                slow = connect()
                try:
                    slow.putrequest("POST", "/ltds/v2/jobs")
                    slow.putheader("Content-Type", "application/json")
                    slow.putheader("Authorization", "Bearer " + VIEWER_BEARER)
                    slow.putheader("Content-Length", str(len(signed_body)))
                    slow.endheaders()
                    slow.send(b"{")
                    response = slow.getresponse()
                    slow_outcome.append((response.status, response.read(1024)))
                except Exception as exc:
                    slow_outcome.append(exc)
                finally:
                    slow.close()

            slow_thread = threading.Thread(target=slow_incomplete_request,
                name="cgroup-incomplete-source-post")
            slow_thread.start()
            deadline = time.monotonic() + 3
            occupied = False
            while time.monotonic() < deadline:
                process.stdin.write("SLOT\n")
                process.stdin.flush()
                occupied = next_json_line(timeout=3)["sourceSlotOccupied"]
                if occupied:
                    break
                time.sleep(0.02)
            if not occupied:
                raise AssertionError("first V2 source POST did not acquire the intake slot")

            busy_connection = connect()
            busy_connection.putrequest("POST", "/ltds/v2/jobs")
            busy_connection.putheader("Content-Type", "application/json")
            busy_connection.putheader("Authorization", "Bearer " + VIEWER_BEARER)
            busy_connection.putheader("Content-Length", str(len(signed_body)))
            busy_connection.endheaders()
            busy = busy_connection.getresponse()
            busy_status, retry_after, connection_close = (busy.status,
                busy.getheader("Retry-After"), busy.getheader("Connection"))
            busy.read(1024)
            busy_connection.close()
            if (busy_status, retry_after, connection_close) != (429, "2", "close"):
                raise AssertionError("concurrent V2 source request was not rejected safely")

            capabilities = connect()
            capability_status, _, _ = _json_response(capabilities, "GET", "/ltds/v2/capabilities",
                bearer=VIEWER_BEARER)
            capabilities.close()
            if capability_status != 200:
                raise AssertionError("ordinary GET was blocked by source intake backpressure")

            slow_thread.join(timeout=5)
            if slow_thread.is_alive() or len(slow_outcome) != 1 or not isinstance(slow_outcome[0], tuple):
                raise AssertionError("incomplete-body request did not exit cleanly")
            if slow_outcome[0][0] != 408:
                raise AssertionError("incomplete-body request did not return a bounded timeout")

            invalid = connect()
            invalid_status, _, _ = _json_response(invalid, "POST", "/ltds/v2/jobs",
                body=b"not-json", bearer=VIEWER_BEARER)
            invalid.close()
            if invalid_status != 400:
                raise AssertionError("malformed authenticated request did not return 400")

            submit = connect()
            submit_status, submit_body, submit_ms = _json_response(submit, "POST", "/ltds/v2/jobs",
                body=signed_body, bearer=VIEWER_BEARER)
            submit_response = json.loads(submit_body)
            submit.close()
            if submit_status != 201:
                raise AssertionError("signed synthetic V2 submission failed: " + str(submit_response))

            replay = connect()
            replay_status, replay_body, replay_ms = _json_response(replay, "POST", "/ltds/v2/jobs",
                body=signed_body, bearer=VIEWER_BEARER)
            replay_response = json.loads(replay_body)
            replay.close()
            if replay_status != 200 or replay_response.get("jobId") != submit_response.get("jobId"):
                raise AssertionError("same signed submission did not replay one job")

            oversized = connect()
            oversized_status = DynamicSourceIntakeScaleTests._content_length_only(
                oversized, "/ltds/v2/jobs", V2_SOURCE_INTAKE_BYTES + 1, bearer=VIEWER_BEARER)
            oversized.close()
            if oversized_status != 413:
                raise AssertionError("V2 HTTP path did not reject a body over 16 MiB")

            process.stdin.write("METRICS\n")
            process.stdin.flush()
            metrics = next_json_line(timeout=10)["metrics"]
            if metrics["queueRows"] != 1 or metrics["phase"] != "queued" or metrics["queueStatus"] != "queued":
                raise AssertionError("expected exactly one queued, non-processing job")
            if (metrics["sourceManifestObjectCount"], metrics["engineManifestObjectCount"],
                    metrics["reservedBytes"], metrics["objectReceiptRows"], metrics["streamedPutCount"]) != (
                    MAX_NAMES, MAX_NAMES, DECLARED_PHOTO_BYTES, 0, 0):
                raise AssertionError("metadata-only source reservation/receipt boundary was violated")
            if metrics["cgroup"]["memoryMax"] != str(256 * 1024**2):
                raise AssertionError("cgroup memory limit changed during the test")
            memory_peak = int(metrics["cgroup"]["memoryPeak"])
            memory_events_start = dict(line.split() for line in ready["cgroup"]["memoryEventsAtStart"].splitlines())
            memory_events_end = dict(line.split() for line in metrics["cgroup"]["memoryEvents"].splitlines())
            oom_delta = {name: int(memory_events_end.get(name, "0")) - int(value)
                for name, value in memory_events_start.items() if name in ("oom", "oom_kill", "max")}
            if memory_peak > 256 * 1024**2 or any(oom_delta.get(name, 0) for name in ("oom", "oom_kill")):
                raise AssertionError("server cgroup exceeded its memory cap or incurred an OOM event")

            print("SOURCE_INTAKE_CGROUP_PROFILE " + json.dumps({
                "producerLocation": "Windows host outside server cgroup",
                "serverContainer": container_name, "serverContainerImage": image,
                "cgroup": metrics["cgroup"], "oomEventDelta": oom_delta,
                "serverProcessPeakRssBytes": metrics["serverPeakRssBytes"],
                "serverProcessCurrentRssBytes": metrics["serverCurrentRssBytes"],
                "bodyBytes": len(signed_body), "declaredPhotoCount": MAX_NAMES,
                "declaredPhotoBytes": DECLARED_PHOTO_BYTES, "actualPhotoBytesCreatedOrUploaded": 0,
                "acceptedStatus": submit_status, "replayStatus": replay_status,
                "submitHandlerMilliseconds": metrics["requestHandlers"][2]["handlerMilliseconds"],
                "submitClientMilliseconds": round(submit_ms, 3),
                "replayClientMilliseconds": round(replay_ms, 3),
                "busyResponse": {"status": busy_status, "retryAfter": retry_after,
                    "connection": connection_close},
                "incompleteBodyStatus": slow_outcome[0][0], "ordinaryGetStatus": capability_status,
                "invalidJsonStatus": invalid_status, "legacyOversizeStatus": legacy_status,
                "v2OversizeStatus": oversized_status, "queueStatus": metrics["queueStatus"],
                "queueRows": metrics["queueRows"], "sourceManifestObjects": metrics["sourceManifestObjectCount"],
                "engineManifestObjects": metrics["engineManifestObjectCount"],
                "sourceObjectReceipts": metrics["objectReceiptRows"], "streamedPuts": metrics["streamedPutCount"],
                "syntheticCapacityBytes": SYNTHETIC_AVAILABLE_BYTES,
                "physicalMountProbe": "disabled; no NAS access"}, sort_keys=True), flush=True)

        process.stdin.write("STOP\n")
        process.stdin.flush()
        if process.wait(timeout=15) != 0:
            errors = process.stderr.read() if process.stderr else ""
            raise RuntimeError("server container exited unsuccessfully: " + errors[-2000:])
        return 0
    finally:
        if slow_thread is not None and slow_thread.is_alive():
            slow_thread.join(timeout=5)
        if process is not None and process.poll() is None:
            try:
                process.stdin.write("STOP\n")
                process.stdin.flush()
            except (OSError, BrokenPipeError):
                pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                identity = subprocess.run(["docker", "inspect", "--format",
                    '{{.Name}}|{{index .Config.Labels "' + owner_label_key + '"}}', container_name],
                    capture_output=True, text=True, check=False)
                expected_identity = "/" + container_name + "|" + owner_label
                if identity.returncode == 0 and identity.stdout.strip() == expected_identity:
                    subprocess.run(["docker", "stop", container_name],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
                elif identity.returncode == 0:
                    raise RuntimeError("refusing to stop a container without this run's owner label")
                process.wait(timeout=10)
        if process is not None:
            if process.stdin:
                try:
                    process.stdin.close()
                except OSError:
                    pass
            if process.stdout:
                process.stdout.close()
            if process.stderr:
                process.stderr.close()
        if output_reader is not None:
            output_reader.join(timeout=3)
            if output_reader.is_alive():
                raise AssertionError("task-owned docker output reader did not exit")


if __name__ == "__main__" and len(sys.argv) == 3 and sys.argv[1] == "--server":
    raise SystemExit(_server_main(Path(sys.argv[2])))
elif __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "--profile-cgroup":
        raise SystemExit(_external_cgroup_profile())
    unittest.main()
