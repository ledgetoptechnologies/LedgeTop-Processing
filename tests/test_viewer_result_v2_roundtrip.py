"""Bounded synthetic source-to-Viewer V2 result round trip over loopback TLS."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from contextlib import nullcontext
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
import zipfile
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.x509.oid import NameOID

from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_driver import ReferenceDriver
from processing.reference_engine_cleanup import EngineCacheCleanup
from processing.reference_engine_worker import ReferenceEngineWorker
from processing.reference_failure import FailureOutbox
from processing.reference_http import create_reference_http_server
from processing.reference_result_api import ReferenceResultAPI
from processing.reference_results import ReferenceResults
from processing.reference_result_proxy import ResultProxy
from processing.reference_store import PROFILE, ReferenceStore
from processing.reference_transfer import BrokerClient
from processing.scratch_policy import ScratchLedger
from processing.source_spool import SourceSpool
from processing.viewer_service import NativeEngine
from processing.worker_outbox import WorkerOutbox

BEARER = "synthetic-roundtrip-viewer-token"
WORKER_BEARER = "synthetic-roundtrip-worker-token"
RESULT_BEARER = "synthetic-roundtrip-result-reader-token"
CAPABILITY_FINGERPRINT = "a" * 64
OPTIONS_FINGERPRINT = "b" * 64
SOURCE_BYTES = b"synthetic-loopback-photo-bytes"


def _pem_private(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                             serialization.NoEncryption()).decode("ascii")


def _pem_public(key):
    return key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def _write_tls_material(directory: Path):
    key = generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)-timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc)+timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256()))
    key_path, cert_path = directory / "loopback-key.pem", directory / "loopback-cert.pem"
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if os.name != "nt":
        key_path.chmod(0o600); cert_path.chmod(0o644)
    return key_path, cert_path


class _NativeTransport:
    def __init__(self, before_remove):
        import io
        self.calls = []
        self.tasks = {}
        self.before_remove = before_remove
        self.options = [dict(name="numeric-probe", type="float", value=0.0, help="Synthetic option.")]
        stream = io.BytesIO()
        with zipfile.ZipFile(stream, "w") as archive:
            archive.writestr("synthetic-result.txt", b"synthetic ODM archive result")
        self.archive = stream.getvalue()

    def initialize(self, task):
        self.calls.append(("initialize", task["engine_uuid"]))
        self.tasks[task["engine_uuid"]] = 40

    def upload(self, job, item): self.calls.append(("upload", job))

    def action(self, action, job):
        self.calls.append((action, job))
        if action == "remove":
            self.before_remove()
            self.tasks.pop(job, None)

    def read(self, route, **_kwargs):
        self.calls.append(("read", route))
        if route == "/info":
            return {"engine": "odm", "engineVersion": "synthetic-engine-1", "taskQueueCount": 0,
                    "maxImages": 1000}
        if route == "/task/list": return [{"uuid": item} for item in self.tasks]
        if route == "/options": return list(self.options)
        if route.endswith("/info"):
            identifier = route.split("/")[2]
            if identifier not in self.tasks: return {"error": "not_found"}
            return {"uuid": identifier, "status": {"code": self.tasks[identifier]}}
        return {"uuid": route.split("/")[2], "status": {"code": 40}}

    def download(self, job):
        import io
        self.calls.append(("download", job))
        response = io.BytesIO(self.archive)
        response.headers = {"Content-Length": str(len(self.archive))}
        return response


@unittest.skipUnless(sys.platform.startswith("linux"), "Viewer V2 durable result storage requires Linux directory fsync")
@unittest.skipUnless(os.environ.get("LTDS_VIEWER_WORKTREE"), "set LTDS_VIEWER_WORKTREE to Viewer V2 checkout")
class ViewerResultV2RoundTripTests(unittest.TestCase):
    def test_actual_viewer_source_intake_result_copy_lost_ack_and_worker_cleanup(self):
        viewer = Path(os.environ["LTDS_VIEWER_WORKTREE"]).resolve()
        viewer_app = viewer / "apps" / "viewer"
        node = os.environ.get("LTDS_NODE") or shutil.which("node")
        if not (viewer_app / "server" / "sourceIntakeClientV2.js").is_file() or not node:
            self.skipTest("Viewer V2 checkout or Node.js unavailable")

        with tempfile.TemporaryDirectory(prefix="ltds-viewer-result-roundtrip-") as temp:
            root = Path(temp)
            queue_dir, spool_root, scratch_root = root / "queue", root / "spool", root / "scratch"
            queue_dir.mkdir(); spool_root.mkdir(mode=0o700); scratch_root.mkdir(mode=0o700)
            marker = spool_root / ".ltds-source-spool-id"
            marker.write_text("c" * 64, encoding="ascii")
            if os.name != "nt": marker.chmod(0o600)
            (root / "photo.jpg").write_bytes(SOURCE_BYTES)
            key_path, cert_path = _write_tls_material(root)
            client_tls = ssl.create_default_context(cafile=str(cert_path))

            source_key = Ed25519PrivateKey.generate()
            snapshot_key = Ed25519PrivateKey.generate()
            viewer_copy_key = Ed25519PrivateKey.generate()
            trusted_source_keys = [{"environment": "staging", "issuerId": "pairing-source-issuer",
                "keyId": "pairing-source-key", "principalId": "viewer-staging",
                "publicKeyPem": _pem_public(source_key)}]
            viewer_receipt_keys = {"pairing-copy-key": {"issuerId": "pairing-copy-issuer",
                "publicKeyPem": _pem_public(viewer_copy_key)}}
            accepted = {"committed": False, "drop_count": 0}
            accepted_copy_requests, accepted_copy_responses = [], []
            source_drops = {"post": 0, "put": 0, "complete": 0}
            stored_path = root / "models"

            queue = Queue(queue_dir, reference_enabled=True)
            queue.allowed_profiles.add(PROFILE)
            gate_patch = patch("processing.node_gate.canonical_gate_path", return_value=root / "node-gate")
            mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            gate_patch.start(); mount_patch.start()
            servers, threads, originals = [], [], []
            try:
                spool = SourceSpool(spool_root, mount_id="c" * 64, safety_floor_bytes=0,
                                    max_object_bytes=1024, chunk_bytes=256)
                store = ReferenceStore(queue)
                worker = ReferenceEngineWorker(root / "worker.sqlite",
                    ledger=ScratchLedger(root / "ledger.sqlite", scratch_root, capacity_probe=lambda _: 10**9,
                        safety_floor_bytes=1024, cleanup_grace_seconds=0),
                    outbox=WorkerOutbox(root / "outbox.sqlite"), engine=NativeEngine("http://127.0.0.1:3001"),
                    engine_id="roundtrip-engine", node_id="cluster", authority_id=queue.authority_id,
                    coordinator_origin="https://127.0.0.1:1", gate_generation=1,
                    worker_id="roundtrip-worker", engine_version="synthetic-engine-1",
                    allowed_odm_options={"numeric-probe"}, viewer_receipt_keys=viewer_receipt_keys,
                    engine_probe=lambda _deadline: "roundtrip-engine")
                transport = _NativeTransport(lambda: None)
                result_bytes = transport.archive
                engine_patches = []
                for method in ("initialize", "upload", "action", "read", "download"):
                    from unittest.mock import patch as mock_patch
                    engine_patches.append(mock_patch.object(worker.engine, method,
                        side_effect=getattr(transport, method)).start())
                # The removal assertion checks both coordinator acceptance and durable Viewer bytes.
                def before_remove():
                    self.assertTrue(accepted["committed"], "engine cleanup preceded the signed Viewer ACK")
                    self.assertTrue(any(path.is_file() for path in stored_path.rglob("*.bin")),
                                    "Viewer copy must exist before native cache cleanup")
                transport.before_remove = before_remove

                result_api = ReferenceResultAPI(ReferenceResults(worker), bearer_readers={RESULT_BEARER: dict(
                    node_id="cluster", worker_id="roundtrip-worker", environments=("staging",),
                    share_policy=lambda metadata: metadata["environment"] == "staging")})
                result_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                result_tls.load_cert_chain(str(cert_path), str(key_path))
                result_server = create_reference_http_server(result_api, "127.0.0.1", 0, ssl_context=result_tls)
                servers.append(result_server)
                result_thread = threading.Thread(target=result_server.serve_forever, kwargs={"poll_interval": .02}, daemon=True)
                result_thread.start(); threads.append(result_thread)
                result_origin = f"https://127.0.0.1:{result_server.server_address[1]}"
                proxy = ResultProxy({"cluster": result_origin}, worker_bearers={"cluster": RESULT_BEARER},
                                    ssl_context=client_tls, admit_read=lambda _binding: nullcontext())
                api = ReferenceControlAPI(store, viewer_bearers={BEARER: "staging"},
                    worker_bearers={WORKER_BEARER: dict(node_id="cluster", worker_id="roundtrip-worker",
                        worker_instance_id="roundtrip-instance")},
                    capability_fingerprint=CAPABILITY_FINGERPRINT, options_fingerprint=OPTIONS_FINGERPRINT,
                    allowed_odm_options={"numeric-probe"}, result_proxy=proxy,
                    viewer_receipt_keys=viewer_receipt_keys, source_spool=spool,
                    source_attestation_keys=trusted_source_keys, authority_id=queue.authority_id)
                original_handle = api.handle
                def track_acceptance(method, route, headers, body=b"", **kwargs):
                    response = original_handle(method, route, headers, body, **kwargs)
                    if method == "POST" and route.endswith("/acceptance") and response.status == 200:
                        accepted["committed"] = True
                        accepted_copy_requests.append(bytes(body))
                        accepted_copy_responses.append(response.body)
                    return response
                api.handle = track_acceptance
                server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                server_tls.load_cert_chain(str(cert_path), str(key_path))
                coordinator_server = create_reference_http_server(api, "127.0.0.1", 0,
                    ssl_context=server_tls, request_timeout=5)
                servers.append(coordinator_server)
                handler = coordinator_server.RequestHandlerClass
                original_reply = handler._reply
                def lose_first_acceptance_reply(request_handler, response):
                    route = request_handler.path.split("?", 1)[0]
                    drop = ("post" if request_handler.command == "POST" and route == "/ltds/v2/jobs"
                            and response.status in (200, 201) else
                            "complete" if request_handler.command == "POST" and route.endswith("/source/complete")
                            and response.status in (200, 201) else
                            "put" if request_handler.command == "PUT" and route.startswith("/ltds/v2/jobs/")
                            and "/source/" in route and response.status in (200, 201) else None)
                    if drop and source_drops[drop] == 0:
                        source_drops[drop] += 1
                        request_handler.close_connection = True
                        try: request_handler.connection.shutdown(socket.SHUT_RDWR)
                        except OSError: pass
                        request_handler.connection.close()
                        return
                    if (request_handler.command == "POST" and request_handler.path.endswith("/acceptance")
                            and response.status == 200 and accepted["drop_count"] == 0):
                        accepted["drop_count"] += 1
                        request_handler.close_connection = True
                        try: request_handler.connection.shutdown(socket.SHUT_RDWR)
                        except OSError: pass
                        request_handler.connection.close()
                        return
                    return original_reply(request_handler, response)
                handler._reply = lose_first_acceptance_reply
                originals.append((handler, original_reply))
                coordinator_thread = threading.Thread(target=coordinator_server.serve_forever,
                    kwargs={"poll_interval": .02}, daemon=True)
                coordinator_thread.start(); threads.append(coordinator_thread)
                coordinator_origin = f"https://127.0.0.1:{coordinator_server.server_address[1]}"
                with patch("processing.node_gate.canonical_gate_path", return_value=root / "node-gate"):
                    from processing.node_gate import NodeGate
                    with NodeGate("roundtrip-engine") as gate:
                        gate.provision("cluster", coordinator_origin, queue.authority_id)
                worker.origin = coordinator_origin
                driver = ReferenceDriver(worker, worker.ledger, worker.outbox,
                    BrokerClient(coordinator_origin, ssl_context=client_tls), coordinator_origin, WORKER_BEARER,
                    state_path=root / "driver.sqlite", ssl_context=client_tls, output_budget=1024 * 1024,
                    transfer_ttl=240, safety_floor=1024, failure_outbox=FailureOutbox(root / "failures.sqlite"),
                    worker_instance_id="roundtrip-instance",
                    engine_cleanup=EngineCacheCleanup(worker, path=root / "engine-cleanup.sqlite",
                        viewer_receipt_keys=viewer_receipt_keys))
                # NativeEngine identity and cleanup use the same synthetic local loopback daemon.
                environment = os.environ.copy()
                config = {"origin": coordinator_origin, "viewerBearer": BEARER, "authorityId": queue.authority_id,
                    "databasePath": str(root / "viewer.sqlite"), "sourceFilePath": str(root / "photo.jpg"),
                    "capabilityFingerprint": CAPABILITY_FINGERPRINT, "optionsFingerprint": OPTIONS_FINGERPRINT,
                    "snapshotPrivateKeyPem": _pem_private(snapshot_key), "snapshotPublicKeyPem": _pem_public(snapshot_key),
                    "attestationPrivateKeyPem": _pem_private(source_key), "attestationPublicKeyPem": _pem_public(source_key),
                    "copyPrivateKeyPem": _pem_private(viewer_copy_key), "copyPublicKeyPem": _pem_public(viewer_copy_key),
                    "modelsRoot": str(stored_path), "resultBytesBase64": __import__('base64').b64encode(result_bytes).decode()}
                config_path = root / "pairing-config.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                if os.name != "nt": config_path.chmod(0o600)
                environment.update({"LTDS_VIEWER_APP_ROOT": str(viewer_app), "LTDS_PAIRING_CONFIG": str(config_path),
                    "NODE_EXTRA_CA_CERTS": str(cert_path)})
                if os.environ.get("LTDS_VIEWER_NODE_MODULES"):
                    environment["NODE_PATH"] = os.environ["LTDS_VIEWER_NODE_MODULES"]
                source_pair = subprocess.run([node, str(Path(__file__).parent / "fixtures" /
                    "viewer_source_intake_v2_pairing.js")], cwd=viewer_app, env=environment,
                    capture_output=True, text=True, timeout=90, check=False)
                self.assertEqual(source_pair.returncode, 0, source_pair.stderr[-3000:])
                self.assertEqual(source_drops, {"post": 1, "put": 1, "complete": 1})
                # Turn the real, durable source-intake attempt into its normal reconcile lease.
                setup = subprocess.run([node, str(Path(__file__).parent / "fixtures" /
                    "viewer_result_roundtrip_setup.js")], cwd=viewer_app, env=environment,
                    capture_output=True, text=True, timeout=30, check=False)
                self.assertEqual(setup.returncode, 0, setup.stderr[-3000:])
                state = json.loads(setup.stdout.strip().splitlines()[-1])
                self.assertEqual(len(store.list(private=True)), 1)
                for _ in range(40):
                    step = driver.step()
                    current = store.get(state["providerTaskId"], private=True)
                    if current["status"] == "succeeded" and current["phase"] == "awaiting_acceptance": break
                    self.assertNotIn(step["state"], ("held", "retry_wait"), f"worker did not advance: {step}")
                else: self.fail("worker did not durably deliver synthetic result metadata")
                self.assertEqual(transport.calls.count(("initialize", state["providerTaskId"])), 1)
                execution_identity = json.loads(worker._row(state["providerTaskId"])["identity"])
                self.assertEqual(execution_identity["sourceAcceptanceBinding"]["jobId"], state["providerTaskId"])
                self.assertTrue((root / "scratch").exists())
                config.update({"reconcileJobId": state["reconcileJobId"], "reconcileOwner": state["reconcileOwner"]})
                config_path.write_text(json.dumps(config), encoding="utf-8")
                reconcile = subprocess.run([node, str(Path(__file__).parent / "fixtures" /
                    "viewer_result_roundtrip_reconcile.js")], cwd=viewer_app, env=environment,
                    capture_output=True, text=True, timeout=60, check=False)
                self.assertEqual(reconcile.returncode, 0, reconcile.stderr[-4000:])
                result = json.loads(reconcile.stdout.strip().splitlines()[-1])
                self.assertTrue(result["firstReplyLost"] and result["exactRequestPersisted"] and result["resultBytesRetained"] and result["accepted"])
                self.assertEqual(accepted["drop_count"], 1)
                self.assertTrue(accepted["committed"])
                self.assertEqual(len(accepted_copy_requests), 2)
                self.assertEqual(hashlib.sha256(accepted_copy_requests[0]).digest(),
                                 hashlib.sha256(accepted_copy_requests[1]).digest(),
                                 "lost ACK retry must send the identical durable receipt request")
                self.assertEqual(accepted_copy_requests[0], accepted_copy_requests[1])
                first_receipt = accepted_copy_responses[0]["viewerReceipt"]
                replay_receipt = accepted_copy_responses[1]["viewerReceipt"]
                self.assertEqual((first_receipt["receiptId"], first_receipt["storageReceiptId"]),
                                 (replay_receipt["receiptId"], replay_receipt["storageReceiptId"]))
                self.assertTrue(any(path.is_file() for path in stored_path.rglob("*.bin")))
                driver_result = driver.step()
                self.assertEqual(driver_result["state"], "completed")
                self.assertTrue(any(call[0] == "remove" and call[1] == state["providerTaskId"] for call in transport.calls))
                self.assertTrue(any(path.is_file() for path in stored_path.rglob("*.bin")), "worker cleanup must not remove Viewer copy")
                self.assertEqual(accepted["drop_count"], 1, "accepted-copy retry must not duplicate engine task execution")
            finally:
                for server in servers:
                    server.shutdown(); server.server_close()
                for thread in threads:
                    thread.join(timeout=3)
                for handler, original in originals:
                    handler._reply = original
                for engine_patch in reversed(locals().get("engine_patches", [])):
                    engine_patch.stop()
                mount_patch.stop(); gate_patch.stop(); queue.close()
                self.assertTrue(all(not thread.is_alive() for thread in threads), "loopback listener leaked")


if __name__ == "__main__": unittest.main()
