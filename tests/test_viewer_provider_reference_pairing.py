"""Actual Viewer provider-to-reference TLS pairing fixture.

The test runs the production dynamic runtime adapter and HTTPS listener with a
synthetic in-memory engine catalog. It never contacts an ODM daemon or uses
production credentials/data.
"""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.x509.oid import NameOID

from processing.config import ProcessingNode
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_runtime import _DynamicAPI, discovered_policy
from processing.reference_store import ReferenceStore, VIEWER_PROFILE
from processing.source_spool import SourceSpool


VIEWER_BEARER = "synthetic-viewer-provider-token-" + "v" * 32
WORKER_BEARER = "synthetic-provider-worker-token-" + "w" * 32


def _public_pem(key):
    return key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def _private_pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode("ascii")


def _write_tls_material(directory: Path):
    key = generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)
            - timedelta(minutes=1)).not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256()))
    key_path, cert_path = directory / "provider-pairing-key.pem", directory / "provider-pairing-cert.pem"
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if os.name != "nt":
        key_path.chmod(0o600)
        cert_path.chmod(0o644)
    return key_path, cert_path


def _engine_snapshot():
    options = [
        {"name": "pc-ept", "type": "bool", "domain": "", "value": "false",
         "help": "Export the georeferenced point cloud in Entwine Point Tile (EPT) format. Default: %(default)s"},
        {"name": "gltf", "type": "bool", "domain": "", "value": "false",
         "help": "Generate single file Binary glTF (GLB) textured models. Default: %(default)s"},
        {"name": "3d-tiles", "type": "bool", "domain": "", "value": "false",
         "help": "Generate OGC 3D Tiles outputs. Default: %(default)s"},
        {"name": "orthophoto-resolution", "type": "float", "domain": "positive float",
         "value": "2", "help": "Synthetic numeric processing setting"},
        {"name": "feature-quality", "type": "enum", "domain": ["low", "medium", "high"],
         "value": "medium", "help": "Synthetic quality setting"},
    ]
    nodes = []
    for role, engine_version in (("cluster", "synthetic-cluster"), ("main", "synthetic-main")):
        nodes.append({"role": role, "available": True, "configured": True,
            "capabilitiesSha256": ("a" if role == "cluster" else "b") * 64,
            "engine": "odm", "engineVersion": engine_version, "options": options})
    return {"ready": True, "nodes": nodes, "commonOptions": options}


@unittest.skipUnless(os.environ.get("LTDS_VIEWER_WORKTREE"),
                     "set LTDS_VIEWER_WORKTREE to the Viewer provider checkout")
class ViewerProviderReferencePairingTests(unittest.TestCase):
    def test_actual_viewer_provider_admission_uses_dynamic_reference_tls(self):
        viewer = Path(os.environ["LTDS_VIEWER_WORKTREE"]).resolve()
        viewer_app = viewer / "apps" / "viewer"
        node = os.environ.get("LTDS_NODE") or shutil.which("node")
        if not node:
            self.skipTest("Node.js is unavailable")
        # This interface belongs to the Viewer owner; do not accept a synthetic
        # HTTP client or test hook in its place.
        fixture = Path(__file__).parent / "fixtures" / "viewer_provider_reference_pairing.js"
        if not fixture.is_file():
            self.skipTest("actual Viewer provider pairing fixture is not ready")

        with tempfile.TemporaryDirectory(prefix="ltds-viewer-provider-pairing-") as tmp:
            root = Path(tmp)
            queue_dir, spool_root = root / "queue", root / "source-spool"
            queue_dir.mkdir(mode=0o700); spool_root.mkdir(mode=0o700)
            marker = spool_root / ".ltds-source-spool-id"
            marker.write_text("c" * 64, encoding="ascii")
            if os.name != "nt": marker.chmod(0o600)
            key_path, cert_path = _write_tls_material(root)
            source_key = Ed25519PrivateKey.generate()
            receipt_key = Ed25519PrivateKey.generate()
            snapshot_key = Ed25519PrivateKey.generate()
            attestation_key = Ed25519PrivateKey.generate()
            source_keys = [{"environment": "staging", "issuerId": "pairing-source-issuer",
                "keyId": "pairing-source-key", "principalId": "viewer-staging",
                "publicKeyPem": _public_pem(attestation_key)}]
            receipt_keys = {"pairing-copy-key": {"issuerId": "pairing-copy-issuer",
                "publicKeyPem": _public_pem(receipt_key)}}
            queue = Queue(queue_dir, node_ids=("cluster", "main"), reference_enabled=True)
            spool_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            spool_patch.start()
            server = thread = None
            try:
                spool = SourceSpool(spool_root, mount_id="c" * 64, safety_floor_bytes=0,
                    max_object_bytes=1024, chunk_bytes=256)
                self.assertTrue(spool.available_bytes() > 0)
                config = SimpleNamespace(prototype_node_ids=("cluster", "main"),
                    node_endpoints=(ProcessingNode("cluster", "http://127.0.0.1:19001", "synthetic"),
                                    ProcessingNode("main", "http://127.0.0.1:19002", "synthetic")),
                    local_max_photos=1000, local_max_input_bytes=20 * 1024**3)
                capability_fingerprint, options_fingerprint, allowed_options = discovered_policy(
                    _engine_snapshot(), config)
                control = ReferenceControlAPI(ReferenceStore(queue, local_max_photos=1000),
                    viewer_bearers={VIEWER_BEARER: "staging"},
                    worker_bearers={WORKER_BEARER: dict(node_id="cluster", worker_id="pairing-worker",
                        worker_instance_id="pairing-instance")},
                    capability_fingerprint=capability_fingerprint, options_fingerprint=options_fingerprint,
                    allowed_odm_options=allowed_options, viewer_receipt_keys=receipt_keys,
                    source_spool=spool, source_attestation_keys=source_keys, authority_id=queue.authority_id)
                settings = SimpleNamespace(viewer_bearers={VIEWER_BEARER: "staging"},
                    viewer_receipt_keys=receipt_keys, source_spool=spool,
                    source_attestation_keys=source_keys)
                capacities = {
                    "cluster": {"ready": True, "maxImages": 1000, "maxInputBytes": 20 * 1024**3,
                                "taskQueueCount": 0},
                    "main": {"ready": True, "maxImages": None, "maxInputBytes": None,
                             "taskQueueCount": 0}}
                api = _DynamicAPI(control, config, settings, _engine_snapshot, lambda: capacities)
                dynamic_requests = []
                option_response_shapes = []
                construction_errors = []
                import processing.reference_runtime as reference_runtime_module
                original_reference_api = reference_runtime_module.ReferenceAPI
                def observe_reference_api(*args, **kwargs):
                    try:
                        return original_reference_api(*args, **kwargs)
                    except Exception as error:
                        construction_errors.append(f"{type(error).__name__}:{error}")
                        raise
                reference_runtime_module.ReferenceAPI = observe_reference_api
                original_handle = api.handle
                def observe_dynamic_request(method, path, headers, body=b"", **kwargs):
                    authorization = headers.get("Authorization") if hasattr(headers, "get") else None
                    response = original_handle(method, path, headers, body, **kwargs)
                    dynamic_requests.append((method, path, authorization == f"Bearer {VIEWER_BEARER}",
                        response.status, response.body.get("error") if type(response.body) is dict else None))
                    if method == "GET" and path == "/ltds/v1/options" and response.status == 200:
                        option_response_shapes.append([{"name": item.get("name"),
                            "type": item.get("type"), "valueType": type(item.get("value")).__name__}
                            for item in response.body.get("options", [])])
                    return response
                api.handle = observe_dynamic_request
                streamed_requests = []
                original_stream = api.stream_source_object
                def observe_stream_request(path, headers, *args, **kwargs):
                    authorization = headers.get("Authorization") if hasattr(headers, "get") else None
                    streamed_requests.append((path, authorization == f"Bearer {VIEWER_BEARER}"))
                    return original_stream(path, headers, *args, **kwargs)
                api.stream_source_object = observe_stream_request
                tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                tls.minimum_version = ssl.TLSVersion.TLSv1_2
                tls.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
                server = create_reference_http_server(api, host="127.0.0.1", port=0,
                    ssl_context=tls, request_timeout=5)
                thread = threading.Thread(target=server.serve_forever,
                    kwargs={"poll_interval": 0.05}, daemon=True)
                thread.start()
                origin = f"https://127.0.0.1:{server.server_address[1]}"
                pairing_config = {"origin": origin, "viewerBearer": VIEWER_BEARER,
                    "providerId": "synthetic-reference-coordinator",
                    "authorityId": queue.authority_id, "databasePath": str(root / "viewer.sqlite"),
                    "certificatePath": str(cert_path), "capabilityFingerprint": capability_fingerprint,
                    "optionsFingerprint": options_fingerprint,
                    "snapshotPrivateKeyPem": _private_pem(snapshot_key),
                    "snapshotPublicKeyPem": _public_pem(snapshot_key),
                    "attestationPrivateKeyPem": _private_pem(attestation_key),
                    "attestationPublicKeyPem": _public_pem(attestation_key)}
                config_path = root / "pairing-config.json"
                config_path.write_text(json.dumps(pairing_config), encoding="utf-8")
                if os.name != "nt": config_path.chmod(0o600)
                environment = os.environ.copy()
                environment.update({"LTDS_VIEWER_APP_ROOT": str(viewer_app),
                    "LTDS_PAIRING_CONFIG": str(config_path), "NODE_EXTRA_CA_CERTS": str(cert_path)})
                completed = subprocess.run([node, str(fixture)], cwd=viewer_app,
                    env=environment, capture_output=True, text=True, timeout=90, check=False)
                if completed.returncode != 0:
                    self.fail(f"actual Viewer provider pairing failed ({completed.returncode}): "
                        f"{completed.stderr[-4000:]} {completed.stdout[-1000:]} "
                        f"backend_routes={dynamic_requests[-8:]} option_shapes={option_response_shapes[-2:]} "
                        f"build_errors={construction_errors[-4:]}")
                result = json.loads(completed.stdout.strip().splitlines()[-1])
                self.assertTrue(result["providerHealthy"])
                self.assertTrue(result["capabilitiesFromCoordinator"])
                self.assertTrue(result["taskAdmitted"])
                self.assertEqual(result["sourceManifestObjectCounts"], [1, 1],
                    f"each synthetic dataset must transfer one real object: {result}")
                self.assertEqual(result["sourceIntakeCalls"],
                    ["createOrReplay", "uploadClosure:1", "createOrReplay", "uploadClosure:1"],
                    f"each actual Viewer submit must transfer exactly its one-object closure: {result}")
                self.assertTrue(all(item["sourceIntakeCompletionPersisted"]
                                    for item in result["attemptDiagnostics"]),
                    f"source-completion ACKs must be durable before admission completes: {result}")
                self.assertTrue(all(authenticated for _, _, authenticated, _, _ in dynamic_requests))
                path_counts = {}
                for method, path, _, status, _ in dynamic_requests:
                    key = (method, "/ltds/v2/jobs/{id}" if path.startswith("/ltds/v2/jobs/")
                           and path.endswith("/source/complete") is False
                           and method == "GET" else
                           "/ltds/v2/jobs/{id}/source/complete" if method == "POST"
                           and path.startswith("/ltds/v2/jobs/") and path.endswith("/source/complete")
                           else path)
                    path_counts[key] = path_counts.get(key, 0) + 1
                self.assertEqual(path_counts, {
                    ("GET", "/ltds/v2/capabilities"): 4,
                    ("GET", "/ltds/v1/options"): 2,
                    ("POST", "/ltds/v2/jobs"): 2,
                    ("GET", "/ltds/v2/jobs/{id}"): 2,
                    ("POST", "/ltds/v2/jobs/{id}/source/complete"): 2,
                }, f"unexpected actual backend route counts: {dynamic_requests}; viewer={result}")
                self.assertFalse(any("token=" in path.lower() or path.endswith("/info")
                                     or path.endswith("/task/new") for _, path, _, _, _ in dynamic_requests))
                self.assertEqual(len(streamed_requests), 2, f"one source object must stream per job: {result}")
                self.assertTrue(all(authenticated and "/source/" in path for path, authenticated in streamed_requests))
                with queue._db() as db:
                    queued = db.execute("SELECT COUNT(*) FROM jobs WHERE status='queued'").fetchone()[0]
                    admitted = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase='queued'").fetchone()[0]
                    intakes = db.execute("SELECT COUNT(*) FROM reference_source_intakes WHERE source_state='accepted' AND derived_state='accepted'").fetchone()[0]
                    receipts = db.execute("SELECT COUNT(*) FROM reference_source_object_receipts").fetchone()[0]
                self.assertEqual((queued, admitted, intakes, receipts), (2, 2, 2, 2),
                    f"two fully accepted source objects must be durable; viewer={result}; routes={dynamic_requests}")
            finally:
                if 'reference_runtime_module' in locals():
                    reference_runtime_module.ReferenceAPI = original_reference_api
                if server is not None:
                    server.shutdown(); server.server_close()
                if thread is not None:
                    thread.join(timeout=3)
                spool_patch.stop()
                queue.close()
                if thread is not None:
                    self.assertFalse(thread.is_alive(), "pairing TLS server thread did not stop")


if __name__ == "__main__":
    unittest.main()
