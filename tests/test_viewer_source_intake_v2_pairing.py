"""Bounded loopback HTTPS pairing test for the real Viewer V2 client/outbox."""
from __future__ import annotations

import ipaddress
import hashlib
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
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import generate_private_key
from cryptography.x509.oid import NameOID

from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_store import PROFILE, ReferenceStore
from processing.source_spool import SourceSpool


BEARER = "synthetic-viewer-source-intake-token"
WORKER_BEARER = "synthetic-pairing-worker-token"
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
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)
            - timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256()))
    key_path, cert_path = directory / "loopback-key.pem", directory / "loopback-cert.pem"
    key_path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL, serialization.NoEncryption()))
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    if os.name != "nt":
        key_path.chmod(0o600)
        cert_path.chmod(0o644)
    return key_path, cert_path


@unittest.skipUnless(os.environ.get("LTDS_VIEWER_WORKTREE"),
                     "set LTDS_VIEWER_WORKTREE to the Viewer V2 checkout")
class ViewerSourceIntakeV2PairingTests(unittest.TestCase):
    def test_actual_viewer_client_replays_post_resumes_upload_and_persists_receipts(self):
        viewer = Path(os.environ["LTDS_VIEWER_WORKTREE"]).resolve()
        viewer_app = viewer / "apps" / "viewer"
        node = os.environ.get("LTDS_NODE") or shutil.which("node")
        if not (viewer_app / "server" / "sourceIntakeClientV2.js").is_file():
            self.skipTest("Viewer V2 client checkout is unavailable")
        if not node:
            self.skipTest("Node.js is unavailable")

        with tempfile.TemporaryDirectory(prefix="ltds-viewer-pairing-") as tmp:
            root = Path(tmp)
            queue_dir, spool_root = root / "queue", root / "spool"
            queue_dir.mkdir(mode=0o700); spool_root.mkdir(mode=0o700)
            marker = spool_root / ".ltds-source-spool-id"
            marker.write_text("c" * 64, encoding="ascii")
            if os.name != "nt":
                marker.chmod(0o600)
            (root / "photo.jpg").write_bytes(SOURCE_BYTES)
            key_path, cert_path = _write_tls_material(root)

            snapshot_key = Ed25519PrivateKey.generate()
            attestation_key = Ed25519PrivateKey.generate()
            acceptance_key = Ed25519PrivateKey.generate()
            trusted_source_keys = [{"environment": "staging", "issuerId": "pairing-source-issuer",
                "keyId": "pairing-source-key", "principalId": "viewer-staging",
                "publicKeyPem": _pem_public(attestation_key)}]
            # Acceptance pins are deliberately a separate generated key and registry.
            viewer_receipt_keys = {"pairing-copy-key": {"issuerId": "pairing-copy-issuer",
                "publicKeyPem": _pem_public(acceptance_key)}}
            self.assertNotEqual(trusted_source_keys[0]["publicKeyPem"],
                                viewer_receipt_keys["pairing-copy-key"]["publicKeyPem"])

            queue = Queue(queue_dir, reference_enabled=True)
            queue.allowed_profiles.add(PROFILE)
            spool_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
            spool_patch.start()
            server = thread = None
            handler = original_reply = None
            drops = {"post": 0, "put": 0, "completion": 0}
            responses = []
            validation_errors = []
            post_hashes = []
            status_reads = []
            upload_routes = []
            try:
                spool = SourceSpool(spool_root, mount_id="c" * 64, safety_floor_bytes=0,
                                    max_object_bytes=1024, chunk_bytes=256)
                api = ReferenceControlAPI(ReferenceStore(queue),
                    viewer_bearers={BEARER: "staging"},
                    worker_bearers={WORKER_BEARER: dict(node_id="cluster",
                        worker_id="synthetic-worker", worker_instance_id="pairing-instance")},
                    capability_fingerprint=CAPABILITY_FINGERPRINT,
                    options_fingerprint=OPTIONS_FINGERPRINT,
                    allowed_odm_options={"numeric-probe"},
                    viewer_receipt_keys=viewer_receipt_keys,
                    source_spool=spool, source_attestation_keys=trusted_source_keys,
                    authority_id=queue.authority_id)
                original_handle = api.handle
                def inspect_v2_request(method, route, headers, body=b"", **kwargs):
                    if method == "POST" and route == "/ltds/v2/jobs":
                        post_hashes.append(hashlib.sha256(body).hexdigest())
                        try:
                            from processing import reference_protocol
                            from processing.reference_limits import V2_SOURCE_INTAKE_BYTES
                            from processing.reference_source_intake import validate_source_intake
                            validate_source_intake(reference_protocol.decode_submission(
                                body, max_bytes=V2_SOURCE_INTAKE_BYTES),
                                environment="staging", capability_fingerprint=CAPABILITY_FINGERPRINT,
                                options_fingerprint=OPTIONS_FINGERPRINT,
                                allowed_odm_options={"numeric-probe"},
                                trusted_source_keys=trusted_source_keys, authority_id=queue.authority_id)
                        except Exception as error:
                            validation_errors.append(f"{type(error).__name__}: {error}")
                    if method == "GET" and route.startswith("/ltds/v2/jobs/"):
                        status_reads.append(route)
                    return original_handle(method, route, headers, body, **kwargs)
                api.handle = inspect_v2_request
                original_stream_source_object = api.stream_source_object
                def inspect_stream_source_object(route, *args, **kwargs):
                    upload_routes.append(route)
                    return original_stream_source_object(route, *args, **kwargs)
                api.stream_source_object = inspect_stream_source_object
                tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                tls.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
                server = create_reference_http_server(api, host="127.0.0.1", port=0,
                    ssl_context=tls, request_timeout=3)
                handler = server.RequestHandlerClass
                original_reply = handler._reply

                def lose_first_committed_response(request_handler, response):
                    route = request_handler.path
                    responses.append((request_handler.command, route, response.status,
                                      response.body.get("error") if isinstance(response.body, dict) else None))
                    drop_key = ("post" if request_handler.command == "POST" and
                        route == "/ltds/v2/jobs" and response.status in (200, 201) else
                        "completion" if request_handler.command == "POST" and
                        route.endswith("/source/complete") and response.status in (200, 201) else
                        "put" if request_handler.command == "PUT" and
                        route.startswith("/ltds/v2/jobs/") and "/source/" in route and
                        response.status in (200, 201) else None)
                    if drop_key and drops[drop_key] == 0:
                        drops[drop_key] += 1
                        request_handler.close_connection = True
                        try:
                            request_handler.connection.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        request_handler.connection.close()
                        return
                    return original_reply(request_handler, response)

                handler._reply = lose_first_committed_response
                thread = threading.Thread(target=server.serve_forever,
                    kwargs={"poll_interval": 0.02}, daemon=True)
                thread.start()
                origin = f"https://127.0.0.1:{server.server_address[1]}"
                config = {
                    "origin": origin, "viewerBearer": BEARER, "authorityId": queue.authority_id,
                    "databasePath": str(root / "viewer.sqlite"), "sourceFilePath": str(root / "photo.jpg"),
                    "capabilityFingerprint": CAPABILITY_FINGERPRINT,
                    "optionsFingerprint": OPTIONS_FINGERPRINT,
                    "snapshotPrivateKeyPem": _pem_private(snapshot_key),
                    "snapshotPublicKeyPem": _pem_public(snapshot_key),
                    "attestationPrivateKeyPem": _pem_private(attestation_key),
                    "attestationPublicKeyPem": _pem_public(attestation_key),
                }
                config_path = root / "pairing-config.json"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                if os.name != "nt":
                    config_path.chmod(0o600)
                environment = os.environ.copy()
                environment.update({"LTDS_VIEWER_APP_ROOT": str(viewer_app),
                    "LTDS_PAIRING_CONFIG": str(config_path), "NODE_EXTRA_CA_CERTS": str(cert_path)})
                completed = subprocess.run([node, str(Path(__file__).parent / "fixtures" /
                    "viewer_source_intake_v2_pairing.js")], cwd=viewer_app, env=environment,
                    capture_output=True, text=True, timeout=90, check=False)
                if completed.returncode != 0:
                    self.fail(f"actual Viewer V2 pairing failed ({completed.returncode}): "
                              f"{completed.stderr[-4000:]} {completed.stdout[-1000:]} "
                              f"backend_responses={responses[-8:]} validation={validation_errors[-2:]}")
                result = json.loads(completed.stdout.strip().splitlines()[-1])
                self.assertEqual(result, {"discovery": True, "lostPostReplay": True,
                    "lostUploadStatusResume": True, "lostCompletionReplay": True,
                    "strictCompletionPersisted": True,
                    "numericOptionExact": True, "sourceSignerPinned": True})
                self.assertEqual(validation_errors, [])
                self.assertEqual(drops, {"post": 1, "put": 1, "completion": 1})
                self.assertEqual(len(post_hashes), 1,
                    "lost admission response must recover the committed binding without another POST")
                submission_reads = [(route, status) for method, route, status, _ in responses
                    if method == "GET" and route.startswith("/ltds/v1/jobs/by-submission/")]
                self.assertEqual(len(submission_reads), 2)
                self.assertEqual(submission_reads[0][0], submission_reads[1][0],
                    "restart must reconcile the exact original submission identity")
                self.assertEqual([status for _, status in submission_reads], [404, 200],
                    "fresh absence then durable accepted admission must be distinguished")
                self.assertEqual(len(status_reads), 3, "upload/completion retry must reconcile from ACK status")
                self.assertEqual(len(upload_routes), 1, "acknowledged stream must not be uploaded twice")
                with queue._db() as db:
                    admitted = db.execute("SELECT COUNT(*) FROM reference_source_intakes").fetchone()[0]
                    received = db.execute("SELECT COUNT(*) FROM reference_source_object_receipts").fetchone()[0]
                self.assertEqual((admitted, received), (1, 1), "lost replies must not duplicate admission or uploads")
            finally:
                if server is not None:
                    server.shutdown(); server.server_close()
                if thread is not None:
                    thread.join(timeout=3)
                if handler is not None and original_reply is not None:
                    handler._reply = original_reply
                spool_patch.stop()
                queue.close()
                if thread is not None:
                    self.assertFalse(thread.is_alive(), "loopback HTTPS listener thread did not stop")


if __name__ == "__main__":
    unittest.main()
