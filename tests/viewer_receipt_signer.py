"""Ephemeral Ed25519 signing helper for synthetic accepted-copy tests."""
import base64
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as p
from processing.viewer_acceptance_receipt import PROTOCOL, SIGNATURE_DOMAIN


class SyntheticViewerSigner:
    def __init__(self):
        self.issuer_id = 'synthetic-viewer-issuer'
        self.key_id = 'synthetic-viewer-key'
        self._key = Ed25519PrivateKey.generate()
        public = self._key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
        self.trusted_keys = {self.key_id: {'issuerId': self.issuer_id, 'publicKeyPem': public}}

    def receipt(self, acceptance, result_manifest):
        objects = {item['id']: item for item in result_manifest['objects']}
        ids = sorted(acceptance['acceptedObjectIds'])
        value = dict(protocol=PROTOCOL, schemaVersion=1, issuerId=self.issuer_id, keyId=self.key_id,
            receiptId='viewer-' + uuid.uuid4().hex, storageReceiptId='viewer-storage-' + uuid.uuid4().hex,
            jobId=acceptance['jobId'], generation=acceptance['generation'], environment=acceptance['environment'],
            inputManifestSha256=acceptance['inputManifestSha256'], pipelineDigest=acceptance['pipelineDigest'],
            resultManifestSha256=acceptance['resultManifestSha256'], acceptedObjectIds=ids,
            objects=[dict(id=object_id, byteSize=objects[object_id]['byteSize'], sha256=objects[object_id]['sha256'])
                     for object_id in ids], acceptedAt='2026-11-02T12:00:00.000Z')
        signature = self._key.sign(SIGNATURE_DOMAIN + p.canonical_json(value))
        value['signature'] = base64.urlsafe_b64encode(signature).decode('ascii').rstrip('=')
        return value

    def envelope(self, acceptance, result_manifest):
        return {'acceptance': acceptance, 'viewerReceipt': self.receipt(acceptance, result_manifest)}
