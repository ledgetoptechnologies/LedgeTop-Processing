"""Ephemeral V2 signer for focused Processing worker and cleanup tests."""
import base64
import hashlib
import uuid

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing.reference_protocol import canonical_json
from processing.viewer_copy_receipt_v2 import ACCEPTANCE_DOMAIN, PROTOCOL, SIGNATURE_DOMAIN


class SyntheticViewerCopyV2Signer:
    def __init__(self):
        self.issuer_id = 'synthetic-viewer-v2-issuer'
        self.key_id = 'synthetic-viewer-v2-key'
        self._key = Ed25519PrivateKey.generate()
        public = self._key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')
        self.trusted_keys = {self.key_id: {'issuerId': self.issuer_id, 'publicKeyPem': public}}

    def receipt(self, expected, acceptance, result_manifest):
        ids = [item['id'] for item in result_manifest['objects']]
        value = dict(protocol=PROTOCOL, schemaVersion=2, issuerId=self.issuer_id,
            keyId=self.key_id, receiptId=str(uuid.uuid4()), storageReceiptId=str(uuid.uuid4()),
            acceptanceReceiptId=acceptance['receiptId'],
            acceptanceStorageReceiptId=acceptance['storageReceiptId'],
            acceptancePayloadSha256=hashlib.sha256(
                ACCEPTANCE_DOMAIN + canonical_json(acceptance)).hexdigest(),
            **expected, acceptedObjectIds=ids,
            objects=[dict(id=item['id'], byteSize=item['byteSize'], sha256=item['sha256'])
                     for item in result_manifest['objects']],
            acceptedAt='2026-11-04T12:00:00.000Z')
        signature = self._key.sign(SIGNATURE_DOMAIN + canonical_json(value))
        value['signature'] = base64.urlsafe_b64encode(signature).decode('ascii').rstrip('=')
        return value

    def envelope(self, expected, acceptance, result_manifest):
        return {'acceptance': acceptance,
                'viewerReceipt': self.receipt(expected, acceptance, result_manifest)}
