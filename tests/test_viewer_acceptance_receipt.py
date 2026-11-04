import copy
import json
from pathlib import Path
import unittest
import base64
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from processing import reference_protocol

from processing.viewer_acceptance_receipt import (
    ViewerReceiptRejected,
    SIGNATURE_DOMAIN,
    verify_viewer_acceptance_receipt,
)
from tests.viewer_receipt_signer import SyntheticViewerSigner


ROOT = Path(__file__).resolve().parents[1]


class ViewerAcceptanceReceiptTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.vector = json.loads((ROOT / "contracts/viewer-accepted-copy-receipt-v1.json").read_text(encoding="utf-8"))
        cls.trusted = {cls.vector["keyId"]: {
            "issuerId": cls.vector["issuerId"],
            "publicKeyPem": cls.vector["publicKeyPem"],
        }}

    def verify(self, receipt=None, *, trusted=None, expected=None, object_ids=None):
        return verify_viewer_acceptance_receipt(
            self.vector["receipt"] if receipt is None else receipt,
            trusted_keys=self.trusted if trusted is None else trusted,
            expected=self.vector["expected"] if expected is None else expected,
            result_manifest=self.vector["resultManifest"],
            accepted_object_ids=self.vector["acceptedObjectIds"] if object_ids is None else object_ids,
        )

    def test_verifies_viewer_node_issued_cross_language_vector(self):
        result = self.verify()
        self.assertEqual(result.issuer_id, "viewer-test-issuer")
        self.assertEqual(result.key_id, "viewer-test-key-2026")
        self.assertEqual(result.receipt["protocol"], "ltds-viewer-accepted-copy-v1")

    def test_rejects_mutated_receipt_binding_even_with_well_formed_hash(self):
        receipt = copy.deepcopy(self.vector["receipt"])
        receipt["generation"] += 1
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_job_binding_mismatch"):
            self.verify(receipt)

    def test_rejects_signature_tampering(self):
        receipt = copy.deepcopy(self.vector["receipt"])
        receipt["signature"] = ("A" if receipt["signature"][0] != "A" else "B") + receipt["signature"][1:]
        with self.assertRaisesRegex(ViewerReceiptRejected, "invalid_receipt_signature"):
            self.verify(receipt)

    def test_rejects_unknown_key_or_different_issuer(self):
        receipt = copy.deepcopy(self.vector["receipt"])
        receipt["keyId"] = "unknown-key"
        with self.assertRaisesRegex(ViewerReceiptRejected, "untrusted_receipt_key"):
            self.verify(receipt)
        receipt = copy.deepcopy(self.vector["receipt"])
        trusted = {self.vector["keyId"]: {"issuerId": "other-viewer", "publicKeyPem": self.vector["publicKeyPem"]}}
        with self.assertRaisesRegex(ViewerReceiptRejected, "untrusted_receipt_issuer"):
            self.verify(receipt, trusted=trusted)

    def test_rejects_wrong_expected_objects_and_changed_manifest(self):
        with self.assertRaisesRegex(ViewerReceiptRejected, "unexpected_expected_object"):
            self.verify(object_ids=["other-object"])
        receipt = copy.deepcopy(self.vector["receipt"])
        receipt["objects"][0]["byteSize"] += 1
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_object_binding_mismatch"):
            self.verify(receipt)

    def test_rejects_valid_signature_for_different_expected_manifest_digest(self):
        receipt=copy.deepcopy(self.vector['receipt'])
        expected=copy.deepcopy(self.vector['expected'])
        other='f'*64 if expected['resultManifestSha256']!='f'*64 else 'e'*64
        receipt['resultManifestSha256']=expected['resultManifestSha256']=other
        key=Ed25519PrivateKey.generate()
        body={k:v for k,v in receipt.items() if k!='signature'}
        signature=key.sign(SIGNATURE_DOMAIN+reference_protocol.canonical_json(body))
        key.public_key().verify(signature,SIGNATURE_DOMAIN+reference_protocol.canonical_json(body))
        receipt['signature']=base64.urlsafe_b64encode(signature).decode().rstrip('=')
        trusted={receipt['keyId']:{'issuerId':receipt['issuerId'],'publicKeyPem':key.public_key().public_bytes(
            serialization.Encoding.PEM,serialization.PublicFormat.SubjectPublicKeyInfo).decode('ascii')}}
        with self.assertRaisesRegex(ViewerReceiptRejected,'expected_result_manifest_digest_mismatch'):
            self.verify(receipt,trusted=trusted,expected=expected)

    def test_signed_zero_byte_report_requires_integer_size(self):
        signer=SyntheticViewerSigner()
        acceptance={**self.vector['expected'],'acceptedObjectIds':['result-2']}
        receipt=signer.receipt(acceptance,self.vector['resultManifest'])
        self.verify(receipt,trusted=signer.trusted_keys,object_ids=['result-2'])
        for mutation in ('boolean', 'negative', 'extra'):
            with self.subTest(mutation=mutation):
                bad=copy.deepcopy(receipt)
                if mutation=='boolean':bad['objects'][0]['byteSize']=False
                elif mutation=='negative':bad['objects'][0]['byteSize']=-1
                else:bad['objects'][0]['unexpected']='extra'
                body={key:value for key,value in bad.items() if key!='signature'}
                signature=signer._key.sign(SIGNATURE_DOMAIN+reference_protocol.canonical_json(body))
                signer._key.public_key().verify(signature,SIGNATURE_DOMAIN+reference_protocol.canonical_json(body))
                bad['signature']=base64.urlsafe_b64encode(signature).decode().rstrip('=')
                with self.assertRaisesRegex(ViewerReceiptRejected,'invalid_receipt_objects'):
                    self.verify(bad,trusted=signer.trusted_keys,object_ids=['result-2'])

    def test_expected_boolean_generation_is_not_equal_to_integer(self):
        expected=copy.deepcopy(self.vector['expected']);expected['generation']=True
        with self.assertRaisesRegex(ViewerReceiptRejected,'invalid_expected_binding'):
            self.verify(expected=expected)


if __name__ == "__main__":
    unittest.main()
