import copy
import json
from pathlib import Path
import unittest

from processing.viewer_acceptance_receipt import (
    ViewerReceiptRejected,
    verify_viewer_acceptance_receipt,
)


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


if __name__ == "__main__":
    unittest.main()
