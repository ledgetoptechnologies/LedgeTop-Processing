"""Cross-language checks for the actual Viewer accepted-copy V2 signer."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest

from processing.viewer_acceptance_receipt import ViewerReceiptRejected
from processing.viewer_copy_receipt_v2 import verify_viewer_copy_receipt_v2


@unittest.skipUnless(os.environ.get("LTDS_VIEWER_WORKTREE"),
                     "set LTDS_VIEWER_WORKTREE to the selected Viewer V2 checkout")
class ViewerCopyReceiptV2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        viewer = Path(os.environ["LTDS_VIEWER_WORKTREE"]).resolve()
        helper = viewer / "apps" / "viewer" / "test" / "helpers" / "accepted-copy-v2-candidate-fixture.js"
        node = os.environ.get("LTDS_NODE") or shutil.which("node")
        if not helper.is_file():
            raise unittest.SkipTest("selected Viewer checkout has no V2 receipt fixture helper")
        if not node:
            raise unittest.SkipTest("Node.js is unavailable")
        result = subprocess.run([node, str(helper)], cwd=viewer / "apps" / "viewer",
            capture_output=True, text=True, timeout=20, check=False)
        if result.returncode != 0:
            raise AssertionError(f"actual Viewer V2 receipt fixture failed: {result.stderr[-3000:]}")
        cls.fixture = json.loads(result.stdout.strip().splitlines()[-1])
        cls.trusted_keys = {key_id: {"issuerId": record["issuerId"],
            "publicKeyPem": record["publicKeyPem"]}
            for key_id, record in cls.fixture["trustedKeys"].items()}

    def verify(self, *, receipt=None, expected=None, acceptance=None, result_manifest=None,
               trusted_keys=None):
        return verify_viewer_copy_receipt_v2(
            self.fixture["viewerReceipt"] if receipt is None else receipt,
            trusted_keys=self.trusted_keys if trusted_keys is None else trusted_keys,
            expected=self.fixture["expected"] if expected is None else expected,
            result_manifest=self.fixture["resultManifest"] if result_manifest is None else result_manifest,
            acceptance=self.fixture["acceptance"] if acceptance is None else acceptance)

    def test_verifies_actual_viewer_javascript_signature_and_full_binding(self):
        verified = self.verify()
        self.assertEqual(verified.receipt, self.fixture["viewerReceipt"])
        self.assertEqual(verified.key_id, "viewer-test-key")
        self.assertEqual(verified.issuer_id, "viewer-test-issuer")
        self.assertEqual(verified.receipt["protocol"], "ltds-viewer-accepted-copy-v2")
        self.assertEqual(verified.receipt["acceptedObjectIds"],
            [item["id"] for item in self.fixture["resultManifest"]["objects"]])

    def test_rejects_changed_server_owned_binding(self):
        expected = copy.deepcopy(self.fixture["expected"])
        expected["sourceReceiptSha256"] = "9" * 64
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_job_binding_mismatch"):
            self.verify(expected=expected)

    def test_rejects_signature_tampering_and_v1_downgrade(self):
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["signature"] = ("A" if receipt["signature"][0] != "A" else "B") + receipt["signature"][1:]
        with self.assertRaisesRegex(ViewerReceiptRejected, "invalid_receipt_signature"):
            self.verify(receipt=receipt)
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["protocol"] = "ltds-viewer-accepted-copy-v1"
        receipt["schemaVersion"] = 1
        with self.assertRaisesRegex(ViewerReceiptRejected, "unsupported_receipt_version"):
            self.verify(receipt=receipt)

    def test_rejects_outer_acceptance_identity_digest_and_object_closure_drift(self):
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["acceptancePayloadSha256"] = "9" * 64
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_acceptance_digest_mismatch"):
            self.verify(receipt=receipt)
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["acceptanceReceiptId"] = "another-acceptance"
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_acceptance_identity_mismatch"):
            self.verify(receipt=receipt)
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["acceptedObjectIds"].reverse()
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_object_binding_mismatch"):
            self.verify(receipt=receipt)

    def test_rejects_receipt_id_reuse_with_outer_acceptance(self):
        receipt = copy.deepcopy(self.fixture["viewerReceipt"])
        receipt["receiptId"] = self.fixture["acceptance"]["receiptId"]
        with self.assertRaisesRegex(ViewerReceiptRejected, "receipt_identity_replay"):
            self.verify(receipt=receipt)

    def test_rejects_unpinned_signer(self):
        with self.assertRaisesRegex(ViewerReceiptRejected, "untrusted_receipt_key"):
            self.verify(trusted_keys={})


if __name__ == "__main__":
    unittest.main()
