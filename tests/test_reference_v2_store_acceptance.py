"""Store/API acceptance pairing for authenticated source-intake V2 jobs."""
from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as protocol
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_store import PROFILE, ReferenceStore
from processing.source_spool import SourceSpool
from tests.viewer_receipt_signer import SyntheticViewerSigner


VIEWER_BEARER = "synthetic-staging-token-for-acceptance"


def _private_pem(key):
    return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption()).decode("ascii")


def _public_pem(key):
    return key.public_key().public_bytes(serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def _sign_with_actual_viewer(viewer_root, *, expected, acceptance, result_manifest,
                             private_key_pem, issuer_id, key_id):
    script = r'''
const fs=require('node:fs'),path=require('node:path');
const input=JSON.parse(fs.readFileSync(0,'utf8'));
const {createViewerReceiptV2SignerCandidate}=require(path.join(input.viewerRoot,
  'apps/viewer/server/referenceReceiptV2Candidate.js'));
const ids=[input.receiptId,input.storageReceiptId];
const signer=createViewerReceiptV2SignerCandidate({issuerId:input.issuerId,keyId:input.keyId,
  privateKeyPem:input.privateKeyPem,clock:()=>new Date('2026-11-04T00:00:01.000Z'),
  idFactory:()=>ids.shift()});
process.stdout.write(JSON.stringify(signer({expected:input.expected,acceptance:input.acceptance,
  resultManifest:input.resultManifest})));
'''
    node = os.environ.get("LTDS_NODE") or shutil.which("node")
    if not node:
        raise unittest.SkipTest("Node.js is unavailable for actual Viewer signer pairing")
    data = {"viewerRoot": str(viewer_root), "expected": expected, "acceptance": acceptance,
        "resultManifest": result_manifest, "privateKeyPem": private_key_pem,
        "issuerId": issuer_id, "keyId": key_id,
        "receiptId": "copy-receipt-test-1", "storageReceiptId": "copy-storage-test-1"}
    completed = subprocess.run([node, "-e", script], input=json.dumps(data), text=True,
        capture_output=True, timeout=20, check=False)
    if completed.returncode:
        raise AssertionError(f"actual Viewer V2 signer failed: {completed.stderr[-3000:]}")
    return json.loads(completed.stdout)


class ReferenceV2StoreAcceptanceTests(unittest.TestCase):
    def _make_source_result_ready(self, directory, viewer_root):
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests
        source_case = ReferenceSourceIntakeTests("runTest")
        source_case.setUp()
        spool_root = Path(directory) / "spool"
        spool_root.mkdir(mode=0o700)
        marker = spool_root / ".ltds-source-spool-id"
        marker.write_text("a" * 64, encoding="ascii")
        if os.name != "nt":
            marker.chmod(0o600)
        mount_patch = patch("processing.source_spool.os.path.ismount", return_value=True)
        mount_patch.start()
        queue = None
        try:
            spool = SourceSpool(spool_root, mount_id="a" * 64, safety_floor_bytes=0,
                max_object_bytes=1024, chunk_bytes=3)
            queue, store, _control, job, claim_token = source_case._make_reserved_source_job(
                directory, spool, "acceptance-v2")
            store.mint_source_transfer_grant(job["jobId"], source_spool=spool)
            store.update_phase(job["jobId"], "fetching")
            store.update_phase(job["jobId"], "processing")

            fixture = json.loads((Path(__file__).resolve().parents[1] /
                "contracts/viewer-accepted-copy-receipt-v1.json").read_text(encoding="utf-8"))
            result = copy.deepcopy(fixture["resultManifest"])
            result.update(jobId=job["jobId"], generation=job["generation"],
                environment=job["environment"], pipelineDigest=job["pipelineDigest"])
            result["resultManifestSha256"] = protocol.digest({key: value for key, value in result.items()
                if key != "resultManifestSha256"})
            claim_request_id = "source-claim-acceptance-v2"
            store.record_worker_result(job["jobId"], result, claim_token=claim_token,
                node_id="cluster", worker_id="worker", worker_instance_id="instance",
                claim_request_id=claim_request_id)
            store.update_phase(job["jobId"], "awaiting_acceptance")
            # Model the real worker terminal-result boundary. This keeps Queue
            # from correctly converting an unfinished running job to held on reopen.
            queue.complete(job["jobId"], {"claimToken": claim_token, "status": "succeeded",
                "result": {"resultManifestSha256": result["resultManifestSha256"]}})
            terminal_job = store.get(job["jobId"])
            self.assertEqual((terminal_job["status"], terminal_job["phase"]),
                ("succeeded", "awaiting_acceptance"))
            acceptance = store.ensure_viewer_transfer(job["jobId"], accepted_at="2026-11-04T00:00:02.000Z")
            leased_transfer = store.claim_viewer_transfer(now=1_800_000_000)
            self.assertIsNotNone(leased_transfer)
            self.assertEqual(leased_transfer["jobId"], job["jobId"])
            self.assertEqual(leased_transfer["acceptance"], acceptance)
            private = store.get(job["jobId"], private=True)
            expected = {**private["sourceAcceptanceBinding"],
                "resultManifestSha256": leased_transfer["viewerResultManifest"]["resultManifestSha256"]}
            key = Ed25519PrivateKey.generate()
            trusted_keys = {"synthetic-copy-key": {"issuerId": "synthetic-copy-issuer",
                "publicKeyPem": _public_pem(key)}}
            api = ReferenceAPI(store, bearer_tokens={VIEWER_BEARER: "staging"},
                capability_fingerprint="a" * 64,
                options_fingerprint=source_case.payload["pipeline"]["odm"]["optionsFingerprint"],
                allowed_odm_options={"numeric-probe"}, source_spool=spool,
                source_attestation_keys=source_case.trusted_keys, authority_id=queue.authority_id,
                viewer_receipt_keys=trusted_keys)
            state = {"source_case": source_case, "spool": spool, "mount_patch": mount_patch,
                "queue": queue, "store": store, "api": api, "job": job,
                "claim_token": claim_token, "result": result, "acceptance": acceptance,
                "viewer_result_manifest": leased_transfer["viewerResultManifest"],
                "expected": expected, "key": key, "trusted_keys": trusted_keys,
                "viewer_root": viewer_root}
            return state
        except Exception:
            mount_patch.stop()
            if queue is not None:
                queue.close()
            raise

    def _signed(self, state, *, expected=None, acceptance=None):
        return _sign_with_actual_viewer(state["viewer_root"],
            expected=state["expected"] if expected is None else expected,
            acceptance=state["acceptance"] if acceptance is None else acceptance,
            result_manifest=state["viewer_result_manifest"],
            private_key_pem=_private_pem(state["key"]),
            issuer_id="synthetic-copy-issuer", key_id="synthetic-copy-key")

    def _post_acceptance(self, state, acceptance, viewer_receipt=None):
        payload = acceptance if viewer_receipt is None else {
            "acceptance": acceptance, "viewerReceipt": viewer_receipt}
        return state["api"].handle("POST",
            f"/ltds/v1/jobs/{state['job']['jobId']}/acceptance",
            {"Authorization": f"Bearer {VIEWER_BEARER}"},
            protocol.canonical_json(payload), protected_transport=True)

    def _close_state(self, state):
        queue = state.get("queue")
        if queue is not None:
            queue.close()
            state["queue"] = None
        state["mount_patch"].stop()
        state["source_case"].doCleanups()

    @unittest.skipUnless(os.environ.get("LTDS_VIEWER_WORKTREE"),
                         "set LTDS_VIEWER_WORKTREE to the exact actual Viewer V2 checkout")
    def test_v2_acceptance_requires_signed_copy_receipt_and_replays_after_reopen(self):
        viewer_root = Path(os.environ["LTDS_VIEWER_WORKTREE"]).resolve()
        with tempfile.TemporaryDirectory(prefix="ltds-store-copy-v2-") as directory:
            state = self._make_source_result_ready(directory, viewer_root)
            try:
                original_acceptance = state["acceptance"]
                # Bare acceptance and an authentic V1 signed receipt are both
                # insufficient for a V2 source-intake result.
                bare = self._post_acceptance(state, original_acceptance)
                self.assertEqual(bare.status, 409, bare.body)
                v1_signer = SyntheticViewerSigner()
                v1_receipt = v1_signer.receipt(original_acceptance, state["viewer_result_manifest"])
                v1_signed = self._post_acceptance(state, original_acceptance, v1_receipt)
                self.assertEqual(v1_signed.status, 409, v1_signed.body)

                changed_source = copy.deepcopy(state["expected"])
                changed_source["sourceReceiptSha256"] = "9" * 64
                response = self._post_acceptance(state, original_acceptance,
                    self._signed(state, expected=changed_source))
                self.assertEqual(response.status, 409, response.body)
                changed_task = copy.deepcopy(state["expected"])
                changed_task["viewerTaskId"] = "99999999-9999-4999-8999-999999999999"
                response = self._post_acceptance(state, original_acceptance,
                    self._signed(state, expected=changed_task))
                self.assertEqual(response.status, 409, response.body)
                changed_outer = copy.deepcopy(original_acceptance)
                changed_outer["receiptId"] = "another-outer-acceptance"
                changed_outer["storageReceiptId"] = "another-outer-storage"
                response = self._post_acceptance(state, original_acceptance,
                    self._signed(state, acceptance=changed_outer))
                self.assertEqual(response.status, 409, response.body)
                with state["queue"]._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_acceptances").fetchone()[0], 0)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_viewer_receipts").fetchone()[0], 0)

                receipt = self._signed(state)
                first = self._post_acceptance(state, original_acceptance, receipt)
                self.assertEqual(first.status, 200, first.body)
                self.assertTrue(first.body["fullyAccepted"])
                self.assertEqual(first.body["viewerReceipt"], receipt)
                self.assertEqual(first.body["job"]["phase"], "completed")
                first_ack = {key: first.body[key] for key in
                    ("fullyAccepted", "acceptedObjectIds", "viewerReceipt")}
                self.assertEqual(first.body["acceptedObjectIds"], original_acceptance["acceptedObjectIds"])
                with state["queue"]._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_acceptances").fetchone()[0], 1)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_viewer_receipts").fetchone()[0], 1)

                # The shared queue claim was already completed above. Reopen must
                # preserve a terminal job and the exact durable receipt identities.
                old_queue = state["queue"]
                old_queue.close()
                state["queue"] = None
                queue = Queue(directory)
                queue.allowed_profiles.add(PROFILE)
                store = ReferenceStore(queue)
                self.assertEqual(store.reconcile_source_spool(state["spool"]),
                    {"jobs": 1, "adopted": 0, "held": 0})
                state["queue"] = queue
                state["store"] = store
                state["api"] = ReferenceAPI(store, bearer_tokens={VIEWER_BEARER: "staging"},
                    capability_fingerprint="a" * 64,
                    options_fingerprint=state["source_case"].payload["pipeline"]["odm"]["optionsFingerprint"],
                    allowed_odm_options={"numeric-probe"}, source_spool=state["spool"],
                    source_attestation_keys=state["source_case"].trusted_keys,
                    authority_id=queue.authority_id, viewer_receipt_keys=state["trusted_keys"])
                second = self._post_acceptance(state, original_acceptance, receipt)
                self.assertEqual(second.status, 200, second.body)
                self.assertEqual(second.body["viewerReceipt"], receipt)
                second_ack = {key: second.body[key] for key in
                    ("fullyAccepted", "acceptedObjectIds", "viewerReceipt")}
                self.assertEqual(second_ack, first_ack)
                self.assertFalse(second.body["created"])
                self.assertEqual(second.body["job"]["phase"], "completed")
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_acceptances").fetchone()[0], 1)
                    self.assertEqual(db.execute("SELECT COUNT(*) FROM reference_viewer_receipts").fetchone()[0], 1)
                    viewer_ids = db.execute("SELECT receipt_id,storage_receipt_id FROM reference_viewer_receipts").fetchone()
                    self.assertEqual(tuple(viewer_ids), (receipt["receiptId"], receipt["storageReceiptId"]))

            finally:
                self._close_state(state)

    def test_v1_job_keeps_bare_acceptance_behavior(self):
        from tests.test_reference_store import ReferenceStoreTests
        viewer_case = ReferenceStoreTests("runTest")
        viewer_case.setUp()
        try:
            job, result = viewer_case.result_ready()
            receipt = viewer_case.receipt(job, result)
            receipt["acceptedObjectIds"] = [item["id"] for item in result["objects"]]
            api = ReferenceAPI(viewer_case.store,
                bearer_tokens={VIEWER_BEARER: "staging"},
                capability_fingerprint=viewer_case.fixture["capabilities"]["capabilityFingerprint"],
                options_fingerprint=viewer_case.submission["pipeline"]["odm"]["optionsFingerprint"])
            response = api.handle("POST", f"/ltds/v1/jobs/{job['jobId']}/acceptance",
                {"Authorization": f"Bearer {VIEWER_BEARER}"}, protocol.canonical_json(receipt),
                protected_transport=True)
            self.assertEqual(response.status, 200, response.body)
            self.assertTrue(response.body["fullyAccepted"])
            self.assertEqual(response.body["job"]["phase"], "completed")
            self.assertNotIn("viewerReceipt", response.body)
        finally:
            viewer_case.tearDown()


if __name__ == "__main__":
    unittest.main()
