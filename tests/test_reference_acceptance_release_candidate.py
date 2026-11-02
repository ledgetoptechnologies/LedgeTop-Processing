"""Synthetic Processing-side bindings for the unagreed Viewer/storage proposal."""
import copy
import json
from pathlib import Path
import unittest

from processing.reference_acceptance_release_candidate import (
    AUTHORIZATION_DOMAIN, CUSTODY_RECEIPT_DOMAIN, SPOOL_CLOSURE_DOMAIN, CandidateRejected,
    canonical_digest, validate_result_acceptance, validate_source_release_pair,
    validate_spool_custody_receipt,
)

ROOT = Path(__file__).resolve().parents[1]
PROPOSAL = json.loads((ROOT / "contracts/reference-v2-candidate/result-acceptance-release-proposal.json").read_text())


def _record(path, kind):
    value = json.loads((ROOT / path).read_text())
    return next(item["value"] for item in value["documents"] if item["type"] == kind)


class ResultAcceptanceReleaseCandidateTests(unittest.TestCase):
    def setUp(self):
        vector = PROPOSAL["releaseVector"]
        self.custody = copy.deepcopy(vector["spoolCustodyReceipt"])
        self.auth = copy.deepcopy(vector["authorization"])
        self.ack = copy.deepcopy(vector["acknowledgement"])
        self.peers = copy.deepcopy(vector["authenticatedContexts"])
        self.expected = dict(
            expected_binding=self.auth["binding"],
            expected_custody_receipt_id=self.auth["sourceCustodyReceiptId"],
            expected_custody_receipt_sha256=canonical_digest(self.custody, CUSTODY_RECEIPT_DOMAIN),
            expected_storage_receipt_id=self.auth["storageReceiptId"],
            expected_spool_root_id=self.auth["spoolRootId"],
            expected_spool_root_generation=self.auth["spoolRootGeneration"],
            expected_source_objects=self.auth["objects"],
            expected_source_closure_sha256=self.auth["sourceClosureSha256"],
            expected_terminal_receipt_id=self.auth["terminalReceiptId"],
            expected_terminal_receipt_sha256=self.auth["terminalReceiptSha256"],
            expected_accepted_copy_ack_ids=self.auth["acceptedCopyAckIds"],
            expected_policy_sha256=self.auth["policySha256"],
            now="2026-11-02T12:01:00.000Z",
        )

    def validate_pair(self, auth=None, ack=None, **overrides):
        kwargs = dict(self.expected)
        kwargs.update(overrides)
        authorization_peer = kwargs.pop("authorization_peer", self.peers["authorizationPeer"])
        acknowledgement_peer = kwargs.pop("acknowledgement_peer", self.peers["acknowledgementPeer"])
        return validate_source_release_pair(
            json.dumps(self.auth if auth is None else auth, separators=(",", ":")).encode(),
            json.dumps(self.ack if ack is None else ack, separators=(",", ":")).encode(),
            authorization_peer=authorization_peer,
            acknowledgement_peer=acknowledgement_peer, **kwargs)

    def test_source_release_pair_binds_exact_inventory_and_external_service_roles(self):
        self.assertEqual(self.auth["sourceCustodyReceiptSha256"],
                         canonical_digest(self.custody, CUSTODY_RECEIPT_DOMAIN))
        self.assertEqual(self.auth["sourceClosureSha256"],
                         canonical_digest(self.auth["objects"], SPOOL_CLOSURE_DOMAIN))
        self.assertEqual(self.ack["authorizationSha256"],
                         canonical_digest(self.auth, AUTHORIZATION_DOMAIN))
        custody_check = validate_spool_custody_receipt(
            json.dumps(self.custody, separators=(",", ":")).encode(),
            authenticated_peer={"role": "sourceStorageExecutor", "principal": self.custody["issuerId"]},
            expected_binding=self.auth["binding"], expected_spool_root_id=self.auth["spoolRootId"],
            expected_spool_root_generation=self.auth["spoolRootGeneration"],
            expected_source_objects=self.auth["objects"],
            expected_custody_receipt_sha256=self.expected["expected_custody_receipt_sha256"],
            expected_writer_fence_receipt_id="writer-fence-1",
            expected_capacity_reservation_receipt_id="capacity-reservation-1",
            expected_reserved_bytes=12, now="2026-11-02T12:00:00.000Z")
        self.assertEqual(custody_check.record_type, "ProcessingSpoolCustodyReceipt")
        checked = self.validate_pair()
        self.assertEqual((checked.record_type, checked.operation_id, checked.receipt_ids),
                         ("SourceReleaseAck", "release-op-1", ("release-ack-1", "release-auth-1")))
        with self.assertRaises(TypeError):
            bool(checked)

    def test_release_authentication_must_come_from_the_expected_transport_roles(self):
        for kwargs in (
            {"authorization_peer": {"role": "sourceStorageExecutor", "principal": "viewer-retention-service"}},
            {"acknowledgement_peer": {"role": "sourceReleaseAuthorizer", "principal": "nas-spool-executor"}},
            {"authorization_peer": {"role": "sourceReleaseAuthorizer", "principal": "forged"}},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(CandidateRejected):
                self.validate_pair(**kwargs)

    def test_release_rejects_stale_job_storage_terminal_and_policy_bindings(self):
        for field, value in (
            ("expected_binding", {**self.auth["binding"], "generation": 2}),
            ("expected_custody_receipt_id", "other-custody"),
            ("expected_storage_receipt_id", "other-storage"),
            ("expected_spool_root_generation", 8),
            ("expected_terminal_receipt_sha256", "0" * 64),
            ("expected_accepted_copy_ack_ids", ["other-ack"]),
            ("expected_policy_sha256", "0" * 64),
        ):
            with self.subTest(field=field), self.assertRaises(CandidateRejected):
                self.validate_pair(**{field: value})

    def test_release_rejects_wrong_or_reordered_source_and_deleted_inventory(self):
        changed_auth = copy.deepcopy(self.auth)
        changed_auth["objects"][0]["sha256"] = "0" * 64
        reordered_auth = copy.deepcopy(self.auth)
        reordered_auth["objects"].append({
            "objectId": "source-2", "physicalObjectId": "physical-2",
            "objectGeneration": 1, "byteSize": 3, "sha256": "1" * 64,
            "custodyReceiptId": "object-custody-2",
        })
        reordered_auth["objects"].reverse()
        for changed in (changed_auth,
                        reordered_auth):
            with self.subTest(changed=changed), self.assertRaises(CandidateRejected):
                self.validate_pair(auth=changed)
        changed_ack = copy.deepcopy(self.ack)
        changed_ack["deletedObjects"][0]["physicalObjectId"] = "another-file"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed_ack)
        changed_ack = copy.deepcopy(self.ack)
        changed_ack["deletedObjects"][0]["deletionReceiptId"] = ""
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed_ack)

    def test_release_rejects_ack_operation_digest_and_binding_replay_mismatch(self):
        for field, value in (
            ("operationId", "other-operation"),
            ("authorizationId", "other-authorization"),
            ("authorizationSha256", "0" * 64),
            ("sourceClosureSha256", "0" * 64),
        ):
            changed = copy.deepcopy(self.ack)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(CandidateRejected):
                self.validate_pair(ack=changed)
        changed = copy.deepcopy(self.ack)
        changed["binding"]["environment"] = "staging"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed)

    def test_v1_and_unmarked_records_cannot_downgrade_or_cross_the_v2_profile(self):
        profiles = (
            ("authorization", self.auth, self.validate_pair, "auth"),
            ("acknowledgement", self.ack, self.validate_pair, "ack"),
        )
        changes = (
            ("foreign protocol", lambda record: record.update(protocol="ltds-reference-v1")),
            ("missing protocol", lambda record: record.pop("protocol")),
            ("v1 schema", lambda record: record.update(schemaVersion=1)),
            ("missing schema", lambda record: record.pop("schemaVersion")),
            ("v1 marker", lambda record: record.update(receiptType="source_release_ack")),
        )
        for name, original, validator, argument in profiles:
            for label, mutate in changes:
                changed = copy.deepcopy(original)
                mutate(changed)
                with self.subTest(profile=name, case=label), self.assertRaises(CandidateRejected):
                    validator(**{argument: changed})

        for label, mutate in changes:
            changed = copy.deepcopy(self.custody)
            mutate(changed)
            with self.subTest(profile="custody", case=label), self.assertRaises(CandidateRejected):
                validate_spool_custody_receipt(
                    json.dumps(changed, separators=(",", ":")).encode(),
                    authenticated_peer={"role": "sourceStorageExecutor", "principal": changed.get("issuerId", "")},
                    expected_binding=self.auth["binding"],
                    expected_spool_root_id=self.auth["spoolRootId"],
                    expected_spool_root_generation=self.auth["spoolRootGeneration"],
                    expected_source_objects=self.auth["objects"],
                    expected_custody_receipt_sha256=self.expected["expected_custody_receipt_sha256"],
                    expected_writer_fence_receipt_id="writer-fence-1",
                    expected_capacity_reservation_receipt_id="capacity-reservation-1",
                    expected_reserved_bytes=12, now="2026-11-02T12:00:00.000Z")

    def test_retention_guarantee_is_specific_to_the_v2_release_ack(self):
        changed = copy.deepcopy(self.custody)
        changed["retentionGuarantee"] = "until_explicit_source_release_ack_v1"
        with self.assertRaises(CandidateRejected):
            validate_spool_custody_receipt(
                json.dumps(changed, separators=(",", ":")).encode(),
                authenticated_peer={"role": "sourceStorageExecutor", "principal": changed["issuerId"]},
                expected_binding=self.auth["binding"],
                expected_spool_root_id=self.auth["spoolRootId"],
                expected_spool_root_generation=self.auth["spoolRootGeneration"],
                expected_source_objects=self.auth["objects"],
                expected_custody_receipt_sha256=canonical_digest(changed, CUSTODY_RECEIPT_DOMAIN),
                expected_writer_fence_receipt_id="writer-fence-1",
                expected_capacity_reservation_receipt_id="capacity-reservation-1",
                expected_reserved_bytes=12, now="2026-11-02T12:00:00.000Z")

    def test_expired_authority_or_late_physical_effect_cannot_start_or_complete_release(self):
        late = "2026-11-02T12:06:00.000Z"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(now=late)
        with self.assertRaises(CandidateRejected):
            self.validate_pair(now="2026-11-02T12:05:00.000Z")
        self.validate_pair(now="2026-11-02T12:04:59.999Z")
        changed = copy.deepcopy(self.ack)
        changed["deletedAt"] = "2026-11-02T12:05:01.000Z"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed, now="2026-11-02T12:05:30.000Z")
        changed = copy.deepcopy(self.ack)
        changed["deletedAt"] = "2026-11-02T12:05:00.000Z"
        changed["acknowledgedAt"] = "2026-11-02T12:05:00.000Z"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed, now="2026-11-02T12:05:00.000Z")
        changed = copy.deepcopy(self.ack)
        changed["acknowledgedAt"] = "2026-11-02T12:05:01.000Z"
        with self.assertRaises(CandidateRejected):
            self.validate_pair(ack=changed, now="2026-11-02T12:05:30.000Z")

    def test_release_requires_two_distinct_principals_and_bounded_strict_json(self):
        with self.assertRaises(CandidateRejected):
            self.validate_pair(acknowledgement_peer={
                "role": "sourceStorageExecutor", "principal": "viewer-retention-service"})
        raw = b'{"schemaVersion":1,"schemaVersion":1}'
        with self.assertRaises(CandidateRejected):
            validate_source_release_pair(
                raw, b"{}", authorization_peer=self.peers["authorizationPeer"],
                acknowledgement_peer=self.peers["acknowledgementPeer"], **self.expected)
        with self.assertRaises(CandidateRejected):
            validate_source_release_pair(b" " * (4 * 1024 * 1024 + 1), b"{}",
                authorization_peer=self.peers["authorizationPeer"],
                acknowledgement_peer=self.peers["acknowledgementPeer"], **self.expected)
        bad_unicode = copy.deepcopy(self.custody)
        bad_unicode["receiptId"] = "bad\ud800"
        with self.assertRaises(CandidateRejected):
            validate_spool_custody_receipt(
                json.dumps(bad_unicode, ensure_ascii=True).encode(),
                authenticated_peer={"role": "sourceStorageExecutor", "principal": self.custody["issuerId"]},
                expected_binding=self.auth["binding"], expected_spool_root_id=self.auth["spoolRootId"],
                expected_spool_root_generation=self.auth["spoolRootGeneration"],
                expected_source_objects=self.auth["objects"],
                expected_custody_receipt_sha256=self.expected["expected_custody_receipt_sha256"],
                expected_writer_fence_receipt_id="writer-fence-1",
                expected_capacity_reservation_receipt_id="capacity-reservation-1",
                expected_reserved_bytes=12, now="2026-11-02T12:00:00.000Z")

    def test_result_ack_is_bound_to_authenticated_viewer_and_expected_manifest_inventory(self):
        acceptance = _record("contracts/reference-v2-candidate/valid/odm_only.json", "PartialAcceptance")
        ack = _record("contracts/reference-v2-candidate/valid/odm_only.json", "AcceptedCopyAck")
        projection = ("id", "sha256", "byteSize", "stageId", "workerId", "reservationId",
                      "producerCustodyReceiptId")
        expected_artifacts = [{key: item[key] for key in projection}
                              for item in acceptance["acceptedArtifacts"]]
        kwargs = dict(
            authenticated_issuer_id=ack["issuerId"], authenticated_role="resultAcceptanceIssuer",
            expected_binding=acceptance["binding"], expected_result_set_id=acceptance["resultSetId"],
            expected_result_manifest_sha256=acceptance["resultManifestSha256"],
            expected_artifacts=expected_artifacts,
        )
        checked = validate_result_acceptance(
            json.dumps(acceptance, separators=(",", ":")).encode(),
            json.dumps(ack, separators=(",", ":")).encode(), **kwargs)
        self.assertEqual(checked.record_type, "AcceptedCopyAck")
        with self.assertRaises(TypeError):
            bool(checked)
        with self.assertRaises(CandidateRejected):
            validate_result_acceptance(
                json.dumps(acceptance).encode(), json.dumps(ack).encode(),
                **{**kwargs, "authenticated_issuer_id": "forged-viewer"})
        with self.assertRaises(CandidateRejected):
            validate_result_acceptance(
                json.dumps(acceptance).encode(), json.dumps(ack).encode(),
                **{**kwargs, "expected_artifacts": [{**expected_artifacts[0], "sha256": "0" * 64}]})


if __name__ == "__main__":
    unittest.main()
