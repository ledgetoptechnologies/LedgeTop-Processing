"""Proof verification and optional actual Viewer canonical-signature pairing."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from processing import reference_protocol as p
from processing import viewer_source_attestation as v


class ViewerSourceAttestationTests(unittest.TestCase):
    def setUp(self):
        self.key = Ed25519PrivateKey.generate()
        self.proof = {
            "schemaVersion": 2, "protocol": v.PROTOCOL,
            "issuerId": "viewer-test", "keyId": "source-test", "principalId": "viewer-staging",
            "authorityId": "00000000-0000-4000-8000-000000000001", "environment": "staging",
            "submissionId": "00000000-0000-4000-8000-000000000002",
            "viewerAttemptId": "00000000-0000-4000-8000-000000000002",
            "viewerTaskId": "00000000-0000-4000-8000-000000000003",
            "providerTaskId": "00000000-0000-4000-8000-000000000004",
            "activationId": "00000000-0000-4000-8000-000000000005",
            **{name: str(i) * 64 for i, name in enumerate(("preparedEnvelopeSha256", "sourceSnapshotSha256",
               "sourceManifestSha256", "derivedManifestSha256", "pipelineManifestSha256", "pipelineDigest"))},
        }
        self.records = [{"environment": "staging", "issuerId": "viewer-test", "keyId": "source-test",
            "principalId": "viewer-staging", "publicKeyPem": self.key.public_key().public_bytes(
                serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")}]
        self.expected = dict(authority_id=self.proof["authorityId"], environment="staging",
            submission_id=self.proof["submissionId"], provider_task_id=self.proof["providerTaskId"],
            attempt_id=self.proof["viewerAttemptId"], viewer_task_id=self.proof["viewerTaskId"],
            source_manifest_sha256=self.proof["sourceManifestSha256"],
            derived_manifest_sha256=self.proof["derivedManifestSha256"],
            pipeline_manifest_sha256=self.proof["pipelineManifestSha256"], pipeline_digest=self.proof["pipelineDigest"])

    def signed(self, proof=None, key=None):
        proof = copy.deepcopy(self.proof if proof is None else proof)
        message = v.DOMAIN + p.canonical_json(proof)
        signature = (key or self.key).sign(message)
        return {"proof": proof, "signature": base64.urlsafe_b64encode(signature).decode("ascii").rstrip("="),
                "attestationSha256": hashlib.sha256(message).hexdigest()}

    def verify(self, attestation=None, **changes):
        return v.validate_and_verify(self.signed() if attestation is None else attestation,
            trusted_keys=v.validate_trusted_keys(self.records), **{**self.expected, **changes})

    def test_valid_proof_is_detached_and_exact_replay_is_allowed(self):
        signed = self.signed()
        verified = self.verify(signed)
        self.assertEqual(verified, self.verify(signed))
        verified["proof"]["viewerTaskId"] = "changed"
        self.assertEqual(signed["proof"]["viewerTaskId"], self.expected["viewer_task_id"])

    def test_every_external_binding_is_checked_even_for_valid_signature(self):
        for field in self.expected:
            with self.subTest(field=field), self.assertRaises(v.SourceAttestationError):
                self.verify(**{field: "production" if field == "environment" else "0" * 64})

    def test_signer_is_scoped_to_environment_issuer_key_and_principal(self):
        for field, changed in (("issuerId", "other-viewer"), ("keyId", "other-key"),
                               ("principalId", "other-principal"), ("environment", "production")):
            proof = {**self.proof, field: changed}
            kwargs = {"environment": changed} if field == "environment" else {}
            with self.subTest(field=field), self.assertRaises(v.SourceAttestationError):
                self.verify(self.signed(proof), **kwargs)
        with self.assertRaises(v.SourceAttestationError):
            self.verify(self.signed(key=Ed25519PrivateKey.generate()))

    def test_proof_tampering_and_digest_substitution_are_rejected(self):
        signed = self.signed()
        signed["proof"]["sourceSnapshotSha256"] = "f" * 64
        with self.assertRaises(v.SourceAttestationError):
            self.verify(signed)
        signed["attestationSha256"] = v.proof_digest(signed["proof"])
        with self.assertRaises(v.SourceAttestationError):
            self.verify(signed)

    def test_closed_shapes_versions_and_signature_encoding(self):
        for change in ({"extra": True}, {"schemaVersion": True}, {"protocol": "other"},
                       {"activationId": "not-a-uuid"}, {"sourceSnapshotSha256": "Z" * 64}):
            with self.subTest(change=change), self.assertRaises(v.SourceAttestationError):
                self.verify(self.signed({**self.proof, **change}))
        for signature in ("x" * 85, "=" * 86, self.signed()["signature"] + "==", None):
            with self.subTest(signature=signature), self.assertRaises(v.SourceAttestationError):
                self.verify({**self.signed(), "signature": signature})
        with self.assertRaises(v.SourceAttestationError):
            self.verify({**self.signed(), "extra": True})

    def test_trust_registry_is_closed_bounded_and_detached(self):
        pins = v.validate_trusted_keys(self.records)
        self.records[0]["principalId"] = "changed-after-load"
        self.assertEqual(v.validate_and_verify(self.signed(), trusted_keys=pins, **self.expected)["proof"], self.proof)
        for bad in ([], self.records * 33, self.records * 2,
                    [{**self.records[0], "publicKeyPem": "not a PEM"}],
                    [{**self.records[0], "extra": True}]):
            with self.subTest(kind=type(bad).__name__), self.assertRaises(v.SourceAttestationError):
                v.validate_trusted_keys(bad)

    @unittest.skipUnless(os.environ.get("LTDS_VIEWER_REPO"), "actual Viewer checkout must be explicitly selected")
    def test_actual_viewer_canonicalizer_and_domain_signature_verify(self):
        repo = Path(os.environ["LTDS_VIEWER_REPO"]).resolve()
        script = r'''
const fs = require('node:fs'), path = require('node:path'), crypto = require('node:crypto');
const input = JSON.parse(fs.readFileSync(0, 'utf8'));
const base = path.join(process.argv[1], 'apps/viewer/server');
const {DOMAIN, PROOF_FIELDS} = require(path.join(base, 'preclaimSourceAttestation.js'));
const {canonicalizeJcs} = require(path.join(base, 'preparedSubmissionOutbox.js'));
if (Object.keys(input.proof).sort().join(',') !== [...PROOF_FIELDS].sort().join(',')) throw Error('proof shape');
const message = Buffer.concat([DOMAIN, canonicalizeJcs(input.proof)]);
const key = crypto.createPrivateKey(input.key);
process.stdout.write(JSON.stringify({proof:input.proof,
  signature:crypto.sign(null,message,key).toString('base64url'),
  attestationSha256:crypto.createHash('sha256').update(message).digest('hex')}));
'''
        private = self.key.private_bytes(serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8, serialization.NoEncryption()).decode("ascii")
        result = subprocess.run([os.environ.get("LTDS_NODE", "node"), "-e", script, str(repo)],
            input=json.dumps({"proof": self.proof, "key": private}), text=True,
            capture_output=True, timeout=30, check=True)
        signed = json.loads(result.stdout)
        self.assertEqual(self.verify(signed)["proof"], self.proof)
        self.assertEqual(signed, self.signed())


if __name__ == "__main__":
    unittest.main()
