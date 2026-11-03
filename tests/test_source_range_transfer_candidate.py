import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from unittest.mock import patch

from processing.source_range_transfer import (
    CHUNK_BYTES, OBJECT_PREFIX, REQUEST_HEADER, SourceIdentity, SourceObject,
    SourceRangeClient, TransferError, _request_id, _validator, build_source_transfer_request,
    source_version_for_manifest, validate_source_grant,
)
from processing import reference_protocol as p


class Response(io.BytesIO):
    def __init__(self, body, *, url, headers, status=206):
        super().__init__(body)
        self.url, self.headers, self.status = url, headers, status

    def geturl(self):
        return self.url


class SourceRangeTransferCandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.body = b"a" * CHUNK_BYTES + b"b" * CHUNK_BYTES + b"xyz"
        self.obj = SourceObject("photo-1", len(self.body), hashlib.sha256(self.body).hexdigest())
        self.manifest = {"id": "dataset-fixture-1", "revision": 1, "totalBytes": self.obj.byte_size,
            "objectCount": 1, "inputCounts": {"images": 1, "lidarObjects": 0, "points": 0},
            "objects": [{"id": self.obj.id, "name": "photo-1.jpg", "byteSize": self.obj.byte_size,
                         "sha256": self.obj.sha256, "kind": "image", "mediaType": "image/jpeg"}]}
        self.manifest["sha256"] = p.manifest_sha256(self.manifest)
        self.source_version = source_version_for_manifest(self.manifest)
        self.identity = SourceIdentity("staging", "job-1", "reservation-1", 1,
                                        self.manifest["sha256"], self.source_version)
        self.job = {"jobId": "job-1", "generation": 1, "environment": "staging",
                    "manifestSha256": self.manifest["sha256"],
                    "submission": {"manifest": self.manifest}}
        self.transfer = {"protocol": "source-range-v2", "reservationId": "reservation-1",
            "generation": 1, "expiresAt": "2036-01-01T00:00:00Z", "manifestSha256": self.manifest["sha256"],
            "sourceVersion": self.source_version, "objectIds": [self.obj.id], "chunkBytes": CHUNK_BYTES,
            "requestBudget": 3, "byteBudget": len(self.body), "attemptBudget": 5, "nonce": "nonce-1"}
        self.job["transferRequest"] = self.transfer
        self.calls = []
        self.now = 100

    def raw_grant(self, token="test-token", **updates):
        raw = {**self.transfer, "expiresAt": "2035-01-01T00:00:00Z",
               "jobId": "job-1", "environment": "staging", "token": token}
        raw.update(updates)
        self.job["transferRequest"] = dict(self.transfer)
        return raw

    def grant(self, token="test-token", **updates):
        return validate_source_grant(self.raw_grant(token, **updates), job=self.job, clock=lambda: 100)

    def test_grant_may_expire_at_reservation_deadline(self):
        grant = self.grant(expiresAt=self.transfer["expiresAt"])
        self.assertEqual(grant.expires, 2082758400.0)

    def test_grant_must_expire_before_it_is_already_stale(self):
        with self.assertRaisesRegex(TransferError, "source grant expired"):
            self.grant(expiresAt="1970-01-01T00:01:40Z")

    def make_client(self, opener=None):
        return SourceRangeClient("https://broker.example", self.root / "progress.sqlite",
                                 opener=opener or self.opener, clock=lambda: self.now)

    def opener(self, request, *, timeout):
        self.assertTrue(request.full_url.startswith("https://broker.example" + OBJECT_PREFIX))
        start, end = map(int, request.get_header("Range")[6:].split("-"))
        self.calls.append((request, start, end))
        body = self.body[start:end + 1]
        headers = {
            "Content-Range": f"bytes {start}-{end}/{len(self.body)}",
            "Content-Length": str(len(body)), "ETag": _validator(self.obj),
            REQUEST_HEADER: request.get_header("X-processing-request-id"), "Content-Encoding": "identity",
        }
        return Response(body, url=request.full_url, headers=headers)

    def test_range_replay_after_restart_and_rotated_token_charges_once(self):
        failed_request_ids = []

        def fail_second(request, *, timeout):
            start = int(request.get_header("Range")[6:].split("-")[0])
            if start == CHUNK_BYTES:
                failed_request_ids.append(request.get_header("X-processing-request-id"))
                raise OSError("synthetic disconnect")
            return self.opener(request, timeout=timeout)

        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant("grant-one"),
                object_id=self.obj.id, destination=self.root / "photo.bin")
        # Both unique intents consume one request and their bytes; replaying the pending second range is free.
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            state = db.execute("SELECT committed_bytes FROM source_objects").fetchone()[0]
            chunks = db.execute("SELECT start,length,state FROM source_ranges ORDER BY start").fetchall()
            budget = db.execute("SELECT requests_used,bytes_used,attempts_used FROM source_budgets").fetchone()
        self.assertEqual(state, CHUNK_BYTES)
        self.assertEqual(chunks, [(0, CHUNK_BYTES, "committed"), (CHUNK_BYTES, CHUNK_BYTES, "intent")])
        self.assertEqual(budget, (2, CHUNK_BYTES * 2, 2))

        renewed = self.grant("rotated-grant", expiresAt="2035-06-01T00:00:00Z")
        result = self.make_client().download_object(grant=renewed,
            object_id=self.obj.id, destination=self.root / "photo.bin")
        self.assertEqual(result.read_bytes(), self.body)
        self.assertEqual(self.calls[0][1:], (0, CHUNK_BYTES - 1))
        request_id_for_second = _request_id(json.dumps(self.identity.canonical(), sort_keys=True,
            separators=(",", ":")), self.obj, CHUNK_BYTES, CHUNK_BYTES)
        second = [call for call in self.calls if call[1] == CHUNK_BYTES]
        self.assertEqual(len(second), 1)
        self.assertEqual(failed_request_ids[0], second[0][0].get_header("X-processing-request-id"))
        self.assertEqual(second[0][0].get_header("X-processing-request-id"), request_id_for_second)
        self.assertEqual(second[0][0].get_header("Authorization"), "Bearer rotated-grant")
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            state = db.execute("SELECT committed_bytes,published FROM source_objects").fetchone()
            charged = db.execute("SELECT requests_used,bytes_used,attempts_used FROM source_budgets").fetchone()
        self.assertEqual(state, (len(self.body), 1))
        self.assertEqual(charged, (3, len(self.body), 4))

    def test_uncommitted_tail_is_truncated_before_retry(self):
        def fail_second(request, *, timeout):
            start = int(request.get_header("Range")[6:].split("-")[0])
            if start == CHUNK_BYTES:
                raise OSError("disconnect")
            return self.opener(request, timeout=timeout)
        target = self.root / "photo.bin"
        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant(), object_id=self.obj.id,
                                                          destination=target)
        partial = next(self.root.glob(".source-range-*.part"))
        with partial.open("ab") as stream:
            stream.write(b"uncommitted")
        self.make_client().download_object(grant=self.grant("grant-2"), object_id=self.obj.id,
                                           destination=target)
        self.assertEqual(target.read_bytes(), self.body)
        self.assertFalse(partial.exists())

    def test_network_attempt_budget_caps_pending_range_replays(self):
        self.transfer["attemptBudget"] = 3
        def fail_second(request, *, timeout):
            if request.get_header("Range").startswith(f"bytes={CHUNK_BYTES}-"):
                raise OSError("synthetic first retry failure")
            return self.opener(request, timeout=timeout)
        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant(), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=self.grant("rotated"), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            used = db.execute("SELECT attempts_used,attempt_budget FROM source_budgets").fetchone()
        self.assertEqual(used, (3, 3))

    def test_network_attempt_budget_caps_pending_range_replays(self):
        self.transfer["attemptBudget"] = 3
        def fail_second(request, *, timeout):
            if request.get_header("Range").startswith(f"bytes={CHUNK_BYTES}-"):
                raise OSError("synthetic first retry failure")
            return self.opener(request, timeout=timeout)
        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant(), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=self.grant("rotated"), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            used = db.execute("SELECT attempts_used,attempt_budget FROM source_budgets").fetchone()
        self.assertEqual(used, (3, 3))

    def test_committed_corruption_or_missing_partial_holds(self):
        def fail_second(request, *, timeout):
            if request.get_header("Range").startswith(f"bytes={CHUNK_BYTES}-"):
                raise OSError("disconnect")
            return self.opener(request, timeout=timeout)
        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant(), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        partial = next(self.root.glob(".source-range-*.part"))
        partial.write_bytes(b"xxxx")
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=self.grant("grant-2"), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        partial.unlink()
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=self.grant("grant-2"), object_id=self.obj.id,
                destination=self.root / "photo.bin")

    def test_deterministic_partial_hardlink_is_rejected(self):
        client = self.make_client()
        key, _ = client._identity_key(self.identity)
        part = self.root / (".source-range-" + key[:24] + "-" + hashlib.sha256(self.obj.id.encode()).hexdigest()[:24] + ".part")
        other = self.root / "unrelated.bin"
        other.write_bytes(b"unrelated")
        try:
            os.link(other, part)
        except OSError:
            self.skipTest("hardlink creation unavailable")
        with self.assertRaises(TransferError):
            client.download_object(grant=self.grant(), object_id=self.obj.id,
                                   destination=self.root / "photo.bin")
        self.assertEqual(other.read_bytes(), b"unrelated")
        self.assertEqual(part.stat().st_nlink, 2)

    def test_partial_replacement_with_same_bytes_is_rejected_by_inode_binding(self):
        def fail_second(request, *, timeout):
            if request.get_header("Range").startswith(f"bytes={CHUNK_BYTES}-"):
                raise OSError("disconnect")
            return self.opener(request, timeout=timeout)
        with self.assertRaises(TransferError):
            self.make_client(fail_second).download_object(grant=self.grant(), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        part = next(self.root.glob(".source-range-*.part"))
        data = part.read_bytes()
        replacement = self.root / "replacement.part"
        replacement.write_bytes(data)
        part.unlink()
        replacement.replace(part)
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=self.grant("rotated"), object_id=self.obj.id,
                destination=self.root / "photo.bin")

    def test_write_before_checkpoint_crash_truncates_then_replays_same_intent(self):
        client = self.make_client()
        original_commit = client._commit_range
        failed = False

        def fail_after_second_write(key, object_id, request_id, start, length, digest):
            nonlocal failed
            if start == CHUNK_BYTES and not failed:
                failed = True
                raise OSError("simulated crash before checkpoint")
            return original_commit(key, object_id, request_id, start, length, digest)

        client._commit_range = fail_after_second_write
        with self.assertRaises(TransferError):
            client.download_object(grant=self.grant(), object_id=self.obj.id,
                                   destination=self.root / "photo.bin")
        part = next(self.root.glob(".source-range-*.part"))
        self.assertEqual(part.read_bytes(), self.body[:CHUNK_BYTES * 2])
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            self.assertEqual(db.execute("SELECT committed_bytes FROM source_objects").fetchone()[0], CHUNK_BYTES)
        retried = self.make_client()
        retried.download_object(grant=self.grant("rotated"), object_id=self.obj.id,
                                destination=self.root / "photo.bin")
        self.assertEqual((self.root / "photo.bin").read_bytes(), self.body)
        # The interrupted chunk reuses its intent ID and consumes budget only once.
        second_range = [call for call in self.calls if call[1] == CHUNK_BYTES]
        self.assertEqual(len(second_range), 2)
        self.assertEqual(second_range[0][0].get_header("X-processing-request-id"),
                         second_range[1][0].get_header("X-processing-request-id"))

    def test_publish_before_partial_unlink_crash_is_cleaned_on_retry(self):
        target = self.root / "photo.bin"
        original_unlink = Path.unlink
        interrupted = False

        def fail_after_publish(path, *args, **kwargs):
            nonlocal interrupted
            if path.name.endswith(".part") and not interrupted:
                interrupted = True
                raise OSError("simulated crash after publish")
            return original_unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", fail_after_publish):
            with self.assertRaises(TransferError):
                self.make_client().download_object(grant=self.grant(), object_id=self.obj.id,
                                                    destination=target)
        partial = next(self.root.glob(".source-range-*.part"))
        self.assertEqual(target.read_bytes(), self.body)
        self.assertEqual(os.stat(partial).st_ino, os.stat(target).st_ino)

        result = self.make_client().download_object(grant=self.grant("rotated"), object_id=self.obj.id,
                                                    destination=target)
        self.assertEqual(result.read_bytes(), self.body)
        self.assertFalse(partial.exists())
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            self.assertEqual(db.execute("SELECT published FROM source_objects").fetchone()[0], 1)

    def test_response_must_match_range_validator_and_request_echo(self):
        def bad_response(request, *, timeout):
            response = self.opener(request, timeout=timeout)
            response.headers["Content-Range"] = f"bytes 1-4/{len(self.body)}"
            return response
        with self.assertRaises(TransferError):
            self.make_client(bad_response).download_object(grant=self.grant(), object_id=self.obj.id,
                destination=self.root / "photo.bin")
        with closing(sqlite3.connect(self.root / "progress.sqlite")) as db:
            self.assertEqual(db.execute("SELECT committed_bytes FROM source_objects").fetchone()[0], 0)

    def test_deterministic_ids_bind_identity_and_reject_noncanonical_overlap(self):
        identity_json = json.dumps(self.identity.canonical(), sort_keys=True, separators=(",", ":"))
        first = _request_id(identity_json, self.obj, 0, CHUNK_BYTES)
        self.assertEqual(first, _request_id(identity_json, self.obj, 0, CHUNK_BYTES))
        changed = SourceIdentity("staging", "job-1", "reservation-1", 1,
            self.identity.manifest_sha256, "source-rev-2")
        changed_json = json.dumps(changed.canonical(), sort_keys=True, separators=(",", ":"))
        self.assertNotEqual(first, _request_id(changed_json, self.obj, 0, CHUNK_BYTES))
        client = self.make_client()
        key, encoded = client._identity_key(self.identity)
        grant = self.grant()
        client._ensure_state(key, encoded, self.obj, self.root / "partial", grant)
        client._record_intent(key, encoded, self.obj, grant, first, 0, CHUNK_BYTES)
        with self.assertRaises(TransferError):
            client._record_intent(key, encoded, self.obj, grant, first, 0, CHUNK_BYTES - 1)
        with self.assertRaises(TransferError):
            client._record_intent(key, encoded, self.obj, grant,
                _request_id(encoded, self.obj, 1, CHUNK_BYTES), 1, CHUNK_BYTES)

    def test_noncanonical_chunk_size_rejected(self):
        client = self.make_client()
        with self.assertRaises(TransferError):
            client.download_object(grant="raw-bearer", object_id=self.obj.id,
                                   destination=self.root / "never.bin")
        with self.assertRaises(TransferError):
            self.grant(requestBudget=2)
        with self.assertRaises(TransferError):
            self.grant(byteBudget=len(self.body) - 1)

    def test_expiry_is_checked_before_every_range_request(self):
        grant = self.grant()
        def expire_after_first(request, *, timeout):
            response = self.opener(request, timeout=timeout)
            if request.get_header("Range").startswith("bytes=0-"):
                self.now = grant.expires
            return response
        with self.assertRaises(TransferError):
            self.make_client(expire_after_first).download_object(grant=grant, object_id=self.obj.id,
                destination=self.root / "expired.bin")
        self.assertEqual(len(self.calls), 1)

    def test_origin_rejects_whitespace_controls_and_backslashes(self):
        for origin in ("https://broker.example/path", "https://broker.example\\evil",
                       "https://broker.example/\n", "https://broker.example\t", "https:// broker.example"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                SourceRangeClient(origin, self.root / "invalid.sqlite")

    def test_grant_is_bound_to_v2_transfer_envelope(self):
        for field in ("sourceVersion", "nonce", "expiresAt", "byteBudget"):
            raw = self.raw_grant()
            transfer_value = self.job["transferRequest"][field]
            self.job["transferRequest"][field] = "mismatch" if isinstance(transfer_value, str) else transfer_value + 1
            with self.subTest(field=field), self.assertRaises(TransferError):
                validate_source_grant(raw, job=self.job, clock=lambda: 100)
            self.job["transferRequest"][field] = transfer_value

    def test_source_version_is_derived_from_immutable_dataset_manifest_identity(self):
        self.assertEqual(self.source_version,
            f"dataset:fixture-1:{self.manifest['sha256']}")
        raw = self.raw_grant()
        forged = "dataset:fixture-1:" + "0" * 64
        self.job["transferRequest"]["sourceVersion"] = forged
        raw["sourceVersion"] = forged
        with self.assertRaises(TransferError):
            validate_source_grant(raw, job=self.job, clock=lambda: 100)
        for bad in ({"id": "manifest-1", "sha256": "a" * 64},
                    {"id": "dataset-1", "sha256": "not-a-hash"}):
            with self.subTest(bad=bad), self.assertRaises(TransferError):
                source_version_for_manifest(bad)

    def test_reservation_builder_emits_closed_bounded_v2_identity(self):
        job = {"jobId": "job-1", "generation": 4, "environment": "production",
            "manifestSha256": self.manifest['sha256'], "submission": {"manifest": self.manifest}}
        request = build_source_transfer_request(job, reservation_id='reserve-v2',
            expires_at='2035-01-01T00:00:00.000Z', nonce='nonce-v2')
        self.assertEqual(request, {"protocol": "source-range-v2", "reservationId": "reserve-v2",
            "generation": 4, "expiresAt": "2035-01-01T00:00:00.000Z",
            "manifestSha256": self.manifest['sha256'], "sourceVersion": self.source_version,
            "objectIds": [self.obj.id], "chunkBytes": CHUNK_BYTES,
            "requestBudget": 3, "byteBudget": len(self.body) * 3,
            "nonce": "nonce-v2", "attemptBudget": 9})
        for invalid_expiry in ('2035-01-01T00:00:00Z', '2035-99-01T00:00:00.000Z', '2020-01-01T00:00:00.000Z'):
            with self.subTest(expiry=invalid_expiry), self.assertRaises(TransferError):
                build_source_transfer_request(job, reservation_id='reserve-v2',
                    expires_at=invalid_expiry, nonce='nonce-v2')

    def test_second_process_transfer_is_rejected_while_object_lock_is_held(self):
        entered, release = threading.Event(), threading.Event()
        grant = self.grant()
        errors = []
        def blocking_opener(request, *, timeout):
            entered.set()
            if not release.wait(5):
                raise OSError("test timeout")
            return self.opener(request, timeout=timeout)
        client = self.make_client(blocking_opener)
        worker = threading.Thread(target=lambda: self._download_capture(client, grant, errors))
        worker.start()
        self.assertTrue(entered.wait(5))
        with self.assertRaises(TransferError):
            self.make_client().download_object(grant=grant, object_id=self.obj.id,
                destination=self.root / "photo.bin")
        release.set()
        worker.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual((self.root / "photo.bin").read_bytes(), self.body)

    def _download_capture(self, client, grant, errors):
        try:
            client.download_object(grant=grant, object_id=self.obj.id,
                                   destination=self.root / "photo.bin")
        except Exception as error:
            errors.append(error)


if __name__ == "__main__":
    unittest.main()
