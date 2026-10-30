import copy
import io
import json
import os
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import patch
from urllib.request import ProxyHandler, build_opener

from processing import reference_protocol as p
from processing.reference_sender import (
    MAX_RECEIPT_BYTES, RESULTS_PATH, ReferenceSender, SenderError, decode_receipt,
)
from processing.worker_outbox import BINDING, WorkerOutbox


class Response(io.BytesIO):
    def __init__(self, body, *, url, status=200, headers=None):
        super().__init__(body)
        self.url, self.status = url, status
        self.headers = {"Content-Type": "application/json"} if headers is None else headers

    def geturl(self):
        return self.url


class ReferenceSenderTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.now = 100
        self.outbox = self.open_outbox()
        fixture = json.loads((Path(__file__).resolve().parents[1] /
            "contracts/reference-v1.contract.json").read_text())
        self.manifest = fixture["resultManifest"]
        self.outbox_id = self.outbox.persist_terminal(job_id=self.manifest["jobId"], generation=1,
            environment="staging", input_manifest_sha256="a"*64,
            pipeline_digest=self.manifest["pipelineDigest"],
            result_manifest_sha256=self.manifest["resultManifestSha256"], result_manifest=self.manifest,
            owned_prefix="jobs/reservation-1", local_objects={obj["id"]:
                "jobs/reservation-1/" + obj["id"] + ".bin" for obj in self.manifest["objects"]})
        self.requests, self.bindings = [], []
        self.bearer, self.claim_token = "private-configured-bearer", "private-original-queue-claim"
        self.url = "https://coordinator.example" + RESULTS_PATH

    def open_outbox(self):
        return WorkerOutbox(self.root / "worker.sqlite", clock=lambda: self.now,
                            random_source=lambda: 0, retry_base_seconds=5)

    def provider(self, binding):
        self.bindings.append(binding)
        return {"claimRequestId": "original-claim-1", "claimToken": self.claim_token}

    def receipt(self, envelope):
        payload = envelope["payload"]
        return dict(type="durable-result-receipt", durable=True, receiptId="receipt-1",
            storageReceiptId="storage-1", outboxId=envelope["outboxId"],
            payloadSha256=envelope["payloadSha256"],
            acceptedObjectIds=[obj["id"] for obj in payload["resultManifest"]["objects"]],
            **{key: payload[key] for key in BINDING})

    def sender(self, *, mutate_receipt=None, raw_body=None, **response_options):
        def opener(request, *, timeout):
            self.requests.append(request)
            envelope = json.loads(request.data)["delivery"]
            receipt = self.receipt(envelope)
            if mutate_receipt:
                mutate_receipt(receipt)
            body = p.canonical_json(receipt) if raw_body is None else raw_body
            return Response(body, url=request.full_url, **response_options)
        return ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                               claim_binding_provider=self.provider, opener=opener)

    def assert_retained(self):
        row = self.outbox.get(self.outbox_id)
        self.assertIsNotNone(row["payload"])
        self.assertEqual(row["local_metadata"]["ownedPrefix"], "jobs/reservation-1")
        return row

    def test_default_internal_sender_ignores_proxy_environment(self):
        with patch.dict(os.environ, {"HTTPS_PROXY": "http://127.0.0.1:9",
                                     "https_proxy": "http://127.0.0.1:9",
                                     "NO_PROXY": "", "no_proxy": ""}):
            inherited = build_opener()
            sender = ReferenceSender(self.outbox, "https://coordinator.example",
                bearer_token=self.bearer, claim_binding_provider=self.provider)
        inherited_handlers = [handler for handler in inherited.handlers if isinstance(handler, ProxyHandler)]
        self.assertTrue(inherited_handlers)
        self.assertEqual(inherited_handlers[0].proxies.get("https"), "http://127.0.0.1:9")
        self.assertFalse(any(isinstance(handler, ProxyHandler) for handler in sender._open.__self__.handlers))

    def test_exact_contract_and_verified_receipt_acknowledge_without_deleting_metadata(self):
        sender = self.sender()
        result = sender.send_next()
        self.assertEqual(result, {"state": "acknowledged", "outboxId": self.outbox_id})
        row = self.assert_retained()
        self.assertEqual(row["state"], "acknowledged")
        request = self.requests[0]
        self.assertEqual(request.full_url, self.url)
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Authorization"), "Bearer " + self.bearer)
        body = json.loads(request.data)
        self.assertEqual(set(body), {"claimRequestId", "claimToken", "delivery"})
        self.assertEqual(body["claimToken"], self.claim_token)
        self.assertEqual(set(body["delivery"]), {"outboxId", "payloadSha256", "payload"})
        self.assertNotIn("jobs/reservation", request.data.decode())
        self.assertEqual(set(self.bindings[0]), set(BINDING))
        self.assertNotIn(self.bearer, repr(sender))
        self.assertNotIn(self.claim_token, repr(sender))
        self.assertEqual(sender.send_next(), {"state": "idle"})
        self.assertEqual(len(self.requests), 1)

    def test_bad_receipts_back_off_and_remain_replayable(self):
        mutations = (lambda r: r.update(durable=False), lambda r: r.pop("receiptId"),
                     lambda r: r.update(environment="production"),
                     lambda r: r.update(acceptedObjectIds=["result-1"]),
                     lambda r: r.update(extra="unknown"), lambda r: r.update(payloadSha256="0"*64))
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                result = self.sender(mutate_receipt=mutation).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                row = self.assert_retained()
                self.assertEqual(row["state"], "pending")
                self.assertIsNone(row["receipt"])
                self.assertGreater(row["next_at"], self.now)
                self.now = row["next_at"]
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")

    def test_duplicate_nonfinite_malformed_and_wrapped_json_never_acknowledge(self):
        for raw in (b'{"durable":true,"durable":false}', b'{"x":NaN}',
                    b'{"x":Infinity}', b'{"x":1e999}', b'{"x":{"a":1,"a":2}}',
                    b'[]', b'{} trailing', b'\xff', b'{"receipt":{}}'):
            with self.subTest(raw=raw):
                result = self.sender(raw_body=raw).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                self.now = self.assert_retained()["next_at"]
        self.assertIsNone(self.outbox.get(self.outbox_id)["receipt"])

    def test_non200_encoded_or_untyped_receipt_does_not_acknowledge(self):
        options = ({"status": 202}, {"status": 302}, {"status": 500}, {"headers": {}},
                   {"headers": {"Content-Type": "text/plain"}},
                   {"headers": {"Content-Type": "application/json", "Content-Encoding": "gzip"}},
                   {"headers": {"Content-Type": "application/json", "Content-Length": "9999"}})
        for option in options:
            with self.subTest(option=option):
                result = self.sender(**option).send_next()
                self.assertEqual(result["state"], "retry_scheduled")
                self.now = self.assert_retained()["next_at"]

    def test_response_byte_limit_is_enforced_even_without_content_length(self):
        result = self.sender(raw_body=b" " * (MAX_RECEIPT_BYTES + 1)).send_next()
        self.assertEqual(result["state"], "retry_scheduled")
        self.assertIsNone(self.assert_retained()["receipt"])
        with self.assertRaises(SenderError):
            decode_receipt(b" " * (MAX_RECEIPT_BYTES + 1))

    def test_transport_errors_cannot_expose_credentials_in_return_value(self):
        def broken(request, **kwargs):
            raise RuntimeError(self.bearer + self.claim_token)
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=broken)
        result = sender.send_next()
        self.assertEqual(result["state"], "retry_scheduled")
        for secret in (self.bearer, self.claim_token):
            self.assertNotIn(secret, repr(result))
            self.assertNotIn(secret, repr(sender))
        self.assert_retained()

    def test_lost_response_and_worker_restart_resend_exact_delivery(self):
        deliveries = []
        def lost(request, **kwargs):
            deliveries.append(json.loads(request.data)["delivery"])
            raise OSError("response lost")
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=lost)
        sender.send_next()
        self.now = self.outbox.get(self.outbox_id)["next_at"]
        self.outbox = self.open_outbox()
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")
        self.assertEqual(deliveries[0], json.loads(self.requests[0].data)["delivery"])
        self.assertEqual(self.outbox.get(self.outbox_id)["attempts"], 2)

    def test_expired_attempt_is_retained_until_reclaim_and_late_receipt_can_ack(self):
        def slow_failure(request, **kwargs):
            self.now += 61
            raise OSError("timeout")
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=slow_failure)
        self.assertEqual(sender.send_next()["state"], "retry_wait")
        self.assertEqual(self.assert_retained()["state"], "sending")
        self.assertEqual(self.sender().send_next()["state"], "acknowledged")

    def test_claim_provider_cannot_add_urls_or_invalid_credentials(self):
        for claim in ({"claimRequestId": "original", "claimToken": "secret", "url": "https://other.example"},
                      {"claimRequestId": "original", "claimToken": "bad\nsecret"}):
            def provider(binding):
                return claim
            sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                claim_binding_provider=provider, opener=lambda *a, **k: self.fail("must not open"))
            self.assertEqual(sender.send_next()["state"], "retry_scheduled")
            self.now = self.assert_retained()["next_at"]

    def test_default_opener_refuses_redirect_without_forwarding_authorization(self):
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider)
        handler = next(h for h in sender._open.__self__.handlers if hasattr(h, "redirect_request"))
        from urllib.request import Request
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code), self.assertRaises(SenderError):
                handler.redirect_request(Request(self.url), None, code, "redirect", {}, "https://other.example")

    def test_changed_final_url_is_not_trusted(self):
        def opener(request, **kwargs):
            receipt = self.receipt(json.loads(request.data)["delivery"])
            return Response(p.canonical_json(receipt), url="https://other.example" + RESULTS_PATH)
        sender = ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
            claim_binding_provider=self.provider, opener=opener)
        self.assertEqual(sender.send_next()["state"], "retry_scheduled")
        self.assertIsNone(self.assert_retained()["receipt"])

    def test_configuration_rejects_arbitrary_paths_credentials_and_insecure_tls(self):
        for origin in ("http://coordinator.example", "https://user:pass@coordinator.example",
                       "https://coordinator.example/custom", "https://coordinator.example?url=other",
                       "https://coordinator.example#fragment", "https://coordinator.example:invalid"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                ReferenceSender(self.outbox, origin, bearer_token=self.bearer,
                                claim_binding_provider=self.provider)
        for token in ("", "x"*4097, "secret\nheader", "non ASCII β"):
            with self.subTest(token_length=len(token)), self.assertRaises(ValueError):
                ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=token,
                                claim_binding_provider=self.provider)
        with self.assertRaises(ValueError):
            ReferenceSender(self.outbox, "https://coordinator.example", bearer_token=self.bearer,
                claim_binding_provider=self.provider, ssl_context=ssl._create_unverified_context())


if __name__ == "__main__":
    unittest.main()
