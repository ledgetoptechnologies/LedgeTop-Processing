import copy
import hashlib
import io
import json
from pathlib import Path
import ssl
import unittest

from processing import reference_protocol as p
from processing.reference_result_proxy import (
    CHUNK_BYTES, RESULT_PREFIX, ResultProxy, ResultProxyError,
)


class Lease:
    def __init__(self):
        self.entered, self.exited = False, False

    def __enter__(self):
        self.entered = True
        return self

    def __exit__(self, *_):
        self.exited = True


class Response(io.BytesIO):
    def __init__(self, body, *, url, headers, status=200):
        super().__init__(body)
        self.url, self.headers, self.status = url, headers, status
        self.read_sizes = []

    def geturl(self):
        return self.url

    def read(self, size):
        self.read_sizes.append(size)
        return super().read(size)


class ReferenceResultProxyTests(unittest.TestCase):
    def setUp(self):
        fixture = json.loads((Path(__file__).resolve().parents[1] /
            "contracts/reference-v1.contract.json").read_text())
        self.manifest = fixture["resultManifest"]
        self.manifest["objects"][0]["role"] = "odm_archive"
        self.reseal()
        self.token = "synthetic-separate-worker-read-token"
        self.requests, self.admissions, self.responses, self.leases = [], [], [], []
        self.origin = "https://worker.example"

    def reseal(self):
        self.manifest["resultManifestSha256"] = p.digest({k: v for k, v in self.manifest.items()
                                                         if k != "resultManifestSha256"})
        self.job = dict(jobId=self.manifest["jobId"], generation=self.manifest["generation"],
            environment=self.manifest["environment"], pipelineDigest=self.manifest["pipelineDigest"],
            resultManifestSha256=self.manifest["resultManifestSha256"], nodeId="cluster")

    def admit(self, binding):
        self.admissions.append(binding)
        lease = Lease()
        self.leases.append(lease)
        return lease

    def proxy(self, body=b"abc", *, status=200, header_updates=None, final_url=None):
        def opener(request, **kwargs):
            self.assertTrue(self.leases[-1].entered)
            self.requests.append(request)
            object_id = request.full_url.rsplit("/", 1)[1]
            obj = next(obj for obj in self.manifest["objects"] if obj["id"] == object_id)
            headers = {"Content-Length": str(obj["byteSize"]), "X-Result-Sha256": obj["sha256"],
                "X-Result-Manifest-Sha256": self.manifest["resultManifestSha256"],
                "Content-Type": "application/zip" if obj["role"] == "odm_archive" else "application/octet-stream"}
            headers.update(header_updates or {})
            response = Response(body, url=final_url or request.full_url, status=status, headers=headers)
            self.responses.append(response)
            return response
        return ResultProxy({"cluster": self.origin}, worker_bearers={"cluster": self.token},
                           admit_read=self.admit, opener=opener)

    def test_exact_origin_auth_binding_zip_headers_and_verified_close(self):
        proxy = self.proxy()
        self.assertNotIn(self.token, repr(proxy))
        with proxy.open(self.job, self.manifest, "result-1") as stream:
            self.assertFalse(stream.verified)
            self.assertEqual(stream.status, 200)
            self.assertEqual(stream.headers["Content-Type"], "application/zip")
            self.assertEqual(b"".join(stream), b"abc")
            self.assertTrue(stream.complete)
            self.assertTrue(stream.verified)
        self.assertTrue(self.responses[0].closed)
        self.assertTrue(self.leases[0].exited)
        request = self.requests[0]
        self.assertEqual(request.full_url, self.origin + RESULT_PREFIX + "job-1/results/result-1")
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.get_header("Authorization"), "Bearer " + self.token)
        self.assertEqual(request.get_header("X-result-generation"), "1")
        self.assertEqual(request.get_header("X-result-sha256"), self.manifest["objects"][0]["sha256"])
        self.assertEqual(request.get_header("X-result-manifest-sha256"), self.manifest["resultManifestSha256"])
        self.assertEqual(self.admissions[0]["byteSize"], 3)
        self.assertEqual(self.admissions[0]["bufferBytes"], CHUNK_BYTES)
        self.assertNotIn(self.token, repr(stream.headers))

    def test_multichunk_memory_bound_and_final_hash(self):
        body = b"x" * (CHUNK_BYTES*3 + 7)
        obj = self.manifest["objects"][0]
        obj.update(byteSize=len(body), sha256=hashlib.sha256(body).hexdigest())
        self.reseal()
        with self.proxy(body).open(self.job, self.manifest, "result-1") as stream:
            chunks = list(stream)
            self.assertEqual(b"".join(chunks), body)
            self.assertTrue(all(len(chunk) <= CHUNK_BYTES for chunk in chunks))
            self.assertTrue(stream.verified)
        self.assertTrue(all(0 < size <= CHUNK_BYTES for size in self.responses[0].read_sizes))

    def test_exact_byte_range_is_forwarded_and_validated_for_retryable_result_reads(self):
        proxy = self.proxy(b"bc", status=206, header_updates={
            "Content-Length": "2", "Content-Range": "bytes 1-2/3"})
        with proxy.open(self.job, self.manifest, "result-1", range_header="bytes=1-2") as stream:
            self.assertEqual(stream.status, 206)
            self.assertEqual(stream.headers["Content-Range"], "bytes 1-2/3")
            self.assertEqual(b"".join(stream), b"bc")
            self.assertTrue(stream.complete)
            self.assertFalse(stream.verified)
        self.assertEqual(self.requests[0].get_header("Range"), "bytes=1-2")
        self.assertEqual(self.admissions[0]["byteSize"], 2)
        self.assertTrue(self.leases[0].exited)

    def test_malformed_or_mismatched_ranges_fail_closed(self):
        for value in ("bytes=-1", "bytes=0-1,2-2", "bytes=2-1", "bytes=0-3"):
            with self.subTest(range=value), self.assertRaises(ResultProxyError):
                self.proxy().open(self.job, self.manifest, "result-1", range_header=value)
        with self.assertRaises(ResultProxyError):
            self.proxy(b"bc", status=206, header_updates={
                "Content-Length": "2", "Content-Range": "bytes 0-1/3"}).open(
                    self.job, self.manifest, "result-1", range_header="bytes=1-2")
        self.assertTrue(self.leases[-1].exited)

    def test_corrupt_truncated_and_extra_bytes_do_not_return_last_chunk(self):
        for body in (b"bad", b"ab", b"abcd"):
            with self.subTest(body=body):
                with self.proxy(body).open(self.job, self.manifest, "result-1") as stream:
                    with self.assertRaises(ResultProxyError):
                        list(stream)
                    self.assertFalse(stream.verified)
                    self.assertTrue(stream.closed)
                self.assertTrue(self.responses[-1].closed)
                self.assertTrue(self.leases[-1].exited)

    def test_corrupt_large_stream_withholds_final_chunk(self):
        expected = b"x" * (CHUNK_BYTES*2 + 7)
        obj = self.manifest["objects"][0]
        obj.update(byteSize=len(expected), sha256=hashlib.sha256(expected).hexdigest())
        self.reseal()
        observed = []
        with self.proxy(b"y"*len(expected)).open(self.job, self.manifest, "result-1") as stream:
            with self.assertRaises(ResultProxyError):
                for chunk in stream:
                    observed.append(chunk)
            self.assertFalse(stream.verified)
        self.assertEqual(sum(map(len, observed)), CHUNK_BYTES*2)

    def test_client_abort_closes_upstream_pin_and_capacity_lease(self):
        with self.proxy().open(self.job, self.manifest, "result-1") as stream:
            self.assertEqual(stream.read(1), b"a")
            self.assertFalse(stream.verified)
        self.assertTrue(stream.closed)
        self.assertTrue(self.responses[0].closed)
        self.assertTrue(self.leases[0].exited)
        with self.assertRaises(ResultProxyError):
            stream.read(1)

    def test_empty_report_verifies_empty_digest(self):
        with self.proxy(b"").open(self.job, self.manifest, "result-2") as stream:
            self.assertEqual(b"".join(stream), b"")
            self.assertTrue(stream.verified)
            self.assertEqual(stream.headers["Content-Type"], "application/octet-stream")

    def test_missing_content_length_still_enforces_exact_object_size(self):
        proxy = self.proxy(header_updates={"Content-Length": None})
        with proxy.open(self.job, self.manifest, "result-1") as stream:
            self.assertEqual(b"".join(stream), b"abc")
            self.assertTrue(stream.verified)

    def test_bad_response_headers_status_or_final_url_close_every_resource(self):
        options = ({"header_updates": {"Content-Length": "4"}},
                   {"header_updates": {"X-Result-Sha256": "0"*64}},
                   {"header_updates": {"X-Result-Manifest-Sha256": "0"*64}},
                   {"header_updates": {"Content-Type": "application/octet-stream"}},
                   {"header_updates": {"Content-Encoding": "gzip"}},
                   {"status": 206}, {"status": 302}, {"final_url": "https://other.example/result"})
        for option in options:
            with self.subTest(option=option), self.assertRaises(ResultProxyError):
                self.proxy(**option).open(self.job, self.manifest, "result-1")
            self.assertTrue(self.responses[-1].closed)
            self.assertTrue(self.leases[-1].exited)

    def test_unknown_node_object_or_unsealed_manifest_never_admit_or_connect(self):
        for job, object_id in ((dict(self.job, nodeId="unconfigured"), "result-1"),
                               (self.job, "outside"),
                               (dict(self.job, resultManifestSha256="0"*64), "result-1")):
            with self.subTest(job=job, object_id=object_id), self.assertRaises(ResultProxyError):
                self.proxy().open(job, self.manifest, object_id)
        self.assertEqual(self.requests, [])
        self.assertEqual(self.admissions, [])

    def test_denied_capacity_prevents_worker_connection(self):
        def deny(binding):
            raise RuntimeError("capacity unavailable")
        proxy = ResultProxy({"cluster": self.origin}, worker_bearers={"cluster": self.token},
            admit_read=deny, opener=lambda *a, **k: self.fail("request before capacity admission"))
        with self.assertRaises(ResultProxyError):
            proxy.open(self.job, self.manifest, "result-1")

    def test_transport_exception_is_secret_free_and_releases_admission(self):
        def broken(request, **kwargs):
            raise RuntimeError(self.token)
        proxy = ResultProxy({"cluster": self.origin}, worker_bearers={"cluster": self.token},
            admit_read=self.admit, opener=broken)
        with self.assertRaises(ResultProxyError) as caught:
            proxy.open(self.job, self.manifest, "result-1")
        self.assertNotIn(self.token, repr(caught.exception))
        self.assertTrue(self.leases[0].exited)

    def test_stream_transport_exception_is_secret_free_and_releases_resources(self):
        proxy = self.proxy()
        stream = proxy.open(self.job, self.manifest, "result-1")
        def broken(size):
            raise RuntimeError(self.token)
        self.responses[0].read = broken
        with self.assertRaises(ResultProxyError) as caught:
            stream.read()
        self.assertNotIn(self.token, repr(caught.exception))
        self.assertTrue(self.responses[0].closed)
        self.assertTrue(self.leases[0].exited)
        self.assertFalse(stream.verified)

    def test_redirect_handler_never_creates_followup_request(self):
        proxy = ResultProxy({"cluster": self.origin}, worker_bearers={"cluster": self.token}, admit_read=self.admit)
        handler = next(h for h in proxy._open.__self__.handlers if hasattr(h, "redirect_request"))
        from urllib.request import Request
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status), self.assertRaises(ResultProxyError):
                handler.redirect_request(Request(self.origin), None, status, "redirect", {}, "https://other.example")

    def test_config_rejects_caller_urls_credentials_and_unverified_tls(self):
        for origin in ("http://worker.example", "https://user:secret@worker.example", "https://worker.example/path",
                       "https://worker.example?url=other", "https://worker.example#x", "https://worker.example:bad"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                ResultProxy({"cluster": origin}, worker_bearers={"cluster": self.token}, admit_read=self.admit)
        for tokens in ({"other": self.token}, {"cluster": "secret\nheader"}):
            with self.assertRaises(ValueError):
                ResultProxy({"cluster": self.origin}, worker_bearers=tokens, admit_read=self.admit)
        with self.assertRaises(ValueError):
            ResultProxy({"cluster": self.origin}, worker_bearers={"cluster": self.token}, admit_read=self.admit,
                        ssl_context=ssl._create_unverified_context())


if __name__ == "__main__":
    unittest.main()
