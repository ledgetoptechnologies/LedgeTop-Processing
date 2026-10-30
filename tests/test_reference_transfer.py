import copy
import hashlib
import io
import json
import os
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from processing import reference_protocol as p
from processing.reference_transfer import (
    BrokerClient, OBJECT_PREFIX, TransferError, TransferResponseUnavailable, TransferObject, ValidatedGrant, validate_grant,
)


class Response(io.BytesIO):
    def __init__(self, body=b"abc", *, status=200, url=None, headers=None):
        super().__init__(body)
        self.status = status
        self.url = url
        self.headers = headers or {}

    def geturl(self):
        return self.url


class ReferenceTransferTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              "contracts/reference-v1.contract.json").read_text())
        self.raw = fixture["transferGrant"]
        self.job = dict(jobId="job-1", generation=1, environment="staging",
            manifestSha256=fixture["submission"]["manifest"]["sha256"],
            submission=fixture["submission"], transferRequest=fixture["transferRequest"])
        self.now = 100
        self.calls = []

    def grant(self):
        return validate_grant(self.raw, job=self.job, clock=lambda: self.now)

    def client(self, body=b"abc", **response_options):
        def opener(request, *, timeout):
            self.calls.append(request)
            return Response(body, url=request.full_url, **response_options)
        return BrokerClient("https://broker.example", opener=opener, clock=lambda: self.now)

    def assert_no_files(self):
        self.assertEqual(list(self.root.iterdir()), [])

    def test_valid_grant_detaches_objects_and_redacts_token(self):
        grant = self.grant()
        self.assertNotIn(self.raw["token"], repr(grant))
        self.assertEqual(grant.objects[0].id, "image-1")
        self.raw["objectIds"].clear()
        self.job["submission"]["manifest"]["objects"][0]["sha256"] = "0" * 64
        self.assertEqual(grant.objects[0].sha256, hashlib.sha256(b"abc").hexdigest())

    def test_retry_allowance_is_bounded_separately_from_final_input_bytes(self):
        for budget in (3, 4, 9):
            self.raw['byteBudget'] = self.job['transferRequest']['byteBudget'] = budget
            self.assertEqual(sum(o.byte_size for o in self.grant().objects), 3)
        for budget in (2, 10, p.MAX_SAFE_INTEGER):
            self.raw['byteBudget'] = self.job['transferRequest']['byteBudget'] = budget
            with self.assertRaises(TransferError):
                self.grant()

    def test_every_grant_identity_field_is_bound(self):
        mutations = dict(reservationId="wrong", generation=2,
            expiresAt="2031-01-01T00:00:00Z", manifestSha256="0"*64,
            byteBudget=4, nonce="wrong", jobId="other", environment="production",
            objectIds=[], token="bad\nsecret")
        for field, value in mutations.items():
            with self.subTest(field=field):
                grant = copy.deepcopy(self.raw)
                grant[field] = value
                with self.assertRaises(ValueError):
                    validate_grant(grant, job=self.job, clock=lambda: self.now)
        for key in ("extra", "url"):
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_grant(dict(self.raw, **{key: "https://elsewhere.example"}), job=self.job)

    def test_duplicate_or_superset_object_ids_rejected(self):
        for ids in (["image-1", "image-1"], ["image-1", "other"], "image-1"):
            with self.subTest(ids=ids), self.assertRaises(ValueError):
                validate_grant(dict(self.raw, objectIds=ids), job=self.job)

    def test_token_is_bounded_printable_ascii(self):
        for token in ("", "x"*4097, "hello world", "β", None):
            with self.subTest(token_type=type(token)), self.assertRaises(ValueError):
                validate_grant(dict(self.raw, token=token), job=self.job)

    def test_expired_grant_never_opens_connection(self):
        grant = self.grant()
        self.now = grant.expires
        with self.assertRaises(TransferError):
            self.client().download_object(grant, "image-1", self.root / "input.jpg")
        self.assertEqual(self.calls, [])
        with self.assertRaises(TransferError):
            self.grant()
        self.assert_no_files()

    def test_fixed_origin_route_and_verified_private_publication(self):
        target = self.root / "input.jpg"
        result = self.client(headers={"Content-Length": "3"}).download_object(self.grant(), "image-1", target)
        self.assertEqual(result, target)
        self.assertEqual(target.read_bytes(), b"abc")
        self.assertEqual(list(self.root.iterdir()), [target])
        self.assertEqual(len(self.calls), 1)
        request = self.calls[0]
        self.assertEqual(request.full_url, "https://broker.example" + OBJECT_PREFIX + "image-1")
        self.assertEqual(request.get_header("Authorization"), "Bearer " + self.raw["token"])
        if os.name != "nt":
            self.assertEqual(target.stat().st_mode & 0o777, 0o600)

    def ranged_fixture(self, payload=b'abcdefghij', *, mutate=None):
        grant=ValidatedGrant('job-1','reservation-1',1,'staging',1000,
            (TransferObject('image-1',len(payload),hashlib.sha256(payload).hexdigest()),),'private-token')
        calls=[]
        def opener(request, *, timeout):
            value=request.get_header('Range');calls.append(value)
            start,end=map(int,value.removeprefix('bytes=').split('-'))
            data=payload[start:end+1]
            options={'status':206,'url':request.full_url,'headers':{'Content-Length':str(len(data)),'Content-Range':f'bytes {start}-{end}/{len(payload)}'}}
            if mutate is not None:data,options=mutate(len(calls),data,options)
            return Response(data,**options)
        return grant,calls,BrokerClient('https://broker.example',opener=opener,clock=lambda:self.now,max_response_bytes=4,enable_ranges=True)

    def test_ranges_are_disabled_by_default(self):
        grant,calls,_=self.ranged_fixture()
        def opener(request,*,timeout):
            calls.append(request.get_header('Range'));return Response(b'abcdefghij',url=request.full_url,headers={'Content-Length':'10'})
        client=BrokerClient('https://broker.example',opener=opener,clock=lambda:self.now,max_response_bytes=4)
        self.assertEqual(client.download_object(grant,'image-1',self.root/'input.jpg').read_bytes(),b'abcdefghij')
        self.assertEqual(calls,[None])

    def test_ranged_object_validates_and_publishes_one_complete_file(self):
        grant,calls,client=self.ranged_fixture()
        path=client.download_object(grant,'image-1',self.root/'input.jpg')
        self.assertEqual(path.read_bytes(),b'abcdefghij')
        self.assertEqual(calls,['bytes=0-3','bytes=4-7','bytes=8-9'])
        self.assertEqual(list(self.root.iterdir()),[path])

    def test_bad_range_headers_status_redirect_encoding_and_hash_never_publish(self):
        def changed(case):
            def mutate(n,data,options):
                if n==2:
                    if case=='range':options['headers']['Content-Range']='bytes 0-3/10'
                    if case=='length':options['headers']['Content-Length']='3'
                    if case=='missing':del options['headers']['Content-Length']
                    if case=='status':options['status']=200
                    if case=='redirect':options['url']='https://elsewhere.example/'
                    if case=='encoding':options['headers']['Content-Encoding']='gzip'
                    if case=='short':data=data[:-1]
                    if case=='corrupt':data=b'zzzz'
                return data,options
            return mutate
        for case in ('range','length','missing','status','redirect','encoding','short','corrupt'):
            with self.subTest(case=case):
                grant,calls,client=self.ranged_fixture(mutate=changed(case))
                with self.assertRaises(TransferError):client.download_object(grant,'image-1',self.root/'input.jpg')
                self.assert_no_files()

    def test_lost_later_range_keeps_no_partial_and_retry_starts_from_zero(self):
        def fail_second(n,data,options):
            if n==2:raise URLError('synthetic unavailable')
            return data,options
        grant,calls,client=self.ranged_fixture(mutate=fail_second)
        with self.assertRaises(TransferResponseUnavailable):client.download_object(grant,'image-1',self.root/'input.jpg')
        self.assertEqual(calls,['bytes=0-3','bytes=4-7']);self.assert_no_files()
        grant,calls,client=self.ranged_fixture()
        client.download_object(grant,'image-1',self.root/'input.jpg')
        self.assertEqual(calls[0],'bytes=0-3')

    def test_expiry_between_ranges_holds_and_never_publishes(self):
        def expire_after_first(n,data,options):
            if n==1:self.now=1000
            return data,options
        grant,calls,client=self.ranged_fixture(mutate=expire_after_first)
        with self.assertRaises(TransferError):client.download_object(grant,'image-1',self.root/'input.jpg')
        self.assertEqual(calls,['bytes=0-3']);self.assert_no_files()

    def test_zero_byte_object_uses_single_full_request(self):
        grant=ValidatedGrant('job-1','reservation-1',1,'staging',1000,
            (TransferObject('empty',0,hashlib.sha256(b'').hexdigest()),),'private-token')
        calls=[]
        def opener(request,*,timeout):
            calls.append(request.get_header('Range'));return Response(b'',url=request.full_url,headers={'Content-Length':'0'})
        client=BrokerClient('https://broker.example',opener=opener,clock=lambda:self.now,max_response_bytes=4,enable_ranges=True)
        self.assertEqual(client.download_object(grant,'empty',self.root/'empty.txt').read_bytes(),b'')
        self.assertEqual(calls,[None])

    def test_unknown_object_and_raw_grant_never_open_connection(self):
        for grant, object_id in ((self.grant(), "outside"), (self.raw, "image-1")):
            with self.subTest(object_id=object_id), self.assertRaises(TransferError):
                self.client().download_object(grant, object_id, self.root / "input.jpg")
        self.assertEqual(self.calls, [])
        self.assert_no_files()

    def test_truncation_corruption_and_oversize_leave_no_target_or_parts(self):
        for body in (b"ab", b"bad", b"abcd"):
            with self.subTest(body=body), self.assertRaises(TransferError):
                self.client(body).download_object(self.grant(), "image-1", self.root / "input.jpg")
            self.assert_no_files()
        self.client().download_object(self.grant(), "image-1", self.root / "input.jpg")

    def test_stream_uses_bounded_reads_and_closes_response(self):
        from processing.reference_transfer import CHUNK_BYTES
        body = b"x" * (CHUNK_BYTES * 2 + 17)
        manifest = self.job["submission"]["manifest"]
        manifest["totalBytes"] = manifest["objects"][0]["byteSize"] = len(body)
        manifest["objects"][0]["sha256"] = hashlib.sha256(body).hexdigest()
        manifest["sha256"] = p.manifest_sha256(manifest)
        self.job["manifestSha256"] = self.raw["manifestSha256"] = manifest["sha256"]
        self.raw["byteBudget"] = len(body)
        self.job["transferRequest"].update(manifestSha256=manifest["sha256"], byteBudget=len(body))
        sizes = []
        class ObservedResponse(Response):
            def read(self, count):
                sizes.append(count)
                return super().read(count)
        response = ObservedResponse(body, url="https://broker.example" + OBJECT_PREFIX + "image-1")
        client = BrokerClient("https://broker.example", opener=lambda *a, **k: response)
        target = self.root / "input.jpg"
        client.download_object(self.grant(), "image-1", target)
        self.assertEqual(target.read_bytes(), body)
        self.assertTrue(response.closed)
        self.assertGreater(len(sizes), 2)
        self.assertTrue(all(0 < size <= CHUNK_BYTES for size in sizes))

    def test_headers_status_and_redirect_final_url_rejected(self):
        for options in ({"headers": {"Content-Length": "4"}},
                        {"headers": {"Content-Length": "-3"}},
                        {"headers": {"Content-Encoding": "gzip"}}, {"status": 302}):
            with self.subTest(options=options), self.assertRaises(TransferError):
                self.client(**options).download_object(self.grant(), "image-1", self.root / "input.jpg")
            self.assert_no_files()
        def wrong_origin(request, **kwargs):
            return Response(url="https://other.example/objects/image-1")
        with self.assertRaises(TransferError):
            BrokerClient("https://broker.example", opener=wrong_origin).download_object(
                self.grant(), "image-1", self.root / "input.jpg")
        self.assert_no_files()

    def test_default_opener_refuses_redirect_before_creating_redirect_request(self):
        client = BrokerClient("https://broker.example")
        handler = next(h for h in client._open.__self__.handlers if hasattr(h, "redirect_request"))
        from urllib.request import Request
        request = Request("https://broker.example/path", headers={"Authorization": "secret"})
        for code in (301, 302, 303, 307, 308):
            with self.subTest(code=code), self.assertRaises(TransferError):
                handler.redirect_request(request, None, code, "redirect", {}, "https://elsewhere.example/")

    def test_existing_destination_and_racing_destination_are_never_overwritten(self):
        target = self.root / "input.jpg"
        target.write_bytes(b"existing")
        with self.assertRaises(TransferError):
            self.client().download_object(self.grant(), "image-1", target)
        self.assertEqual(self.calls, [])
        target.unlink()
        def opener(request, **kwargs):
            target.write_bytes(b"racer")
            return Response(url=request.full_url)
        with self.assertRaises(TransferError):
            BrokerClient("https://broker.example", opener=opener).download_object(self.grant(), "image-1", target)
        self.assertEqual(target.read_bytes(), b"racer")
        self.assertEqual(list(self.root.iterdir()), [target])

    def test_symlink_destination_is_never_followed(self):
        target, elsewhere = self.root / "input.jpg", self.root / "outside"
        try:
            target.symlink_to(elsewhere)
        except OSError:
            self.skipTest("symlink creation unavailable")
        with self.assertRaises(TransferError):
            self.client().download_object(self.grant(), "image-1", target)
        self.assertFalse(elsewhere.exists())
        self.assertEqual(self.calls, [])

    def test_no_response_has_distinct_bounded_outcome_but_http_and_integrity_do_not(self):
        target = self.root / 'input.jpg'
        def unavailable(request, **kwargs):
            raise URLError('private diagnostic')
        with self.assertRaises(TransferResponseUnavailable) as caught:
            BrokerClient('https://broker.example', opener=unavailable).download_object(self.grant(), 'image-1', target)
        self.assertNotIn('private diagnostic', str(caught.exception))
        self.assert_no_files()
        def denied(request, **kwargs):
            raise HTTPError(request.full_url, 403, 'private', {}, None)
        with self.assertRaises(TransferError) as caught:
            BrokerClient('https://broker.example', opener=denied).download_object(self.grant(), 'image-1', target)
        self.assertNotIsInstance(caught.exception, TransferResponseUnavailable)
        with self.assertRaises(TransferError) as caught:
            self.client(body=b'bad').download_object(self.grant(), 'image-1', target)
        self.assertNotIsInstance(caught.exception, TransferResponseUnavailable)
        self.assert_no_files()

    def test_tls_errors_direct_or_wrapped_are_never_response_unavailable(self):
        for error in (ssl.SSLError('private'), URLError(ssl.SSLError('private'))):
            with self.subTest(error=type(error).__name__):
                def broken(request, **kwargs):
                    raise error
                with self.assertRaises(TransferError) as caught:
                    BrokerClient('https://broker.example', opener=broken).download_object(
                        self.grant(), 'image-1', self.root / 'input.jpg')
                self.assertNotIsInstance(caught.exception, TransferResponseUnavailable)
                self.assertNotIn('private', str(caught.exception))
                self.assert_no_files()

    def test_transport_and_fsync_errors_are_secret_free_and_retryable(self):
        token = self.raw["token"]
        def broken(request, **kwargs):
            raise RuntimeError("transport leaked " + token)
        with self.assertRaises(TransferError) as caught:
            BrokerClient("https://broker.example", opener=broken).download_object(
                self.grant(), "image-1", self.root / "input.jpg")
        self.assertNotIn(token, str(caught.exception))
        self.assertNotIn(token, repr(caught.exception))
        self.assert_no_files()

    def test_exclusive_part_collision_does_not_remove_existing_file(self):
        existing = self.root / (".reference-" + "0"*32 + ".part")
        existing.write_bytes(b"other attempt")
        with patch("processing.reference_transfer.secrets.token_hex", return_value="0"*32):
            with self.assertRaises(TransferError):
                self.client().download_object(self.grant(), "image-1", self.root / "input.jpg")
        self.assertEqual(existing.read_bytes(), b"other attempt")
        self.assertEqual(self.calls, [])
        self.assertEqual(list(self.root.iterdir()), [existing])

    def test_failed_file_fsync_does_not_publish(self):
        with patch("processing.reference_transfer.os.fsync", side_effect=OSError("disk error")):
            with self.assertRaises(TransferError):
                self.client().download_object(self.grant(), "image-1", self.root / "input.jpg")
        self.assert_no_files()

    def test_expiry_during_stream_aborts_publication(self):
        grant = self.grant()
        test = self
        class ExpiringResponse(Response):
            def read(self, count):
                test.now = grant.expires
                return super().read(count)
        def opener(request, **kwargs):
            return ExpiringResponse(url=request.full_url)
        with self.assertRaises(TransferError):
            BrokerClient("https://broker.example", opener=opener, clock=lambda: self.now).download_object(
                grant, "image-1", self.root / "input.jpg")
        self.assert_no_files()

    def test_invalid_origins_and_insecure_context_rejected(self):
        for origin in ("http://broker.example", "https://u:p@broker.example", "https://broker.example/path",
                       "https://broker.example?x=1", "https://broker.example#x", "https://broker.example:bad"):
            with self.subTest(origin=origin), self.assertRaises(ValueError):
                BrokerClient(origin)
        with self.assertRaises(ValueError):
            BrokerClient("https://broker.example", ssl_context=ssl._create_unverified_context())


if __name__ == "__main__":
    unittest.main()
