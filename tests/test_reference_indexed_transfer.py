"""Factory-sealed indexed lookup and the actual shared HTTPS publisher."""
import copy
from datetime import datetime, timezone
import gc
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch
import weakref

from processing import reference_protocol as p
from processing import reference_transfer as t
from tests.test_reference_transfer import Response


class _IndexedTransferFixture:
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        if os.name != 'nt': self.root.chmod(0o700)
        fixture = json.loads((Path(__file__).resolve().parents[1] /
                              'contracts/reference-v1.contract.json').read_text())
        self.raw = fixture['transferGrant']
        self.job = dict(jobId='job-1', generation=1, environment='staging',
            manifestSha256=fixture['submission']['manifest']['sha256'],
            submission=fixture['submission'], transferRequest=fixture['transferRequest'])
        expiry = datetime.fromtimestamp(1000, timezone.utc).isoformat()
        self.raw['expiresAt'] = self.job['transferRequest']['expiresAt'] = expiry
        self.now, self.mono, self.requests = 100, 10, []

    def grant(self):
        return t.validate_indexed_grant(self.raw, job=self.job,
            clock=lambda: self.now, monotonic=lambda: self.mono)

    def client(self, body=b'abc', **options):
        def open_(request, **kwargs):
            self.requests.append(request.full_url)
            return Response(body, url=request.full_url, **options)
        return t.BrokerClient('https://broker.example', opener=open_, clock=lambda: self.now)

    def assert_empty(self):
        self.assertEqual(list(self.root.iterdir()), [])

    def set_objects(self, count):
        manifest = self.job['submission']['manifest']
        manifest['objects'] = [dict(id='image-'+str(i), name='image-'+str(i)+'.jpg',
            byteSize=3, sha256=hashlib.sha256(b'abc').hexdigest(), kind='image',
            mediaType='image/jpeg') for i in range(count)]
        manifest['totalBytes'] = count*3
        manifest['objectCount'] = count
        manifest['inputCounts'] = dict(images=count, lidarObjects=0, points=0)
        manifest['sha256'] = p.manifest_sha256(manifest)
        self.job['manifestSha256'] = manifest['sha256']
        self.raw['manifestSha256'] = self.job['transferRequest']['manifestSha256'] = manifest['sha256']
        self.raw['byteBudget'] = self.job['transferRequest']['byteBudget'] = count*3
        self.raw['objectIds'] = [o['id'] for o in manifest['objects']]

class IndexedTransferTests(_IndexedTransferFixture, unittest.TestCase):
    def test_factory_once_detaches_original_identity_and_redacts_token(self):
        with patch.object(t, 'validate_grant', wraps=t.validate_grant) as validate:
            handle = self.grant()
            self.assertEqual(validate.call_count, 1)
        self.assertEqual((handle.job_id, handle.reservation_id, handle.generation,
                          handle.environment, handle.manifest_sha256, handle.nonce,
                          handle.byte_budget, handle.grant_sha256),
            (self.raw['jobId'], self.raw['reservationId'], self.raw['generation'],
             self.raw['environment'], self.raw['manifestSha256'], self.raw['nonce'],
             self.raw['byteBudget'], p.digest(self.raw)))
        self.assertNotIn(self.raw['token'], repr(handle))
        self.raw['objectIds'].clear()
        self.job['submission']['manifest']['objects'][0]['sha256'] = '0'*64
        self.assertEqual(handle.object_ids, ('image-1',))
        self.assertEqual(handle.object_for('image-1').sha256, hashlib.sha256(b'abc').hexdigest())
        self.client().download_indexed_object(handle, 'image-1', self.root/'input.jpg')
        self.assertEqual((self.root/'input.jpg').read_bytes(), b'abc')

    def test_forged_raw_legacy_descriptor_and_unregistered_handles_refuse(self):
        forged = object.__new__(t.IndexedValidatedGrant)
        legacy = t.validate_grant(self.raw, job=self.job, clock=lambda: self.now)
        for value in (forged, self.raw, legacy, legacy.objects[0], object()):
            with self.subTest(value_type=type(value)), self.assertRaises(t.TransferError):
                self.client().download_indexed_object(value, 'image-1', self.root/'input.jpg')
        with self.assertRaises(t.TransferError): t.IndexedValidatedGrant()
        self.assertEqual(self.requests, []); self.assert_empty()

    def test_handle_fields_maps_and_returned_descriptors_cannot_substitute_authority(self):
        handle = self.grant()
        for field, value in (('job_id','other'), ('object_ids',('other',)),
                             ('index',{'image-1':t.TransferObject('image-1',3,'0'*64)})):
            with self.subTest(field=field), self.assertRaises(AttributeError):
                object.__setattr__(handle, field, value)
        descriptor = handle.object_for('image-1')
        object.__setattr__(descriptor, 'sha256', '0'*64)
        object.__setattr__(descriptor, 'id', 'other')
        self.assertNotEqual(descriptor, handle.object_for('image-1'))
        self.client().download_indexed_object(handle, 'image-1', self.root/'input.jpg')
        self.assertEqual((self.root/'input.jpg').read_bytes(), b'abc')

    def test_unknown_object_and_nonstring_ids_refuse_before_network(self):
        handle = self.grant()
        for object_id in ('outside', [], {}, None, True):
            with self.subTest(object_id=object_id), self.assertRaises(t.TransferError):
                self.client().download_indexed_object(handle, object_id, self.root/'input.jpg')
        self.assertEqual(self.requests, []); self.assert_empty()

    def test_full_original_identity_and_closure_validation_is_not_bypassed(self):
        for field, value in dict(jobId='other', reservationId='other', generation=2,
                environment='production', nonce='other', manifestSha256='0'*64,
                byteBudget=4, objectIds=[], expiresAt='invalid', token='bad token').items():
            raw = copy.deepcopy(self.raw); raw[field] = value
            with self.subTest(field=field), self.assertRaises(t.TransferError):
                t.validate_indexed_grant(raw, job=self.job, clock=lambda: self.now)
        self.assert_empty()

    def test_factory_10001_and_oversized_metadata_refuse(self):
        self.set_objects(10001)
        with patch.object(t, 'validate_grant', side_effect=AssertionError('over bound')) as validate:
            with self.assertRaises(t.TransferError): self.grant()
            self.assertEqual(validate.call_count, 0)
        self.set_objects(1)
        self.job['submission']['manifest']['objects'][0]['name'] = 'x'*(p.MAX_BODY_BYTES+1)
        with self.assertRaises(t.TransferError): self.grant()
        self.assert_empty()

    def test_10000_objects_use_one_full_validation_and_exactly_one_map_get_per_lookup(self):
        self.set_objects(10000)
        with patch.object(t, 'validate_grant', wraps=t.validate_grant) as validate:
            handle = self.grant()
            index = t._indexed_state(handle).record.index
            calls = []
            def count(frame, event, arg):
                if event == 'c_call' and getattr(arg, '__self__', None) is index and getattr(arg, '__name__', None) == 'get':
                    calls.append(1)
            previous = sys.getprofile()
            try:
                sys.setprofile(count)
                with patch.object(p, 'manifest_sha256', side_effect=AssertionError('hot manifest scan')):
                    for i in range(10000):
                        self.assertEqual(handle.object_for('image-'+str(i)).id, 'image-'+str(i))
            finally: sys.setprofile(previous)
            self.assertEqual(validate.call_count, 1)
            self.assertEqual(len(calls), 10000)
            self.assertEqual(len(handle.object_ids), 10000)
        self.assert_empty()

    def test_process_local_registry_does_not_retain_discarded_handle(self):
        handle = self.grant(); reference = weakref.ref(handle)
        del handle; gc.collect()
        self.assertIsNone(reference())

    def test_wall_rollback_expiry_and_monotonic_deadline_hold_without_get(self):
        for change in ('rollback','expiry','monotonic','monotonic_rollback'):
            with self.subTest(change=change):
                self.now, self.mono = 100, 10
                handle = self.grant()
                if change == 'rollback': self.now = 99
                if change == 'expiry': self.now = 1000
                if change == 'monotonic': self.mono = 910
                if change == 'monotonic_rollback': self.mono = 9
                with self.assertRaises(t.TransferError):
                    self.client().download_indexed_object(handle, 'image-1', self.root/'input.jpg')
                self.now, self.mono = 100, 10
                with self.assertRaises(t.TransferError):
                    self.client().download_indexed_object(handle, 'image-1', self.root/'input.jpg')
        self.assertEqual(self.requests, []); self.assert_empty()

    def test_invalid_hot_clock_values_are_secret_free_and_permanently_hold_handle(self):
        for value in (True, float('nan'), float('inf'), 'secret-clock'):
            self.now, self.mono = 100, 10; handle = self.grant(); self.now = value
            with self.assertRaises(t.TransferError) as caught:
                self.client().download_indexed_object(handle, 'image-1', self.root/'input.jpg')
            self.assertNotIn('secret-clock', str(caught.exception))
        self.assertEqual(self.requests, []); self.assert_empty()

    def test_full_validation_time_cannot_extend_original_monotonic_expiry(self):
        validate = t.validate_grant
        def slow_validation(*args, **kwargs):
            result = validate(*args, **kwargs)
            self.mono = 910
            return result
        with patch.object(t, 'validate_grant', side_effect=slow_validation) as calls:
            with self.assertRaises(t.TransferError): self.grant()
            self.assertEqual(calls.call_count, 1)
        self.assertEqual(self.requests, []); self.assert_empty()

    def test_explicit_expiry_gate_covers_reconciliation_without_network(self):
        handle=self.grant()
        self.assertIsNone(handle.check_expiry(clock=lambda:self.now))
        self.mono=910
        with self.assertRaises(t.TransferError): handle.check_expiry(clock=lambda:self.now)
        self.mono=10
        with self.assertRaises(t.TransferError): handle.check_expiry(clock=lambda:self.now)
        self.assertEqual(self.requests,[]);self.assert_empty()

    def test_midstream_clock_expiry_and_rollback_clean_parts(self):
        for bad_time in (99,1000):
            self.now, self.mono = 100,10; handle = self.grant(); owner = self
            class ChangingResponse(Response):
                def read(self, count):
                    body = super().read(count); owner.now = bad_time; return body
            def open_(request, **kwargs): return ChangingResponse(url=request.full_url)
            with self.assertRaises(t.TransferError):
                t.BrokerClient('https://broker.example', opener=open_, clock=lambda:self.now).download_indexed_object(
                    handle,'image-1',self.root/'input.jpg')
            self.assert_empty()

    def test_corruption_truncation_oversize_and_fsync_failure_use_same_publisher(self):
        for body in (b'bad',b'ab',b'abcd'):
            with self.subTest(body=body), self.assertRaises(t.TransferError):
                self.client(body).download_indexed_object(self.grant(),'image-1',self.root/'input.jpg')
            self.assert_empty()
        with patch.object(t.os,'fsync',side_effect=OSError('synthetic disk error')):
            with self.assertRaises(t.TransferError):
                self.client().download_indexed_object(self.grant(),'image-1',self.root/'input.jpg')
        self.assert_empty()

    def test_single_call_has_no_retry_and_network_errors_do_not_expose_token(self):
        calls = []
        def fail(request, **kwargs):
            calls.append(1); raise RuntimeError(self.raw['token'])
        with self.assertRaises(t.TransferError) as caught:
            t.BrokerClient('https://broker.example', opener=fail, clock=lambda:self.now).download_indexed_object(
                self.grant(),'image-1',self.root/'input.jpg')
        self.assertEqual(calls,[1]); self.assertNotIn(self.raw['token'],str(caught.exception)); self.assert_empty()


@unittest.skipUnless(os.name=='posix','Linux actual private TLS broker fixture')
class IndexedHTTPSPublisherTests(_IndexedTransferFixture, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        temp = tempfile.TemporaryDirectory();cls.addClassCleanup(temp.cleanup)
        root = Path(temp.name);cls.cert=root/'cert.pem';cls.key=root/'key.pem'
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=indexed-synthetic','-addext','subjectAltName=IP:127.0.0.1',
            '-keyout',str(cls.key),'-out',str(cls.cert)],check=True,
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

    def setUp(self):
        super().setUp();self.mode='good';self.served=[];self.race=None;owner=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self):
                owner.served.append(self.path)
                if self.headers.get('Authorization')!='Bearer '+owner.raw['token']:
                    self.send_error(401);return
                if owner.race is not None: owner.race()
                if owner.mode=='redirect':
                    self.send_response(302);self.send_header('Location',owner.origin+'/do-not-follow')
                    self.send_header('Content-Length','0');self.end_headers();return
                body=b'abc' if owner.mode=='good' else b'bad'
                self.send_response(200);self.send_header('Content-Length',str(len(body)))
                self.end_headers();self.wfile.write(body)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(self.cert,self.key)
        server.socket=context.wrap_socket(server.socket,server_side=True)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':.01});thread.start()
        self.origin='https://127.0.0.1:'+str(server.server_port)
        def stop():server.shutdown();server.server_close();thread.join(3);self.assertFalse(thread.is_alive())
        self.addCleanup(stop)
        self.tls=ssl.create_default_context(cafile=str(self.cert))

    def https_client(self, context=None):
        return t.BrokerClient(self.origin,ssl_context=context or self.tls,clock=lambda:self.now)

    def test_actual_https_bytes_hash_and_private_no_replace_publication(self):
        handle=self.grant();target=self.root/'input.jpg'
        self.https_client().download_indexed_object(handle,'image-1',target)
        self.assertEqual(target.read_bytes(),b'abc');self.assertEqual(self.served,[t.OBJECT_PREFIX+'image-1'])
        self.assertEqual(target.stat().st_mode&0o777,0o600)
        with self.assertRaises(t.TransferError):self.https_client().download_indexed_object(handle,'image-1',target)
        self.assertEqual(target.read_bytes(),b'abc');self.assertEqual(len(self.served),1)

    def test_actual_https_hash_corruption_and_redirect_are_not_published_or_followed(self):
        for mode in ('corrupt','redirect'):
            self.mode=mode;self.served=[]
            with self.assertRaises(t.TransferError):
                self.https_client().download_indexed_object(self.grant(),'image-1',self.root/'input.jpg')
            self.assert_empty();self.assertEqual(self.served,[t.OBJECT_PREFIX+'image-1'])

    def test_actual_https_tls_verification_and_expiry_prevent_publication(self):
        with self.assertRaises(t.TransferError):
            self.https_client(ssl.create_default_context()).download_indexed_object(self.grant(),'image-1',self.root/'input.jpg')
        self.assertEqual(self.served,[]);self.assert_empty()
        handle=self.grant();self.now=1000
        with self.assertRaises(t.TransferError):self.https_client().download_indexed_object(handle,'image-1',self.root/'input.jpg')
        self.assertEqual(self.served,[]);self.assert_empty()

    def test_actual_https_racing_target_is_retained_without_overwrite(self):
        target=self.root/'input.jpg';self.race=lambda:target.write_bytes(b'existing-racer')
        with self.assertRaises(t.TransferError):self.https_client().download_indexed_object(self.grant(),'image-1',target)
        self.assertEqual(target.read_bytes(),b'existing-racer');self.assertEqual(list(self.root.iterdir()),[target])

    def test_actual_https_expiry_after_real_stream_read_refuses_publication(self):
        client=self.https_client();open_=client._open;owner=self
        class ExpiringRealResponse:
            def __init__(self,response): self.response=response
            def __getattr__(self,name): return getattr(self.response,name)
            def read(self,count):
                value=self.response.read(count);owner.now=1000;return value
        def observed_open(request,**kwargs): return ExpiringRealResponse(open_(request,**kwargs))
        client._open=observed_open
        with self.assertRaises(t.TransferError):client.download_indexed_object(self.grant(),'image-1',self.root/'input.jpg')
        self.assertEqual(self.served,[t.OBJECT_PREFIX+'image-1']);self.assert_empty()


if __name__=='__main__':unittest.main()
