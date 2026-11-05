"""Real SourceRangeClient pairing against the authenticated local source broker."""
import json
import os
from pathlib import Path
import tempfile
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_protocol as protocol
from processing.prototype import Queue
from processing.reference_control import ReferenceControlAPI
from processing.reference_store import PROFILE, ReferenceStore
from processing.source_range_transfer import (SourceRangeClient, RENEWAL_PATH,
    RENEWAL_ACK_PATH, OBJECT_PREFIX, _request_id, SourceIdentity, validate_source_grant)
from processing.source_spool import SourceSpool


class _Response:
    def __init__(self,status,url,headers,body):
        self.status=status; self._url=url; self.headers=headers; self._body=body; self._offset=0
    def geturl(self): return self._url
    def read(self,size=-1):
        if size is None or size < 0: size=len(self._body)-self._offset
        data=self._body[self._offset:self._offset+size]; self._offset+=len(data); return data
    def close(self): pass


class _ControlOpener:
    def __init__(self,control,*,drop=None): self.control=control; self.drop=drop; self.dropped=False
    def __call__(self,request,timeout=None): return self.open(request,timeout=timeout)
    def open(self,request,timeout=None):
        from urllib.parse import urlsplit
        route=urlsplit(request.full_url).path
        response=self.control.handle(request.get_method(),route,dict(request.header_items()),
            request.data or b'',protected_transport=True)
        if hasattr(response,'stream'):
            try: body=b''.join(response.stream.chunks())
            finally: response.stream.close()
            headers=dict(response.headers); status=response.status
        else:
            body=protocol.canonical_json(response.body); headers={'Content-Type':'application/json',
                'Content-Length':str(len(body))}; status=response.status
        if self.drop == route and not self.dropped:
            self.dropped=True
            raise OSError('synthetic lost response after server commit')
        return _Response(status,request.full_url,headers,body)


class ReferenceSourceRangeBrokerTests(unittest.TestCase):
    def test_range_stream_stops_at_range_end_and_checks_pinned_identity(self):
        import os
        from processing.reference_worker_api import _SourceRangeBrokerStream
        with tempfile.TemporaryFile() as output:
            output.write(b'abcdef'); output.flush(); output.seek(2)
            info=os.fstat(output.fileno())
            identity=(info.st_dev,info.st_ino,info.st_size,info.st_mode,info.st_nlink,
                info.st_uid,info.st_mtime_ns,info.st_ctime_ns)
            stream=_SourceRangeBrokerStream(output,byte_size=3,file_identity=identity)
            self.assertEqual(b''.join(stream.chunks()),b'cde')
            stream.close()

    def test_ranges_renewal_replay_restart_and_corruption_hold(self):
        directory=tempfile.TemporaryDirectory()
        from tests.test_reference_source_intake import ReferenceSourceIntakeTests
        source_case=ReferenceSourceIntakeTests('runTest'); source_case.setUp()
        spool_root=Path(directory.name)/'spool'; spool_root.mkdir(mode=0o700)
        marker=spool_root/'.ltds-source-spool-id'
        marker.write_text('a'*64,encoding='ascii')
        if os.name!='nt': marker.chmod(0o600)
        mount=patch('processing.source_spool.os.path.ismount',return_value=True); mount.start()
        queue=None
        try:
            spool=SourceSpool(spool_root,mount_id='a'*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
            source_case.payload['manifest']['id']='dataset-range-broker'
            source_case.rehash()
            queue,store,control,job,claim_token=source_case._make_reserved_source_job(
                directory.name,spool,'range-broker',source_range_v2=True)
            worker=dict(node_id='cluster',worker_id='worker',worker_instance_id='instance',claim_token=claim_token)
            renewal_headers={'Content-Type':'application/json','X-Claim-Token':claim_token,
                'Authorization':'Bearer synthetic-worker-token-012345'}
            grant=store.mint_source_transfer_grant(job['jobId'],source_spool=spool)
            self.assertEqual(grant['protocol'],'source-range-v2')
            from tests.test_reference_source_intake import GCP_ID
            self.assertNotIn(GCP_ID,grant['objectIds'])
            store.update_phase(job['jobId'],'fetching')
            base=__import__('time').time()
            now=[base]
            # Simulate an initially short-lived grant without changing its
            # identity or bypassing grant validation.
            grant['expiresAt']=__import__('datetime').datetime.fromtimestamp(base+1,__import__('datetime').timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
            with queue._db() as db:
                db.execute('UPDATE reference_transfer_grants SET grant_json=?,digest=? WHERE job_id=?',
                    (protocol.canonical_json(grant).decode(),protocol.digest(grant),job['jobId']))
            client=SourceRangeClient('https://broker.invalid',Path(directory.name)/'worker-ledger.sqlite',
                opener=_ControlOpener(control),clock=lambda:now[0])
            identity=SourceIdentity(grant['environment'],grant['jobId'],grant['reservationId'],
                grant['generation'],grant['manifestSha256'],grant['sourceVersion'])
            validated=validate_source_grant(grant,job=store.get(job['jobId'],private=True),clock=lambda:now[0])
            objects=list(validated.objects)
            identity_json=json.dumps(validated.identity.canonical(),sort_keys=True,separators=(',',':'))
            # Exercise the actual HTTPS listener and SourceRangeClient together.
            # The in-process opener below intentionally remains useful for lost
            # renewal/ACK replay cases, but cannot catch headers dropped by the
            # HTTP adapter's binary response writer.
            from cryptography import x509
            from cryptography.hazmat.primitives import hashes,serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
            import ipaddress, ssl, threading
            from datetime import datetime, timedelta, timezone
            from processing.reference_http import create_reference_http_server
            from urllib.request import HTTPSHandler, build_opener
            tls_key=rsa.generate_private_key(public_exponent=65537,key_size=2048)
            subject=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,'127.0.0.1')])
            cert_now=datetime.now(timezone.utc).replace(tzinfo=None)
            certificate=(x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
                .public_key(tls_key.public_key()).serial_number(x509.random_serial_number())
                .not_valid_before(cert_now-timedelta(minutes=1)).not_valid_after(cert_now+timedelta(minutes=10))
                .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),
                    critical=False).sign(tls_key,hashes.SHA256()))
            cert_path=Path(directory.name)/'range-broker-cert.pem'; key_path=Path(directory.name)/'range-broker-key.pem'
            cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
            key_path.write_bytes(tls_key.private_bytes(serialization.Encoding.PEM,
                serialization.PrivateFormat.TraditionalOpenSSL,serialization.NoEncryption()))
            server_tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER); server_tls.load_cert_chain(cert_path,key_path)
            client_tls=ssl.create_default_context(cafile=str(cert_path))
            https_server=create_reference_http_server(control,ssl_context=server_tls,request_timeout=3)
            https_thread=threading.Thread(target=https_server.serve_forever,kwargs={'poll_interval':0.02},daemon=True)
            https_thread.start()
            try:
                real_open=build_opener(HTTPSHandler(context=client_tls)).open
                observed=[]
                def record_https_response(request,timeout=None):
                    response=real_open(request,timeout=timeout); observed.append(dict(response.headers.items())); return response
                https_client=SourceRangeClient(f'https://127.0.0.1:{https_server.server_address[1]}',
                    Path(directory.name)/'https-worker-ledger.sqlite',opener=record_https_response,
                    ssl_context=client_tls,clock=lambda:now[0],timeout=3)
                https_destination=Path(directory.name)/'https-range-download.bin'
                https_client.download_object(grant=validated,object_id=objects[0].id,destination=https_destination)
                self.assertEqual(https_destination.read_bytes(),b'abc')
                self.assertTrue(observed)
                response_headers=observed[0]
                for header in ('Content-Range','Content-Length','ETag','X-Processing-Request-ID'):
                    self.assertIn(header,response_headers)
                self.assertEqual(response_headers['Content-Range'],f'bytes 0-2/{objects[0].byte_size}')
                self.assertEqual(response_headers['Content-Length'],'3')
                self.assertEqual(response_headers['ETag'],'"sha256-'+objects[0].sha256+'"')
                self.assertTrue(response_headers['X-Processing-Request-ID'])
                self.assertEqual(response_headers.get('Content-Encoding','identity'),'identity')
            finally:
                https_server.shutdown(); https_server.server_close(); https_thread.join(2)
            # New range journal rows are charged before mutation. Set the limit
            # to exactly current usage and prove the next distinct range holds.
            next_obj=objects[1]
            next_length=min(validated.chunk_bytes,next_obj.byte_size)
            next_request_id=_request_id(identity_json,next_obj,0,next_length)
            with queue._db() as db:
                used_bytes=store._metadata_used_bytes(db)
                before_usage=db.execute('SELECT requests_used,bytes_used,attempts_used FROM reference_source_range_usage WHERE job_id=? AND generation=?',
                    (job['jobId'],job['generation'])).fetchone()
                before_requests=db.execute('SELECT COUNT(*) FROM reference_source_range_requests WHERE job_id=? AND generation=?',
                    (job['jobId'],job['generation'])).fetchone()[0]
            previous_limit=store.max_metadata_bytes
            store.max_metadata_bytes=used_bytes
            try:
                from processing.maintenance import Conflict
                with self.assertRaisesRegex(Conflict,'metadata quota'):
                    store.source_range_for_grant(grant['token'],next_obj.id,range_start=0,range_length=next_length,
                        request_id=next_request_id,if_match='"sha256-'+next_obj.sha256+'"',source_spool=spool,
                        clock=lambda:now[0])
                with queue._db() as db:
                    self.assertEqual(tuple(db.execute('SELECT requests_used,bytes_used,attempts_used FROM reference_source_range_usage WHERE job_id=? AND generation=?',
                        (job['jobId'],job['generation'])).fetchone()),tuple(before_usage))
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_source_range_requests WHERE job_id=? AND generation=?',
                        (job['jobId'],job['generation'])).fetchone()[0],before_requests)
            finally:
                store.max_metadata_bytes=previous_limit
            probe_id=_request_id(identity_json,objects[0],0,min(validated.chunk_bytes,objects[0].byte_size))
            from urllib.request import Request
            probe=Request('https://broker.invalid'+OBJECT_PREFIX+objects[0].id,method='GET',headers={
                'Authorization':'Bearer '+grant['token'],'Range':f'bytes=0-{objects[0].byte_size-1}',
                'If-Match':'"sha256-'+objects[0].sha256+'"','Accept-Encoding':'identity',
                'X-Processing-Request-ID':probe_id})
            probe_response=_ControlOpener(control).open(probe)
            self.assertEqual(probe_response.status,206,probe_response.read())
            wrong_token=Request('https://broker.invalid'+OBJECT_PREFIX+objects[0].id,method='GET',headers={
                'Authorization':'Bearer '+grant['token']+'x','Range':f'bytes=0-{objects[0].byte_size-1}',
                'If-Match':'"sha256-'+objects[0].sha256+'"','Accept-Encoding':'identity',
                'X-Processing-Request-ID':probe_id})
            self.assertEqual(_ControlOpener(control).open(wrong_token).status,409)
            destination=Path(directory.name)/'downloaded-0.bin'
            client.download_object(grant=validated,object_id=objects[0].id,destination=destination)
            self.assertEqual(destination.read_bytes(),b'abc')
            now[0]=base+2

            # First renewal response is deliberately lost after the server's
            # durable insert; the same client request must replay the successor.
            old=grant
            previous_sha=__import__('hashlib').sha256(protocol.canonical_json(old)).hexdigest()
            renewal={'protocol':'source-range-v2-renewal','renewalId':'renewal-range-test',
                'sequence':1,'previousGrantSha256':previous_sha,'previousGrant':old}
            renewal_body=protocol.canonical_json(renewal)
            with queue._db() as db:
                used_bytes=store._metadata_used_bytes(db)
                renewal_count=db.execute('SELECT COUNT(*) FROM reference_source_grant_renewals WHERE job_id=?',
                    (job['jobId'],)).fetchone()[0]
            previous_limit=store.max_metadata_bytes
            store.max_metadata_bytes=used_bytes
            try:
                from processing.maintenance import Conflict
                with self.assertRaisesRegex(Conflict,'metadata quota'):
                    store.renew_source_transfer_grant(renewal,source_spool=spool,worker=worker,clock=lambda:now[0])
                with queue._db() as db:
                    self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_source_grant_renewals WHERE job_id=?',
                        (job['jobId'],)).fetchone()[0],renewal_count)
            finally:
                store.max_metadata_bytes=previous_limit
            lost=_ControlOpener(control,drop=RENEWAL_PATH)
            with patch('processing.reference_source_intake_store.time.time',side_effect=lambda:now[0]):
                with self.assertRaises(Exception):
                    lost.open(__import__('urllib.request').request.Request('https://broker.invalid'+RENEWAL_PATH,
                        data=renewal_body,method='POST',headers=renewal_headers))
                replay=_ControlOpener(control).open(__import__('urllib.request').request.Request(
                    'https://broker.invalid'+RENEWAL_PATH,data=renewal_body,method='POST',
                    headers=renewal_headers))
            renewal_reply=json.loads(replay.read())
            if replay.status != 200:
                try:
                    store.renew_source_transfer_grant(renewal,source_spool=spool,worker=worker,clock=lambda:now[0])
                except Exception as error:
                    self.fail(f'renewal route failed: {renewal_reply}; store reason: {error}')
            self.assertEqual(replay.status,200,renewal_reply)
            successor=renewal_reply['grant']
            self.assertEqual(renewal_reply['sequence'],1)
            self.assertGreater(successor['expiresAt'],old['expiresAt'])
            ack={'protocol':'source-range-v2-renewal-ack','renewalId':'renewal-range-test',
                'sequence':1,'grantSha256':__import__('hashlib').sha256(protocol.canonical_json(successor)).hexdigest(),
                'token':successor['token']}
            ack_body=protocol.canonical_json(ack)
            lost_ack=_ControlOpener(control,drop=RENEWAL_ACK_PATH)
            with patch('processing.reference_source_intake_store.time.time',side_effect=lambda:now[0]):
                with self.assertRaises(Exception):
                    lost_ack.open(__import__('urllib.request').request.Request('https://broker.invalid'+RENEWAL_ACK_PATH,
                        data=ack_body,method='POST',headers=renewal_headers))
            client=SourceRangeClient('https://broker.invalid',Path(directory.name)/'worker-ledger.sqlite',
                opener=_ControlOpener(control),clock=lambda:now[0])
            validated_new=validate_source_grant(successor,job=store.get(job['jobId'],private=True),clock=lambda:now[0])
            target=Path(directory.name)/'downloaded-1.bin'
            client.download_object(grant=validated_new,object_id=objects[1].id,destination=target)
            self.assertEqual(target.read_bytes(),b'1,2,3\n')

            # If the NAS object changes after grant mint, range reads are still
            # scoped, but the downloader's full-object SHA check must refuse it.
            private=store.get(job['jobId'],private=True)
            receipt=next(item for item in private['sourceObjectReceipts']+private['derivedObjectReceipts']
                if item['objectId']==objects[1].id)
            path=spool.root/spool.storage_key(job['jobId'],job['generation'],objects[1].id)
            path.write_bytes(b'X,2,3\n')
            target_bad=Path(directory.name)/'downloaded-corrupt.bin'
            corrupt_client=SourceRangeClient('https://broker.invalid',Path(directory.name)/'corrupt-ledger.sqlite',
                opener=_ControlOpener(control),clock=lambda:now[0])
            with self.assertRaises(Exception):
                corrupt_client.download_object(grant=validated_new,object_id=objects[1].id,destination=target_bad)
            self.assertFalse(target_bad.exists())
            lease_exp=__import__('datetime').datetime.fromisoformat(
                store.get(job['jobId'],private=True)['transferRequest']['expiresAt'].replace('Z','+00:00')).timestamp()
            renewal_after_lease={'protocol':'source-range-v2-renewal','renewalId':'renewal-after-lease',
                'sequence':2,'previousGrantSha256':__import__('hashlib').sha256(protocol.canonical_json(successor)).hexdigest(),
                'previousGrant':successor}
            with patch('processing.reference_source_intake_store.time.time',return_value=lease_exp+1):
                expired_reply=_ControlOpener(control).open(__import__('urllib.request').request.Request(
                    'https://broker.invalid'+RENEWAL_PATH,data=protocol.canonical_json(renewal_after_lease),
                    method='POST',headers={'Content-Type':'application/json'}))
            self.assertEqual((expired_reply.status,json.loads(expired_reply.read())),
                (401,{'error':'unauthorized'}))
            queue.close(); queue=Queue(directory.name); queue.allowed_profiles.add(PROFILE)
            store=ReferenceStore(queue)
            control=ReferenceControlAPI(store,viewer_bearers={'synthetic-staging-token':'staging'},
                worker_bearers={'synthetic-worker-token-012345':dict(node_id='cluster',worker_id='worker',worker_instance_id='instance')},
                capability_fingerprint='a'*64,options_fingerprint=source_case.payload['pipeline']['odm']['optionsFingerprint'],
                allowed_odm_options={'numeric-probe'},source_spool=spool,
                source_attestation_keys=source_case.trusted_keys,authority_id=queue.authority_id)
            with queue._db() as db:
                ack_replay_quota=store._metadata_used_bytes(db)
            store.max_metadata_bytes=ack_replay_quota
            with patch('processing.reference_source_intake_store.time.time',side_effect=lambda:now[0]):
                replay_ack=_ControlOpener(control).open(__import__('urllib.request').request.Request(
                    'https://broker.invalid'+RENEWAL_ACK_PATH,data=ack_body,method='POST',
                    headers=renewal_headers))
            self.assertEqual(json.loads(replay_ack.read()),{'protocol':'source-range-v2-renewal-ack',
                'renewalId':'renewal-range-test','sequence':1,'grantSha256':ack['grantSha256']})
            with queue._db() as db:
                self.assertEqual(store._metadata_used_bytes(db),ack_replay_quota)
            revoked=validated_new
            denied_request=Request('https://broker.invalid'+OBJECT_PREFIX+objects[0].id,method='GET',headers={
                'Authorization':'Bearer '+revoked.token,'Range':f'bytes=0-{objects[0].byte_size-1}',
                'If-Match':'"sha256-'+objects[0].sha256+'"','Accept-Encoding':'identity',
                'X-Processing-Request-ID':_request_id(
                    json.dumps(revoked.identity.canonical(),sort_keys=True,separators=(',',':')),
                    objects[0],0,objects[0].byte_size)})
            denied=_ControlOpener(control).open(denied_request)
            self.assertEqual(denied.status,409)
        finally:
            if queue is not None: queue.close()
            mount.stop(); source_case.tearDown(); directory.cleanup()


if __name__ == '__main__':
    unittest.main()
