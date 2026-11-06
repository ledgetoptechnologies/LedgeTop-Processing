import http.client
import json
import ssl
import threading
import unittest
import uuid
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace

from processing.direct_snapshot_http import create_snapshot_server
from processing.direct_worker_preparation_api import DirectPreparationAPI
from tests import test_direct_snapshot_http as snapshot_fixture


class PreparationListenerTests(unittest.TestCase):
    @unittest.skipUnless(os.name=='posix' and hasattr(os,'memfd_create'),'Linux protected descriptors required')
    def test_protected_credential_reader_rejects_fifo_symlink_and_writable_ancestor(self):
        from processing.direct_protected_transport import _read, _sealed
        with tempfile.TemporaryDirectory(dir='/run') as directory:
            root=Path(directory);root.chmod(0o700)
            key=root/'key';key.write_bytes(b'synthetic-key');key.chmod(0o600)
            self.assertEqual(_read(str(key),private=True),b'synthetic-key')
            fd=_sealed(b'synthetic-key')
            try:
                with self.assertRaises(OSError):os.write(fd,b'x')
            finally:os.close(fd)
            pipe=root/'fifo';os.mkfifo(pipe,0o600)
            with self.assertRaises((OSError,ValueError)):_read(str(pipe),private=True)
            link=root/'link';link.symlink_to(key)
            with self.assertRaises((OSError,ValueError)):_read(str(link),private=True)
            root.chmod(0o777)
            with self.assertRaises(ValueError):_read(str(key),private=True)
            root.chmod(0o700)
    @unittest.skipIf(os.name=='nt','Linux scratch fixture required')
    def test_preparation_persists_signed_inventory_before_reply(self):
        from tests.test_direct_worker_prepare_v2 import WorkerPrepareTests
        fixture=WorkerPrepareTests();f,kwargs=fixture.fixture();self.addCleanup(fixture.doCleanups)
        from processing import reference_protocol as protocol
        policy=dict(directPreparationEnabled=True,capabilityFingerprint=f.options['capability_fingerprint'],
                    optionsFingerprint=f.options['options_fingerprint'],allowedOdmOptions=[])
        worker=SimpleNamespace(engine_id=kwargs['engine_id'],origin=kwargs['coordinator_origin'],gate_generation=1)
        backend=DirectPreparationAPI(driver=SimpleNamespace(direct_source_policy=lambda:policy,worker=worker),
            ledger=f.ledger,worker=f.worker,mount_path='/tmp',mount_id='a'*32,marker_name=f.spool.marker.name,
            source_keys=f.options['trusted_source_keys'],signer=f.proof_key,issuer_id='worker-issuer',key_id='worker-key')
        raw=protocol.canonical_json(dict(source=f.payload,item=f.item,challengeId=kwargs['challenge_id']))
        response=backend._prepare_from_listener(('coord-a','staging'),raw)
        self.assertEqual(response.status,200)
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_inventory_blobs').fetchone()[0],1)
        again=backend._prepare_from_listener(('coord-a','staging'),raw)
        self.assertEqual(again.status,200);self.assertEqual(again.body,response.body)

    @unittest.skipUnless(os.name=='posix' and hasattr(os,'memfd_create'),'Linux TLS fixture required')
    def test_real_mtls_preparation_retains_before_response(self):
        from tests.test_direct_worker_prepare_v2 import WorkerPrepareTests
        worker_fixture=WorkerPrepareTests();f,kwargs=worker_fixture.fixture();self.addCleanup(worker_fixture.doCleanups)
        tls=snapshot_fixture.SnapshotListenerTests();tls.setUp();self.addCleanup(tls.doCleanups)
        from processing import reference_protocol as protocol
        policy=dict(directPreparationEnabled=True,capabilityFingerprint=f.options['capability_fingerprint'],
                    optionsFingerprint=f.options['options_fingerprint'],allowedOdmOptions=[])
        worker=SimpleNamespace(engine_id=kwargs['engine_id'],origin=kwargs['coordinator_origin'],gate_generation=1)
        backend=DirectPreparationAPI(driver=SimpleNamespace(direct_source_policy=lambda:policy,worker=worker),
            ledger=f.ledger,worker=f.worker,mount_path='/tmp',mount_id='a'*32,marker_name=f.spool.marker.name,
            source_keys=f.options['trusted_source_keys'],signer=f.proof_key,issuer_id='worker-issuer',key_id='worker-key')
        # Pin the fixture certificate to the exact environment and principal.
        scopes={key:dict(principal='coord-a',environment='staging') for key in tls.scopes}
        server=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=tls.context,client_scopes=scopes,prepare_backend=backend)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(3)))
        raw=protocol.canonical_json(dict(source=f.payload,item=f.item,challengeId=kwargs['challenge_id']))
        status,reply=tls.request(server,cert=tls.good,key=tls.goodkey,path='/internal/v2/worker/prepare',body=raw)
        self.assertEqual(status,200);self.assertFalse(reply['body']['sourceCustodyAccepted'])
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_inventory_blobs').fetchone()[0],1)

    def test_mtls_identity_and_disabled_fleet_policy_hold_before_gate(self):
        f=snapshot_fixture.SnapshotListenerTests();f.setUp();self.addCleanup(f.doCleanups)
        calls=[]
        driver=SimpleNamespace(direct_source_policy=lambda:(calls.append(True) or {'directPreparationEnabled':False}))
        backend=DirectPreparationAPI(driver=driver,ledger=None,worker={},mount_path=None,mount_id=None,
            marker_name=None,source_keys=None,signer=None,issuer_id=None,key_id=None)
        server=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=f.context,client_scopes=f.scopes,
                                      prepare_backend=backend)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(3)))
        path='/internal/v2/worker/prepare'
        payload=json.dumps({'source':{},'item':{},'challengeId':str(uuid.uuid4())}).encode()
        for cert,key in ((None,None),(f.other,f.otherkey)):
            with self.assertRaises((ssl.SSLError,OSError,http.client.HTTPException)):
                f.request(server,cert=cert,key=key,path=path,body=payload)
        self.assertEqual(calls,[])
        status,reply=f.request(server,cert=f.good,key=f.goodkey,path=path,body=payload)
        self.assertEqual(status,503);self.assertEqual(reply['error'],'direct_preparation_disabled')
        self.assertEqual(len(calls),1)
