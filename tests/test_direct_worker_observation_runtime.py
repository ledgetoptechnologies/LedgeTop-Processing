import io
import os
import threading
import unittest
import uuid
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from processing.direct_worker_prepare_v2 import prepare_signed_job
from processing.direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from processing.direct_worker_grant_v2 import DirectWorkerGrant
from processing.direct_worker_observation_owner import ObservationOwner
from processing.direct_snapshot_http import create_snapshot_server
from processing.direct_worker_preparation_api import DirectPreparationAPI
from processing import reference_protocol as p
from tests.test_direct_worker_prepare_v2 import WorkerPrepareTests
from tests.test_direct_snapshot_http import SnapshotListenerTests
from tests.test_reference_source_intake import GCP_ID

@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class ObservationRuntimeTests(unittest.TestCase):
    def test_mtls_resume_only_after_consumed_proof_and_exact_inventory(self):
        helper=WorkerPrepareTests();f,kwargs=helper.fixture();self.addCleanup(helper.doCleanups)
        tls=SnapshotListenerTests();tls.setUp();self.addCleanup(tls.doCleanups)
        index,spool,proof=prepare_signed_job(f.ledger,**kwargs)
        boundary=DirectWorkerBoundaryV2(ledger=f.ledger,spool=spool,inventory=index,item=f.item,
            reservation_binding=f.reservation,expected_worker=f.worker,engine_id=kwargs['engine_id'],
            coordinator_origin=kwargs['coordinator_origin'],gate_generation=1,bearer=f.bearer,
            readiness_proof=proof,challenge_id=kwargs['challenge_id'],issuer_id='worker-issuer',key_id='worker-key',
            public_key=f.proof_key.public_key(),closure_expected_binding=f.expected,closure_verification=f.options,
            worker_signer=f.signer)
        grant=DirectWorkerGrant(boundary,coordinator_principal='coord-a')
        grant.authorize('coord-a',f.headers,dict(operationId=str(uuid.uuid4()),item=f.item))
        boundary.put(f.headers,protected_transport=True,object_id=GCP_ID,content_length=0,
                     content_sha256=__import__('hashlib').sha256(b'').hexdigest(),reader=io.BytesIO(b''))
        worker=SimpleNamespace(engine_id=kwargs['engine_id'],origin=kwargs['coordinator_origin'],gate_generation=1)
        policy=dict(directPreparationEnabled=True,capabilityFingerprint=f.options['capability_fingerprint'],
                    optionsFingerprint=f.options['options_fingerprint'],allowedOdmOptions=[])
        driver=SimpleNamespace(worker=worker,direct_source_policy=lambda:policy)
        clients={tls.fingerprint:dict(principal='coord-a',environment='staging')}
        owner=ObservationOwner(ledger=f.ledger,worker=f.worker,driver=driver,mount_path='/tmp',mount_id='a'*32,
            marker_name=f.spool.marker.name,signer=f.proof_key,issuer_id='worker-issuer',key_id='worker-key',
            host='127.0.0.1',port=0,bearer=f.bearer,tls_context=tls.context,clients=clients)
        self.addCleanup(lambda:owner.close(3))
        backend=DirectPreparationAPI(driver=driver,ledger=f.ledger,worker=f.worker,mount_path='/tmp',mount_id='a'*32,
            marker_name=f.spool.marker.name,source_keys=f.options['trusted_source_keys'],signer=f.proof_key,
            issuer_id='worker-issuer',key_id='worker-key',observation_owner=owner)
        control=create_snapshot_server('127.0.0.1',0,enabled=True,ssl_context=tls.context,client_scopes=clients,prepare_backend=backend)
        thread=threading.Thread(target=control.serve_forever);thread.start()
        self.addCleanup(lambda:(control.shutdown(),control.server_close(),thread.join(3)))
        raw=p.canonical_json(dict(item=f.item,challengeId=kwargs['challenge_id'],viewerPrincipal='viewer-staging'))
        status,reply=tls.request(control,cert=tls.good,key=tls.goodkey,path='/internal/v2/worker/resume-observation',body=raw)
        self.assertEqual(status,200);self.assertEqual(reply['state'],'observation_only')
        self.assertFalse(reply['sourceCustodyAccepted'])
        wrong=p.canonical_json(dict(item=f.item,challengeId=kwargs['challenge_id'],viewerPrincipal='wrong-viewer'))
        status,_=tls.request(control,cert=tls.good,key=tls.goodkey,path='/internal/v2/worker/resume-observation',body=wrong)
        self.assertEqual(status,503)
        upload=owner.owner._server
        status,reply=tls.request(upload,cert=tls.good,key=tls.goodkey,method='GET',path='/direct/v2/objects/'+GCP_ID,
                                 body=b'',headers=f.headers)
        self.assertEqual(status,200);self.assertTrue(reply['localObjectPresent']);self.assertFalse(reply['localObjectDurable'])
        with f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_writer_pins').fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM direct_custody_fences').fetchone()[0],0)
        status,_=tls.request(upload,cert=tls.good,key=tls.goodkey,method='PUT',path='/direct/v2/objects/'+GCP_ID,
                             body=b'',headers=dict(f.headers,**{'Content-SHA256':__import__('hashlib').sha256(b'').hexdigest()}))
        self.assertEqual(status,403)
        for method,path,body in (('POST','/direct/v2/complete',b''),('GET','/direct/v2/retention',b''),
                                 ('POST','/direct/v2/retention-challenge',b'a'*64)):
            status,_=tls.request(upload,cert=tls.good,key=tls.goodkey,method=method,path=path,body=body,headers=f.headers)
            self.assertEqual(status,403)
        # Terminate the task-owned listener and rebuild it in a fresh process.
        self.assertTrue(owner.close(3))
        from cryptography.hazmat.primitives import serialization
        signer=f.proof_key.private_bytes(serialization.Encoding.PEM,serialization.PrivateFormat.PKCS8,serialization.NoEncryption()).decode()
        options=dict(f.options,allowed_odm_options=list(f.options['allowed_odm_options']))
        wire=dict(root=str(f.root),ledger=str(f.ledger.path),worker=f.worker,item=f.item,challenge=kwargs['challenge_id'],
                  options=options,signer=signer,origin=kwargs['coordinator_origin'],marker=f.spool.marker.name,
                  cert=str(tls.root/'fixture-cert.pem'),key=str(tls.root/'fixture-key.pem'),ca=str(tls.root/'client-ca.pem'),
                  clients=clients,bearer=f.bearer)
        script=r"""import json,sys,ssl
from types import SimpleNamespace
from cryptography.hazmat.primitives import serialization
from processing.scratch_policy import ScratchLedger
from processing.direct_worker_observation_owner import ObservationOwner
import processing.node_gate as gate
x=json.loads(sys.stdin.readline()); gate.canonical_gate_path=lambda _:__import__('pathlib').Path(x['root'])/'gate'
ledger=ScratchLedger(x['ledger'],x['root'],capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.minimum_version=ssl.TLSVersion.TLSv1_2
ctx.load_cert_chain(x['cert'],x['key']);ctx.load_verify_locations(cafile=x['ca']);ctx.verify_mode=ssl.CERT_REQUIRED
worker=SimpleNamespace(engine_id='fixture-v2',origin=x['origin'],gate_generation=1)
o=ObservationOwner(ledger=ledger,worker=x['worker'],driver=SimpleNamespace(worker=worker),mount_path='/tmp',mount_id='a'*32,
 marker_name=x['marker'],signer=serialization.load_pem_private_key(x['signer'].encode(),password=None),issuer_id='worker-issuer',key_id='worker-key',
 host='127.0.0.1',port=0,bearer=x['bearer'],tls_context=ctx,clients=x['clients'])
x['options']['allowed_odm_options']=set(x['options']['allowed_odm_options'])
o.resume(item=x['item'],challenge=x['challenge'],principal='coord-a',environment='staging',verification=x['options'])
print(o.owner._server.server_port,flush=True);sys.stdin.readline();assert o.close(3)
"""
        child=subprocess.Popen([sys.executable,'-u','-c',script],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            child.stdin.write(json.dumps(wire)+'\n');child.stdin.flush()
            portline=child.stdout.readline()
            if not portline:raise AssertionError(child.stderr.read())
            resumed=SimpleNamespace(server_port=int(portline))
            status,observed=tls.request(resumed,cert=tls.good,key=tls.goodkey,method='GET',path='/direct/v2/objects/'+GCP_ID,body=b'',headers=f.headers)
            self.assertEqual(status,200);self.assertTrue(observed['localObjectPresent']);self.assertFalse(observed['localObjectDurable'])
        finally:
            if child.poll() is None:
                try:child.stdin.write('stop\n');child.stdin.flush();child.wait(timeout=5)
                except Exception:child.kill();child.wait(timeout=5)
            child.stdin.close();child.stdout.close();child.stderr.close()
        self.assertEqual(child.returncode,0)
        # Damaged/lost grant must not be recreated by observation mode.
        import sqlite3
        from processing.scratch_policy import DIRECT_GRANT_TRIGGERS,PolicyConflict
        db=sqlite3.connect(f.ledger.path)
        db.execute('DROP TRIGGER direct_grant_no_delete')
        db.execute('DELETE FROM direct_worker_grants')
        saved=json.loads(db.execute("SELECT value FROM metadata WHERE key='config'").fetchone()[0])
        saved['directGrantCount']=0
        db.execute("UPDATE metadata SET value=? WHERE key='config'",(json.dumps(saved,sort_keys=True,separators=(',',':')),))
        db.execute(DIRECT_GRANT_TRIGGERS[2]);db.commit();db.close()
        boundary._observation_only=True
        with self.assertRaises(PolicyConflict):
            grant.authorize('coord-a',f.headers,dict(operationId=str(uuid.uuid4()),item=f.item))
