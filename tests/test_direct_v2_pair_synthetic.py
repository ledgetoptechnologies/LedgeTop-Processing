"""Synthetic signed admission→held reservation→worker TLS local closure.

No real Viewer or ODM engine runs, and local closure is not a Viewer ACK.
"""
import io
import sqlite3
import ssl
import tempfile
import threading
import unittest
import uuid
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from ops.qualify_enrolled_phase_transport import configuration_material
from processing.direct_v2_reservation import DirectV2Reservations
from processing.direct_worker_http import create_direct_worker_server
from processing.direct_worker_sender_v2 import DirectWorkerSenderV2
from processing.prototype import Queue
from processing.reference_source_intake import validate_source_intake
from processing.reference_store import PROFILE, ReferenceStore
from tests import test_direct_worker_receiver_v2 as fixture
from tests import test_reference_source_intake as source_fixture


class DirectV2PairSyntheticTests(unittest.TestCase):
    def test_signed_admission_reservation_and_tls_local_closure(self):
        self.run_pair(False)

    def test_lost_local_closure_response_remains_held(self):
        self.run_pair(True)

    def test_lost_object_observation_recovers_without_reupload(self):
        self.run_pair(False,True)

    def run_pair(self, lose_closure, lose_object=False):
        f=fixture.WorkerV2Tests();f.setUp();self.addCleanup(f.doCleanups)
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with closing(sqlite3.connect(root/'jobs.sqlite3')) as db, db:
                db.execute('CREATE TABLE coordinator_identity(singleton INTEGER PRIMARY KEY,authority_id TEXT NOT NULL)')
                db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(f.worker['authorityId'],))
            queue=Queue(root,reference_enabled=True);self.addCleanup(queue.close)
            queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue,source_routing_keys=f.options['trusted_source_keys'])
            opts=f.options
            validated=validate_source_intake(f.payload,environment='staging',
                capability_fingerprint=opts['capability_fingerprint'],options_fingerprint=opts['options_fingerprint'],
                allowed_odm_options=opts['allowed_odm_options'],trusted_source_keys=opts['trusted_source_keys'],
                authority_id=opts['authority_id'])
            job,created=store.submit_source_intake(validated,capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'],
                source_spool=None,trusted_source_keys=opts['trusted_source_keys'],authority_id=opts['authority_id'],
                ingress_mode='direct-v2',authenticated_principal='viewer-staging')
            self.assertTrue(created);self.assertEqual(job['status'],'held')
            workers={node:dict(identity=dict(f.worker,nodeId=node),issuerId='worker-issuer',keyId='worker-key',
                               publicKey=f.proof_key.public_key()) for node in queue.node_ids}
            reservation=DirectV2Reservations(store,trusted_workers=workers).reserve(payload=f.payload,
                authenticated_principal='viewer-staging',item=f.item,expected_worker=f.worker,
                readiness_proof=f.readiness,challenge_id=f.challenge,gate_generation=1,
                issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                trusted_source_keys=opts['trusted_source_keys'],capability_fingerprint=opts['capability_fingerprint'],
                options_fingerprint=opts['options_fingerprint'],allowed_odm_options=opts['allowed_odm_options'])
            self.assertEqual(reservation['nodeId'],'cluster');self.assertFalse(reservation['sourceCustodyAccepted'])
            material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))['configurationFiles']
            for name in ('fixture-ca.pem','fixture-cert.pem','fixture-key.pem'):(root/name).write_bytes(material[name])
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
            server=create_direct_worker_server(f.boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                sender=DirectWorkerSenderV2(origin='https://127.0.0.1:'+str(server.server_port),
                    connect_ip='127.0.0.1',ca_file=str(root/'fixture-ca.pem'),bearer=f.bearer,
                    inventory=f.index,item=f.item,reservation_binding=f.reservation,expected_worker=f.worker,
                    issuer_id='worker-issuer',key_id='worker-key',worker_public_key=f.proof_key.public_key(),
                    idle_timeout=2,total_timeout=5)
                from processing.direct_v2_handoff_journal import DirectV2Handoff
                token='synthetic-viewer-handoff-token-0123456789'
                handoff=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                for object_id,data in ((source_fixture.GCP_ID,b''),(source_fixture.IMAGE_ID,b'abc'),
                                       ('gcp_'+f.item['attemptId'],b'1,2,3\n')):
                    if lose_object:
                        from processing.direct_worker_sender import DirectDeliveryUncertain
                        original_put=sender.put
                        def lose_after_worker_commit(*args,**kwargs):
                            original_put(*args,**kwargs)
                            raise DirectDeliveryUncertain('synthetic lost object observation')
                        with patch.object(sender,'put',side_effect=lose_after_worker_commit):
                            with self.assertRaises(DirectDeliveryUncertain):
                                handoff.send_object(token,protected_transport=True,object_id=object_id,reader=io.BytesIO(data))
                        with queue._db() as db:
                            self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_objects').fetchone()[0],'intent')
                        handoff=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                        with patch.object(sender,'put',side_effect=AssertionError('must not reupload')):
                            observed=handoff.reconcile_object(token,protected_transport=True,object_id=object_id)
                        lose_object=False
                    else:
                        observed=handoff.send_object(token,protected_transport=True,object_id=object_id,reader=io.BytesIO(data))
                    self.assertTrue(observed['localObjectDurable']);self.assertFalse(observed['sourceCustodyAccepted'])
                if lose_closure:
                    from processing.direct_worker_sender import DirectDeliveryUncertain
                    original=sender.complete
                    def lose_after_worker_commit():
                        original()
                        raise DirectDeliveryUncertain('synthetic lost closure observation')
                    with patch.object(sender,'complete',side_effect=lose_after_worker_commit):
                        with self.assertRaises(DirectDeliveryUncertain):handoff.complete(token,protected_transport=True)
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_sessions').fetchone()[0],'closure_pending')
                    handoff=DirectV2Handoff(store,sender,principal='viewer-staging',bearer=token)
                    recovered=handoff.complete(token,protected_transport=True)
                    self.assertFalse(recovered['closure']['sourceCustodyAccepted'])
                    with queue._db() as db:
                        self.assertEqual(db.execute('SELECT state FROM direct_v2_handoff_sessions').fetchone()[0],'closure_observed')
                    self.assertEqual(queue.get(job['jobId'])['status'],'held')
                    self.assertTrue((f.spool.root.parent/'direct-v2-worker-signed-closure.json').exists())
                    return
                closure=handoff.complete(token,protected_transport=True)
                self.assertEqual(handoff.complete(token,protected_transport=True),closure)
                self.assertEqual((closure['closure']['sourceObjectCount'],closure['closure']['derivedObjectCount']),(2,1))
                self.assertFalse(closure['closure']['sourceCustodyAccepted'])
                with queue._db() as db:
                    self.assertEqual(db.execute("SELECT state FROM direct_v2_handoff_sessions").fetchone()[0],'closure_observed')
                    self.assertEqual(db.execute("SELECT count(*) FROM direct_v2_handoff_objects WHERE state='observed'").fetchone()[0],3)
                self.assertEqual(queue.get(job['jobId'])['status'],'held')
            finally:
                server.shutdown();server.server_close();thread.join(3);self.assertFalse(thread.is_alive())
                queue.close()
