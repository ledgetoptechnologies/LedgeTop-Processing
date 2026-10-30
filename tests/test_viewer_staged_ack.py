import base64
import hashlib
import io
import http.client
import json
from pathlib import Path
import shutil
import ssl
import subprocess
import threading
import unittest
import uuid

from processing import reference_protocol as p
from processing.reference_runtime import _DynamicAPI
from processing.reference_http import create_reference_http_server
from processing.viewer_staged_ack import ROUTE,NAMESPACE,ViewerStagedAck
from tests.test_viewer_service import ViewerServiceTests


class StagedAckTests(ViewerServiceTests):
    def test_scoped_idempotent_assertion_binds_exact_task_receipt(self):
        token=base64.urlsafe_b64encode(b'a'*32).decode().rstrip('=')
        cap=base64.urlsafe_b64encode(b'b'*32).decode().rstrip('=')
        identifier=str(uuid.uuid4());self.service.reserve(identifier,'production',cap)
        self.service.initialize(identifier,'synthetic',[],None,'production',cap)
        content=b'synthetic';self.service.upload(identifier,'photo.jpg',io.BytesIO(content),len(content),'production',cap)
        self.service.commit(identifier,'production',cap);self.service.tick();self.finish(identifier);self.service.tick()
        stream,_=self.service.download(identifier);stream.close();task=self.task(identifier)
        receipt=self.service._archive_receipt(task,Path(task['archive_path']).with_suffix('.zip.receipt'))
        value={'schemaVersion':1,'taskId':identifier,'generation':task['generation'],
            'archiveSha256':receipt['sha256'],'archiveBytes':receipt['bytes'],
            'coordinatorReceiptSha256':hashlib.sha256(p.canonical_json(receipt)).hexdigest(),
            'attemptId':str(uuid.uuid4()),'modelId':str(uuid.uuid4()),
            'modelVersionId':str(uuid.uuid4()),'stagingCommitId':str(uuid.uuid4()),
            'issuer':'viewer-a','audience':self.queue.authority_id,'environment':'production','namespace':NAMESPACE}
        other=base64.urlsafe_b64encode(b'd'*32).decode().rstrip('=')
        bindings={token:{'environment':'production','principalId':'viewer-a'},other:{'environment':'production','principalId':'viewer-b'}}
        ack=ViewerStagedAck(self.service,bindings);headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':cap}
        def call(method,request=value,headers=headers):
            return ack.handle(method,ROUTE+identifier+('/generations/'+str(value['generation']) if method=='GET' else ''),headers,p.canonical_json(request) if method=='POST' else b'',protected_transport=True)
        self.assertEqual(call('GET').status,404)
        commitment=ack.handle('GET',ROUTE+identifier+'/commitment',headers,protected_transport=True)
        self.assertEqual(commitment.status,200)
        for key in ('taskId','generation','archiveSha256','archiveBytes','coordinatorReceiptSha256','issuer','audience','environment','namespace'):
            self.assertEqual(commitment.body[key],value[key])
        first=call('POST');self.assertEqual(first.status,200);self.assertTrue(first.body['created'])
        self.assertFalse(call('POST').body['created']);self.assertEqual(call('GET').body['assertion'],value)
        self.assertEqual(call('GET',headers={'Authorization':'Bearer '+other,'X-LedgeTop-Capability':cap}).status,404)
        self.assertEqual(call('POST',dict(value,issuer='viewer-b')).status,409)
        self.assertEqual(call('POST',dict(value,audience=str(uuid.uuid4()))).status,409)
        self.assertEqual(call('POST',dict(value,environment='staging')).status,409)
        self.assertEqual(call('POST',dict(value,namespace='other')).status,409)
        changed=dict(value,stagingCommitId=str(uuid.uuid4()));self.assertEqual(call('POST',changed).status,409)
        changed=dict(value,archiveSha256='0'*64);self.assertEqual(call('POST',changed).status,409)
        second=str(uuid.uuid4());second_cap=base64.urlsafe_b64encode(b'c'*32).decode().rstrip('=')
        self.service.reserve(second,'production',second_cap)
        self.service.initialize(second,'other synthetic',[],None,'production',second_cap)
        self.service.upload(second,'photo.jpg',io.BytesIO(content),len(content),'production',second_cap)
        self.service.commit(second,'production',second_cap);self.service.tick();self.finish(second);self.service.tick()
        stream,_=self.service.download(second);stream.close();second_task=self.task(second)
        second_receipt=self.service._archive_receipt(second_task,Path(second_task['archive_path']).with_suffix('.zip.receipt'))
        reuse=dict(value,taskId=second,generation=second_task['generation'],archiveSha256=second_receipt['sha256'],
            archiveBytes=second_receipt['bytes'],coordinatorReceiptSha256=hashlib.sha256(p.canonical_json(second_receipt)).hexdigest())
        second_headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':second_cap}
        for collision in (dict(reuse,stagingCommitId=str(uuid.uuid4())),dict(reuse,attemptId=str(uuid.uuid4()))):
            self.assertEqual(ack.handle('POST',ROUTE+second,second_headers,p.canonical_json(collision),protected_transport=True).status,409)
        self.assertEqual(call('GET',headers={'Authorization':'Bearer '+token,'X-LedgeTop-Capability':'wrong'}).status,404)
        self.assertEqual(call('GET',headers={'Authorization':'Bearer '+cap,'X-LedgeTop-Capability':cap}).status,401)
        self.assertEqual(ack.handle('GET',ROUTE+identifier,headers,protected_transport=False).status,503)
        self.assertEqual(self.task(identifier)['job']['status'],'succeeded')
        # Lookup survives loss of the local archive receipt; it is only an
        # authenticated stored claim, not proof that Viewer still retains bytes.
        Path(task['archive_path']).with_suffix('.zip.receipt').unlink()
        self.assertEqual(call('GET').body['assertion'],value)
        self.assertEqual(call('POST').status,409)
        self.service.close();self.queue.close();self.make_queue_service()
        rotated=base64.urlsafe_b64encode(b'e'*32).decode().rstrip('=')
        bindings={rotated:{'environment':'production','principalId':'viewer-a'},other:{'environment':'production','principalId':'viewer-b'}}
        ack=ViewerStagedAck(self.service,bindings)
        self.assertEqual(call('GET').status,401)
        headers={'Authorization':'Bearer '+rotated,'X-LedgeTop-Capability':cap}
        self.assertEqual(call('GET',headers=headers).body['assertion'],value)
        with self.queue._db() as db:db.execute('UPDATE viewer_tasks SET generation=generation+1 WHERE uuid=?',(identifier,))
        self.assertEqual(call('GET',headers=headers).body['assertion'],value)
        # The existing reserved-task lifecycle does not expose a successful
        # remove/cancel after completion. Simulate both mutable row changes to
        # test that the stored verifier, not those rows, authorizes lookup.
        with self.queue._db() as db:
            db.execute('UPDATE viewer_tasks SET removed=1 WHERE uuid=?',(identifier,))
            db.execute("UPDATE viewer_reservations SET state='cancelled' WHERE uuid=?",(identifier,))
        self.assertEqual(call('GET',headers=headers).body['assertion'],value)
        self.assertEqual(call('GET',headers={'Authorization':'Bearer '+rotated,'X-LedgeTop-Capability':second_cap}).status,404)
        self.assertEqual(call('GET',headers={'Authorization':'Bearer '+other,'X-LedgeTop-Capability':cap}).status,404)
        api=_DynamicAPI(None,None,None,None,ack)
        server=create_reference_http_server(api,'127.0.0.1',0)
        thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));thread.start()
        try:
            connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
            connection.request('GET',ROUTE+identifier+'/generations/'+str(value['generation']),headers=headers)
            response=connection.getresponse();self.assertEqual(response.status,200)
            self.assertEqual(json.loads(response.read())['assertion'],value);connection.close()
            connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
            connection.putrequest('GET',ROUTE+identifier+'/generations/'+str(value['generation']));connection.putheader('Authorization','Bearer '+token)
            connection.putheader('X-LedgeTop-Capability',cap);connection.putheader('X-LedgeTop-Capability',cap);connection.endheaders()
            response=connection.getresponse();self.assertEqual(response.status,400);response.read();connection.close()
        finally:
            server.shutdown();thread.join(3);server.server_close();self.assertFalse(thread.is_alive())
        if shutil.which('openssl'):
            cert=self.root/'synthetic-cert.pem';key=self.root/'synthetic-key.pem'
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN=127.0.0.1','-addext','subjectAltName=IP:127.0.0.1',
                '-keyout',str(key),'-out',str(cert)],check=True,capture_output=True,timeout=10)
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(cert,key)
            server=create_reference_http_server(api,'127.0.0.1',0,ssl_context=context)
            thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=.01));thread.start()
            try:
                client=ssl.create_default_context(cafile=str(cert))
                connection=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=client,timeout=3)
                connection.request('GET',ROUTE+identifier+'/generations/'+str(value['generation']),headers=headers)
                response=connection.getresponse();self.assertEqual(response.status,200)
                self.assertEqual(json.loads(response.read())['assertion'],value);connection.close()
            finally:
                server.shutdown();thread.join(3);server.server_close();self.assertFalse(thread.is_alive())
        else:
            self.fail('openssl required for the actual TLS listener regression')

    def test_listener_route_is_default_disabled(self):
        api=_DynamicAPI(None,None,None,None)
        self.assertEqual(api.handle('GET',ROUTE+str(uuid.uuid4()),{},protected_transport=True).status,404)

    def test_old_unmigrated_ack_schema_is_refused(self):
        with self.queue._db() as db:
            db.execute('CREATE TABLE viewer_staged_acks (task_id TEXT PRIMARY KEY, assertion_json TEXT NOT NULL)')
        token=base64.urlsafe_b64encode(b'a'*32).decode().rstrip('=')
        with self.assertRaisesRegex(ValueError,'schema requires migration'):
            ViewerStagedAck(self.service,{token:{'environment':'production','principalId':'viewer-a'}})
