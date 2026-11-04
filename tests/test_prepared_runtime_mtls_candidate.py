import hashlib
import http.client
import json
import ssl
import subprocess
import unittest
import uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from processing.reference_runtime import ReferenceRuntime, RuntimeConfigurationError, load_reference_settings
from tests.test_reference_inbound_mtls_candidate import InboundMTLSTests


class PreparedRuntimeTLS(unittest.TestCase):
    def setUp(self):
        self.fixture=InboundMTLSTests('test_default_preserves_tls_without_client_certificate')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)
        self.root=self.fixture.root
        self.fixture.token='ordinary-viewer-'+'x'*32
        self.production='prepared-production-'+'p'*32;self.staging='prepared-staging-'+'s'*32
        self.other_principal='prepared-other-'+'q'*32
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
            '-subj','/CN=third','-addext','extendedKeyUsage=clientAuth','-keyout',str(self.root/'third.key'),
            '-out',str(self.root/'third.pem')],check=True,capture_output=True,timeout=15)
        bundle=self.root/'prepared-ca.pem'
        bundle.write_text(''.join((self.root/(name+'.pem')).read_text() for name in ('client','other','next','third')))
        fingerprint=lambda name:hashlib.sha256(ssl.PEM_cert_to_DER_cert((self.root/(name+'.pem')).read_text())).hexdigest()
        raw=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        caps=raw['capabilities'];self.submission=raw['submission'];self.submission['submissionId']=str(uuid.uuid4())
        legacy=dict(capabilityFingerprint=caps['capabilityFingerprint'],optionsFingerprint=caps['engines']['odm']['optionsFingerprint'],allowedOdmOptions=['resolution'],availableEngines=['odm'])
        typed=dict(capabilityFingerprint=legacy['capabilityFingerprint'],optionsFingerprint=legacy['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}}})
        principal=[dict(token=self.production,principalId='viewer-a',environment='production'),dict(token=self.staging,principalId='viewer-a',environment='staging'),dict(token=self.other_principal,principalId='viewer-b',environment='staging')]
        prepared=dict(authorityId=self.fixture.queue.authority_id,principals=principal,legacyPolicies={'production':legacy,'staging':legacy},jcsPolicies={'production':typed,'staging':typed})
        tokens=[('client',self.fixture.token),('other',self.staging),('next',self.production),('third',self.other_principal)]
        bindings=[dict(certificateSha256=fingerprint(name),bearer=token) for name,token in tokens]
        bindings.extend(dict(certificateSha256=str(i+1)*64,bearer=worker['bearer']) for i,worker in enumerate(self.fixture.workers.values()))
        config=dict(schemaVersion=6,listener={**self.fixture.listener,'port':19443},viewerBearers={'staging':self.fixture.token},workers=self.fixture.workers,
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=1000),inboundClientCA=str(bundle),inboundClientBindings=bindings,preparedViewer=prepared)
        path=self.root/'prepared-runtime.json';path.write_text(json.dumps(config));path.chmod(0o600)
        self.settings=load_reference_settings(str(path),self.fixture.config)
        self.settings=replace(self.settings,listener=dict(self.settings.listener,port=0))

    def request(self,runtime,cert,token,path,value=None):
        context=ssl.create_default_context(cafile=str(self.root/'server.pem'))
        context.load_cert_chain(str(self.root/(cert+'.pem')),str(self.root/(cert+'.key')))
        conn=http.client.HTTPSConnection('127.0.0.1',runtime._server.server_port,context=context,timeout=3)
        try:
            headers={'Authorization':'Bearer '+token,'X-LTDS-Expected-Coordinator-Authority':self.fixture.queue.authority_id}
            body=None if value is None else json.dumps(value)
            conn.request('GET' if value is None else 'POST',path,body,headers)
            response=conn.getresponse();raw=response.read();self.assertNotIn(token.encode(),raw)
            return response.status,json.loads(raw)
        finally:conn.close()

    def test_guarded_real_tls_principals_and_held_activation(self):
        with self.assertRaises(RuntimeConfigurationError):ReferenceRuntime(self.fixture.queue,self.fixture.config,self.settings)
        with patch('processing.reference_runtime.PREPARED_RUNTIME_SCHEMA_QUALIFIED',True):
            runtime=ReferenceRuntime(self.fixture.queue,self.fixture.config,self.settings,discover=lambda:{'ready':False})
        runtime.start();self.addCleanup(runtime.close)
        env=dict(schemaVersion=3,protocol='ltds-viewer-prepared-envelope-jcs-v3',submission=self.submission,
            viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        path='/ltds/prepared/v3/submissions'
        status,job=self.request(runtime,'other',self.staging,path,env);self.assertEqual(status,201,job)
        self.assertEqual(self.request(runtime,'next',self.staging,path,env)[0],401)
        self.assertEqual(self.request(runtime,'other',self.production,path,env)[0],401)
        self.assertEqual(self.request(runtime,'other','unknown-token-'+'x'*32,path,env)[0],401)
        # Properly bound production principal cannot read staging submission.
        status_path=path+'/'+self.submission['submissionId']+'/status'
        self.assertEqual(self.request(runtime,'next',self.production,status_path)[0],404)
        self.assertEqual(self.request(runtime,'third',self.other_principal,status_path)[0],404)
        self.assertEqual(self.request(runtime,'other',self.staging,status_path)[0],200)
        stored=runtime.store.find_viewer_envelope_v2(self.submission['submissionId'],authenticated_principal='viewer-a',authenticated_environment='staging',expected_protocol=env['protocol'])
        activation=dict(activationId=env['activationId'],viewerTaskId=env['viewerTaskId'],viewerAttemptId=env['viewerAttemptId'],envelopeSha256=stored['envelopeSha256'])
        self.assertEqual(self.request(runtime,'other',self.staging,path+'/'+self.submission['submissionId']+'/activation',activation)[0],503)

    def test_default_runtime_does_not_mount_prepared(self):
        runtime=self.fixture.runtime();self.assertEqual(self.request(runtime,'client',self.fixture.token,'/ltds/prepared/v3/submissions',{})[0],404)


if __name__=='__main__':unittest.main()
