import base64
import hashlib
import http.client
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from processing.config import Config
from processing.prototype import Queue
from processing.reference_runtime import ReferenceRuntime, ReferenceSettings


@unittest.skipUnless(shutil.which('openssl'),'synthetic local certificate tool required')
class InboundMTLSTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        for name,extension in (('server','subjectAltName=IP:127.0.0.1'),('client','extendedKeyUsage=clientAuth'),('other','extendedKeyUsage=clientAuth')):
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN='+name,'-addext',extension,'-keyout',str(self.root/(name+'.key')),
                '-out',str(self.root/(name+'.pem'))],check=True,capture_output=True,timeout=15)
        self.config=Config(api_token='legacy-'+'a'*32,local_node_url='http://127.0.0.1:19001',
            main_node_url='http://127.0.0.1:19002',local_max_photos=1000,local_max_input_bytes=20*1024**3,
            prototype_state_dir=str(self.root/'state'),prototype_node_ids=('cluster','main'),treedar_node_ids=('main',))
        self.queue=Queue(self.config.prototype_state_dir,node_ids=('cluster','main'),reference_enabled=True)
        self.addCleanup(self.queue.close)
        self.listener=dict(host='127.0.0.1',port=0,certificate=str(self.root/'server.pem'),
            privateKey=str(self.root/'server.key'),clientCA=str(self.root/'server.pem'))
        self.workers={name:dict(bearer='worker-'+name+'x'*32,workerId='worker-'+name,instanceId='instance-'+name,
            resultOrigin='https://'+name+'.example',resultBearer='read-'+name+'x'*32) for name in ('cluster','main')}
        self.token='viewer-token-'+'x'*32

    def runtime(self, inbound=None):
        pins={} if inbound is None else dict(inbound_ca_sha256=hashlib.sha256(Path(inbound).read_bytes()).hexdigest(),
            outbound_ca_sha256=hashlib.sha256(Path(self.listener['clientCA']).read_bytes()).hexdigest())
        settings=ReferenceSettings(self.listener,{self.token:'staging'},self.workers,dict(maxConcurrent=1,maxObjectBytes=1000),inbound_client_ca=inbound,**pins)
        runtime=ReferenceRuntime(self.queue,self.config,settings,discover=lambda:{'ready':False})
        runtime.start();self.addCleanup(runtime.close)
        return runtime

    def request(self,runtime,cert=None,token=None):
        context=ssl.create_default_context(cafile=str(self.root/'server.pem'))
        if cert:context.load_cert_chain(str(self.root/(cert+'.pem')),str(self.root/(cert+'.key')))
        conn=http.client.HTTPSConnection('127.0.0.1',runtime._server.server_port,context=context,timeout=3)
        try:
            conn.request('GET','/ltds/v1/capabilities',headers={'Authorization':'Bearer '+(token or self.token)})
            response=conn.getresponse();response.read();return response.status
        finally:conn.close()

    def test_explicit_inbound_ca_requires_trusted_client_and_bearer(self):
        runtime=self.runtime(str(self.root/'client.pem'))
        self.assertEqual(runtime._server_context.verify_mode,ssl.CERT_REQUIRED)
        self.assertNotEqual(self.request(runtime,'client'),401)
        self.assertEqual(self.request(runtime,'client','wrong-token-xxxxxxxxxxxxxxxxxxxx'),401)
        for cert in (None,'other'):
            with self.subTest(cert=cert),self.assertRaises((ssl.SSLError,ConnectionError,OSError)):
                self.request(runtime,cert)

    def test_swapped_disjoint_inbound_ca_after_settings_is_refused(self):
        from dataclasses import replace
        from processing.reference_runtime import RuntimeConfigurationError, load_reference_settings
        inbound=self.root/'client.pem'
        configfile=self.root/'reference.json'
        configfile.write_text(json.dumps(dict(schemaVersion=6,listener={**self.listener,'port':19443},
            viewerBearers={'staging':self.token},workers=self.workers,
            streamBudget=dict(maxConcurrent=1,maxObjectBytes=1000),inboundClientCA=str(inbound))))
        if os.name!='nt':configfile.chmod(0o600)
        settings=load_reference_settings(str(configfile),self.config)
        settings=replace(settings,listener=dict(settings.listener,port=0))
        # The replacement is also disjoint from the outbound CA; the saved
        # config-load fingerprint must still prevent silently changing trust.
        inbound.write_bytes((self.root/'other.pem').read_bytes())
        with self.assertRaises(RuntimeConfigurationError):
            ReferenceRuntime(self.queue,self.config,settings,discover=lambda:{'ready':False})

    def test_default_preserves_tls_without_client_certificate(self):
        runtime=self.runtime()
        self.assertEqual(runtime._server_context.verify_mode,ssl.CERT_NONE)
        self.assertNotEqual(self.request(runtime),401)
        self.assertEqual(self.request(runtime,token='wrong-token-xxxxxxxxxxxxxxxxxxxx'),401)


@unittest.skipUnless(shutil.which('openssl'),'synthetic local certificate tool required')
class InboundConfigurationTests(unittest.TestCase):
    def test_explicit_v6_and_default_config_validation(self):
        # Reuse the existing config fixture without its runtime or servers.
        from tests.test_reference_runtime import ReferenceRuntimeTests
        from processing.reference_runtime import RuntimeConfigurationError
        fixture=ReferenceRuntimeTests('test_plaintext_or_missing_tls_configuration_cannot_construct_listener')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        self.assertIsNone(fixture.settings().inbound_client_ca)
        fixture.value['schemaVersion']=6
        fixture.write_config()
        with self.assertRaises(RuntimeConfigurationError):fixture.settings()
        fixture.value['inboundClientCA']=fixture.value['listener']['clientCA']
        fixture.write_config()
        with self.assertRaises(RuntimeConfigurationError):fixture.settings()
        for name in ('inbound','outbound'):
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1',
                '-subj','/CN='+name,'-keyout',str(fixture.root/(name+'.key')),
                '-out',str(fixture.root/(name+'.pem'))],check=True,capture_output=True,timeout=15)
        separate=fixture.root/'inbound.pem'
        outbound=fixture.root/'outbound.pem'
        fixture.value['listener']['clientCA']=str(outbound)
        duplicate=fixture.root/'duplicate.pem';duplicate.write_bytes(outbound.read_bytes())
        fixture.value['inboundClientCA']=str(duplicate);fixture.write_config()
        with self.assertRaises(RuntimeConfigurationError):fixture.settings()
        # Different certificates with the same CA public key still overlap.
        same_key=fixture.root/'same-key.pem'
        subprocess.run(['openssl','req','-x509','-key',str(fixture.root/'outbound.key'),'-days','1',
            '-subj','/CN=other-certificate','-out',str(same_key)],check=True,capture_output=True,timeout=15)
        fixture.value['inboundClientCA']=str(same_key);fixture.write_config()
        with self.assertRaises(RuntimeConfigurationError):fixture.settings()
        fixture.value['inboundClientCA']=str(separate);fixture.write_config()
        self.assertEqual(fixture.settings().inbound_client_ca,str(separate))
        # Version 6 may retain the entire v5 feature set and recovery binding.
        from tests.viewer_receipt_signer import SyntheticViewerSigner
        signer=SyntheticViewerSigner()
        secret=fixture.root/'viewer-secret';secret.write_text('synthetic-viewer-service-key-material')
        if os.name!='nt':secret.chmod(0o600)
        fixture.value=fixture.recovery_config();fixture.value.update(schemaVersion=6,
            inboundClientCA=str(separate),viewerReceiptKeys=signer.trusted_keys,
            viewerAcceptance={'staging':{'origin':'https://viewer.example','keyId':'viewer-service-v1','secretFile':str(secret)}},
            stagedAckPrincipals=[{'environment':'staging','principalId':'viewer-a','bearer':base64.urlsafe_b64encode(b'z'*32).decode().rstrip('=')}])
        fixture.write_config();loaded=fixture.settings()
        self.assertIsNotNone(loaded.recovery_bindings);self.assertTrue(loaded.viewer_receipt_keys)
        self.assertTrue(loaded.viewer_acceptance_targets);self.assertTrue(loaded.staged_ack_bearers)
        fixture.value['schemaVersion']=1;fixture.write_config()
        with self.assertRaises(RuntimeConfigurationError):fixture.settings()
        from processing.reference_runtime import _read_ca_bundle
        if os.name!='nt':
            link=fixture.root/'ca-link.pem';link.symlink_to(separate)
            with self.assertRaises(RuntimeConfigurationError):_read_ca_bundle(str(link))
        # Replace after lstat but before open: fd/path identity mismatch holds.
        replacement=fixture.root/'replacement.pem';replacement.write_bytes(outbound.read_bytes())
        original_open=os.open
        def swap(path,flags,*args,**kwargs):
            if str(path)==str(separate):os.replace(replacement,separate)
            return original_open(path,flags,*args,**kwargs)
        with patch('processing.reference_runtime.os.open',side_effect=swap):
            with self.assertRaises(RuntimeConfigurationError):_read_ca_bundle(str(separate))


if __name__=='__main__':unittest.main()
