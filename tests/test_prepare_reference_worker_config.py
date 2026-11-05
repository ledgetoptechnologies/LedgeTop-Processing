"""Private synthetic fixtures only; never read installed credentials/services."""
import copy
from datetime import datetime,timedelta,timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
import uuid
from unittest.mock import patch

from cryptography import x509
from cryptography.hazmat.primitives import hashes,serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID,ExtendedKeyUsageOID
from ops import prepare_reference_worker_config as prep
from ops import enroll_reference_worker as install


@unittest.skipUnless(os.name=='posix' and os.geteuid()!=0,'owner-private Linux fixture')
class PrivateWorkerPreparationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700);self.home=self.root/'home';self.home.mkdir(mode=0o700)
        self.install=self.root/'opt';self.install.mkdir(mode=0o700)
        p=patch.object(install,'INSTALL_BASE',self.install);p.start();self.addCleanup(p.stop)
        self.now=datetime.now(timezone.utc);self.op=str(uuid.uuid4());self.authority=str(uuid.uuid4())
        self.engine='actual.test-engine:abc';self.source=self.file('source.tar',b'synthetic-pinned-source')
        self.source.update(commit='1'*40,footprintSha256='2'*64,requirementsSha256='3'*64)
        self.deps=self.file('dependencies.tar',b'synthetic-pinned-deps')
        self.deps.update(pythonSha256='4'*64,pythonVersion='3.10.12',versions=dict(cryptography='50.0.2',
            cffi='2.1.1',rfc8785='0.1.4',pycparser='2.23'))
        gate=self.home/'.local/state/LedgeTop-Processing/node-gates'/hashlib.sha256(self.engine.encode()).hexdigest()/'gate.json'
        gate.parent.mkdir(parents=True,mode=0o700)
        self.gate=dict(schema=1,engineId=self.engine,nodeId='main',authorityId=self.authority,generation=7,
            coordinatorOrigin='http://127.0.0.1:18080',reservation=None)
        gate.write_bytes(prep.canonical(self.gate));gate.chmod(0o600)
        self.output=self.root/'new-private'
        self.m=dict(schemaVersion=1,protocol=install.PROTOCOL,operationId=self.op,
            host=dict(hostname='fake-worker',bootId=str(uuid.uuid4()),dockerEngineId=self.engine),
            service=dict(name='fixture',uid=os.getuid(),gid=os.getgid(),home=str(self.home)),
            installationRoot=str(self.install/self.op),source=self.source,dependencies=self.deps,
            config=dict(path=str(self.output/'worker-config.json'),sha256='0'*64,bytes=1,ownerUid=os.getuid()),
            unitSha256='0'*64,gate=dict(path=str(gate),sha256=hashlib.sha256(gate.read_bytes()).hexdigest(),nodeId='main',
                authorityId=self.authority,generation=7,coordinatorOrigin=self.gate['coordinatorOrigin']),
            engine=dict(containerId='5'*64,imageId='sha256:'+'6'*64,version='3.8.5',loopbackPort=13004,catalogSha256='7'*64))
        self.m['unitSha256']=hashlib.sha256(install.render_unit(self.m).encode()).hexdigest()
        self.runtime=dict(schemaVersion=6,listener=dict(host='0.0.0.0',port=8443,certificate='/run/public.crt',
            privateKey='/run/private.key',clientCA='/run/ca.crt'),viewerBearers=dict(production='viewer-'+'v'*40),
            workers=dict(main=dict(bearer='claim-'+'c'*40,workerId='existing-worker',instanceId='original-instance',
                resultOrigin='https://192.0.2.8:18445',resultBearer='reader-'+'r'*40)),
            recoveryBindings=dict(main=dict(engineId=self.engine,authorityId=self.authority,
                coordinatorOrigin=self.gate['coordinatorOrigin'],gateGeneration=7,outputBudget=123456)),viewerReceiptKeys={})
        self.ca_key=ec.generate_private_key(ec.SECP256R1());self.ca=self.certificate('CA',self.ca_key,self.ca_key,None,None,ca=True)
        workerkey=ec.generate_private_key(ec.SECP256R1());worker=self.certificate('worker',workerkey,self.ca_key,self.ca,['192.0.2.8','127.0.0.1'])
        coordkey=ec.generate_private_key(ec.SECP256R1());coord=self.certificate('coordinator',coordkey,self.ca_key,self.ca,['192.0.2.9'])
        pem=lambda c:c.public_bytes(serialization.Encoding.PEM)
        self.spec=dict(schemaVersion=1,protocol=prep.PROTOCOL,manifest=self.m,
            runtime=self.file('runtime.json',prep.canonical(self.runtime)),
            nativeCredential=dict(artifact=self.file('native.env',b'NATIVE_TOKEN=native-synthetic-value\n'),key='NATIVE_TOKEN'),
            tls=dict(ca=self.file('ca.crt',pem(self.ca)),certificate=self.file('worker.crt',pem(worker)),
                privateKey=self.file('worker.key',workerkey.private_bytes(serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,serialization.NoEncryption()))),
            coordinator=dict(origin='https://192.0.2.9:18443',listenerPort=8443,certificate=self.file('coord.crt',pem(coord))),
            listener=dict(host='192.0.2.8',port=18445),policy=dict(safetyFloor=1,transferTtl=3600,cleanupGrace=30,
                pollSeconds=1,allowedEnvironments=['production','staging'],cleanupEnabled=False,
                engineCleanupEnabled=False,sourceRangeV2Enabled=True,minimumTlsValiditySeconds=3600),outputDirectory=str(self.output))

    def certificate(self,cn,key,issuerkey,issuer,ips,ca=False,expires=None):
        name=x509.Name([x509.NameAttribute(NameOID.COMMON_NAME,cn)])
        b=x509.CertificateBuilder().subject_name(name).issuer_name(issuer.subject if issuer else name).public_key(key.public_key())
        b=b.serial_number(x509.random_serial_number()).not_valid_before(self.now-timedelta(days=1)).not_valid_after(expires or self.now+timedelta(days=10))
        b=b.add_extension(x509.BasicConstraints(ca=ca,path_length=None),critical=True)
        if not ca:
            b=b.add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),critical=False)
            b=b.add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address(i)) for i in ips]),critical=False)
        return b.sign(issuerkey,hashes.SHA256())

    def file(self,name,raw):
        p=self.root/name;p.write_bytes(raw);p.chmod(0o600)
        return dict(path=str(p),sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw),ownerUid=os.getuid())

    def update_runtime(self):self.spec['runtime']=self.file('runtime.json',prep.canonical(self.runtime))

    def test_real_load_config_and_manifest_preserve_original_identity_without_effects(self):
        with patch('ops.reference_worker_daemon.build_worker',side_effect=AssertionError('factory forbidden')):
            receipt=prep.prepare(self.spec,now=self.now)
        config=json.loads((self.output/'worker-config.json').read_bytes())
        m=json.loads((self.output/'enrollment-manifest.json').read_bytes());install.validate_manifest(m)
        self.assertEqual(config['workerId'],'existing-worker');self.assertEqual(config['workerInstanceId'],'original-instance')
        self.assertEqual(config['gateGeneration'],7);self.assertEqual(config['authorityId'],self.authority)
        self.assertEqual(config['outputBudget'],123456);self.assertEqual(config['workerBearer'],self.runtime['workers']['main']['bearer'])
        self.assertEqual(config['resultReadBearer'],self.runtime['workers']['main']['resultBearer'])
        self.assertNotIn(config['maintenanceReadiness']['bearer'],str(receipt))
        self.assertNotEqual(config['maintenanceReadiness']['bearer'],config['workerBearer'])
        self.assertIsNone(config['maintenanceReadiness']['activation']);self.assertTrue(receipt['preparationOnly'])
        self.assertEqual(set(receipt),{'schemaVersion','protocol','workerConfigSha256','manifestSha256','unitSha256','preparationOnly'})
        self.assertFalse(Path(config['stateRoot']).exists());self.assertFalse((self.home/'.config').exists())
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode),0o700)
        self.assertTrue(all(stat.S_IMODE(p.stat().st_mode)==0o600 for p in self.output.iterdir()))

    def test_existing_output_is_never_overwritten(self):
        self.output.mkdir(mode=0o700);p=self.output/'worker-config.json';p.write_bytes(b'original')
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertEqual(p.read_bytes(),b'original')

    def test_changed_runtime_hash_fails_before_any_output(self):
        Path(self.spec['runtime']['path']).write_bytes(b'{}')
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())

    def test_gate_generation_and_physical_engine_mismatch_refused(self):
        for field,value in [('gateGeneration',8),('engineId','foreign')]:
            with self.subTest(field=field):
                original=self.runtime['recoveryBindings']['main'][field]
                self.runtime['recoveryBindings']['main'][field]=value;self.update_runtime()
                with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
                self.assertFalse(self.output.exists());self.runtime['recoveryBindings']['main'][field]=original

    def test_changed_original_gate_snapshot_refused(self):
        Path(self.m['gate']['path']).write_bytes(b'{}')
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())

    def test_native_role_collision_is_not_reused(self):
        self.spec['nativeCredential']['artifact']=self.file('native.env',('NATIVE_TOKEN='+self.runtime['workers']['main']['bearer']).encode())
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())

    def test_worker_result_role_collision_is_not_reused(self):
        self.runtime['workers']['main']['resultBearer']=self.runtime['workers']['main']['bearer'];self.update_runtime()
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)

    def test_wrong_worker_or_coordinator_san_refused(self):
        for section in ('listener','coordinator'):
            with self.subTest(section=section):
                spec=copy.deepcopy(self.spec)
                if section=='listener':
                    spec['listener']['host']='192.0.2.7'
                    # Keep role listener coherent to reach physical SAN check.
                    runtime=copy.deepcopy(self.runtime);runtime['workers']['main']['resultOrigin']='https://192.0.2.7:18445'
                    spec['runtime']=self.file('runtime2.json',prep.canonical(runtime))
                else:spec['coordinator']['origin']='https://192.0.2.7:18443'
                with self.assertRaises(Exception):prep.prepare(spec,now=self.now)
                self.assertFalse(self.output.exists())

    def test_expired_certificate_refused(self):
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now+timedelta(days=11))
        self.assertFalse(self.output.exists())

    def test_changed_archive_and_unsafe_tls_mode_fail_before_output(self):
        Path(self.source['path']).write_bytes(b'changed')
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())
        self.source.update(self.file('source.tar',b'synthetic-pinned-source'))
        Path(self.spec['tls']['privateKey']['path']).chmod(0o644)
        with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())

    def test_unknown_spec_and_policy_fields_fail_closed(self):
        for part in ('spec','policy'):
            spec=copy.deepcopy(self.spec);(spec if part=='spec' else spec['policy'])['unexpected']=True
            with self.assertRaises(Exception):prep.prepare(spec,now=self.now)
            self.assertFalse(self.output.exists())

    def test_readiness_role_collision_does_not_publish(self):
        with patch.object(prep.secrets,'token_urlsafe',return_value=self.runtime['workers']['main']['bearer']):
            with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse(self.output.exists())

    def test_parent_swap_does_not_redirect_private_writes(self):
        foreign=self.root/'foreign';foreign.mkdir(mode=0o700)
        parent=self.root/'parent';parent.mkdir(mode=0o700);self.output=parent/'new'
        self.spec['outputDirectory']=str(self.output);self.m['config']['path']=str(self.output/'worker-config.json')
        realopen=os.open;moved=self.root/'moved';done=False
        def swap(path,*args,**kwargs):
            nonlocal done
            if Path(path)==parent and not done:
                done=True;parent.rename(moved);parent.symlink_to(foreign,target_is_directory=True)
            return realopen(path,*args,**kwargs)
        with patch.object(prep.os,'open',side_effect=swap):
            with self.assertRaises(Exception):prep.prepare(self.spec,now=self.now)
        self.assertFalse((foreign/'new').exists());self.assertFalse((moved/'new').exists())


if __name__=='__main__':unittest.main()
