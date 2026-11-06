"""Actual private runtime parser with fixture credentials; no Queue/factory."""
import copy
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from ops.qualify_enrolled_phase_transport import configuration_material
from processing.config import from_environment
from processing.prototype import Queue
from processing.reference_runtime import load_reference_settings,ReferenceRuntime,RuntimeConfigurationError


@unittest.skipUnless(os.name=='posix','real Linux tmpfs/source-spool validation')
class PhaseConfigurationValidationTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(os.path.ismount('/tmp'),'run in bounded tmpfs controller')
        self.temp=tempfile.TemporaryDirectory(dir='/tmp');self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.material=configuration_material(str(uuid.uuid4()),str(uuid.uuid4()))
        for name in ('fixture-key.pem','fixture-cert.pem','fixture-ca.pem'):
            path=self.root/name;path.write_bytes(self.material['configurationFiles'][name]);path.chmod(0o600)
        marker_name='.phase-config-'+uuid.uuid4().hex
        self.marker=Path('/tmp')/marker_name;self.marker.write_bytes(b'a'*64);self.marker.chmod(0o600)
        self.addCleanup(lambda:self.marker.unlink(missing_ok=True))
        self.spool=self.root/'spool';self.spool.mkdir(mode=0o700)
        self.environment=dict(self.material['credentials'],PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',
            PROCESSING_PROTOTYPE_STATE_DIR=str(self.root/'unused-state'),
            PROCESSING_PROTOTYPE_OBSERVER_TOKEN='synthetic-observer-'+'o'*32,
            LOCAL_NODE_TOKEN='synthetic-local-'+'l'*32,MAIN_NODE_TOKEN='synthetic-main-'+'m'*32,
            LOCAL_NODE_URL='http://127.0.0.1:19001',MAIN_NODE_URL='http://127.0.0.1:19002')
        with patch.dict(os.environ,self.environment,clear=True):self.config=from_environment()
        self.values={role:copy.deepcopy(self.material[role]) for role in ('bootstrap','final')}
        for value in self.values.values():
            value['listener'].update(certificate=str(self.root/'fixture-cert.pem'),
                privateKey=str(self.root/'fixture-key.pem'),clientCA=str(self.root/'fixture-ca.pem'))
        self.values['final']['sourceSpool'].update(root=str(self.spool),mountPath='/tmp',mountId='a'*64,markerName=marker_name)

    def validate(self,role,value=None):
        path=self.root/(role+'.json');path.write_text(json.dumps(self.values[role] if value is None else value));path.chmod(0o600)
        with patch.object(Queue,'__init__',side_effect=AssertionError('Queue creation forbidden')),\
             patch.object(ReferenceRuntime,'__init__',side_effect=AssertionError('runtime factory forbidden')):
            return load_reference_settings(str(path),self.config)

    def test_generated_bootstrap_and_final_accept_all_disjoint_actual_roles(self):
        native={self.config.api_token,self.config.prototype_operator_token,self.config.prototype_observer_token,
            self.config.viewer_token,self.config.viewer_staging_token,self.config.local_node_token,self.config.main_node_token}-{''}
        reference=list(self.values['bootstrap']['viewerBearers'].values())
        for worker in self.values['bootstrap']['workers'].values():reference.extend((worker['bearer'],worker['resultBearer']))
        self.assertEqual(len(reference),len(set(reference)))
        self.assertFalse(bool(set(reference)&native),'credential roles overlap; values suppressed')
        for role in ('bootstrap','final'):
            result=self.validate(role)
            self.assertEqual(set(result.viewer_bearers.values()),{'production','staging'})
        self.assertFalse((self.root/'unused-state').exists(),'parser may not open coordinator state')

    def test_old_native_alias_and_cross_reference_role_are_rejected(self):
        for role in ('bootstrap','final'):
            bad=copy.deepcopy(self.values[role]);bad['viewerBearers']['production']=self.config.viewer_token
            with self.subTest(role=role),self.assertRaises(RuntimeConfigurationError):self.validate(role,bad)
        bad=copy.deepcopy(self.values['bootstrap'])
        bad['workers']['main']['resultBearer']=bad['workers']['cluster']['bearer']
        with self.assertRaises(RuntimeConfigurationError):self.validate('bootstrap',bad)


if __name__=='__main__':unittest.main()
