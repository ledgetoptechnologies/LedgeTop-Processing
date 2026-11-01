"""Opt-in runtime integration under an irreversible synthetic recovery hold."""
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from processing import config as cfg
from processing.admission_drain import AdmissionDrain
from processing.app import make_handler
from processing.authority_lease import AuthorityLease,record_required
from processing.broker_intent_fence import install_offline as install_intents
from processing.config import Config
from processing.durable_upload_evidence import install_offline as install_evidence
from processing.incremental_upload_census import install_offline as install_incremental
from processing.prototype import Queue, PROFILE
from processing.upload_intent_census import install_offline as install_census
from processing.viewer_tasks import TaskStore

class CensusRecoveryRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-census-recovery-runtime-')).resolve()
        self.addCleanup(lambda:print('retained synthetic census recovery root:',self.root,flush=True))
        self.state=self.root/'state';self.lease_dir=self.root/'lease';self.lease_dir.mkdir(mode=0o700)
        q=Queue(self.state,node_ids=('cluster','main'))
        lease=AuthorityLease(self.lease_dir,q.authority_id)
        try:
            q.authority_lease=lease;record_required(q,lease);TaskStore(q,self.state/'viewer-tasks')
            with q._db() as db:install_intents(db,enabled=True)
            with q._db() as db:install_evidence(db,enabled=True)
            with q._db() as db:install_census(db,enabled=True)
            with q._db() as db:install_incremental(db,enabled=True)
            self.job,_=q.submit({'requestId':'synthetic-runtime','environment':'production','profile':PROFILE})
            AdmissionDrain(q).arm_recovery_hold()
        finally:q.close();lease.close()
        self.config=Config('synthetic-api-'+'a'*32,'http://127.0.0.1:1','http://127.0.0.1:2',1000,100000,
            prototype_state_dir=str(self.state),prototype_operator_token='synthetic-op-'+'b'*32,
            prototype_node_ids=('cluster','main'),admission_drain_enabled=True,recovery_hold_required=True,
            incremental_upload_census_required=True,authority_lease_dir=str(self.lease_dir))
    def qualified(self):
        return patch.multiple(cfg,ADMISSION_DRAIN_QUALIFIED=True,RECOVERY_HOLD_QUALIFIED=True,INCREMENTAL_UPLOAD_CENSUS_QUALIFIED=True)
    def test_actual_handler_requires_qualification_and_holds_both_nodes(self):
        with self.assertRaisesRegex(RuntimeError,'incremental_upload_census_not_qualified'):make_handler(self.config)
        with self.qualified():handler=make_handler(self.config)
        q=handler.prototype_queue;lease=q.authority_lease
        try:
            self.assertTrue(q.require_incremental_upload_census);self.assertTrue(q.recovery_hold_required)
            with self.assertRaises(AttributeError):q.require_incremental_upload_census=False
            with self.assertRaises(AttributeError):del q.require_incremental_upload_census
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'cluster'}))
            self.assertIsNone(q.claim({'workerId':'synthetic','nodeId':'main'}))
            self.assertEqual(q.get(self.job['id'])['status'],'queued')
        finally:q.close();lease.close()
    def test_missing_census_marker_refuses_before_job_state_change(self):
        q=Queue(self.state,node_ids=('cluster','main'),require_incremental_upload_census=True,recovery_hold_required=True)
        try:
            with q._db() as db:db.execute('DROP TABLE upload_incremental_required')
        finally:q.close()
        with self.qualified():
            with self.assertRaises(ValueError):make_handler(self.config)
        q=Queue(self.state,node_ids=('cluster','main'))
        try:self.assertEqual(q.get(self.job['id'])['status'],'queued')
        finally:q.close()
    def test_environment_mode_is_explicit_and_disabled_by_default(self):
        env={'PROCESSING_API_TOKEN':'synthetic-api-'+'a'*32,
             'PROCESSING_PROTOTYPE_STATE_DIR':str(self.state),'PROCESSING_AUTHORITY_LEASE_DIR':str(self.lease_dir),
             'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':'synthetic-op-'+'b'*32,
             'PROCESSING_ADMISSION_DRAIN_ENABLED':'1','PROCESSING_RECOVERY_HOLD_REQUIRED':'1',
             'PROCESSING_INCREMENTAL_UPLOAD_CENSUS_REQUIRED':'1'}
        with patch.dict(os.environ,env,clear=True),self.qualified():
            self.assertTrue(cfg.from_environment().incremental_upload_census_required)
        with patch.dict(os.environ,{**env,'PROCESSING_RECOVERY_HOLD_REQUIRED':'0'},clear=True),self.qualified():
            with self.assertRaises(ValueError):cfg.from_environment()
        with patch.dict(os.environ,env,clear=True),patch.multiple(cfg,ADMISSION_DRAIN_QUALIFIED=True,RECOVERY_HOLD_QUALIFIED=True):
            with self.assertRaisesRegex(ValueError,'not qualified'):cfg.from_environment()

if __name__=='__main__':unittest.main()
