"""Synthetic bounded supervisor admission and retry policy tests."""
import io
import unittest
from unittest.mock import patch

from ops.reference_worker_daemon import DaemonStateLock
from processing.viewer_service import NativeEngine
from processing.viewer_synthetic_supervisor import SupervisorHeld,TransientTransport,run,tick
from processing.viewer_tasks import TaskStore
from processing.viewer_node_inputs import ViewerNodeInputs
from tests import test_viewer_synthetic_submission as submission_tests


class SupervisorPolicyTests(unittest.TestCase):
    def test_auxiliary_object_count_blocks_before_inventory_fetch(self):
        fixture=submission_tests.SyntheticSubmissionTests('test_exact_submission_once_and_replay_without_cleanup')
        self.addCleanup(fixture.doCleanups)
        original=TaskStore.commit
        def commit_with_aux(store,identifier,*args,**kwargs):
            store.upload(identifier,'gcp_list.txt',io.BytesIO(b'aux'),3,'production')
            return original(store,identifier,*args,**kwargs)
        with patch.object(TaskStore,'commit',commit_with_aux):fixture.setUp()
        with DaemonStateLock(fixture.base.base.worker) as owner:
            worker=fixture.base.base.make(owner)
            with patch.object(ViewerNodeInputs,'freeze_inventory',side_effect=AssertionError('inventory fetched')):
                with self.assertRaisesRegex(SupervisorHeld,'synthetic_object_limit'):
                    tick(worker,NativeEngine(fixture.origin),fixture.base.base.fixture.job,'enroll-1','production',
                         input_root=fixture.base.inputs,input_root_identity=fixture.base.input_id,
                         max_total=100000,max_object=100000,max_objects=1,synthetic_fixture=True)
            with worker._db() as db:claim=db.execute('SELECT claim FROM intent').fetchone()[0]
            import json
            claim=json.loads(claim);self.assertEqual(claim['imageCount'],1);self.assertEqual(claim['objectCount'],2)
        self.assertEqual(fixture.calls,[])

    def test_only_explicit_transient_errors_retry(self):
        args=(object(),object(),'job','request','staging')
        with patch('processing.viewer_synthetic_supervisor.tick',side_effect=[TransientTransport('retry'),{'ok':True}]) as call:
            self.assertEqual(run(*args,synthetic_fixture=True,initial_delay=0,max_delay=0),{'ok':True})
            self.assertEqual(call.call_count,2)
        for error in (SupervisorHeld('integrity'),ValueError('unknown')):
            with self.subTest(error=type(error).__name__):
                with patch('processing.viewer_synthetic_supervisor.tick',side_effect=error) as call:
                    with self.assertRaises(type(error)):run(*args,synthetic_fixture=True,initial_delay=0,max_delay=0)
                    self.assertEqual(call.call_count,1)
