"""Portable ownership safety for the opt-in actual-daemon harness."""
import contextlib
import io
import unittest

from ops.qualify_enrolled_docker_transport import cleanup_owned,TASK_LABEL
from ops.qualify_enrolled_holder_transport import DropConfirmedReply
from ops.enrolled_coordinator_transport import TransportHeld


class HarnessOwnershipTests(unittest.TestCase):
    def test_unknown_mutation_does_not_scan_or_delete_even_if_scan_would_be_empty(self):
        class Unknown:
            mutation_unknown=True
            def matching(self,*args):raise AssertionError('unknown mutation cannot use empty scan as proof')
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(TransportHeld,'unknown_resources_retained'):
                cleanup_owned(Unknown(),'synthetic-operation',['synthetic-volume'])

    def test_unowned_container_is_never_stopped_or_removed(self):
        class Other:
            mutation_unknown=False
            def matching(self,filters):return ['foreign-container']
            def inspect(self,identifier):return {'Config':{'Labels':{TASK_LABEL:'another-operation'}}}
            def stop(self,*args):raise AssertionError('foreign stop')
            def command(self,*args,**kwargs):raise AssertionError('foreign mutation')
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(TransportHeld,'cleanup_unverified'):cleanup_owned(Other(),'ours',[])

    def test_a_cleanup_mutation_becoming_unknown_prevents_volume_removal(self):
        class StopsUnknown:
            mutation_unknown=False
            def matching(self,filters):return ['our-container']
            def inspect(self,identifier):return {'Config':{'Labels':{TASK_LABEL:'ours'}},'State':{'Running':False}}
            def command(self,*args,**kwargs):
                if args[:2]==('container','rm'):
                    self.mutation_unknown=True;raise TransportHeld('deliberate unknown cleanup')
                raise AssertionError('volume must be retained after unknown')
        with contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(TransportHeld,'cleanup_unverified'):
                cleanup_owned(StopsUnknown(),'ours',['owned-volume'])

    def test_reply_loss_model_drops_only_after_confirmed_delegate_success(self):
        class Actual:
            def __init__(self):self.calls=0
            def exec_python(self,*args,**kwargs):self.calls+=1;return {'recorded':True}
        actual=Actual();drop=DropConfirmedReply(actual)
        with self.assertRaisesRegex(TransportHeld,'dropped_confirmed'):drop.exec_python('id',b'code',mutation=True)
        self.assertEqual(actual.calls,1);self.assertEqual(drop.dispatched,1)
        self.assertEqual(drop.exec_python('id',b'inspect',mutation=False),{'recorded':True})
        self.assertEqual(drop.dispatched,1)
