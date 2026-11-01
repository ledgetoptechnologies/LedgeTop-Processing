"""Design tests only: matching tuples are not authenticated evidence."""
import unittest
from tests.reference_v2_authority_model import Held, Model, token

class AuthorityBoundaryModelTests(unittest.TestCase):
    def pin(self): return Model().transition('pin',token('pin','coordinator'))
    def charge(self): return self.pin().transition('charge',token('charge','viewer'))
    def stream(self): return self.charge().transition('stream',token('stream','viewer'))
    def stop(self,model): return model.transition('external_stop_proved',token('stop','fence-authority'))
    def test_crash_disconnect_and_duplicate_charge_remain_held(self):
        for model in (self.pin(),self.charge(),self.stream()):
            for event in ('crash','disconnect'):
                after=model.transition(event)
                self.assertEqual((after.pin,after.fence),('held','held'))
                with self.assertRaises(Held):after.transition('release',token('release','coordinator'))
                with self.assertRaises(Held):after.transition('adopt',token('adopt','coordinator'))
        with self.assertRaises(Held):self.charge().transition('charge',token('charge','viewer'))
    def test_receipt_and_independent_fence_before_release(self):
        received=self.stream().transition('verified_receipt',token('receipt','original'))
        with self.assertRaises(Held):received.transition('release',token('release','coordinator'))
        stopped=self.stop(received)
        with self.assertRaises(Held):stopped.transition('release',token('release','coordinator'))
        viewer_ack=stopped.transition('ack_viewer',token('ack-viewer','viewer'))
        self.assertEqual(viewer_ack.transition('crash').transition('ack_viewer',token('ack-viewer','viewer')),viewer_ack)
        with self.assertRaises(Held):viewer_ack.transition('release',token('release','coordinator'))
        stopped=viewer_ack.transition('ack_coordinator',token('ack-coordinator','coordinator'))
        self.assertEqual(stopped.transition('disconnect').transition('ack_coordinator',token('ack-coordinator','coordinator')),stopped)
        for wrong in (None,token('release','viewer'),('release','coordinator','wrong-epoch','delivery-1','request-hash-1','receipt-hash-1'),('release','coordinator','epoch-1','wrong-delivery','request-hash-1','receipt-hash-1'),('release','coordinator','epoch-1','delivery-1','request-hash-1','wrong-receipt')):
            with self.assertRaises(Held):stopped.transition('release',wrong)
        released=stopped.transition('release',token('release','coordinator'))
        self.assertEqual(released.pin,'released')
        self.assertEqual(released.transition('crash').transition('release',token('release','coordinator')),released)
        self.assertEqual(released.transition('ack_viewer',token('ack-viewer','viewer')),released)
        self.assertEqual(released.transition('terminal_read',token('terminal-read','coordinator')),released)
        for event,evidence in (('terminal_read',token('terminal-read','viewer')),('release',('release','coordinator','epoch-1','other','request-hash-1','receipt-hash-1')),('stream',token('stream','viewer'))):
            with self.assertRaises(Held):released.transition(event,evidence)
        with self.assertRaises(Held):released.transition('anchor_lost').transition('terminal_read',token('terminal-read','coordinator'))
    def test_cancel_requires_quiescence_and_coordinator(self):
        for model in (self.pin(),self.charge(),self.stream()):
            with self.assertRaises(Held):model.transition('cancel',token('cancel','coordinator'))
            stopped=self.stop(model)
            with self.assertRaises(Held):stopped.transition('cancel',token('cancel','viewer'))
            with self.assertRaises(Held):stopped.transition('cancel',token('cancel','coordinator'))
            event,scope=('account_absent','absent-covered') if model.delivery=='none' else ('account_charged','exact-charge')
            for bad in (None,token(scope,'original'),(scope,'viewer','epoch-1','wrong-delivery'),('unknown','viewer','epoch-1','delivery-1'),('pruned','viewer','epoch-1','delivery-1')):
                with self.assertRaises(Held):stopped.transition(event,bad)
            accounted=stopped.transition(event,token(scope,'viewer'))
            cancelled=accounted.transition('cancel',token('cancel','coordinator'))
            self.assertEqual(cancelled.pin,'cancelled')
            self.assertEqual(cancelled.charged,model.charged)
            self.assertEqual(cancelled.transition('crash').transition('cancel',token('cancel','coordinator')),cancelled)
            self.assertEqual(cancelled.transition('terminal_read',token('terminal-read','coordinator')),cancelled)
            with self.assertRaises(Held):cancelled.transition('cancel',token('cancel','viewer'))
            with self.assertRaises(Held):cancelled.transition('anchor_lost').transition('terminal_read',token('terminal-read','coordinator'))
    def test_revocation_expiry_restore_and_anchor_loss_hold(self):
        for event in ('revoke','expiry','restore','anchor_lost'):
            for model,next_event,evidence in ((self.pin(),'charge',token('charge','viewer')),(self.charge(),'stream',token('stream','viewer')),(self.stream(),'external_stop_proved',token('stop','fence-authority')),(self.stop(self.charge()),'adopt',token('adopt','coordinator'))):
                with self.assertRaises(Held):model.transition(event).transition(next_event,evidence)
    def test_adoption_requires_fence_and_never_recharges(self):
        charged=self.charge()
        with self.assertRaises(Held):charged.transition('adopt',token('adopt','coordinator'))
        stopped=self.stop(charged)
        with self.assertRaises(Held):stopped.transition('adopt',token('adopt','original'))
        with self.assertRaises(Held):stopped.transition('adopt',token('adopt','coordinator'))
        stopped=stopped.transition('account_charged',token('exact-charge','viewer'))
        adopted=stopped.transition('adopt',token('adopt','coordinator'))
        self.assertEqual((adopted.worker,adopted.generation,adopted.charged),('recovery',1,1))
        for event in ('adopt','charge','stream','release'):
            with self.assertRaises(Held):adopted.transition(event,token(event,'coordinator'))
