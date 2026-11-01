"""Pure design model: tokens below are simulated, never authenticated proofs."""
from dataclasses import dataclass, replace

class Held(ValueError): pass

@dataclass(frozen=True)
class Model:
    pin: str = 'none'
    fence: str = 'none'
    delivery: str = 'none'
    charged: int = 0
    accounting: str = 'unknown'
    ack_viewer: bool = False
    ack_coordinator: bool = False
    worker: str = 'original'
    generation: int = 0
    anchor: str = 'current'
    def transition(self, event, evidence=None):
        # An exact tuple represents the *required shape* of a hypothetical proof;
        # matching Python values never authenticates an issuer or stops a stream.
        def require(scope, actor):
            if self.anchor != 'current' or evidence != (scope, actor, 'epoch-1', 'delivery-1', 'request-hash-1', 'receipt-hash-1'):
                raise Held('modeled authority binding missing')
        if event == 'pin' and self.pin == 'none' and self.delivery == 'none':
            require('pin','coordinator');return replace(self,pin='held',fence='held')
        if event == 'charge' and self.pin == self.fence == 'held' and self.delivery == 'none':
            require('charge','viewer');return replace(self,delivery='charged',charged=1)
        if event == 'stream' and self.pin == self.fence == 'held' and self.delivery == 'charged':
            require('stream','viewer');return replace(self,delivery='streaming')
        if event == 'verified_receipt' and self.pin == self.fence == 'held' and self.delivery == 'streaming':
            require('receipt',self.worker);return replace(self,delivery='received')
        if event == 'ack_viewer' and self.pin in ('held','released') and self.delivery == 'received':
            require('ack-viewer','viewer');return replace(self,ack_viewer=True)
        if event == 'ack_coordinator' and self.pin in ('held','released') and self.delivery == 'received' and self.ack_viewer:
            require('ack-coordinator','coordinator');return replace(self,ack_coordinator=True)
        if event == 'external_stop_proved' and self.pin == self.fence == 'held' and self.delivery in ('none','charged','streaming','received'):
            require('stop','fence-authority');return replace(self,fence='quiesced')
        if event == 'account_absent' and self.pin == 'held' and self.fence == 'quiesced' and self.delivery == 'none':
            require('absent-covered','viewer');return replace(self,accounting='absent-covered')
        if event == 'account_charged' and self.pin == 'held' and self.fence == 'quiesced' and self.delivery in ('charged','streaming'):
            require('exact-charge','viewer');return replace(self,accounting='exact-charge')
        if event == 'release' and self.pin == 'held' and self.fence == 'quiesced' and self.delivery == 'received' and self.ack_viewer and self.ack_coordinator:
            require('release','coordinator');return replace(self,pin='released')
        if event == 'release' and self.pin == 'released' and self.fence == 'quiesced' and self.delivery == 'received' and self.ack_viewer and self.ack_coordinator:
            require('release','coordinator');return self
        if event == 'terminal_read' and self.pin in ('released','cancelled'):
            require('terminal-read','coordinator');return self
        if event == 'cancel' and self.pin == 'held' and self.fence == 'quiesced' and ((self.delivery == 'none' and self.accounting == 'absent-covered') or (self.delivery in ('charged','streaming') and self.accounting == 'exact-charge')):
            require('cancel','coordinator');return replace(self,pin='cancelled')
        if event == 'cancel' and self.pin == 'cancelled' and self.fence == 'quiesced' and ((self.delivery == 'none' and self.accounting == 'absent-covered') or (self.delivery in ('charged','streaming') and self.accounting == 'exact-charge')):
            require('cancel','coordinator');return self
        if event == 'adopt' and self.pin == 'held' and self.fence == 'quiesced' and self.delivery in ('charged','streaming') and self.accounting == 'exact-charge' and self.worker == 'original':
            require('adopt','coordinator');return replace(self,worker='recovery',generation=self.generation+1)
        if event in ('anchor_lost','expiry','revoke','restore'):return replace(self,anchor='unavailable')
        if event in ('crash','disconnect'):return self
        raise Held('transition not authorized in model')

def token(scope,actor): return (scope,actor,'epoch-1','delivery-1','request-hash-1','receipt-hash-1')
