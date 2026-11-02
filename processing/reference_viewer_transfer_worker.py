"""Single-step durable retry loop for sealed Processing results to Viewer."""
import re
import time

from .maintenance import Conflict
from .viewer_acceptance_transfer import ViewerTransferUnavailable


class ReferenceViewerTransferWorker:
    """Lease and push one queued result; source data remains until signed ACK."""

    def __init__(self, store, sender, *, trusted_receipt_keys, clock=time.time,
                 lease_seconds=120):
        if not callable(getattr(store, 'claim_viewer_transfer', None)):
            raise ValueError('durable reference store required')
        if not callable(getattr(sender, 'transfer', None)):
            raise ValueError('Viewer acceptance transfer sender required')
        if not callable(clock) or type(lease_seconds) is not int or not 1 <= lease_seconds <= 3600:
            raise ValueError('invalid Viewer transfer worker settings')
        from .viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
        self.store, self.sender = store, sender
        self.trusted_receipt_keys = validate_trusted_viewer_receipt_keys(trusted_receipt_keys)
        if not self.trusted_receipt_keys:
            raise ValueError('Viewer acceptance receipt pins are required')
        self.clock, self.lease_seconds = clock, lease_seconds

    @staticmethod
    def _reason(error):
        value = str(error)
        return value if re.fullmatch(r'[a-z][a-z0-9_]{0,79}', value) else 'viewer_transfer_unavailable'

    def run_once(self):
        """Advance discovery, transfer, signature validation and durable receipt.

        A transport timeout never deletes or acknowledges result objects. The
        stable receipt ID and Viewer-side byte offsets make the next claim a
        resume, including a lost response after the remote commit.
        """
        now = self.clock()
        if type(now) not in (int, float) or now < 0:
            return {'state': 'held', 'reason': 'viewer_transfer_clock_unavailable'}
        try:
            pending = self.store.pending_viewer_transfer_jobs(limit=20)
            if pending:
                self.store.ensure_viewer_transfer(pending[0])
            claim = self.store.claim_viewer_transfer(now=now, lease_seconds=self.lease_seconds)
        except Exception:
            return {'state': 'held', 'reason': 'viewer_transfer_state_unavailable'}
        if claim is None:
            return {'state': 'idle'}
        job_id, lease_token = claim['jobId'], claim['leaseToken']
        try:
            job = self.store.get(job_id, private=True)
            pair = self.sender.transfer(job, claim['acceptance'])
            accepted = self.store.accept(job_id, pair['acceptance'],
                viewer_receipt=pair['viewerReceipt'],
                trusted_viewer_receipt_keys=self.trusted_receipt_keys)
            if not accepted['fullyAccepted']:
                raise Conflict('Viewer acceptance closure is incomplete')
            # accept() durably completes the matching transfer atomically with
            # its receipts; the lease token may have been cleared by that txn.
            return {'state': 'accepted', 'jobId': job_id}
        except ViewerTransferUnavailable as error:
            reason = self._reason(error)
        except Exception:
            reason = 'viewer_transfer_state_unavailable'
        try:
            self.store.fail_viewer_transfer(job_id, lease_token, now=self.clock(), reason=reason)
        except Exception:
            return {'state': 'retry_wait', 'jobId': job_id}
        return {'state': 'retry_wait', 'jobId': job_id, 'reason': reason}
