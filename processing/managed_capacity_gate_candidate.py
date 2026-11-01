"""Inactive managed-reference-writer guard; not an engine ingress fence.

Only the same-process local capacity owner can issue or validate permits. HMAC
keys are provisioned by the caller; no persistence, remote key exchange, or
host-wide bypass protection is implemented. Profile cannot be enabled live.
"""
from contextlib import contextmanager
import hashlib
import hmac
import json
import os
import re

from .crossfs_admission_candidate import CrossFsHeld
from .local_capacity_authority_candidate import LocalCapacityAuthority

MANAGED_CAPACITY_QUALIFIED=False


def _canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


class ManagedCapacityGate:
    def __init__(self, authority, secret, *, ledger):
        if MANAGED_CAPACITY_QUALIFIED is not True:
            raise CrossFsHeld('managed writer profile not qualified')
        if type(authority) is not LocalCapacityAuthority or type(secret) is not bytes or len(secret)<32:
            raise CrossFsHeld('managed local authority and private key required')
        labels=[name for name,item in authority.ledgers.items() if item is ledger]
        if len(labels)!=1:raise CrossFsHeld('exact worker ledger required')
        self.authority=authority;self._secret=secret;self.pid=os.getpid();self.ledger=ledger;self._ledger_name=labels[0]
        with self.authority._mutex:
            self.authority._check()
            with self.authority._gate._db() as db:
                db.execute('CREATE TABLE IF NOT EXISTS managed_release_intents (job TEXT PRIMARY KEY, binding TEXT NOT NULL, signature TEXT NOT NULL, state TEXT NOT NULL)')

    def issue(self, attempt_id, *, pipeline_digest):
        """Sign the exact current local hold; caller must protect this channel."""
        if (self.pid!=os.getpid() or type(pipeline_digest) is not str
                or not re.fullmatch('[a-f0-9]{64}',pipeline_digest)):
            raise CrossFsHeld('managed owner process or pipeline changed')
        with self.authority._mutex:
            self.authority._check()
            with self.authority._gate._db() as db:
                row=db.execute('select plan,state from attempts where id=?',(attempt_id,)).fetchone()
            if row is None or row[1]!='admitted':raise CrossFsHeld('managed hold unavailable')
            plan=json.loads(row[0]);binding=json.loads(self.authority._binding_value)
            fields={'version':1,'authorityId':binding['authorityId'],'attemptId':attempt_id,
                    'jobId':plan['job'],'generation':plan['generation'],'environment':plan['environment'],
                    'manifestSha256':plan['manifest'],'pipelineDigest':pipeline_digest,
                    'holdSha256':self.authority.hold_evidence(attempt_id)}
            return {'fields':fields,'signature':hmac.new(self._secret,_canonical(fields),hashlib.sha256).hexdigest()}

    def _permit_fields(self, job, permit):
        if self.pid!=os.getpid() or type(permit) is not dict or set(permit)!={'fields','signature'}:
            raise CrossFsHeld('managed permit required')
        if type(permit['fields']) is not dict:raise CrossFsHeld('managed permit required')
        fields=json.loads(_canonical(permit['fields']));signature=permit['signature']
        required={'version','authorityId','attemptId','jobId','generation','environment','manifestSha256','pipelineDigest','holdSha256'}
        if (type(fields) is not dict or set(fields)!=required or type(signature) is not str
                or not hmac.compare_digest(hmac.new(self._secret,_canonical(fields),hashlib.sha256).hexdigest(),signature)
                or type(job) is not dict or (fields['jobId'],fields['generation'],fields['environment'],fields['manifestSha256'],fields['pipelineDigest'])
                    != (job.get('jobId'),job.get('generation'),job.get('environment'),job.get('manifestSha256'),job.get('pipelineDigest'))):
            raise CrossFsHeld('managed permit binding changed')
        return fields

    @contextmanager
    def hold(self, job, permit, *, reservation_id, terminal_validator=None):
        """Keep local owner mutex across controlled mutation; no external fence."""
        fields=self._permit_fields(job,permit)
        with self.authority._mutex:
            self.authority._check()
            binding=json.loads(self.authority._binding_value)
            with self.authority._gate._db() as db:
                if db.execute("select 1 from managed_release_intents where state!='complete' limit 1").fetchone():
                    raise CrossFsHeld('managed terminal release requires reconciliation')
                part=db.execute('select state from parts where attempt=? and reservation=? and ledger=?',(fields['attemptId'],reservation_id,self._ledger_name)).fetchone()
            if part is None or part[0]!='acquired':raise CrossFsHeld('worker reservation not covered')
            if (binding['authorityId']!=fields['authorityId']
                    or self.authority.hold_evidence(fields['attemptId'])!=fields['holdSha256']):
                raise CrossFsHeld('managed capacity hold changed')
            yield
            self.authority._check()
            try:
                same=self.authority.hold_evidence(fields['attemptId'])==fields['holdSha256']
            except CrossFsHeld:
                same=False
            if not same and (terminal_validator is None or terminal_validator() is not True):
                raise CrossFsHeld('managed capacity hold changed during mutation')

    @contextmanager
    def hold_terminal(self, job, permit, *, reservation_id, before, after, release_binding=None):
        """Guard an exact terminal acknowledgment; no physical storage grant."""
        fields=self._permit_fields(job,permit)
        with self.authority._mutex:
            self.authority._check()
            binding=json.loads(self.authority._binding_value)
            with self.authority._gate._db() as db:
                part=db.execute('select state from parts where attempt=? and reservation=? and ledger=?',
                    (fields['attemptId'],reservation_id,self._ledger_name)).fetchone()
            if (binding['authorityId']!=fields['authorityId'] or part is None or part[0]!='acquired'
                    or before() is not True):
                raise CrossFsHeld('exact terminal capacity proof required')
            if release_binding is not None:self._record_release(fields,release_binding,'pending')
            yield
            self.authority._check()
            if after() is not True:raise CrossFsHeld('terminal acknowledgment requires reconciliation')
            if release_binding is not None:self._record_release(fields,release_binding,'complete')

    def _record_release(self, fields, binding, state):
        if type(binding) is not dict or binding.get('jobId')!=fields['jobId'] or state not in ('pending','complete'):
            raise CrossFsHeld('exact managed release binding required')
        encoded=_canonical(binding).decode()
        signature=hmac.new(self._secret,b'managed-release-v1\0'+encoded.encode(),hashlib.sha256).hexdigest()
        with self.authority._gate._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT binding,signature,state FROM managed_release_intents WHERE job=?',(fields['jobId'],)).fetchone()
            if old is None:
                if state!='pending':raise CrossFsHeld('managed release intent missing')
                db.execute('INSERT INTO managed_release_intents VALUES(?,?,?,?)',(fields['jobId'],encoded,signature,state))
            elif old[0]!=encoded or not hmac.compare_digest(old[1],signature) or old[2] not in ('pending','complete'):
                raise CrossFsHeld('managed release evidence changed')
            elif state=='complete':
                db.execute("UPDATE managed_release_intents SET state='complete' WHERE job=?",(fields['jobId'],))

    def release_verified(self, job, permit, binding):
        self.authority._check()
        fields=self._permit_fields(job,permit)
        if type(binding) is not dict or binding.get('jobId')!=fields['jobId']:return False
        encoded=_canonical(binding).decode()
        signature=hmac.new(self._secret,b'managed-release-v1\0'+encoded.encode(),hashlib.sha256).hexdigest()
        with self.authority._gate._db() as db:
            row=db.execute('SELECT binding,signature,state FROM managed_release_intents WHERE job=?',(fields['jobId'],)).fetchone()
        return bool(row and row[0]==encoded and hmac.compare_digest(row[1],signature) and row[2]=='complete')
