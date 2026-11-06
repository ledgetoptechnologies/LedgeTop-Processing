"""Durable mTLS principal binding for a pre-enrolled, single V2 boundary.

This is not a reservation creator, Viewer custody receipt, or execution grant.
The caller must supply the authenticated TLS principal, never an HTTP header.
"""
import hashlib
import hmac
import json
import uuid

from . import reference_protocol as p
from .direct_worker_boundary import DirectBoundaryDenied
from .direct_worker_receiver import check_scope
from .direct_worker_scratch_v2 import require_fenced
from .node_gate import NodeGate
from .scratch_policy import DIRECT_WRITER_PIN_ALLOWANCE, PolicyConflict


class DirectWorkerGrant:
    def __init__(self, boundary, *, coordinator_principal):
        from .direct_worker_receiver_v2 import DirectWorkerBoundaryV2
        if not isinstance(boundary, DirectWorkerBoundaryV2):
            raise ValueError('pre-enrolled V2 boundary required')
        p._slug(coordinator_principal, 'coordinator principal')
        self.boundary = boundary
        self.principal = coordinator_principal

    def _auth(self, principal, headers):
        from .direct_worker_boundary import DirectWorkerBoundary
        token=DirectWorkerBoundary._header(headers,'Authorization')
        authority=DirectWorkerBoundary._header(headers,'X-Coordinator-Authority')
        if (principal!=self.principal or type(token) is not str or not token.startswith('Bearer ')
                or not token[7:].isascii() or not hmac.compare_digest(token[7:],self.boundary._bearer)
                or authority!=self.boundary.item['authorityId']):
            raise DirectBoundaryDenied('coordinator identity required')

    def authorize(self, principal, headers, payload):
        self._auth(principal,headers)
        p._closed(payload,('operationId','item'),'worker reservation grant')
        p._uuid(payload['operationId'],'operation')
        item,digest,scratch_id=require_fenced(self.boundary.ledger,self.boundary.item,expected_worker=self.boundary.worker)
        if p.canonical_json(payload['item'])!=p.canonical_json(item):
            raise DirectBoundaryDenied('reservation binding changed')
        with NodeGate(self.boundary.engine_id) as gate:
            check_scope(self.boundary.ledger,self.boundary.spool,item,digest,scratch_id,gate,
                coordinator_origin=self.boundary.origin,gate_generation=self.boundary.generation)
            with self.boundary.ledger._transaction() as db:
                existing=db.execute('SELECT * FROM direct_worker_grants WHERE reservation=?',(scratch_id,)).fetchone()
                if existing is not None:
                    if (existing['operation'],existing['principal'],existing['binding_sha'])!=(payload['operationId'],principal,digest):
                        raise PolicyConflict('reservation grant replay differs')
                else:
                    count=db.execute('SELECT COUNT(*) FROM direct_worker_grants').fetchone()[0]
                    if count>=4096:raise PolicyConflict('direct grant budget exceeded')
                    outstanding=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
                    outstanding+=db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
                    for table in ('direct_writer_pins','direct_v2_readiness_uses','direct_custody_fences','direct_worker_grants'):
                        outstanding+=db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]*DIRECT_WRITER_PIN_ALLOWANCE
                    if self.boundary.ledger.probe(self.boundary.ledger.root)-outstanding-self.boundary.ledger.floor<DIRECT_WRITER_PIN_ALLOWANCE:
                        raise PolicyConflict('direct grant metadata budget unavailable')
                    db.execute('INSERT INTO direct_worker_grants VALUES(?,?,?,?)',(scratch_id,payload['operationId'],principal,digest))
                    self.boundary.ledger._latch(db,directGrantVersion=1,directGrantCount=count+1)
        return dict(reservationId=item['reservationId'],operationId=payload['operationId'],
                    sourceCustodyAccepted=False,transferComplete=False,processingAuthorized=False)

    def require(self, principal, headers):
        self._auth(principal,headers)
        item,digest,scratch_id=require_fenced(self.boundary.ledger,self.boundary.item,expected_worker=self.boundary.worker)
        with self.boundary.ledger._transaction() as db:
            row=db.execute('SELECT principal,binding_sha FROM direct_worker_grants WHERE reservation=?',(scratch_id,)).fetchone()
            if row is None or (row['principal'],row['binding_sha'])!=(principal,digest):
                raise DirectBoundaryDenied('durable reservation grant required')
