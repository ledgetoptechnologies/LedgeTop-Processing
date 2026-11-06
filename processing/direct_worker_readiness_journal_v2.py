"""Durable exact readiness issuance; no public endpoint or custody claim.

`principal` must be supplied by an authenticated, configured local caller.
An interrupted issue stays held; this module never clears an unknown pin.
"""
import hashlib
import json
import time

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from . import reference_protocol as p
from .direct_worker_scratch_v2 import identity, require_fenced
from .direct_worker_fence_proof_v2 import issue_fenced, verify
from .scratch_policy import DIRECT_WRITER_PIN_ALLOWANCE, PolicyConflict
from .node_gate import NodeGate
from .direct_worker_receiver import check_scope


class ReadinessIssueUncertain(RuntimeError):
    pass


def issue(ledger, spool, item, *, principal, expected_worker, engine_id,
          coordinator_origin, gate_generation, challenge_id, validity_seconds,
          issuer_id, key_id, private_key):
    """Journal intent before fencing, persist exact signed result before return."""
    p._slug(principal,'coordinator principal');p._uuid(challenge_id,'challenge')
    p._slug(issuer_id,'issuer');p._slug(key_id,'key')
    if not isinstance(private_key,Ed25519PrivateKey):raise ValueError('worker signing key required')
    if type(validity_seconds) is not int or not 1<=validity_seconds<=60:raise ValueError('invalid readiness lifetime')
    try:
        item,digest,scratch_id=require_fenced(ledger,item,expected_worker=expected_worker)
    except Exception:
        # The journal may already contain a committed issue and retained pin;
        # inability to inspect scratch is never evidence of an unstarted job.
        raise ReadinessIssueUncertain('readiness issuance requires reconciliation') from None
    pub=private_key.public_key()
    raw_key=pub.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
    request=dict(principal=principal,item=item,worker=expected_worker,engineId=engine_id,
        coordinatorOrigin=coordinator_origin,gateGeneration=gate_generation,challengeId=challenge_id,
        validitySeconds=validity_seconds,issuerId=issuer_id,keyId=key_id,workerKeySha256=hashlib.sha256(raw_key).hexdigest())
    request_sha=p.digest(request)
    verify_args=dict(expected_item=item,expected_worker=expected_worker,challenge_id=challenge_id,
        gate_generation=gate_generation,issuer_id=issuer_id,key_id=key_id,public_key=pub)
    replay_wire=None; intent_may_exist=False; replay_seen=False
    try:
        with ledger._transaction() as db:
            row=db.execute('SELECT * FROM direct_readiness_issues WHERE challenge=?',(challenge_id,)).fetchone()
            if row is not None:
                replay_seen=True
                if (row['reservation'],row['request_sha'])!=(scratch_id,request_sha):
                    raise PolicyConflict('readiness issue replay differs')
                if row['state']!='issued':raise ReadinessIssueUncertain('readiness issuance requires reconciliation')
                replay_wire=row['proof']
            else:
                count=db.execute('SELECT COUNT(*) FROM direct_readiness_issues').fetchone()[0]
                if count>=4096:raise PolicyConflict('readiness issue capacity exceeded')
                outstanding=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
                outstanding+=db.execute('SELECT COALESCE(SUM(bytes),0) FROM control_charges').fetchone()[0]
                for table in ('direct_writer_pins','direct_v2_readiness_uses','direct_custody_fences','direct_worker_grants','direct_readiness_issues'):
                    outstanding+=db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]*DIRECT_WRITER_PIN_ALLOWANCE
                if ledger.probe(ledger.root)-outstanding-ledger.floor<DIRECT_WRITER_PIN_ALLOWANCE:
                    raise PolicyConflict('readiness issue budget unavailable')
                intent_may_exist=True
                db.execute("INSERT INTO direct_readiness_issues VALUES(?,?,?,'issuing',NULL)",(challenge_id,scratch_id,request_sha))
                ledger._latch(db,directReadinessIssueVersion=1,directReadinessIssueCount=count+1)
    except Exception:
        if intent_may_exist or replay_seen:
            raise ReadinessIssueUncertain('readiness issuance requires reconciliation') from None
        raise
    if replay_wire is not None:
        try:
            proof=json.loads(replay_wire)
            with NodeGate(engine_id) as gate:
                check_scope(ledger,spool,item,digest,scratch_id,gate,
                            coordinator_origin=coordinator_origin,gate_generation=gate_generation)
                with ledger._transaction() as db:
                    row=db.execute('SELECT * FROM direct_readiness_issues WHERE challenge=?',(challenge_id,)).fetchone()
                    if (row is None or row['state']!='issued' or row['proof']!=replay_wire
                            or row['request_sha']!=request_sha or row['reservation']!=scratch_id
                            or not db.execute('SELECT 1 FROM direct_writer_pins WHERE id=? AND reservation=?',
                                              (proof['body']['pinId'],scratch_id)).fetchone()
                            or db.execute('SELECT 1 FROM direct_v2_readiness_uses WHERE challenge=?',(challenge_id,)).fetchone()):
                        raise ReadinessIssueUncertain('readiness replay requires reconciliation')
                    verify(proof,now=int(time.time()),**verify_args)
            return proof
        except Exception:
            raise ReadinessIssueUncertain('readiness replay requires reconciliation') from None
    try:
        proof=issue_fenced(ledger,spool,item,expected_worker=expected_worker,engine_id=engine_id,
            coordinator_origin=coordinator_origin,gate_generation=gate_generation,challenge_id=challenge_id,
            validity_seconds=validity_seconds,issuer_id=issuer_id,key_id=key_id,private_key=private_key)
        verify(proof,now=int(time.time()),**verify_args)
        wire=p.canonical_json(proof)
        with ledger._transaction() as db:
            pin=proof['body']['pinId']
            if not db.execute('SELECT 1 FROM direct_writer_pins WHERE id=? AND reservation=?',(pin,scratch_id)).fetchone():
                raise PolicyConflict('readiness pin changed')
            changed=db.execute("UPDATE direct_readiness_issues SET state='issued',proof=? WHERE challenge=? AND reservation=? AND request_sha=? AND state='issuing' AND proof IS NULL",
                (wire,challenge_id,scratch_id,request_sha)).rowcount
            if changed!=1:raise PolicyConflict('readiness issue changed')
        verify(proof,now=int(time.time()),**verify_args)
        return proof
    except Exception:
        raise ReadinessIssueUncertain('readiness issuance requires reconciliation') from None
