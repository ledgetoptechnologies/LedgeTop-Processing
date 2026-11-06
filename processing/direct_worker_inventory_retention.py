"""Retain a signed direct inventory for reconciliation, not custody or write authority."""
import hashlib
import json
from . import reference_protocol as p
from . import direct_v2_inventory as inventory
from . import direct_worker_scratch_v2 as scratch
from .scratch_policy import (PolicyConflict, DIRECT_INVENTORY_MAX_BYTES,
    DIRECT_INVENTORY_MAX_ROWS, DIRECT_WRITER_PIN_ALLOWANCE)


def _validate(payload, item, worker, verification):
    checked=inventory.prepare(payload,**verification)
    item,digest,reservation=scratch.identity(item,expected_worker=worker)
    if (item['jobId']!=checked['providerTaskId'] or item['attemptId']!=checked['submissionId']
            or item['inventorySha256']!=checked['inventorySha256'] or
            any(item[k]!=checked[k] for k in ('authorityId','environment','sourceManifestSha256',
                                               'derivedManifestSha256','pipelineManifestSha256'))):
        raise PolicyConflict('direct inventory binding changed')
    return checked,item,reservation


def retain(ledger, *, payload, item, worker, verification, coordinator_principal):
    p._slug(coordinator_principal,'coordinator principal')
    checked,item,reservation=_validate(payload,item,worker,verification)
    wire=p.canonical_json(payload)
    if not 1<=len(wire)<=DIRECT_INVENTORY_MAX_BYTES:raise PolicyConflict('direct inventory size exceeded')
    digest=hashlib.sha256(wire).hexdigest();charge=len(wire)*4+DIRECT_WRITER_PIN_ALLOWANCE
    with ledger._transaction() as db:
        row=ledger._reservation(db,reservation)
        if (row['state']!='reserved' or row['allocated']!=0 or json.loads(row['settings'])!=scratch.settings(item,reservation)
                or not db.execute('SELECT 1 FROM transfer_fences WHERE reservation=?',(reservation,)).fetchone()):
            raise PolicyConflict('direct inventory fence changed')
        old=db.execute('SELECT * FROM direct_inventory_blobs WHERE reservation=?',(reservation,)).fetchone()
        if old:
            if (old['principal']!=coordinator_principal or old['inventory_sha']!=checked['inventorySha256']
                    or old['payload_sha']!=digest or old['payload']!=wire or old['charge']!=charge):
                raise PolicyConflict('direct inventory replay changed')
        else:
            count=db.execute('SELECT COUNT(*) FROM direct_inventory_blobs').fetchone()[0]
            if count>=DIRECT_INVENTORY_MAX_ROWS:raise PolicyConflict('direct inventory capacity exceeded')
            outstanding=db.execute('SELECT COALESCE(SUM(remaining),0) FROM reservations').fetchone()[0]
            for table,column in (('control_charges','bytes'),('direct_inventory_blobs','charge')):
                outstanding+=db.execute('SELECT COALESCE(SUM('+column+'),0) FROM '+table).fetchone()[0]
            for table in ('direct_writer_pins','direct_v2_readiness_uses','direct_custody_fences','direct_worker_grants','direct_readiness_issues'):
                outstanding+=db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]*DIRECT_WRITER_PIN_ALLOWANCE
            if ledger.probe(ledger.root)-outstanding-ledger.floor<charge:raise PolicyConflict('direct inventory budget unavailable')
            db.execute('INSERT INTO direct_inventory_blobs VALUES(?,?,?,?,?,?)',
                       (reservation,coordinator_principal,checked['inventorySha256'],digest,wire,charge))
            ledger._latch(db,directInventoryVersion=1,directInventoryCount=count+1)
    return {'payloadSha256':digest,'sourceCustodyAccepted':False,'processingAuthorized':False}


def recover(ledger, *, item, worker, verification, coordinator_principal):
    p._slug(coordinator_principal,'coordinator principal')
    item,_,reservation=scratch.identity(item,expected_worker=worker)
    with ledger._transaction() as db:
        row=ledger._reservation(db,reservation)
        if json.loads(row['settings'])!=scratch.settings(item,reservation):raise PolicyConflict('direct inventory reservation changed')
        saved=db.execute('SELECT * FROM direct_inventory_blobs WHERE reservation=?',(reservation,)).fetchone()
        if not saved or saved['principal']!=coordinator_principal or saved['inventory_sha']!=item['inventorySha256']:
            raise PolicyConflict('direct inventory unavailable')
        wire=saved['payload']
        if type(wire) is not bytes or hashlib.sha256(wire).hexdigest()!=saved['payload_sha']:
            raise PolicyConflict('direct inventory bytes changed')
    try:payload=p.decode_submission(wire,max_bytes=DIRECT_INVENTORY_MAX_BYTES)
    except Exception:raise PolicyConflict('direct inventory bytes invalid') from None
    if p.canonical_json(payload)!=wire:raise PolicyConflict('direct inventory encoding changed')
    _validate(payload,item,worker,verification)
    return payload
