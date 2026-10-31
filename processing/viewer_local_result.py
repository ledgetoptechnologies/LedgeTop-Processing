"""Synthetic Viewer result capture and local intent; not remote custody.

Requires the existing worker owner, committed synthetic submission and exact
gate/slot identity. Does not connect to the coordinator, mark a job complete,
send results, acknowledge Viewer custody or delete any data.
"""
import json
import http.client
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import urlsplit

from . import reference_protocol as p
from .direct_outbox_contract import validate_binding
from .direct_output_slot import _identity,open_slot
from .direct_slot_capture import begin_capture,open_sealed_reader,_persist,_read_record,_check_record
from .node_gate import NodeGate
from .viewer_node_enrollment import ViewerNodeEnrollment
from .viewer_node_inputs import ViewerNodeInputs
from .viewer_service import NativeEngine
from .viewer_locked_upload import strict_response


class LocalResultHeld(RuntimeError):pass

class ArchiveStatusTransportUnavailable(LocalResultHeld):
    """Known connection failure during an idempotent status GET."""


def _local(worker,*,include_grant=False):
    if type(worker) is not ViewerNodeEnrollment:raise LocalResultHeld('enrollment_required')
    worker.owner.check();c=worker.config
    with worker._db() as db:
        row=db.execute('SELECT request,receipt,claim,binding,slot_phase,slot_identity FROM intent WHERE singleton=1').fetchone()
    if not row or row[4]!='committed' or not all(isinstance(row[n],str) for n in (0,1,2,3,5)):
        raise LocalResultHeld('enrollment_unavailable')
    request=p.decode_submission(row[0].encode());claim=worker._validate_claim(p.decode_submission(row[2].encode()),request)
    binding=p.decode_submission(row[3].encode());digest=validate_binding(binding);identity=p.decode_submission(row[5].encode())
    if (binding['gateReservationId']!=row[1] or binding['jobId']!=claim['jobId']
        or binding['engineTaskId']!=claim['engineTaskId'] or binding['viewerTaskId']!=claim['viewerTaskId']
        or binding['viewerGeneration']!=claim['viewerGeneration'] or binding['environment']!=claim['environment']
        or binding['inputSetSha256']!=claim['inputInventorySha256'] or binding['pipelineSha256']!=claim['pipelineSha256']
        or binding['authorityId']!=c['authorityId'] or binding['nodeId']!=c['nodeId'] or binding['workerId']!=c['workerId']
        or binding['engineId']!=c['engineId'] or binding['coordinatorOrigin']!=c['gateOrigin']
        or binding['gateGeneration']!=c['gateGeneration'] or binding['outputBudgetBytes']!=c['outputBudget']):
        raise LocalResultHeld('enrollment_binding_changed')
    path=worker.owner.root/'viewer-inputs.sqlite';info=path.lstat()
    if (path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
            or stat.S_IMODE(info.st_mode)!=0o600 or info.st_size>64*1024*1024):raise LocalResultHeld('input_journal_invalid')
    for suffix in ('-journal','-wal','-shm'):
        side=Path(str(path)+suffix)
        if side.exists() or side.is_symlink():raise LocalResultHeld('input_journal_sidecar_requires_inspection')
    db=sqlite3.connect(path.absolute().as_uri()+'?mode=ro',uri=True)
    try:
        ViewerNodeInputs._schema(db)
        grant_row=db.execute('SELECT value FROM execution_grant WHERE singleton=1').fetchone()
        submitted=db.execute('SELECT grant_value,origin,phase,ordinal,object_id FROM synthetic_submission WHERE singleton=1').fetchone()
        manifest=db.execute('SELECT phase,count FROM manifest WHERE singleton=1').fetchone()
        if (not grant_row or not submitted or not manifest or submitted[0]!=grant_row[0]
                or submitted[2]!='committed' or submitted[3]!=manifest[1] or submitted[4] is not None
                or manifest[0]!='verified'):raise LocalResultHeld('synthetic_submission_unavailable')
        grant=p.decode_submission(grant_row[0].encode())
        if (grant.get('kind')!='synthetic_execution_grant' or grant.get('jobId')!=claim['jobId']
                or grant.get('engineTaskId')!=claim['engineTaskId'] or grant.get('viewerTaskId')!=claim['viewerTaskId']
                or grant.get('viewerGeneration')!=claim['viewerGeneration'] or grant.get('environment')!=claim['environment']
                or grant.get('authorityId')!=claim['authorityId'] or grant.get('nodeId')!=claim['nodeId']
                or grant.get('workerId')!=claim['workerId'] or grant.get('workerInstanceId')!=claim['workerInstanceId']
                or grant.get('inputInventorySha256')!=claim['inputInventorySha256'] or grant.get('pipelineSha256')!=claim['pipelineSha256']
                or grant.get('inputCount')!=manifest[1] or grant.get('inputBytes')!=claim['inputBytes']
                or grant.get('taskNameSha256')!=claim['taskNameSha256'] or p.digest(grant.get('taskName'))!=claim['taskNameSha256']
                or grant.get('enrollmentReceipt')!=p.digest(dict(binding=binding,slotIdentity=identity))):
            raise LocalResultHeld('grant_binding_changed')
        after=path.lstat()
        if (after.st_dev,after.st_ino)!=(info.st_dev,info.st_ino):raise LocalResultHeld('input_journal_changed')
        worker.owner.check()
        result=(claim,binding,digest,identity,submitted[1])
        return (*result,grant) if include_grant else result
    finally:db.close()


def _gate(worker,claim,binding,gate):
    state=worker._check_gate(gate);reservation=state['reservation'];c=worker.config
    job=dict(id=claim['jobId'],workerId=c['workerId'],nodeId=c['nodeId'],authorityId=c['authorityId'])
    if not reservation or reservation['receiptId']!=binding['gateReservationId'] or reservation['job']!=job or reservation['completionDigest'] is not None:
        raise LocalResultHeld('gate_binding_changed')
    return reservation


def _strict_engine_info(engine,identifier):
    parsed=urlsplit(engine.origin)
    kind=http.client.HTTPSConnection if parsed.scheme=='https' else http.client.HTTPConnection
    connection=kind(parsed.hostname,parsed.port,timeout=30)
    try:
        connection.request('GET',engine.route('/task/'+identifier+'/info'))
        return strict_response(connection.getresponse())
    except Exception:raise LocalResultHeld('engine_status_unavailable') from None
    finally:connection.close()


def _observation(claim,digest,grant):
    return dict(schemaVersion=1,kind='viewer-engine-completion-observation',bindingSha256=digest,
        grantSha256=p.digest(grant),jobId=claim['jobId'],viewerTaskId=claim['viewerTaskId'],
        viewerGeneration=claim['viewerGeneration'],engineTaskId=claim['engineTaskId'],environment=claim['environment'],engineStatus=40)


def _check_observation(slot,digest,expected):
    name='slot-'+digest+'.viewer-completion-observation';value,fd=_read_record(slot,name)
    try:
        _check_record(slot,fd,name,value)
        if p.canonical_json(value)!=p.canonical_json(expected):raise LocalResultHeld('completion_observation_changed')
        os.fsync(fd);_check_record(slot,fd,name,value);os.fsync(slot._root);_check_record(slot,fd,name,value)
        return value
    finally:os.close(fd)


def resume_capture_status(worker,*,synthetic_fixture=False):
    """Read-only offline status; legacy, lost and ambiguous state is held."""
    if synthetic_fixture is not True:raise LocalResultHeld('synthetic_fixture_required')
    claim,binding,digest,identity,origin,grant=_local(worker,include_grant=True)
    parsed=urlsplit(origin)
    if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.port is None:
        raise LocalResultHeld('synthetic_engine_required')
    with worker._db() as db:
        guard=db.execute('SELECT binding_sha,slot_identity,phase FROM capture_guard WHERE singleton=1').fetchone()
    if not guard or guard[0]!=digest or guard[1]!=p.canonical_json(identity).decode():
        raise LocalResultHeld('capture_guard_invalid')
    if guard[2]=='started':return resume_sealed(worker,synthetic_fixture=True)
    if guard[2]!='ready':raise LocalResultHeld('capture_guard_invalid')
    c=worker.config
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        slot=open_slot(c['outputRoot'],c['outputRootIdentity'],digest,c['outputBudget'],max_budget=c['maxBudget'],create=False)
        try:
            actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity:raise LocalResultHeld('output_identity_changed')
            allowed={'slot-'+digest+'.alloc','slot-'+digest+'.json'}
            if any(name.startswith('slot-'+digest+'.') and name not in allowed for name in os.listdir(slot._root)):
                raise LocalResultHeld('capture_requires_inspection')
            slot._check()
            with worker._db() as db:
                current=db.execute('SELECT binding_sha,slot_identity,phase FROM capture_guard WHERE singleton=1').fetchone()
                if current!=guard:raise LocalResultHeld('capture_guard_changed')
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return dict(state='absent_no_dispatch',jobId=claim['jobId'])
        finally:slot.close()


def resume_sealed(worker,*,synthetic_fixture=False):
    """Inspect a verified sealed result offline, without creation or dispatch.

    Missing or incomplete journals fail closed. This cannot prove that capture
    has never started if artifacts were lost, so it never reports "absent".
    It does not prove coordinator, Viewer, or physical storage custody.
    """
    if synthetic_fixture is not True:raise LocalResultHeld('synthetic_fixture_required')
    claim,binding,digest,identity,origin,grant=_local(worker,include_grant=True)
    parsed=urlsplit(origin)
    if parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.port is None:
        raise LocalResultHeld('synthetic_engine_required')
    with worker._db() as db:
        guard=db.execute('SELECT binding_sha,slot_identity,phase FROM capture_guard WHERE singleton=1').fetchone()
    if not guard or guard!=(digest,p.canonical_json(identity).decode(),'started'):
        raise LocalResultHeld('capture_guard_invalid')
    c=worker.config
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot
            actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity:raise LocalResultHeld('output_identity_changed')
            _check_observation(slot,digest,_observation(claim,digest,grant))
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return dict(state='sealed_no_dispatch',jobId=claim['jobId'],viewerTaskId=claim['viewerTaskId'],
                        viewerGeneration=claim['viewerGeneration'],archiveBytes=reader.length,archiveSha256=reader.sha256)


def register(worker):
    """Verify sealed bytes and persist/replay a local-only immutable intent."""
    claim,binding,digest,identity,_,grant=_local(worker,include_grant=True);c=worker.config
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity:raise LocalResultHeld('output_identity_changed')
            _check_observation(slot,digest,_observation(claim,digest,grant))
            record=dict(schemaVersion=1,kind='viewer-local-result-intent',bindingSha256=digest,
                jobId=claim['jobId'],viewerTaskId=claim['viewerTaskId'],viewerGeneration=claim['viewerGeneration'],
                engineTaskId=claim['engineTaskId'],environment=claim['environment'],archiveBytes=reader.length,
                archiveSha256=reader.sha256,allocation=actual['allocation'])
            name='slot-'+digest+'.viewer-outbox'
            try:_persist(slot._root,slot._root_identity,name,record)
            except FileExistsError:
                existing,fd=_read_record(slot,name)
                try:
                    _check_record(slot,fd,name,existing)
                    if existing!=record:raise LocalResultHeld('result_intent_changed')
                    # A previous creation may have failed after linking the
                    # name but before either durable sync. Replay must not
                    # treat mere existence as proof of persistence.
                    os.fsync(fd);_check_record(slot,fd,name,existing)
                    os.fsync(slot._root);_check_record(slot,fd,name,existing)
                finally:os.close(fd)
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return record


def send_metadata(worker,*,synthetic_fixture=False):
    """Attempt one idempotent metadata delivery; never deletes retained bytes.

    The caller can retry this same operation after failure or process restart.
    This is metadata registration only and is not Viewer/archive custody.
    """
    if synthetic_fixture is not True:raise LocalResultHeld('synthetic_fixture_required')
    record=register(worker);claim,binding,digest,identity,_=_local(worker);c=worker.config
    response=worker._request(record,job_id=claim['jobId'],claim_token=claim['claimToken'],kind='result-metadata')
    expected=dict(schemaVersion=1,kind='viewer-result-metadata-receipt',jobId=claim['jobId'],payloadSha256=p.digest(record),
                  archiveSha256=record['archiveSha256'],archiveBytes=record['archiveBytes'])
    if p.canonical_json(response)!=p.canonical_json(expected):raise LocalResultHeld('metadata_receipt_changed')
    if register(worker)!=record:raise LocalResultHeld('result_intent_changed')
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity or reader.sha256!=record['archiveSha256'] or reader.length!=record['archiveBytes']:
                raise LocalResultHeld('result_intent_changed')
            name='slot-'+digest+'.viewer-metadata-receipt'
            try:_persist(slot._root,slot._root_identity,name,expected)
            except FileExistsError:
                existing,fd=_read_record(slot,name)
                try:
                    _check_record(slot,fd,name,existing)
                    if existing!=expected:raise LocalResultHeld('metadata_receipt_changed')
                    os.fsync(fd);_check_record(slot,fd,name,existing)
                    os.fsync(slot._root);_check_record(slot,fd,name,existing)
                finally:os.close(fd)
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
    return expected


def send_archive(worker,*,synthetic_fixture=False):
    """Send sealed bytes once or replay a verified coordinator spool receipt."""
    if synthetic_fixture is not True:raise LocalResultHeld('synthetic_fixture_required')
    record=register(worker);claim,binding,digest,identity,_=_local(worker);c=worker.config
    parsed=urlsplit(c['origin']);path='/ltds/worker/v1/viewer-claims/'+claim['jobId']
    metadata_sha=p.digest(record)
    slot_key=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=claim['jobId'],metadataSha256=metadata_sha))
    expected=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=claim['jobId'],metadataSha256=metadata_sha,
                  archiveBytes=record['archiveBytes'],archiveSha256=record['archiveSha256'],slotKey=slot_key)
    headers={'Authorization':'Bearer '+worker._bearer,'X-Claim-Token':claim['claimToken']}
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity or reader.sha256!=record['archiveSha256'] or reader.length!=record['archiveBytes']:
                raise LocalResultHeld('result_intent_changed')
            metadata_receipt=dict(schemaVersion=1,kind='viewer-result-metadata-receipt',jobId=claim['jobId'],payloadSha256=metadata_sha,
                                  archiveSha256=record['archiveSha256'],archiveBytes=record['archiveBytes'])
            existing,fd=_read_record(slot,'slot-'+digest+'.viewer-metadata-receipt')
            try:
                _check_record(slot,fd,'slot-'+digest+'.viewer-metadata-receipt',existing)
                if p.canonical_json(existing)!=p.canonical_json(metadata_receipt):raise LocalResultHeld('metadata_receipt_changed')
            finally:os.close(fd)
            def connection():return http.client.HTTPSConnection(parsed.hostname,parsed.port,context=worker._ssl_context,timeout=30)
            client=connection()
            try:
                client.request('GET',path+'/archive-receipt',headers=headers)
                status=strict_response(client.getresponse())
            except (ConnectionRefusedError,ConnectionResetError,TimeoutError):
                raise ArchiveStatusTransportUnavailable('archive_status_transport_unavailable') from None
            except Exception:raise LocalResultHeld('archive_status_unavailable') from None
            finally:client.close()
            if status=={'state':'held'}:raise LocalResultHeld('coordinator_spool_requires_inspection')
            if status=={'state':'missing'}:
                client=connection()
                try:
                    client.putrequest('POST',path+'/archive')
                    for key,value in headers.items():client.putheader(key,value)
                    client.putheader('Content-Type','application/octet-stream');client.putheader('Content-Length',str(reader.length))
                    client.endheaders()
                    while True:
                        data=reader.read(1024*1024)
                        if not data:break
                        client.send(data)
                    response=strict_response(client.getresponse())
                except Exception:raise LocalResultHeld('archive_delivery_uncertain') from None
                finally:client.close()
            elif type(status) is dict and set(status)=={'state','receipt'} and status['state']=='sealed':response=status['receipt']
            else:raise LocalResultHeld('archive_status_invalid')
            if p.canonical_json(response)!=p.canonical_json(expected):raise LocalResultHeld('archive_receipt_changed')
            name='slot-'+digest+'.viewer-archive-receipt'
            try:_persist(slot._root,slot._root_identity,name,expected)
            except FileExistsError:
                old,fd=_read_record(slot,name)
                try:
                    _check_record(slot,fd,name,old)
                    if p.canonical_json(old)!=p.canonical_json(expected):raise LocalResultHeld('archive_receipt_changed')
                    os.fsync(fd);_check_record(slot,fd,name,old);os.fsync(slot._root);_check_record(slot,fd,name,old)
                finally:os.close(fd)
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return expected


def send_completion(worker,*,synthetic_fixture=False):
    """Attest the durable status observation and existing spool receipt."""
    if synthetic_fixture is not True:raise LocalResultHeld('synthetic_fixture_required')
    spool_receipt=send_archive(worker,synthetic_fixture=True)
    claim,binding,digest,identity,_,grant=_local(worker,include_grant=True);c=worker.config
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity:raise LocalResultHeld('output_identity_changed')
            observation=_check_observation(slot,digest,_observation(claim,digest,grant))
            payload=dict(observation=observation,spoolReceipt=spool_receipt)
            receipt=worker._request(payload,job_id=claim['jobId'],claim_token=claim['claimToken'],kind='completion-attestation')
            expected=dict(schemaVersion=1,kind='viewer-completion-attestation-receipt',jobId=claim['jobId'],
                          observationSha256=p.digest(observation),spoolReceiptSha256=p.digest(spool_receipt))
            if p.canonical_json(receipt)!=p.canonical_json(expected):raise LocalResultHeld('completion_receipt_changed')
            name='slot-'+digest+'.viewer-completion-receipt'
            try:_persist(slot._root,slot._root_identity,name,expected)
            except FileExistsError:
                old,fd=_read_record(slot,name)
                try:
                    _check_record(slot,fd,name,old)
                    if p.canonical_json(old)!=p.canonical_json(expected):raise LocalResultHeld('completion_receipt_changed')
                    os.fsync(fd);_check_record(slot,fd,name,old);os.fsync(slot._root);_check_record(slot,fd,name,old)
                finally:os.close(fd)
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return expected


def capture(worker,engine,*,synthetic_fixture=False):
    """Capture a finished synthetic engine archive without coordinator access."""
    claim,binding,digest,identity,origin,grant=_local(worker,include_grant=True);c=worker.config
    parsed=urlsplit(origin)
    if (synthetic_fixture is not True or type(engine) is not NativeEngine or engine.origin!=origin
            or parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.port is None):
        raise LocalResultHeld('synthetic_engine_required')
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        info=_strict_engine_info(engine,claim['engineTaskId'])
        if (not isinstance(info,dict) or info.get('uuid')!=claim['engineTaskId'] or not isinstance(info.get('status'),dict)
                or type(info['status'].get('code')) is not int or info['status']['code']!=40):
            raise LocalResultHeld('engine_not_confirmed_complete')
        # Durable monotonic transition precedes every capture-side filesystem write.
        with worker._db() as db:
            recorded=db.execute('SELECT binding_sha,slot_identity,phase FROM capture_guard WHERE singleton=1').fetchone()
            if not recorded or recorded[0]!=digest or recorded[1]!=p.canonical_json(identity).decode() or recorded[2]!='ready':
                raise LocalResultHeld('capture_requires_inspection')
            if db.execute("UPDATE capture_guard SET phase='started' WHERE singleton=1 AND phase='ready'").rowcount!=1:
                raise LocalResultHeld('capture_requires_inspection')
        response=engine.download(claim['engineTaskId'])
        try:
            lengths=response.headers.get_all('Content-Length',[])
            if (len(lengths)!=1 or response.headers.get_all('Transfer-Encoding',[]) or not lengths[0].isascii()
                    or not lengths[0].isdecimal() or len(lengths[0])>20 or not 1<=int(lengths[0])<=c['outputBudget']):
                raise LocalResultHeld('engine_archive_length_invalid')
            capture=begin_capture(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget'],
                                  declared_length=int(lengths[0]),expected_slot_identity=identity)
            try:
                _persist(capture._slot._root,capture._slot._root_identity,'slot-'+digest+'.viewer-completion-observation',
                         _observation(claim,digest,grant))
                capture.consume(response)
            finally:capture.close()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
        finally:response.close()
    return register(worker)
