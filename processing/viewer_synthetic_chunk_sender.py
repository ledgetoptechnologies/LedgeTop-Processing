"""Disabled synthetic TLS chunk sender; no Viewer custody or cleanup."""
import hashlib
import http.client
from urllib.parse import urlsplit
from .direct_output_slot import _identity
from .direct_slot_capture import open_sealed_reader
from .node_gate import NodeGate
from .viewer_chunk_journal import MAX_CHUNK,MAX_CHUNKS
from .viewer_local_result import _local,_gate,_observation,_check_observation,send_metadata,register,LocalResultHeld
from . import reference_protocol as p
from .viewer_locked_upload import strict_response


def send(worker,*,synthetic_fixture=False,chunk_bytes=MAX_CHUNK):
    if synthetic_fixture is not True or type(chunk_bytes) is not int or not 1<=chunk_bytes<=MAX_CHUNK:
        raise LocalResultHeld('synthetic_fixture_required')
    send_metadata(worker,synthetic_fixture=True)
    record=register(worker);metadata_sha=p.digest(record)
    claim,binding,digest,identity,_,grant=_local(worker,include_grant=True);c=worker.config;parsed=urlsplit(c['origin'])
    if parsed.scheme!='https' or parsed.hostname not in ('127.0.0.1','localhost') or parsed.port is None:
        raise LocalResultHeld('synthetic_coordinator_required')
    prefix='/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/chunks/'
    base={'Authorization':'Bearer '+worker._bearer,'X-Claim-Token':claim['claimToken'],'Content-Type':'application/octet-stream'}
    def call(operation,body=b'',extra=None):
        conn=http.client.HTTPSConnection(parsed.hostname,parsed.port,context=worker._ssl_context,timeout=10)
        try:
            headers={**base,**(extra or {}),'Content-Length':str(len(body))}
            conn.request('POST',prefix+operation,body=body,headers=headers)
            return strict_response(conn.getresponse())
        except Exception:raise LocalResultHeld('chunk_transport_or_response_unavailable') from None
        finally:conn.close()
    with NodeGate(c['engineId']) as gate:
        reservation=_gate(worker,claim,binding,gate)
        with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
            slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if actual!=identity:raise LocalResultHeld('output_identity_changed')
            _check_observation(slot,digest,_observation(claim,digest,grant))
            if (reader.length+chunk_bytes-1)//chunk_bytes>MAX_CHUNKS:raise LocalResultHeld('chunk_count_exceeded')
            chunk_binding=dict(jobId=claim['jobId'],taskId=claim['viewerTaskId'],generation=claim['viewerGeneration'],environment=claim['environment'],archiveBytes=reader.length,archiveSha256=reader.sha256)
            key=p.digest(dict(kind='viewer-coordinator-spool-v1',jobId=claim['jobId'],metadataSha256=metadata_sha))
            expected=dict(schemaVersion=1,kind='synthetic-chunk-seal',jobId=claim['jobId'],metadataSha256=metadata_sha,
                          archiveBytes=reader.length,archiveSha256=reader.sha256,slotKey=key)
            standard=dict(expected,kind='viewer-coordinator-archive-receipt')
            beginning=call('begin',extra={'X-Chunk-Size':str(chunk_bytes)})
            if type(beginning) is dict and beginning.get('state')=='promoted':
                if p.canonical_json(beginning.get('receipt'))!=p.canonical_json(standard):raise LocalResultHeld('chunk_receipt_invalid')
                reader._check()
                if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
                return standard
            if type(beginning) is dict and beginning.get('state')=='sealed':
                result=beginning.get('receipt')
                if p.canonical_json(result)!=p.canonical_json(expected):
                    raise LocalResultHeld('chunk_seal_invalid')
                reader._check()
                if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
                promoted=call('promote',extra={'X-Chunk-Count':str((reader.length+chunk_bytes-1)//chunk_bytes)})
                if p.canonical_json(promoted)!=p.canonical_json(standard):raise LocalResultHeld('chunk_receipt_invalid')
                reader._check()
                if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
                return promoted
            if beginning!={'state':'ready'}:raise LocalResultHeld('chunk_begin_unavailable')
            offset=0;index=0
            while offset<reader.length:
                if index>=MAX_CHUNKS:raise LocalResultHeld('chunk_count_exceeded')
                data=reader.read(min(chunk_bytes,reader.length-offset));sha=hashlib.sha256(data).hexdigest()
                operation='part-'+str(index)
                extra={'X-Chunk-Index':str(index),'X-Chunk-Offset':str(offset),'X-Chunk-SHA256':sha}
                state=call('status-'+operation,extra={**extra,'X-Chunk-Length':str(len(data))})
                if state=={'state':'missing'}:
                    state=call(operation,data,extra)
                    intent=dict(schemaVersion=1,bindingSha256=p.digest(chunk_binding),index=index,offset=offset,length=len(data),sha256=sha)
                    expected_receipt=dict(schemaVersion=1,intentSha256=p.digest(intent))
                    if state=={'state':'finished'}:
                        if call('status-'+operation,extra={**extra,'X-Chunk-Length':str(len(data))})!={'state':'finished'}:raise LocalResultHeld('chunk_receipt_invalid')
                    elif p.canonical_json(state)!=p.canonical_json(dict(state='finished',receipt=expected_receipt)):
                        raise LocalResultHeld('chunk_receipt_invalid')
                elif state!={'state':'finished'}:raise LocalResultHeld('chunk_status_invalid')
                offset+=len(data);index+=1
            result=call('finish',extra={'X-Chunk-Count':str(index)})
            if p.canonical_json(result)!=p.canonical_json(expected):
                raise LocalResultHeld('chunk_seal_invalid')
            promoted=call('promote',extra={'X-Chunk-Count':str(index)})
            if p.canonical_json(promoted)!=p.canonical_json(standard):raise LocalResultHeld('chunk_receipt_invalid')
            reader._check()
            if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
            return promoted
