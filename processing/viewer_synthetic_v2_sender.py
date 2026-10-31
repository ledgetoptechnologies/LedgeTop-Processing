"""Disabled synthetic v2 sender; seal is not Viewer custody or cleanup."""
import hashlib,http.client
from urllib.parse import urlsplit
from .direct_output_slot import _identity
from .direct_slot_capture import open_sealed_reader
from .node_gate import NodeGate
from .viewer_local_result import _local,_gate,_observation,_check_observation,send_metadata,register,LocalResultHeld
from .viewer_chunk_v2_geometry import geometry
from .viewer_locked_upload import strict_response
from . import reference_protocol as p

def send(worker,*,synthetic_fixture=False,chunk_bytes=1024*1024,promote=False):
 if synthetic_fixture is not True or type(promote) is not bool:raise LocalResultHeld('synthetic_fixture_required')
 send_metadata(worker,synthetic_fixture=True)
 metadata_sha=p.digest(register(worker)) if promote else None
 claim,binding,digest,identity,_,grant=_local(worker,include_grant=True);c=worker.config;parsed=urlsplit(c['origin'])
 if parsed.scheme!='https' or parsed.hostname not in ('127.0.0.1','localhost') or parsed.port is None:raise LocalResultHeld('synthetic_coordinator_required')
 prefix='/ltds/worker/v1/viewer-claims/'+claim['jobId']+'/chunks-v2/'
 base={'Authorization':'Bearer '+worker._bearer,'X-Claim-Token':claim['claimToken'],'Content-Type':'application/octet-stream','X-Chunk-Version':'2'}
 def call(op,body=b'',extra=None):
  conn=http.client.HTTPSConnection(parsed.hostname,parsed.port,context=worker._ssl_context,timeout=10)
  try:
   conn.request('POST',prefix+op,body=body,headers={**base,**(extra or {}),'Content-Length':str(len(body))})
   return strict_response(conn.getresponse())
  except Exception:raise LocalResultHeld('v2_transport_or_response_unavailable') from None
  finally:conn.close()
 with NodeGate(c['engineId']) as gate:
  reservation=_gate(worker,claim,binding,gate)
  with open_sealed_reader(c['outputRoot'],c['outputRootIdentity'],binding,max_budget=c['maxBudget']) as reader:
   slot=reader._slot;actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
   if actual!=identity:raise LocalResultHeld('output_identity_changed')
   _check_observation(slot,digest,_observation(claim,digest,grant))
   g=geometry(reader.length,chunk_bytes,2)
   journal_binding=dict(jobId=claim['jobId'],taskId=claim['viewerTaskId'],generation=claim['viewerGeneration'],environment=claim['environment'],archiveBytes=reader.length,archiveSha256=reader.sha256,version=2,chunkBytes=chunk_bytes,chunkCount=g['chunkCount'])
   begin=call('begin',extra={'X-Chunk-Size':str(chunk_bytes)})
   if begin not in ({'state':'ready'},{'state':'sealed'}):raise LocalResultHeld('v2_begin_unavailable')
   if begin['state']=='ready':
    offset=0;index=0
    while offset<reader.length:
     data=reader.read(min(chunk_bytes,reader.length-offset));sha=hashlib.sha256(data).hexdigest()
     extra={'X-Chunk-Index':str(index),'X-Chunk-Offset':str(offset),'X-Chunk-SHA256':sha}
     state=call('status-'+str(index),extra={**extra,'X-Chunk-Length':str(len(data))})
     if state=={'state':'missing'}:
      state=call('part-'+str(index),body=data,extra=extra)
      if not isinstance(state,dict) or state.get('state')!='finished':raise LocalResultHeld('v2_chunk_unavailable')
      if call('status-'+str(index),extra={**extra,'X-Chunk-Length':str(len(data))})!={'state':'finished'}:raise LocalResultHeld('v2_chunk_unavailable')
     elif state!={'state':'finished'}:raise LocalResultHeld('v2_chunk_unavailable')
     offset+=len(data);index+=1
   result=call('finish',extra={'X-Chunk-Count':str(g['chunkCount'])})
   if not isinstance(result,dict) or result.get('state')!='sealed_no_receipt' or not isinstance(result.get('seal'),dict):raise LocalResultHeld('v2_seal_unavailable')
   seal=result['seal'];expected=dict(schemaVersion=2,bindingSha256=p.digest(journal_binding),count=g['chunkCount'],bytes=reader.length,sha256=reader.sha256)
   if any(seal.get(k)!=v or type(seal.get(k)) is not type(v) for k,v in expected.items()):raise LocalResultHeld('v2_seal_changed')
   reader._check()
   if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
   if promote:
    key=p.digest(dict(kind='synthetic-v2-slot',binding=journal_binding))
    expected_receipt=dict(schemaVersion=1,kind='viewer-coordinator-archive-receipt',jobId=claim['jobId'],metadataSha256=metadata_sha,archiveBytes=reader.length,archiveSha256=reader.sha256,slotKey=key)
    receipt=call('promote',extra={'X-Chunk-Count':str(g['chunkCount'])})
    if p.canonical_json(receipt)!=p.canonical_json(expected_receipt):raise LocalResultHeld('v2_receipt_changed')
    reader._check()
    if _gate(worker,claim,binding,gate)!=reservation:raise LocalResultHeld('gate_binding_changed')
    return receipt
   return result
