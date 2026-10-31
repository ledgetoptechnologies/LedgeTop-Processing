"""Experimental strict NodeODM mutation transport; not runtime dispatch authority.

Callers must commit a unique mutation intent before every request. On any error,
response loss or interruption they must hold; neither operation retries itself.
Real-node acknowledgement compatibility is not yet established.
"""
import http.client
import json
import uuid

from . import reference_protocol as p
from .direct_outbox_contract import direct_pipeline_digest
from .odx_smoke_worker import SmokeError
from .viewer_locked_upload import strict_response
from .viewer_service import NativeEngine


def _post(engine,route,body,headers):
    if type(engine) is not NativeEngine:raise SmokeError('native engine required')
    connection=None
    try:
        parsed=engine.client.parsed
        kind=http.client.HTTPSConnection if parsed.scheme=='https' else http.client.HTTPConnection
        connection=kind(parsed.hostname,parsed.port,timeout=120)
        connection.request('POST',engine.route(route),body=body,headers=headers)
        return strict_response(connection.getresponse())
    except SmokeError:raise
    except Exception:raise SmokeError('engine mutation uncertain; hold without retry') from None
    finally:
        if connection is not None:connection.close()


def initialize(engine,identifier,plan,pipeline_sha,task_name,task_name_sha):
    p._uuid(identifier,'engine task ID');p._hash(pipeline_sha,'pipeline digest')
    p._hash(task_name_sha,'task name digest')
    if (type(task_name) is not str or len(task_name)>256 or any(ord(c)<32 for c in task_name)
            or p.digest(task_name)!=task_name_sha):raise SmokeError('task name changed')
    p._closed(plan,('options','outputs'),'Viewer plan')
    if direct_pipeline_digest(plan['options'],plan['outputs'])!=pipeline_sha:raise SmokeError('plan digest changed')
    fields={'name':task_name,'options':json.dumps(plan['options'])}
    if plan['outputs'] is not None:fields['outputs']=json.dumps(plan['outputs'])
    boundary='smoke-'+uuid.uuid4().hex;parts=[]
    for name,value in fields.items():
        parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"'
                      f'\r\n\r\n{value}\r\n').encode())
    parts.append(f'--{boundary}--\r\n'.encode())
    result=_post(engine,'/task/new/init',b''.join(parts),
                 {'Content-Type':'multipart/form-data; boundary='+boundary,'set-uuid':identifier})
    if (not isinstance(result,dict) or 'error' in result or ('success' in result and result['success'] is not True)
            or result.get('uuid')!=identifier):
        raise SmokeError('initialization acknowledgment uncertain')


def commit(engine,identifier):
    p._uuid(identifier,'engine task ID')
    result=_post(engine,'/task/new/commit/'+identifier,b'',{})
    if (not isinstance(result,dict) or 'error' in result or result.get('success') is not True
            or ('uuid' in result and result['uuid']!=identifier)):
        raise SmokeError('commit acknowledgment uncertain')
