"""Experimental NodeODM transport seam, not an authorization or retry policy.

Caller must hold its NodeGate, a verified input slot, and a durable upload intent.
An exception or lost response is ambiguous and MUST hold; never retry blindly.
This module is not wired to a worker runtime or any live dispatch path.
"""
import hashlib
import http.client
import json
import os
import uuid

from . import reference_protocol as p
from .direct_output_slot import OutputSlot, _identity
from .odx_smoke_worker import SmokeError
from .viewer_tasks import TaskStore
from .viewer_service import NativeEngine


def upload_locked(engine, identifier, item, slot, expected_identity):
    """Send one exact slot through NativeEngine's multipart upload wire route.

    Caller retains the exclusive slot and gate through this entire call. Hashes
    independently detect a changed file but cannot revoke bytes already sent.
    This function itself does not establish any claim, engine task, or journal.
    """
    if type(engine) is not NativeEngine or type(slot) is not OutputSlot:
        raise SmokeError('locked upload requires native engine and slot')
    p._uuid(identifier,'engine task ID')
    p._closed(item,('name','bytes','sha256'),'upload item')
    TaskStore._filename(item['name']);p._integer(item['bytes'],'input bytes');p._hash(item['sha256'],'input hash')
    if item['bytes']!=slot.budget:raise SmokeError('slot length changed')
    p._closed(expected_identity,('root','allocation','metadata'),'slot identity')
    actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
    if expected_identity!=actual:raise SmokeError('slot identity changed')
    try:
        boundary='processing-'+uuid.uuid4().hex
        prefix=(f'--{boundary}\r\nContent-Disposition: form-data; name="images"; '
                f'filename="{item["name"]}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode('ascii')
    except UnicodeEncodeError:raise SmokeError('unsupported filename encoding') from None
    suffix=f'\r\n--{boundary}--\r\n'.encode('ascii')
    slot._check();digest=hashlib.sha256();offset=0
    while offset<item['bytes']:
        part=os.pread(slot._fd,min(1024*1024,item['bytes']-offset),offset)
        if not part:raise SmokeError('input truncated')
        digest.update(part);offset+=len(part)
    slot._check()
    if digest.hexdigest()!=item['sha256']:raise SmokeError('input hash changed')
    connection=None
    try:
        parsed=engine.client.parsed
        kind=http.client.HTTPSConnection if parsed.scheme=='https' else http.client.HTTPConnection
        connection=kind(parsed.hostname,parsed.port,timeout=120)
        connection.putrequest('POST',engine.route('/task/new/upload/'+identifier))
        connection.putheader('Content-Type','multipart/form-data; boundary='+boundary)
        connection.putheader('Content-Length',str(len(prefix)+item['bytes']+len(suffix)))
        connection.endheaders();connection.send(prefix)
        digest=hashlib.sha256();offset=0
        while offset<item['bytes']:
            slot._check();part=os.pread(slot._fd,min(1024*1024,item['bytes']-offset),offset)
            if not part:raise SmokeError('input truncated')
            digest.update(part);offset+=len(part);connection.send(part);slot._check()
        if digest.hexdigest()!=item['sha256']:raise SmokeError('input hash changed')
        slot._check();connection.send(suffix)
        response=connection.getresponse()
        if not 200<=response.status<300:raise SmokeError('upload acknowledgment uncertain')
        raw=response.read(65537)
        if len(raw)>65536:raise SmokeError('upload acknowledgment uncertain')
        result=json.loads(raw)
        if not isinstance(result,dict) or result.get('success') is not True or 'error' in result:
            raise SmokeError('upload acknowledgment uncertain')
        slot._check()
    except SmokeError:raise
    except Exception:raise SmokeError('upload outcome uncertain; hold without retry') from None
    finally:
        if connection is not None:connection.close()
