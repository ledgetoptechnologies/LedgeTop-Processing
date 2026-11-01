"""Hard-disabled, in-process dispatcher integration experiment.

Only a bounded verified file snapshot is supported. This is not a production
transport, node isolation, writer fence, or large-dataset capacity mechanism.
"""
import hashlib
import json
import os
from pathlib import Path
import select
import socket
import stat
import threading
import time
import uuid

from .engine_ingress_guard import EngineIngressGuard
from .odx_smoke_worker import SmokeError
from .reference_protocol import decode_submission

QUALIFIED = False
MAX_SNAPSHOT_BYTES = 16 * 1024 * 1024


class GuardedNativeEngine:
    def __init__(self, store, guard):
        from . import engine_upload_contract
        if QUALIFIED is not True or engine_upload_contract.QUALIFIED is not True:
            raise SmokeError('guarded dispatcher not qualified')
        if type(guard) is not EngineIngressGuard or guard.queue is not store.queue:
            raise SmokeError('exact in-process guard authority required')
        self.store,self.guard=store,guard
        self.task=None
        self._unreaped=[]

    def _headers(self, task, *, upload_id=None):
        pairs=[('Authorization','Bearer '+self.guard.binding.coordinator_secret),
               ('X-LedgeTop-Job',task['job_id']),('X-LedgeTop-Claim',task['claim_token']),
               ('Set-UUID',task['engine_uuid'])]
        if upload_id is not None:pairs.append(('X-LedgeTop-Upload-ID',upload_id))
        return pairs

    @staticmethod
    def _json(result):
        try:
            status,body=result
            if status!=200:raise ValueError()
            value=decode_submission(body)
            if type(value) is not dict or 'error' in value:raise ValueError()
            return value
        except (ValueError,TypeError):raise SmokeError('guarded engine response uncertain') from None

    def initialize(self, task):
        if task['node_id']!=self.guard.binding.node_id:
            raise SmokeError('guarded node identity differs')
        self.task={'uuid':task['uuid'],'job_id':task['job_id'],'claim_token':task['claim_token'],
                   'engine_uuid':task['engine_uuid'],'generation':task['generation']}
        boundary='guard-init-'+uuid.uuid4().hex
        fields=[('name',task['name']),('options',json.dumps(task['options']))]
        if task['outputs'] is not None:fields.append(('outputs',json.dumps(task['outputs'])))
        body=b''.join((f'--{boundary}\r\nContent-Disposition: form-data; name="{name}"\r\n\r\n{value}\r\n').encode('utf-8') for name,value in fields)+f'--{boundary}--\r\n'.encode()
        value=self._json(self.guard.mutate('POST','/task/new/init',self._headers(self.task),body,
                                          'multipart/form-data; boundary='+boundary))
        if (value.get('uuid')!=task['engine_uuid']
            or ('success' in value and value['success'] is not True)):
            raise SmokeError('guarded initialization uncertain')

    @staticmethod
    def _snapshot(item):
        if os.name!='posix' or not hasattr(os,'O_NOFOLLOW'):
            raise SmokeError('guarded source descriptor unavailable')
        if type(item['bytes']) is not int or not 0<item['bytes']<=MAX_SNAPSHOT_BYTES:
            raise SmokeError('guarded snapshot limit exceeded')
        path=Path(item['path']);flags=os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|getattr(os,'O_CLOEXEC',0)
        fd=os.open(path,flags)
        try:
            first=os.fstat(fd);named=path.lstat()
            def identity(s):return (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_nlink)
            if (not stat.S_ISREG(first.st_mode) or first.st_nlink!=1 or identity(first)!=identity(named)
                or first.st_size!=item['bytes'] or (os.name!='nt' and first.st_uid!=os.geteuid())):
                raise SmokeError('guarded source identity changed')
            data=bytearray()
            while len(data)<item['bytes']:
                part=os.read(fd,min(65536,item['bytes']-len(data)))
                if not part:raise SmokeError('guarded source incomplete')
                data.extend(part)
            if os.read(fd,1) or identity(os.fstat(fd))!=identity(first) or identity(path.lstat())!=identity(first):
                raise SmokeError('guarded source identity changed')
            if hashlib.sha256(data).hexdigest()!=item['sha256']:
                raise SmokeError('guarded source digest differs')
            return bytes(data)
        finally:os.close(fd)

    def upload(self, identifier, item):
        task=self.task
        if task is None or identifier!=task['engine_uuid']:
            raise SmokeError('guarded task identity unavailable')
        upload_id=str(uuid.uuid4())
        self.store.prepare_guarded_engine_upload(task['job_id'],task['claim_token'],item['name_key'],upload_id,
                                                 expected_generation=task['generation'])
        data=self._snapshot(item)
        source,writer=socket.socketpair();stop=threading.Event();errors=[]
        deadline=time.monotonic()+self.guard.binding.deadline_seconds
        def feed():
            try:
                writer.setblocking(False);offset=0
                while offset<len(data) and not stop.is_set():
                    remaining=deadline-time.monotonic()
                    if remaining<=0:raise OSError('sender deadline')
                    if not select.select([],[writer],[],min(.1,remaining))[1]:continue
                    try:count=writer.send(memoryview(data)[offset:offset+65536])
                    except BlockingIOError:continue
                    if not count:raise OSError('sender closed')
                    offset+=count
                if offset!=len(data):raise OSError('sender interrupted')
                writer.shutdown(socket.SHUT_WR)
            except (OSError,ValueError) as error:errors.append(error)
        thread=threading.Thread(target=feed,daemon=True);thread.start()
        try:
            result=self.guard.stream_bound_upload('/task/new/upload/'+identifier,self._headers(task,upload_id=upload_id),source)
            thread.join(.2)
        finally:
            stop.set()
            try:source.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            try:writer.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            source.close();writer.close();thread.join(2)
            if thread.is_alive():
                self._unreaped.append(thread)
                raise SmokeError('guarded sender did not exit')
        if errors:raise SmokeError('guarded sender uncertain')
        return result

    def action(self, action, identifier):
        if action!='commit' or self.task is None or identifier!=self.task['engine_uuid']:
            raise SmokeError('guarded mutation not qualified')
        value=self._json(self.guard.mutate('POST','/task/new/commit/'+identifier,self._headers(self.task)))
        if (('uuid' in value and value['uuid']!=identifier)
            or ('success' in value and value['success'] is not True)
            or (value.get('uuid')!=identifier and value.get('success') is not True)):
            raise SmokeError('guarded commit uncertain')
        return value

    def read(self, route, *, missing=False, **query):
        from .viewer_service import EngineUnavailable
        if query:raise EngineUnavailable('guarded_read_unqualified')
        try:
            value=self._json(self.guard.read('GET',route,[('Authorization','Bearer '+self.guard.binding.management_secret)]))
            return value
        except Exception:raise EngineUnavailable('guarded_read_unavailable') from None

    def download(self, identifier):
        from .viewer_service import EngineUnavailable
        raise EngineUnavailable('guarded_download_unqualified')
