"""Hard-disabled, in-process dispatcher integration experiment.

Chunked file-descriptor reads are bounded in memory, but filesystem calls can
stall. This is not a production transport, node isolation or writer fence.
"""
import json
import os
from pathlib import Path
import select
import socket
import stat
import threading
import time
import uuid

from .engine_ingress_guard import EngineIngressGuard,MAX_STREAM_BYTES
from .odx_smoke_worker import SmokeError
from .reference_protocol import decode_submission

QUALIFIED = False
CHUNK_BYTES = 65536


def _read_source(fd, count):
    return os.read(fd, count)


class SenderCompletion:
    """Owned in-process sender evidence; not an interprocess security boundary."""
    def __init__(self, *, source, writer, thread, stop, done, success, errors, engine, upload_id, deadline):
        self.source,self.writer,self.thread,self.stop_event=source,writer,thread,stop
        self.done,self.success,self.errors,self.engine,self.upload_id=done,success,errors,engine,upload_id
        self.deadline=deadline
        self.stopped=False

    def verify(self, limit):
        if not self.done.wait(max(0,limit-time.monotonic())) or not self.success.is_set() or self.errors:
            raise SmokeError('guarded sender uncertain')

    def stop(self):
        if not self.stopped:
            self.stopped=True;self.stop_event.set()
            try:self.source.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            try:self.writer.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            self.source.close();self.writer.close();self.thread.join(2)
            if self.thread.is_alive():self.engine._unreaped.append(self.thread)
        if self.thread.is_alive():raise SmokeError('guarded sender did not exit')


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
    def _open_source(item):
        if os.name!='posix' or not hasattr(os,'O_NOFOLLOW'):
            raise SmokeError('guarded source descriptor unavailable')
        if type(item['bytes']) is not int or not 0<item['bytes']<=MAX_STREAM_BYTES:
            raise SmokeError('guarded source limit exceeded')
        path=Path(item['path']);flags=os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW|getattr(os,'O_CLOEXEC',0)
        fd=os.open(path,flags)
        try:
            first=os.fstat(fd);named=path.lstat()
            def identity(s):return (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_nlink)
            if (not stat.S_ISREG(first.st_mode) or first.st_nlink!=1 or identity(first)!=identity(named)
                or first.st_size!=item['bytes'] or (os.name!='nt' and first.st_uid!=os.geteuid())):
                raise SmokeError('guarded source identity changed')
            return fd,path,identity(first)
        except BaseException:
            os.close(fd)
            raise

    def upload(self, identifier, item):
        task=self.task
        if task is None or identifier!=task['engine_uuid']:
            raise SmokeError('guarded task identity unavailable')
        upload_id=str(uuid.uuid4())
        self.store.prepare_guarded_engine_upload(task['job_id'],task['claim_token'],item['name_key'],upload_id,
                                                 expected_generation=task['generation'])
        fd,path,original=self._open_source(item)
        try:source,writer=socket.socketpair()
        except BaseException:
            os.close(fd);raise
        stop=threading.Event();done=threading.Event();success=threading.Event();errors=[]
        deadline=time.monotonic()+self.guard.binding.upload_deadline_seconds
        def feed():
            try:
                writer.setblocking(False);remaining_bytes=item['bytes']
                while remaining_bytes and not stop.is_set():
                    chunk=_read_source(fd,min(CHUNK_BYTES,remaining_bytes))
                    if not chunk or stop.is_set():raise OSError('sender interrupted')
                    remaining_bytes-=len(chunk);offset=0
                    while offset<len(chunk) and not stop.is_set():
                        remaining=deadline-time.monotonic()
                        if remaining<=0:raise OSError('sender deadline')
                        if not select.select([],[writer],[],min(.1,remaining))[1]:continue
                        try:count=writer.send(memoryview(chunk)[offset:])
                        except BlockingIOError:continue
                        if not count:raise OSError('sender closed')
                        offset+=count
                    if offset!=len(chunk):raise OSError('sender interrupted')
                if remaining_bytes or stop.is_set() or _read_source(fd,1):raise OSError('source changed')
                def identity(s):return (s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_nlink)
                if identity(os.fstat(fd))!=original or identity(path.lstat())!=original:
                    raise OSError('source identity changed')
                writer.shutdown(socket.SHUT_WR)
                success.set()
            except BaseException as error:errors.append(error)
            finally:
                try:os.close(fd)
                except OSError as error:errors.append(error)
                done.set()
        thread=None
        try:
            thread=threading.Thread(target=feed,daemon=True);thread.start()
        except BaseException:
            stop.set()
            try:source.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            try:writer.shutdown(socket.SHUT_RDWR)
            except OSError:pass
            source.close();writer.close()
            if thread is not None and thread.ident is not None:
                thread.join(2)
                if thread.is_alive():self._unreaped.append(thread)
            else:os.close(fd)
            raise
        completion=SenderCompletion(source=source,writer=writer,thread=thread,stop=stop,done=done,
                                    success=success,errors=errors,engine=self,upload_id=upload_id,deadline=deadline)
        try:
            return self.guard.stream_bound_upload('/task/new/upload/'+identifier,self._headers(task,upload_id=upload_id),source,completion=completion)
        finally:
            completion.stop()

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
