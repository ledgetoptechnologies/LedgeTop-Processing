"""Inactive bounded mTLS upload experiment; callers supply authenticated sockets.

No listener or credential provisioning. Sender evidence is in-process only;
unknown outcomes are intentionally retained for inspection.
"""
import hashlib
import socket
import threading
import time

from .node_authority_session import _peer, _read, _write, _remaining, SessionHeld
from .node_mutation_broker import LocalUploadSender, NodeMutationBroker, _json, sign_grant, _sender_confirmation
from .guarded_native_engine import start_guarded_sender


class UploadEvidence:
    """Coordinator-owned transient evidence; not a cross-process security boundary."""
    def __init__(self):
        self._condition=threading.Condition();self._verified=set()
    def _record_verified(self, grant):
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._condition:
            self._verified.add(digest);self._condition.notify_all()
    def wait_verified(self, grant, deadline):
        digest=hashlib.sha256(_json(grant)).hexdigest()
        with self._condition:
            while digest not in self._verified:
                remaining=deadline-time.monotonic()
                if remaining<=0:return False
                self._condition.wait(remaining)
            return True


class RemoteUploadSender:
    """Read an exact stored descriptor and send over an already authenticated TLS stream."""
    def __init__(self):
        self._unreaped=[];self._send_lock=threading.Lock()
    def send(self, channel, *, peer_sha256, store, claim_token, grant, signature, secret, evidence, timeout=10):
        try:
            if not self._send_lock.acquire(blocking=False):raise SessionHeld('upload sender busy')
            try:
                return self._send(channel,peer_sha256=peer_sha256,store=store,claim_token=claim_token,
                    grant=grant,signature=signature,secret=secret,evidence=evidence,timeout=timeout)
            finally:self._send_lock.release()
        finally:channel.close()

    def _send(self, channel, *, peer_sha256, store, claim_token, grant, signature, secret, evidence, timeout=10):
        from .durable_upload_evidence import DurableUploadEvidence
        if (type(evidence) is not DurableUploadEvidence or getattr(store,'queue',None) is not evidence.queue
                or self._unreaped or not 0<timeout<=900):
            raise SessionHeld('owned upload sender required')
        _peer(channel,peer_sha256,False)
        if sign_grant(grant,secret)!=signature:raise SessionHeld('upload signature invalid')
        item=LocalUploadSender._issued_item(store,grant,claim_token)
        deadline=time.monotonic()+timeout
        completion=None
        try:
            _write(channel,{'grant':grant,'signature':signature},deadline)
            preflight=_read(channel,deadline)
            if preflight=={'state':'complete'}:return {'success':True}
            if preflight!={'state':'ready'}:raise SessionHeld('node preflight uncertain')
            completion=start_guarded_sender(item,grant['operationId'],deadline,self)
            remaining=item['bytes']
            while remaining:
                completion.source.settimeout(_remaining(deadline))
                chunk=completion.source.recv(min(65536,remaining))
                if not chunk:raise SessionHeld('sender stream truncated')
                channel.settimeout(_remaining(deadline));channel.sendall(chunk);remaining-=len(chunk)
            completion.verify(deadline)
            if LocalUploadSender._issued_item(store,grant,claim_token)!=item:
                raise SessionHeld('source identity changed')
            evidence._record_verified(grant,completion=completion,owner=self,store=store,claim_token=claim_token,item=item,deadline=deadline)
            response=_read(channel,deadline)
            if type(response) is not dict or response!={'success':True}:raise SessionHeld('node response uncertain')
            return response
        finally:
            if completion is not None:completion.stop()


def receive_upload(channel, *, peer_sha256, broker, timeout=10):
    """Forward one bounded stream to a node-local broker over its dedicated socket."""
    try:return _receive_upload(channel,peer_sha256=peer_sha256,broker=broker,timeout=timeout)
    finally:channel.close()


def _receive_upload(channel, *, peer_sha256, broker, timeout=10):
    if type(broker) is not NodeMutationBroker or not 0<timeout<=900:raise SessionHeld('node broker required')
    _peer(channel,peer_sha256,True);deadline=time.monotonic()+timeout
    header=_read(channel,deadline)
    if type(header) is not dict or set(header)!={'grant','signature'}:raise SessionHeld('upload header invalid')
    grant=header['grant'];signature=header['signature'];broker._validate_upload(grant,signature)
    prior=broker.lookup_upload(grant,signature)
    if prior is not None:
        try:_write(channel,{'state':'complete'},deadline);return prior
        finally:channel.close()
    _write(channel,{'state':'ready'},deadline)
    source,writer=socket.socketpair();stop=threading.Event();errors=[]
    def relay():
        try:
            remaining=grant['fileBytes']
            while remaining and not stop.is_set():
                channel.settimeout(_remaining(deadline));chunk=channel.recv(min(65536,remaining))
                if not chunk:raise SessionHeld('upload stream truncated')
                remaining-=len(chunk);writer.settimeout(_remaining(deadline));writer.sendall(chunk)
            if remaining:raise SessionHeld('upload stream interrupted')
            writer.shutdown(socket.SHUT_WR)
        except BaseException as exc:
            errors.append(exc)
            try:writer.shutdown(socket.SHUT_WR)
            except OSError:pass
    thread=threading.Thread(target=relay)
    try:thread.start()
    except BaseException:
        source.close();writer.close();channel.close();raise
    try:
        result=broker.upload(grant,signature,source)
        thread.join(max(0,deadline-time.monotonic()))
        if thread.is_alive() or errors:raise SessionHeld('data relay uncertain')
        broker.confirm_upload(grant,signature,_sender_confirmation(grant,broker.secret))
        _write(channel,{'success':True},deadline)
        return result
    finally:
        stop.set()
        try:writer.shutdown(socket.SHUT_RDWR)
        except OSError:pass
        writer.close()
        try:channel.shutdown(socket.SHUT_RDWR)
        except OSError:pass
        channel.close();thread.join(2)
        if thread.is_alive():raise SessionHeld('owned data relay did not exit')
