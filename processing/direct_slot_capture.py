"""Opt-in local streaming capture journal. Not an engine, auth or custody API."""
import hashlib
import io
import json
import os
import threading
import zipfile

from . import reference_protocol as p
from .direct_outbox_contract import validate_binding
from .direct_output_slot import SlotHeld, _file, _identity, _open_slot

CHUNK = 1024 * 1024


class _Prefix(io.RawIOBase):
    """Seekable exact prefix; preallocation bytes beyond length are invisible."""
    def __init__(self,fd,length): self.fd,self.length,self.pos=fd,length,0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.pos
    def seek(self,offset,whence=0):
        position = offset if whence==0 else self.pos+offset if whence==1 else self.length+offset if whence==2 else -1
        if position<0: raise ValueError('invalid seek')
        self.pos=position;return self.pos
    def readinto(self,buffer):
        data=os.pread(self.fd,min(len(buffer),max(0,self.length-self.pos)),self.pos)
        buffer[:len(data)]=data;self.pos+=len(data);return len(data)


def _digest_and_zip(slot,length):
    slot._check();digest=hashlib.sha256();offset=0
    while offset<length:
        data=os.pread(slot._fd,min(CHUNK,length-offset),offset)
        if not data: raise SlotHeld('capture_read_incomplete')
        digest.update(data);offset+=len(data)
    try:
        with zipfile.ZipFile(io.BufferedReader(_Prefix(slot._fd,length))) as archive:
            archive.infolist()  # Structural ZIP directory check, not semantic or decompression validation.
    except (OSError, ValueError, zipfile.BadZipFile, zipfile.LargeZipFile):
        raise SlotHeld('capture_zip_invalid') from None
    slot._check();return digest.hexdigest()


def _names(digest): return 'slot-'+digest+'.intent','slot-'+digest+'.seal'


def _exists(root_fd,name):
    try: os.stat(name,dir_fd=root_fd,follow_symlinks=False);return True
    except FileNotFoundError:return False


def _persist(root_fd,root_identity,name,record):
    data=p.canonical_json(record)
    fd=os.open(name,os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=root_fd)
    try:
        _file(fd,root_identity)
        if os.write(fd,data)!=len(data): raise SlotHeld('capture_journal_incomplete')
        os.fsync(fd);os.fsync(root_fd)
    finally:os.close(fd)


def _read_record(slot,name):
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=slot._root)
    try:
        info=_file(fd,slot._root_identity)
        if info.st_size>32768:raise SlotHeld('capture_journal_invalid')
        data=os.read(fd,32769)
        try:value=json.loads(data)
        except (ValueError,UnicodeError):raise SlotHeld('capture_journal_invalid') from None
        if p.canonical_json(value)!=data:raise SlotHeld('capture_journal_invalid')
        return value,fd
    except BaseException:
        os.close(fd);raise


def _check_record(slot,fd,name,value):
    info=_file(fd,slot._root_identity); named=os.stat(name,dir_fd=slot._root,follow_symlinks=False)
    data=p.canonical_json(value)
    if ((info.st_dev,info.st_ino)!=(named.st_dev,named.st_ino) or info.st_size!=len(data)
            or os.pread(fd,len(data)+1,0)!=data):raise SlotHeld('capture_journal_changed')


class Capture:
    def __init__(self,slot,intent,intent_name,seal_name):
        self._slot,self._intent,self._intent_name,self._seal_name=slot,intent,intent_name,seal_name
        self._position=0;self._digest=hashlib.sha256();self._held=False
        self._pid=os.getpid();self._lock=threading.RLock()

    def _check(self):
        if os.getpid()!=self._pid or self._slot is None or self._held:raise SlotHeld('capture_requires_inspection')
        self._slot._check()
        intent,fd=_read_record(self._slot,self._intent_name)
        try:
            if p.canonical_json(intent)!=p.canonical_json(self._intent) or _exists(self._slot._root,self._seal_name):
                raise SlotHeld('capture_journal_changed')
            _check_record(self._slot,fd,self._intent_name,intent)
        finally:os.close(fd)

    def _write(self,data):
        if os.getpid()!=self._pid:raise SlotHeld('capture_requires_inspection')
        with self._lock:
            self._check()
            if type(data) is not bytes or not data:
                self._held=True;raise SlotHeld('capture_chunk_invalid')
            limit=self._intent['declaredLength'] or self._slot.budget
            if self._position+len(data)>limit:
                self._held=True;raise SlotHeld('capture_length_exceeded')
            try:self._slot.write_at(self._position,data)
            except BaseException:self._held=True;raise
            self._digest.update(data);self._position+=len(data)

    def _finish(self):
        if os.getpid()!=self._pid:raise SlotHeld('capture_requires_inspection')
        with self._lock:
            self._check()
            try:
                if not self._position or (self._intent['declaredLength'] is not None and self._position!=self._intent['declaredLength']):
                    raise SlotHeld('capture_length_incomplete')
                self._slot.sync()
                actual=_digest_and_zip(self._slot,self._position)
                if actual!=self._digest.hexdigest():raise SlotHeld('capture_content_changed')
                seal=dict(schemaVersion=1,intentSha256=p.digest(self._intent),bytes=self._position,
                          sha256=actual,file=_identity(self._slot._fd))
                self._check()
                _persist(self._slot._root,self._slot._root_identity,self._seal_name,seal)
                return dict(seal)
            except BaseException:self._held=True;raise

    def consume(self,stream):
        """Read bounded chunks to actual EOF, then verify and seal locally."""
        if os.getpid()!=self._pid:raise SlotHeld('capture_requires_inspection')
        with self._lock:
            self._check()
            try:
                while True:
                    data=stream.read(min(CHUNK,self._slot.budget-self._position+1))
                    if type(data) is not bytes:raise SlotHeld('capture_chunk_invalid')
                    if not data:break
                    self._write(data)
                return self._finish()
            except BaseException:self._held=True;raise

    def close(self):
        if os.getpid()!=self._pid:raise SlotHeld('capture_requires_inspection')
        with self._lock:
            if self._slot is not None:self._slot.close();self._slot=None


def begin_capture(root,root_identity,binding,*,max_budget,declared_length=None,expected_slot_identity=None):
    """Durably bind a streaming attempt before allowing any capture writes."""
    digest=validate_binding(binding);budget=binding['outputBudgetBytes']
    if declared_length is not None and (type(declared_length) is not int or not 1<=declared_length<=budget):
        raise SlotHeld('capture_declared_length_invalid')
    slot=_open_slot(root,root_identity,digest,budget,max_budget=max_budget,allow_capture=True)
    intent_name,seal_name=_names(digest)
    try:
        if expected_slot_identity is not None:
            actual=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
            if expected_slot_identity!=actual:raise SlotHeld('capture_slot_identity_changed')
        if _exists(slot._root,intent_name) or _exists(slot._root,seal_name):
            raise SlotHeld('capture_requires_inspection')
        intent=dict(schemaVersion=1,binding=json.loads(p.canonical_json(binding)),
                    bindingSha256=digest,declaredLength=declared_length,file=_identity(slot._fd))
        _persist(slot._root,slot._root_identity,intent_name,intent)
        slot._check()
        return Capture(slot,intent,intent_name,seal_name)
    except BaseException:
        slot.close();raise


def _open_verified(root,root_identity,binding,*,max_budget):
    """Return verified, locked slot plus open journal descriptors."""
    digest=validate_binding(binding);budget=binding['outputBudgetBytes']
    slot=_open_slot(root,root_identity,digest,budget,max_budget=max_budget,allow_capture=True)
    intent_name,seal_name=_names(digest);intent_fd=seal_fd=None
    try:
        intent,intent_fd=_read_record(slot,intent_name);seal,seal_fd=_read_record(slot,seal_name)
        if (type(intent) is not dict or set(intent)!={'schemaVersion','binding','bindingSha256','declaredLength','file'}
            or type(intent['schemaVersion']) is not int or intent['schemaVersion']!=1
            or p.canonical_json(intent['binding'])!=p.canonical_json(binding) or intent['bindingSha256']!=digest
            or p.canonical_json(intent['file'])!=p.canonical_json(_identity(slot._fd))):
            raise SlotHeld('capture_intent_invalid')
        length=intent['declaredLength']
        if length is not None and (type(length) is not int or not 1<=length<=budget):
            raise SlotHeld('capture_intent_invalid')
        if (type(seal) is not dict or set(seal)!={'schemaVersion','intentSha256','bytes','sha256','file'}
            or type(seal['schemaVersion']) is not int or seal['schemaVersion']!=1
            or type(seal['bytes']) is not int or not 1<=seal['bytes']<=budget
            or (length is not None and length!=seal['bytes']) or seal['intentSha256']!=p.digest(intent)
            or p.canonical_json(seal['file'])!=p.canonical_json(_identity(slot._fd))):
            raise SlotHeld('capture_seal_invalid')
        p._hash(seal['sha256'],'capture sha256')
        if _digest_and_zip(slot,seal['bytes'])!=seal['sha256']:raise SlotHeld('capture_content_changed')
        os.fsync(slot._fd);os.fsync(intent_fd);os.fsync(seal_fd);os.fsync(slot._root);slot._check()
        _check_record(slot,intent_fd,intent_name,intent);_check_record(slot,seal_fd,seal_name,seal)
        return slot,intent_name,intent,intent_fd,seal_name,seal,seal_fd
    except BaseException as error:
        if intent_fd is not None:os.close(intent_fd)
        if seal_fd is not None:os.close(seal_fd)
        slot.close()
        if isinstance(error,(OSError,ValueError)):raise SlotHeld('capture_requires_inspection') from None
        raise


class SealedReader:
    """Exclusive locked, revalidated local stream; conveys no remote custody."""
    def __init__(self,slot,intent_name,intent,intent_fd,seal_name,seal,seal_fd):
        self._slot,self._intent_name,self._intent,self._intent_fd=slot,intent_name,intent,intent_fd
        self._seal_name,self._seal,self._seal_fd=seal_name,seal,seal_fd
        self._position=0;self._digest=hashlib.sha256();self._pid=os.getpid()
        self._lock=threading.RLock();self._held=False

    @property
    def length(self):return self._seal['bytes']

    @property
    def sha256(self):return self._seal['sha256']

    def _check(self):
        if os.getpid()!=self._pid or self._slot is None or self._held:raise SlotHeld('reader_requires_inspection')
        self._slot._check()
        _check_record(self._slot,self._intent_fd,self._intent_name,self._intent)
        _check_record(self._slot,self._seal_fd,self._seal_name,self._seal)

    def read(self,size):
        if os.getpid()!=self._pid:raise SlotHeld('reader_requires_inspection')
        if type(size) is not int or not 1<=size<=CHUNK:raise SlotHeld('reader_chunk_invalid')
        with self._lock:
            try:
                self._check()
                if self._position==self.length:return b''
                count=min(size,self.length-self._position)
                data=os.pread(self._slot._fd,count,self._position)
                if len(data)!=count:raise SlotHeld('reader_incomplete')
                self._digest.update(data);self._position+=count
                self._check()
                if self._position==self.length:
                    # Do not release the last chunk until a second full read
                    # validates bytes again while the same flock is held.
                    if self._digest.hexdigest()!=self.sha256 or _digest_and_zip(self._slot,self.length)!=self.sha256:
                        raise SlotHeld('reader_content_changed')
                    self._check()
                return data
            except OSError:
                self._held=True;raise SlotHeld('reader_requires_inspection') from None
            except BaseException:
                self._held=True;raise

    def close(self):
        if os.getpid()!=self._pid:raise SlotHeld('reader_requires_inspection')
        with self._lock:
            if self._slot is not None:
                try:os.close(self._intent_fd)
                finally:
                    try:os.close(self._seal_fd)
                    finally:
                        self._slot.close();self._slot=None

    def __enter__(self):return self
    def __exit__(self,*_):self.close()


def open_sealed_reader(root,root_identity,binding,*,max_budget):
    """Open a verified stream and retain its cooperative lock until close."""
    return SealedReader(*_open_verified(root,root_identity,binding,max_budget=max_budget))


def verify_sealed(root,root_identity,binding,*,max_budget):
    """Point-in-time verification only; the lock is released on return."""
    with open_sealed_reader(root,root_identity,binding,max_budget=max_budget) as reader:
        return dict(reader._seal)
