"""Unwired synthetic v2 chunk journal; no HTTP, auth, deployment or cleanup.

Caller supplies a previously allocated exclusive OutputSlot and a verified
synthetic binding. Legacy/ambiguous spool state must never be imported.
"""
import hashlib
import os
from . import reference_protocol as p
from .direct_output_slot import OutputSlot,SlotHeld,_identity,_open_slot
from .direct_slot_capture import _persist,_read_record,_check_record,_exists

MAX_CHUNK=1024*1024
MAX_CHUNKS=4096

class V2ChunkHeld(RuntimeError):pass

def open_synthetic_v2_chunk_slot(root,root_identity,digest,budget,*,max_budget,binding,legacy_state,synthetic_fixture=False):
    """Explicit fixture-only open, qualified by exact journal binding."""
    if synthetic_fixture is not True:raise V2ChunkHeld('synthetic_fixture_required')
    slot=_open_slot(root,root_identity,digest,budget,max_budget=max_budget,allow_chunk_journal=True)
    try:
        SyntheticV2ChunkJournal(slot,binding,legacy_state=legacy_state,synthetic_fixture=True)
        return slot
    except BaseException:
        slot.close();raise


class SyntheticV2ChunkJournal:
    def __init__(self,slot,binding,*,legacy_state,synthetic_fixture=False,allow_create=False):
        if synthetic_fixture is not True or type(slot) is not OutputSlot:raise V2ChunkHeld('synthetic_fixture_required')
        if legacy_state!='new':raise V2ChunkHeld('legacy_spool_requires_inspection')
        p._closed(binding,('jobId','taskId','generation','environment','archiveBytes','archiveSha256','version','chunkBytes','chunkCount'),'chunk binding')
        p._uuid(binding['jobId'],'job');p._uuid(binding['taskId'],'task');p._integer(binding['generation'],'generation');p._environment(binding['environment']);p._integer(binding['archiveBytes'],'bytes');p._hash(binding['archiveSha256'],'hash')
        from .viewer_chunk_v2_geometry import geometry
        expected_geometry=geometry(binding['archiveBytes'],binding['chunkBytes'],binding['version'])
        if binding['generation']<1 or binding['archiveBytes']>slot.budget or expected_geometry['chunkCount']!=binding['chunkCount']:raise V2ChunkHeld('archive_size_invalid')
        self.slot=slot;self.binding=p.decode_submission(p.canonical_json(binding));self.digest=p.digest(self.binding)
        self.prefix=slot._name+'.chunks-v2.'
        self.header=self.prefix+'header';self.seal=self.prefix+'seal'
        self.expected=dict(schemaVersion=2,binding=self.binding,root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
        slot._check()
        if not _exists(slot._root,self.header):
            if not allow_create:raise V2ChunkHeld('v2_header_missing')
            # Only absence of the header needs a full ambiguity scan. Once it
            # exists, exact header verification below is sufficient; scanning
            # thousands of prior records on every reopen was quadratic.
            if any(n.startswith(self.prefix) for n in os.listdir(slot._root)):
                raise V2ChunkHeld('journal_requires_inspection')
            _persist(slot._root,slot._root_identity,self.header,self.expected)
        self._verify(self.header,self.expected)

    def _verify(self,name,expected):
        value,fd=_read_record(self.slot,name)
        try:
            _check_record(self.slot,fd,name,value)
            if p.canonical_json(value)!=p.canonical_json(expected):raise V2ChunkHeld('journal_changed')
            os.fsync(fd);_check_record(self.slot,fd,name,value)
        finally:os.close(fd)
        os.fsync(self.slot._root);self.slot._check()
        return value

    def _names(self,index):
        if type(index) is not int or not 0<=index<MAX_CHUNKS:raise V2ChunkHeld('chunk_index_invalid')
        base=self.prefix+str(index).zfill(4)
        return base+'.intent',base+'.receipt'

    def verify_chunk(self,index,offset,length,sha256):
        """Revalidate exact committed chunk bytes while holding the slot lock."""
        with self.slot._operation_lock:
            if type(offset) is not int or offset<0 or type(length) is not int or not 1<=length<=MAX_CHUNK or offset+length>self.binding['archiveBytes']:
                raise V2ChunkHeld('chunk_spec_invalid')
            if (type(index) is not int or not 0<=index<self.binding['chunkCount'] or offset!=index*self.binding['chunkBytes']
                    or length!=min(self.binding['chunkBytes'],self.binding['archiveBytes']-offset)):
                raise V2ChunkHeld('chunk_geometry_changed')
            p._hash(sha256,'chunk sha256')
            self._verify(self.header,self.expected)
            intent_name,receipt_name=self._names(index)
            intent=dict(schemaVersion=2,bindingSha256=self.digest,index=index,offset=offset,length=length,sha256=sha256)
            receipt=dict(schemaVersion=2,intentSha256=p.digest(intent))
            self._verify(intent_name,intent);self._verify(receipt_name,receipt)
            self.slot._check();data=os.pread(self.slot._fd,length,offset);self.slot._check()
            if len(data)!=length or hashlib.sha256(data).hexdigest()!=sha256:raise V2ChunkHeld('chunk_content_changed')
            return receipt

    def write(self,index,offset,data):
        with self.slot._operation_lock:return self._write(index,offset,data)

    def _write(self,index,offset,data):
        if type(data) is not bytes or not 1<=len(data)<=MAX_CHUNK:raise V2ChunkHeld('chunk_invalid')
        if (type(index) is not int or not 0<=index<self.binding['chunkCount'] or offset!=index*self.binding['chunkBytes']
                or len(data)!=min(self.binding['chunkBytes'],self.binding['archiveBytes']-offset)):
            raise V2ChunkHeld('chunk_geometry_changed')
        if type(offset) is not int or offset<0 or offset+len(data)>self.binding['archiveBytes']:raise V2ChunkHeld('offset_invalid')
        self._verify(self.header,self.expected);self.slot._check()
        if _exists(self.slot._root,self.seal):raise V2ChunkHeld('already_sealed')
        intent_name,receipt_name=self._names(index)
        intent=dict(schemaVersion=2,bindingSha256=self.digest,index=index,offset=offset,length=len(data),sha256=hashlib.sha256(data).hexdigest())
        receipt=dict(schemaVersion=2,intentSha256=p.digest(intent))
        if _exists(self.slot._root,intent_name):
            if not _exists(self.slot._root,receipt_name):raise V2ChunkHeld('chunk_requires_inspection')
            self._verify(intent_name,intent);self._verify(receipt_name,receipt)
            if os.pread(self.slot._fd,len(data),offset)!=data:raise V2ChunkHeld('chunk_content_changed')
            return receipt
        if _exists(self.slot._root,receipt_name):raise V2ChunkHeld('journal_requires_inspection')
        if index:
            previous_name=self._names(index-1)[0]
            previous,fd=_read_record(self.slot,previous_name)
            try:_check_record(self.slot,fd,previous_name,previous)
            finally:os.close(fd)
            self._verify(self._names(index-1)[1],dict(schemaVersion=2,intentSha256=p.digest(previous)))
            if previous.get('index')!=index-1 or previous.get('bindingSha256')!=self.digest or previous.get('offset',-1)+previous.get('length',0)!=offset:raise V2ChunkHeld('chunk_sequence_changed')
        elif offset!=0:raise V2ChunkHeld('offset_invalid')
        _persist(self.slot._root,self.slot._root_identity,intent_name,intent)
        self.slot.write_at(offset,data);self.slot.sync()
        if os.pread(self.slot._fd,len(data),offset)!=data:raise V2ChunkHeld('chunk_content_changed')
        _persist(self.slot._root,self.slot._root_identity,receipt_name,receipt)
        self.slot._check();return receipt

    def finish(self,count):
        with self.slot._operation_lock:return self._finish(count)

    def _finish(self,count):
        if type(count) is not int or count!=self.binding['chunkCount']:raise V2ChunkHeld('count_invalid')
        self._verify(self.header,self.expected);offset=0;digest=hashlib.sha256()
        for index in range(count):
            intent_name,receipt_name=self._names(index);intent,fd=_read_record(self.slot,intent_name)
            try:_check_record(self.slot,fd,intent_name,intent)
            finally:os.close(fd)
            if intent.get('bindingSha256')!=self.digest or intent.get('index')!=index or intent.get('offset')!=offset or type(intent.get('length')) is not int or not 1<=intent['length']<=MAX_CHUNK:raise V2ChunkHeld('chunk_sequence_changed')
            self._verify(receipt_name,dict(schemaVersion=2,intentSha256=p.digest(intent)))
            data=os.pread(self.slot._fd,intent['length'],offset)
            if len(data)!=intent['length'] or hashlib.sha256(data).hexdigest()!=intent.get('sha256'):raise V2ChunkHeld('chunk_content_changed')
            digest.update(data);offset+=len(data)
        if offset!=self.binding['archiveBytes'] or digest.hexdigest()!=self.binding['archiveSha256']:raise V2ChunkHeld('archive_mismatch')
        seal=dict(schemaVersion=2,bindingSha256=self.digest,count=count,bytes=offset,sha256=digest.hexdigest(),allocation=_identity(self.slot._fd))
        self.slot.sync()
        if _exists(self.slot._root,self.seal):self._verify(self.seal,seal)
        else:_persist(self.slot._root,self.slot._root_identity,self.seal,seal)
        self.slot._check();return seal
