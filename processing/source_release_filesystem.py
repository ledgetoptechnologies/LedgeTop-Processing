"""Exact-object Linux quarantine mechanics, authorized by SourceReleaseService.

This module owns no scheduler, lease, expiry policy or cleanup controller. Its
caller retains the existing coordinator lease and an exclusive release pin.
"""
from contextlib import contextmanager, ExitStack
import ctypes
import errno
import hashlib
import json
import os
import stat
import sys

from . import reference_protocol as p
from .source_spool import _close_owned_fd, SourceSpoolError

MAX_PHYSICAL_BYTES=1024


def validate_qualification_value(value):
    p._closed(value,('mountId','rootDevice','rootInode','mountDevice','mountInode',
        'markerDevice','markerInode','renameNoReplace','directoryFsync'),'source release filesystem qualification')
    p._hash(value['mountId'],'qualified source mount')
    for name in ('rootDevice','rootInode','mountDevice','mountInode','markerDevice','markerInode'):
        text=value[name]
        p._require(type(text) is str and text.isascii() and text.isdecimal()
            and len(text)<=20 and str(int(text))==text and int(text)<2**64,'qualified filesystem identity required')
    p._require(type(value['renameNoReplace']) is bool and type(value['directoryFsync']) is bool,'explicit qualification flags required')


class ReleaseFilesystemError(ValueError):
    def __init__(self,reason):
        self.release_reason=reason
        super().__init__(reason)


def file_identity(info):
    return [getattr(info,name) for name in ('st_dev','st_ino','st_size','st_mode',
        'st_nlink','st_uid','st_mtime_ns','st_ctime_ns')]


def directory_identity(info):
    return [info.st_dev,info.st_ino,info.st_mode,info.st_uid]


def validate_physical(phase,physical,obj):
    allowed={'root','job','generation','quarantine','file','quarantinedFile','effectIntent'}
    if type(physical) is not dict or set(physical)-allowed:raise ReleaseFilesystemError('journal_invalid')
    if phase=='planned':
        if physical:raise ReleaseFilesystemError('journal_invalid')
        return
    required={'root','job','generation','file'}
    if phase in ('quarantining','quarantined','unlinking','removed'):required.add('quarantine')
    if phase in ('quarantined','unlinking','removed'):required.add('quarantinedFile')
    if phase not in ('verified','quarantining','quarantined','unlinking','removed') or not required<=set(physical):
        raise ReleaseFilesystemError('journal_invalid')
    if phase in ('verified','quarantining') and 'quarantinedFile' in physical:
        raise ReleaseFilesystemError('journal_invalid')
    for name in ('root','job','generation','quarantine','file','quarantinedFile'):
        if name not in physical:continue
        identity=physical[name];size=8 if name in ('file','quarantinedFile') else 4
        if type(identity) is not list or len(identity)!=size or any(type(value) is not int or not 0<=value<2**64 for value in identity):
            raise ReleaseFilesystemError('journal_invalid')
        if size==8:
            if (identity[2]!=obj['byteSize'] or identity[3]>0o177777 or identity[5]>=2**32
                    or identity[6]>=2**63 or identity[7]>=2**63
                    or not stat.S_ISREG(identity[3]) or identity[4]!=1):
                raise ReleaseFilesystemError('journal_invalid')
        elif identity[2]>0o177777 or identity[3]>=2**32 or not stat.S_ISDIR(identity[2]) or identity[2]&0o022:
            raise ReleaseFilesystemError('journal_invalid')
    if 'quarantinedFile' in physical and physical['quarantinedFile'][:-1]!=physical['file'][:-1]:
        raise ReleaseFilesystemError('journal_invalid')
    if 'effectIntent' in physical:
        intent=physical['effectIntent'];p._closed(intent,('id','kind','outcome'),'source effect intent')
        p._uuid(intent['id'],'source effect ID')
        if intent['kind'] not in ('create_quarantine','rename','sync_quarantine','unlink') or intent['outcome'] not in ('prepared','completed'):
            raise ReleaseFilesystemError('journal_invalid')
        by_phase={'verified':('create_quarantine',),'quarantining':('rename','sync_quarantine'),
            'unlinking':('unlink',)}
        if intent['kind'] not in by_phase.get(phase,()):raise ReleaseFilesystemError('journal_invalid')


def encode_physical(phase,physical,obj):
    """Bound current AND completed effect outcome before issuing any effect."""
    validate_physical(phase,physical,obj)
    encoded=p.canonical_json(physical).decode('ascii')
    longest=encoded
    if physical.get('effectIntent',{}).get('outcome')=='prepared':
        completed=dict(physical,effectIntent=dict(physical['effectIntent'],outcome='completed'))
        longest=p.canonical_json(completed).decode('ascii')
    if max(len(encoded),len(longest))>MAX_PHYSICAL_BYTES:
        raise ReleaseFilesystemError('metadata_quota')
    return encoded


class SourceReleaseFilesystem:
    def __init__(self,spool,qualification):
        self.spool,self.qualification=spool,qualification

    def check_qualification(self):
        q=self.qualification
        validate_qualification_value(q)
        p._require(self.spool is not None,'source spool required')
        p._require(q['mountId']==self.spool.mount_id and q['renameNoReplace'] is True
            and q['directoryFsync'] is True,'source filesystem qualification required')
        identities=dict(rootDevice=self.spool._root_identity[0],rootInode=self.spool._root_identity[1],
            mountDevice=self.spool._mount_identity[0],mountInode=self.spool._mount_identity[1],
            markerDevice=self.spool._marker_identity[0],markerInode=self.spool._marker_identity[1])
        for name,expected in identities.items():
            p._require(type(q[name]) is str and q[name]==str(expected),'source filesystem qualification changed')
        if not sys.platform.startswith('linux') or not hasattr(os,'O_NOFOLLOW'):
            raise ReleaseFilesystemError('filesystem_unsupported')
        required={'open','stat','unlink','mkdir'}
        if not required<={call.__name__ for call in os.supports_dir_fd}:
            raise ReleaseFilesystemError('filesystem_unsupported')
        try:self.rename=ctypes.CDLL(None,use_errno=True).renameat2
        except AttributeError:raise ReleaseFilesystemError('filesystem_unsupported') from None
        self.rename.argtypes=(ctypes.c_int,ctypes.c_char_p,ctypes.c_int,ctypes.c_char_p,ctypes.c_uint)
        self.rename.restype=ctypes.c_int
        try:self.spool._assert_mount()
        except SourceSpoolError:raise ReleaseFilesystemError('qualification_changed') from None

    def _safe_directory(self,fd):
        info=os.fstat(fd)
        if (not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid()
                or info.st_mode&0o022 or info.st_dev!=self.spool._root_identity[0]):
            raise ReleaseFilesystemError('source_identity_changed')
        return directory_identity(info)

    def _directory(self,stack,parent,name):
        fd=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
        stack.callback(_close_owned_fd,fd)
        self._safe_directory(fd)
        return fd

    @contextmanager
    def _context(self,auth):
        self.check_qualification()
        with ExitStack() as stack:
            root=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
            stack.callback(_close_owned_fd,root)
            for part in self.spool.root.parts[1:]:
                root=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=root)
                stack.callback(_close_owned_fd,root)
            root_identity=self._safe_directory(root)
            if root_identity[:2]!=list(self.spool._root_identity):
                raise ReleaseFilesystemError('qualification_changed')
            try:
                job=self._directory(stack,root,auth['jobId'])
                generation=self._directory(stack,job,str(auth['generation']))
            except FileNotFoundError:raise ReleaseFilesystemError('source_missing') from None
            qname='.ltds-release-'+auth['releaseId']
            try:quarantine=self._directory(stack,generation,qname)
            except FileNotFoundError:quarantine=None
            context=dict(root=root,job=job,generation=generation,quarantine=quarantine,
                quarantineName=qname,stack=stack)
            self._parents(auth,context)
            yield context

    def _parents(self,auth,ctx,physical=None):
        self.spool._assert_mount()
        observed=dict(root=self._safe_directory(ctx['root']),job=self._safe_directory(ctx['job']),
            generation=self._safe_directory(ctx['generation']))
        if directory_identity(os.stat(auth['jobId'],dir_fd=ctx['root'],follow_symlinks=False))!=observed['job']:
            raise ReleaseFilesystemError('source_identity_changed')
        if directory_identity(os.stat(str(auth['generation']),dir_fd=ctx['job'],follow_symlinks=False))!=observed['generation']:
            raise ReleaseFilesystemError('source_identity_changed')
        if ctx['quarantine'] is not None:
            observed['quarantine']=self._safe_directory(ctx['quarantine'])
            if directory_identity(os.stat(ctx['quarantineName'],dir_fd=ctx['generation'],follow_symlinks=False))!=observed['quarantine']:
                raise ReleaseFilesystemError('quarantine_conflict')
        if physical:
            for key in ('root','job','generation','quarantine'):
                if key in physical and physical[key]!=observed.get(key):
                    raise ReleaseFilesystemError('source_identity_changed')
        return observed

    def _names(self,auth,obj):
        original=self.spool.storage_key(auth['jobId'],auth['generation'],obj['id']).split('/')[-1]
        quarantine=obj['objectKind']+'-'+hashlib.sha256(obj['id'].encode('ascii')).hexdigest()+'.blob'
        return original,quarantine

    @staticmethod
    def _stat(fd,name):
        if fd is None:return None
        try:return os.stat(name,dir_fd=fd,follow_symlinks=False)
        except FileNotFoundError:return None

    def _verified(self,parent,name,obj,expected=None,renamed=False):
        try:fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC,dir_fd=parent)
        except FileNotFoundError:raise ReleaseFilesystemError('source_missing') from None
        try:
            info=os.fstat(fd);identity=file_identity(info)
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.geteuid()
                    or info.st_dev!=self.spool._root_identity[0] or info.st_size!=obj['byteSize']):
                raise ReleaseFilesystemError('source_identity_changed')
            if expected is not None and (identity[:-1] if renamed else identity)!=(expected[:-1] if renamed else expected):
                raise ReleaseFilesystemError('source_identity_changed')
            digest=hashlib.sha256();total=0
            while True:
                chunk=os.read(fd,self.spool.chunk_bytes)
                if not chunk:break
                total+=len(chunk)
                if total>obj['byteSize']:raise ReleaseFilesystemError('source_identity_changed')
                digest.update(chunk)
            if (total!=obj['byteSize'] or digest.hexdigest()!=obj['sha256']
                    or file_identity(os.fstat(fd))!=identity
                    or file_identity(os.stat(name,dir_fd=parent,follow_symlinks=False))!=identity):
                raise ReleaseFilesystemError('source_identity_changed')
            return identity
        finally:_close_owned_fd(fd)

    def _rename_owned(self,source_fd,source_name,destination_fd,destination_name):
        if self.rename(source_fd,source_name.encode('ascii'),destination_fd,destination_name.encode('ascii'),1)!=0:
            if ctypes.get_errno()==errno.EEXIST:raise ReleaseFilesystemError('quarantine_conflict')
            raise OSError(ctypes.get_errno(),'source quarantine rename failed')

    def audit(self,auth,journal,preserved,*,verify_remaining=False):
        """Closed namespace and permanent-object check; never repair or unlink."""
        with self._context(auth) as ctx:
            rows={(row['object_kind'],row['object_id']):row for row in journal}
            expected={};quarantine_expected=set()
            for obj in auth['eligibleObjects']:
                row=rows.get((obj['objectKind'],obj['id']))
                if not row:raise ReleaseFilesystemError('journal_invalid')
                original,quarantine=self._names(auth,obj)
                if row['phase'] not in ('planned','verified','quarantining','quarantined','unlinking','removed'):
                    raise ReleaseFilesystemError('journal_invalid')
                source=self._stat(ctx['generation'],original);copy=self._stat(ctx['quarantine'],quarantine)
                if row['phase'] in ('planned','verified'):
                    if source is None or copy is not None:raise ReleaseFilesystemError('source_missing')
                elif row['phase']=='quarantining':
                    if (source is None)==(copy is None):raise ReleaseFilesystemError('quarantine_conflict')
                elif row['phase']=='quarantined':
                    if source is not None or copy is None:raise ReleaseFilesystemError('quarantine_conflict')
                elif row['phase'] in ('unlinking','removed'):
                    if source is not None or (row['phase']=='removed' and copy is not None):
                        raise ReleaseFilesystemError('quarantine_conflict')
                if source is not None:expected[original]=obj
                if copy is not None:quarantine_expected.add(quarantine)
                physical=json.loads(row['physical_json'])
                validate_physical(row['phase'],physical,obj)
                if physical:self._parents(auth,ctx,physical)
                if verify_remaining:
                    if source is not None:self._verified(ctx['generation'],original,obj,physical.get('file'))
                    if copy is not None:self._verified(ctx['quarantine'],quarantine,obj,
                        physical.get('quarantinedFile') or physical.get('file'),renamed='quarantinedFile' not in physical)
            for obj in preserved:
                original,_=self._names(auth,obj);expected[original]=obj
                self._verified(ctx['generation'],original,obj)
            names=set(os.listdir(ctx['generation']))
            if ctx['quarantine'] is not None:names.discard(ctx['quarantineName'])
            if names!=set(expected):raise ReleaseFilesystemError('source_identity_changed')
            if ctx['quarantine'] is not None and set(os.listdir(ctx['quarantine']))!=quarantine_expected:
                raise ReleaseFilesystemError('quarantine_conflict')

    def remove_one(self,auth,obj,row,persist,effect):
        with self._context(auth) as ctx:
            original,name=self._names(auth,obj)
            physical=json.loads(row['physical_json']);phase=row['phase']
            validate_physical(phase,physical,obj)
            if phase=='planned':
                physical=self._parents(auth,ctx)
                physical['file']=self._verified(ctx['generation'],original,obj)
                persist('verified',physical);phase='verified'
            self._parents(auth,ctx,physical)
            if ctx['quarantine'] is None:
                if phase!='verified':raise ReleaseFilesystemError('source_missing')
                def create():
                    self._parents(auth,ctx,physical)
                    os.mkdir(ctx['quarantineName'],0o700,dir_fd=ctx['generation'])
                    os.fsync(ctx['generation'])
                effect('create_quarantine',create)
                ctx['quarantine']=self._directory(ctx['stack'],ctx['generation'],ctx['quarantineName'])
            if 'quarantine' not in physical:
                physical['quarantine']=self._safe_directory(ctx['quarantine'])
                persist(phase,physical)
            self._parents(auth,ctx,physical)
            if phase in ('verified','quarantining'):
                if phase=='verified':
                    persist('quarantining',physical);phase='quarantining'
                source=self._stat(ctx['generation'],original);copy=self._stat(ctx['quarantine'],name)
                if source is not None and copy is None:
                    self._verified(ctx['generation'],original,obj,physical['file'])
                    def rename():
                        self._parents(auth,ctx,physical)
                        if file_identity(os.stat(original,dir_fd=ctx['generation'],follow_symlinks=False))!=physical['file']:
                            raise ReleaseFilesystemError('source_identity_changed')
                        self._rename_owned(ctx['generation'],original,ctx['quarantine'],name)
                        os.fsync(ctx['generation']);os.fsync(ctx['quarantine'])
                    effect('rename',rename)
                elif source is not None or copy is None:raise ReleaseFilesystemError('quarantine_conflict')
                physical['quarantinedFile']=self._verified(ctx['quarantine'],name,obj,physical['file'],renamed=True)
                # Recovery after the rename also re-establishes durability.
                effect('sync_quarantine',lambda:(self._parents(auth,ctx,physical),os.fsync(ctx['generation']),os.fsync(ctx['quarantine'])))
                persist('quarantined',physical);phase='quarantined'
            if phase in ('quarantined','unlinking'):
                if self._stat(ctx['generation'],original) is not None:raise ReleaseFilesystemError('quarantine_conflict')
                copy=self._stat(ctx['quarantine'],name)
                if copy is None and phase!='unlinking':raise ReleaseFilesystemError('source_missing')
                if copy is not None:self._verified(ctx['quarantine'],name,obj,physical['quarantinedFile'])
                if phase=='quarantined':persist('unlinking',physical)
                def unlink():
                    self._parents(auth,ctx,physical)
                    current=self._stat(ctx['quarantine'],name)
                    if current is not None:
                        if file_identity(current)!=physical['quarantinedFile']:
                            raise ReleaseFilesystemError('source_identity_changed')
                        os.unlink(name,dir_fd=ctx['quarantine'])
                    os.fsync(ctx['quarantine']);os.fsync(ctx['generation'])
                effect('unlink',unlink)
                persist('removed',physical)
