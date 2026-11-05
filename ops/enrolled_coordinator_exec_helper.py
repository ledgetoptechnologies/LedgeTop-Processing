"""Private Docker-exec lifecycle evidence; never an execution authority.

Only actual terminal PID/start observation retires an observer. An existing
request directory is inspected, never executed again after a lost CLI reply.
"""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import uuid


def require(value):
    if not value: raise RuntimeError('remote_exec_lifecycle_unavailable')


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def validate(settings):
    require(type(settings) is dict and set(settings) in (
        {'operationId','requestId','sourceSha256','maximum'},
        {'operationId','requestId','sourceSha256','maximum','ownerUid'}))
    for name in ('operationId','requestId'):
        require(type(settings[name]) is str and str(uuid.UUID(settings[name]))==settings[name])
    require(type(settings['sourceSha256']) is str and len(settings['sourceSha256'])==64
            and all(c in '0123456789abcdef' for c in settings['sourceSha256']))
    require(type(settings['maximum']) is int and 1<=settings['maximum']<=16*1024*1024)
    require(type(settings.get('ownerUid',10001)) is int and 0<settings.get('ownerUid',10001)<2**31)
    return Path('/tmp/ltds-exec-'+settings['operationId']+'-'+settings['requestId'])


def start_time(pid):
    return Path('/proc/'+str(pid)+'/stat').read_text().rsplit(')',1)[1].split()[19]


def write(path,value):
    raw=canonical(value)
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    try:
        offset=0
        while offset<len(raw):
            count=os.write(fd,raw[offset:]);require(count>0);offset+=count
        os.fsync(fd)
    finally:os.close(fd)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(fd)
    finally:os.close(fd)


def read(path,maximum,uid=10001):
    before=path.lstat()
    require(stat.S_ISREG(before.st_mode) and stat.S_IMODE(before.st_mode)==0o600
            and before.st_uid==uid and before.st_nlink==1 and before.st_size<=maximum)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    identity=lambda s:(s.st_dev,s.st_ino,s.st_mode,s.st_uid,s.st_nlink,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
    try:
        require(identity(before)==identity(os.fstat(fd)))
        raw=bytearray()
        while len(raw)<before.st_size:
            value=os.read(fd,min(65536,before.st_size-len(raw)));require(bool(value));raw.extend(value)
        require(identity(before)==identity(os.fstat(fd))==identity(path.lstat()))
    finally:os.close(fd)
    value=json.loads(raw);require(canonical(value)==raw);return value


class BoundedOutput(io.TextIOBase):
    def __init__(self,maximum):self.maximum=maximum;self.value=bytearray()
    def write(self,value):
        raw=value.encode('utf8');require(len(self.value)+len(raw)<=self.maximum)
        self.value.extend(raw);return len(value)
    def flush(self):pass


def execute(settings,source):
    root=validate(settings)
    require(os.geteuid()==settings.get('ownerUid',10001) and type(source) is bytes
            and hashlib.sha256(source).hexdigest()==settings['sourceSha256'])
    # No exist_ok: even an earlier request that never wrote its start marker
    # stays ambiguous. Its absence is not permission to dispatch the body again.
    root.mkdir(mode=0o700)
    identity=dict(settings=settings,pid=os.getpid(),start=start_time(os.getpid()))
    write(root/'start.json',identity)
    output=BoundedOutput(settings['maximum']);success=False;body_completed=False
    try:
        with contextlib.redirect_stdout(output),contextlib.redirect_stderr(BoundedOutput(16384)):
            exec(compile(source,'<reviewed-transport-request>','exec'),{'__name__':'__main__'})
        body_completed=True
    except SystemExit as error:
        # Existing reviewed stdin helpers conventionally end in SystemExit(0).
        # This denotes body completion, never the future OS process exit status.
        body_completed=error.code is None or type(error.code) is int and error.code==0
    except BaseException:
        body_completed=False
    try:
        require(body_completed)
        result=json.loads(bytes(output.value))
        require(len(canonical(result))<=settings['maximum'])
        write(root/'result.json',result);success=True
    except BaseException:
        # Never emit source exceptions, stderr, private inputs or partial output.
        success=False
    write(root/'exit.json',dict(identity=identity,success=success,
          resultSha256=hashlib.sha256(canonical(result)).hexdigest() if success else None))
    # This is only a dispatch response. The caller still checks actual process
    # absence; the exit marker precedes process exit by construction.
    print(json.dumps({'recorded':True},separators=(',',':')))


def inspect(settings):
    root=validate(settings);info=root.lstat()
    uid=settings.get('ownerUid',10001)
    require(root.resolve()==root and stat.S_ISDIR(info.st_mode)
            and info.st_uid==uid and stat.S_IMODE(info.st_mode)==0o700)
    start=read(root/'start.json',4096,uid)
    require(type(start) is dict and set(start)=={'settings','pid','start'} and start['settings']==settings
            and type(start['pid']) is int and start['pid']>0 and type(start['start']) is str and start['start'].isdigit())
    try:live=start_time(start['pid'])==start['start']
    except FileNotFoundError:live=False
    require(not live)
    end=read(root/'exit.json',8192,uid)
    require(type(end) is dict and set(end)=={'identity','success','resultSha256'}
            and end['identity']==start and type(end['success']) is bool)
    result=None
    if end['success']:
        result=read(root/'result.json',settings['maximum'],uid)
        require(hashlib.sha256(canonical(result)).hexdigest()==end['resultSha256'])
    else:require(end['resultSha256'] is None)
    return dict(settings=settings,processRetired=True,pid=start['pid'],start=start['start'],
                success=end['success'],result=result)


def serializable_source(settings,*,source=None):
    validate(settings)
    module=Path(__file__).read_text()
    tail=('execute('+repr(settings)+','+repr(source)+')' if source is not None else
          'print(json.dumps(inspect('+repr(settings)+'),sort_keys=True,separators=(",",":")))')
    return (module+'\n'+tail+'\n').encode()
