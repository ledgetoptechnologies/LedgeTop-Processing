"""Exact reviewed historical producers plus current original namespace custody.

No arbitrary approval boolean, old PID guess, script execution or new process
controller. The reviewed receipt is pinned by the existing cutover gate. Local
producer terminal evidence does not substitute for current native/ingress/Gate
checks, nor claim the failed e8 processing pipeline succeeded.
"""
import hashlib
import json
from pathlib import Path
import re
import uuid

from .enrolled_coordinator_transport import canonical,decode,digest,require,private_bytes

PROTOCOL='ltds-historical-pilot-retirement-v1'
OPS=('lt-aa5-045fe15bf4766f6984da90be919257d5','lt-aa5-23bce8ed95d85869c2ff243f8c481fc7',
     'lt-aa5-e8f8fe018ae87130994c3a92d36f2f26')
# Full original newline-record hashes, independently traced from the two exact
# pilot-author rollouts. Exec sessions below are NOT Windows process IDs.
RECORDS=(
 ('acquire',OPS[0],'65c80f8e7d69dee6491ad6c318519cc0f345fdc0a439649399fe5a30b84b59e0','400e3e531d8447b64b773a33188a42e5177742dd0e7028753e032b112dd48d36','call_ac81927654b64e819bddeee1dfc76524','21072','2026-10-30T20:42:00.822Z','2026-10-30T20:42:11.626Z','447c830f871bac97f69af7e827929b75d7faac4ec7e63ec0199bcbd1e7df18e8','cbe9f77dc3bafdc500ad54a04299b908a87463a16ca19a3d53f513ce804d6324','15545d4bf4dd9ce4edb9c69b11d7a449b1783af08f357280f2b8a76c00fc12c5',0),
 ('release',OPS[0],'81279d6392901c792873fb4842e6305a69827bc4631ff83f0e4815b2a29efd66','7e016129df469ad6c5893629ff313f8f6580bd09735c37189d4d173cfb91fca8','call_9f856429f7e24ff29db7372d46eaa91b','46744','2026-10-30T20:56:28.927Z','2026-10-30T20:56:46.721Z','1c06ad3dcfbbf398864b309b1e906ad487181363e99202a12c5c3d11ab52eaab','340998b6777e11b59a87436d4aab60899c8bb83da5b4d3f4cdeb683947f79f77','bf7336a4ed6aae4804c4d73e15d07f3bf01e7f188a692888e6beeba3e82daecd',0),
 ('acquire',OPS[1],'65c80f8e7d69dee6491ad6c318519cc0f345fdc0a439649399fe5a30b84b59e0','400e3e531d8447b64b773a33188a42e5177742dd0e7028753e032b112dd48d36','call_420c4ec8b0494831a7b45bdee44787aa','39989','2026-10-30T22:34:52.424Z','2026-10-30T22:35:03.685Z','43d8c1e802c2bcc4d35d5dc8c4739bdeab26787c2581d5c1af2bd6feec464365','4b24a5957b6de7691a600f3faf0d83a413d3dc78d09174864224b77bf612b8f8','1aec13ec93b21a30a3f82bef58c4cb301f9a280ef5ca0099d0fcd6eadfbeb0a7',0),
 ('release',OPS[1],'3c329e21e277af07496ed796eddedd0e5c370768a3add826d591c134c966c644','4ef8549f3d79d25ad61eb6e92dd5d2360b0e786213909f6e673590f048b4ad50','call_483dec794df548268d3eba23ed89d94e','72598','2026-10-30T22:44:34.097Z','2026-10-30T22:44:50.457Z','6398d0cf3111bc20252348d00e8f1ae5d0a0505c90c32dc25d9ec2b64637165f','a41e80f9d2037777f8a83caa39da7b8c360c8f0e8978cb0279299d363b427fe5','c77cc34624e1b011356348e30781450aed3c79dc3d9258e6ed0606e22f9207b5',0),
 ('owner',OPS[2],'ce0100de374426a0bfa00ec9dcb6fc89c7f034fc561b68cb1ec0d2f38f89d333','1fbf2e2712d22671e62a71a34286d4f7af5e36226b8f4131cf5b860ab9c7c512','call_5dcbe5b118cd46848eaf7bd38192b6dd','99958','2026-10-31T07:03:20.291Z','2026-10-31T07:12:41.491Z','04c76a30d5862db2594f144d42dbd350a374bfc9edcf447d461cba32c4f14bff','c271f67f1fb5896d8e616c0ac4618bcb897b670eac3130b53ac3d03a6162fc45','264e1617ba5191776d9d18c2f2e0a4c2690c8393fd30b672a6c6e32117ee4ddc',1),
)
FIELDS=('action','operation','sourceSha256','effectiveSourceSha256','callId','execSession','invokedAt','terminalAt',
        'invocationRecordSha256','returnedSessionRecordSha256','terminalRecordSha256','exitCode')
GUARDS=(('2f7bca166840cf2b014f89831ec7bfc22286abde0161acea569a07fa581f7c41',64),
        ('8e81d1dfd2afe80495f880e2e8caca04700747c463bf46ed7c9908fb51d726b0',67),
        ('8d7a0ad14641ce8511d1262c0babb18df3666c2e5271b906766b3a1cc5521d08',56))


def historical_receipt(authority,predecessor,opaque):
    require(type(authority) is str and str(uuid.UUID(authority))==authority
            and type(predecessor) is str and re.fullmatch('[a-f0-9]{64}',predecessor),'exact_historical_authority_required')
    require(type(opaque) is dict and set(opaque)=={'files','directories','bytes','manifestSha256'}
            and opaque['files']==24 and opaque['directories']==4 and opaque['bytes']==6378
            and type(opaque['manifestSha256']) is str and re.fullmatch('[a-f0-9]{64}',opaque['manifestSha256']),
            'exact_retained_pilot_manifest_required')
    return dict(schemaVersion=1,protocol=PROTOCOL,authorityId=authority,predecessorContainerId=predecessor,
        opaquePilotJournals=opaque,invocations=[dict(zip(FIELDS,row)) for row in RECORDS],
        guardTransforms=[dict(sha256=pin,bytes=size) for pin,size in GUARDS],
        lifetimeProfiles=['foreground-synchronous-bounded-subprocess','windows-job-kill-on-close-0x2000-no-breakaway'],
        scope='historical-local-producer-termination')


def validate_historical_receipt(raw,*,authority,predecessor,opaque):
    require(type(raw) is bytes and len(raw)<=65536,'bounded_historical_pilot_receipt_required')
    expected=historical_receipt(authority,predecessor,opaque)
    value=decode(raw,65536)
    require(value==expected and raw==canonical(expected),'historical_pilot_source_or_terminal_binding_changed')
    return value


def opaque_pilot_source(key):
    require(type(key) is bytes and len(key)==32,'existing_private_census_key_required')
    return ('''import os,stat,json,hmac,hashlib,time
from pathlib import Path
key=bytes.fromhex('''+repr(key.hex())+''');operations='''+repr(OPS)+'''
root=Path('/prototype-state/pilot-journals');manifest={};files=0;directories=0;total=0;deadline=time.monotonic()+10
def same(s):return (s.st_dev,s.st_ino,s.st_mode,s.st_nlink,s.st_uid,s.st_gid,s.st_size,s.st_mtime_ns,s.st_ctime_ns)
def checked(path,directory):
 s=path.lstat()
 if time.monotonic()>deadline or path.resolve()!=path or s.st_uid!=10001 or stat.S_IMODE(s.st_mode)!=(0o700 if directory else 0o600) or not (stat.S_ISDIR(s.st_mode) if directory else stat.S_ISREG(s.st_mode) and s.st_nlink==1):raise RuntimeError('private_retained_pilot_path_changed')
 return s
def directory(path):
 global directories
 info=checked(path,True);directories+=1
 rel=path.relative_to(root.parent).as_posix();name=hmac.new(key,b'path\\0'+rel.encode(),hashlib.sha256).hexdigest()
 manifest[name]=dict(kind='directory',mode=0o700)
 return info
before=directory(root)
with os.scandir(root) as scan:
 children=[]
 for entry in scan:
  if len(children)>=3:raise RuntimeError('unknown_pilot_operation')
  children.append(entry.name)
if set(children)!=set(operations):raise RuntimeError('retained_pilot_operation_set_changed')
for operation in operations:
 parent=root/operation;old=directory(parent);count=0
 with os.scandir(parent) as scan:
  for entry in scan:
   count+=1
   if count>8:raise RuntimeError('retained_pilot_file_count_changed')
   path=parent/entry.name;first=checked(path,False)
   if first.st_size>65536:raise RuntimeError('retained_pilot_file_budget_exceeded')
   fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
   try:
    if same(os.fstat(fd))!=same(first):raise RuntimeError('retained_pilot_identity_changed')
    data=os.read(fd,65537)
    if len(data)!=first.st_size or same(os.fstat(fd))!=same(first) or same(path.lstat())!=same(first):raise RuntimeError('retained_pilot_changed')
   finally:os.close(fd)
   rel=path.relative_to(root.parent).as_posix();name=hmac.new(key,b'path\\0'+rel.encode(),hashlib.sha256).hexdigest()
   manifest[name]=dict(kind='file',bytes=len(data),content=hmac.new(key,b'file\\0'+data,hashlib.sha256).hexdigest(),mode=0o600)
   files+=1;total+=len(data)
 if count!=8 or same(parent.lstat())!=same(old):raise RuntimeError('retained_pilot_directory_changed')
if same(root.lstat())!=same(before):raise RuntimeError('retained_pilot_root_changed')
print(json.dumps(dict(files=files,directories=directories,bytes=total,manifestSha256=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()),sort_keys=True,separators=(',',':')))
''').encode()


class HistoricalPilotClosure:
    def observe_current(self,driver,manifest):
        state=__import__('ops.enrolled_coordinator_cutover',fromlist=['FirstEnrolledCutover']).FirstEnrolledCutover(
            driver.root,driver.plan,driver)._load()
        reference=driver._prior(state,'capture-full-custody')['reference']
        baseline=driver._custody().artifacts.load(reference,'baseline')['body']['baseline']
        opaque=baseline['opaquePilotJournals']
        entry=driver.bundle['qualifications']['pilotClosureSha256']
        require(entry is not None and driver.plan['gates']['pilotClosureSha256']==entry['sha256'],
                'reviewed_historical_pilot_receipt_missing')
        raw=private_bytes(entry['path'],maximum=65536)
        require(hashlib.sha256(raw).hexdigest()==entry['sha256'],'historical_pilot_receipt_hash_changed')
        value=validate_historical_receipt(raw,authority=driver.plan['authorityId'],
            predecessor=driver.plan['predecessor']['containerId'],opaque=opaque)
        old=driver._original()
        require(old['State']['Running'] is False and old['State']['Pid']==0 and old['State']['FinishedAt']
                and old['HostConfig'].get('PidMode','')=='' and old['HostConfig'].get('Privileged') is False,
                'original_private_pid_namespace_not_terminal')
        current=driver._find('bootstrap');require(current is not None and current['State']['Running'] is True,
                                                'known_bootstrap_for_pilot_observation_required')
        key=private_bytes(driver.bundle['artifacts']['censusKey']['path'],maximum=32)
        actual=driver.boundary.fresh(current['Id'],opaque_pilot_source(key))
        require(actual==opaque,'current_retained_pilot_manifest_changed')
        return dict(schemaVersion=1,protocol='ltds-pilot-namespace-retirement-v1',
            producerSourceSha256=digest([row[2:4] for row in RECORDS]),originalInvocationSha256=digest(value['invocations']),
            namespaceTerminationSha256=digest(dict(containerId=old['Id'],imageId=old['Image'],
                finishedAt=old['State']['FinishedAt'],privatePidNamespace='terminated',retainedManifest=actual)),
            noPersistentResumerSha256=digest(dict(sources=[row[2] for row in RECORDS],profiles=value['lifetimeProfiles'])))
