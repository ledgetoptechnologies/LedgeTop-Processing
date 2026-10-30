"""Explicit reference-to-reference sole-coordinator update; never run on import.

This is a new checkpoint ABI. Historical native-to-reference install artifacts
are neither loaded nor modified. Uncertain mutation responses permit inspection
only, never automatic mutation replay or database restoration.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import http.client
import inspect
import json
import os
from pathlib import Path
import re
import select
import socket
import stat
import time
import uuid
from dataclasses import asdict
from urllib.parse import urlencode
from ops import incremental_coordinator_binding as incremental

class _HistoricalTransport:
    """Defer the Linux-only historical helper until an operation uses it."""
    def __getattr__(self,name):
        if os.name!='posix':raise Stopped('linux_reference_update_required')
        from ops import install_reference_coordinator
        return getattr(install_reference_coordinator,name)


legacy=_HistoricalTransport()
NAME='ledgetop-processing-prototype-c3fcb3a'
VOLUME=NAME+'-state'
AUTHORITY='7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
OLD_ID = '7ebe34636518961ddb4398b545a6a73d0254a47a093b198cf5d2d8411345cd07'
OLD_IMAGE = 'sha256:48651a4f4b0ab88f9c4969ed2536344842ff974d262e1aa9c5bdff5e27b38de9'
OLD_SOURCE = 'e2ccf489ab41160acff3f90e2b4bcd568f72cf24'
LABEL = 'org.ledgetop.reference.update'
MODE = 'reference-to-reference-v1'
# Independent source review and frozen-source Linux/Windows plus actual dual-host
# gate checks accepted source 890cd265c9ff9bad0e83aeddc1b096883200d335.
# Explicit preparation, immutable argument binding and every live fence remain.
# Follow-up census compatibility includes the existing TerminalService table;
# its reference_* zero-row admission fence remains unchanged.
ACTIVATION_REVIEWED = True
# Historical pair qualified by private-config/state-copy evidence0354562.
# This is not a generic approval of future coordinator images or worker ABIs.
INCREMENTAL_ACTIVATION_REVIEWED = True
# This draft only makes the bounded retained-input backup path testable. It is
# not a writer fence or qualification of any upgrade pair.
RETAINED_VIEWER_BACKUP_QUALIFIED = False
# Explicit future-only profile; no current release may activate this path.
QUARANTINED_A75_REVIEWED = False
QUARANTINED_A75_SOURCE = 'a75f699706e2798a0d88f4295b2ba2f1f648ca14'
QUALIFIED_INCREMENTAL_PAIR = (
    '671fd2a6bde40088bdfad91ccf67eab45ee3d8d7',
    'sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249',
    '6bbd468e1177746cb695e43195e75355b0681f23',
    'sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b',
)
# Surgical NodeODM3 pair: actual on-host old/candidate/rollback application
# qualification preserved private configuration and copied state byte-for-byte.
# Evidence: processing-nodeodm3-incremental-qualification-20261028/
# binary-corrected-packet/root-independent-readback.json. No worker ABI release.
QUALIFIED_INCREMENTAL_PAIRS = frozenset((QUALIFIED_INCREMENTAL_PAIR, (
    '6bbd468e1177746cb695e43195e75355b0681f23',
    'sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b',
    'd738edb209972a1723fa5af374578951b2a8448c',
    'sha256:a04271d08115481d5dd3a8a171b9b609567b823c7d5ea8af34b56b8915db5837',
)))
INCREMENTAL_MODE='reference-incremental-v1'
class Stopped(ValueError):pass


def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def digest(value):return hashlib.sha256(canonical(value)).hexdigest()


def incremental_release_reviewed(profile,candidate_image,candidate_source):
    """Exact qualified application/image pairs; preparation is separate."""
    return (INCREMENTAL_ACTIVATION_REVIEWED is True
        and type(profile) is incremental.IncrementalBinding
        and profile.candidate_source==candidate_source
        and (profile.predecessor_source,profile.predecessor_image,
             candidate_source,candidate_image) in QUALIFIED_INCREMENTAL_PAIRS)


def incremental_census_source():
    # The candidate application deliberately does not contain host ops helpers.
    # Serialize the pinned inert checker, importing only its real Maintenance.
    return inspect.getsource(incremental)


def cluster_http_json(route,token,deadline,limit=65536):
    """Fixed private NodeODM reads with one absolute header/body deadline."""
    if (route not in ('/info','/task/list') or type(token) is not str or not 1<=len(token)<=4096
            or any(not 0x21<=ord(char)<=0x7e for char in token)):
        raise Stopped('fixed_private_cluster_route_required')
    connection=None
    try:
        remaining=deadline-time.monotonic()
        if remaining<=0:raise ValueError()
        connection=socket.create_connection(('127.0.0.1',13001),timeout=remaining)
        connection.setblocking(False)
        target=route+'?'+urlencode({'token':token})
        request=('GET '+target+' HTTP/1.1\r\nHost: 127.0.0.1:13001\r\nAccept: application/json\r\nConnection: close\r\n\r\n').encode('ascii')
        def ready(read):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise ValueError()
            r,w,_=select.select([connection] if read else [],[] if read else [connection],[],remaining)
            if not r and not w or time.monotonic()>=deadline:raise ValueError()
        while request:
            ready(False)
            try:sent=connection.send(request)
            except BlockingIOError:continue
            if sent<=0:raise ValueError()
            request=request[sent:]
        data=bytearray()
        while b'\r\n\r\n' not in data:
            ready(True)
            try:chunk=connection.recv(1024)
            except BlockingIOError:continue
            if not chunk:raise ValueError()
            data.extend(chunk)
            if len(data)>8192+limit or b'\r\n\r\n' not in data and len(data)>8192:raise ValueError()
        header,body=bytes(data).split(b'\r\n\r\n',1)
        if len(header)>8192:raise ValueError()
        lines=header.decode('ascii').split('\r\n')
        if not re.fullmatch(r'HTTP/1\.[01] 200(?: [^\r\n]*)?',lines[0]):raise ValueError()
        headers={}
        for line in lines[1:]:
            key,value=line.split(':',1);key=key.lower()
            if key in headers or not re.fullmatch('[a-z0-9-]+',key):raise ValueError()
            headers[key]=value.strip()
        if (headers.get('content-type','').split(';',1)[0].lower()!='application/json'
                or 'transfer-encoding' in headers or headers.get('content-encoding','identity')!='identity'):raise ValueError()
        length=None
        if 'content-length' in headers:
            if not re.fullmatch('[0-9]{1,8}',headers['content-length']):raise ValueError()
            length=int(headers['content-length'])
            if length>limit:raise ValueError()
        while length is None or len(body)<length:
            if len(body)>limit:raise ValueError()
            ready(True)
            try:chunk=connection.recv(min(4096,limit-len(body)+1))
            except BlockingIOError:continue
            if not chunk:break
            body+=chunk
        if len(body)>limit or length is not None and len(body)!=length:raise ValueError()
        def unique(pairs):
            value={}
            for key,item in pairs:
                if key in value:raise ValueError()
                value[key]=item
            return value
        result=json.loads(body,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
        if time.monotonic()>=deadline:raise ValueError()
        return result
    except Exception:raise Stopped('cluster_readonly_api_unavailable') from None
    finally:
        if connection is not None:connection.close()


def volume_census(root, *, uid=10001, offline=False):
    """Pure read-only census; also serialized into the network-none helper.

    Job/receipt contents never leave this function. Only the two system node
    rows are retained privately to prove exact operation-owned CAS transitions.
    """
    import hashlib, json, os, sqlite3, stat
    from pathlib import Path
    root=Path(root)
    root_info=root.lstat()
    assert stat.S_ISDIR(root_info.st_mode) and root_info.st_uid==uid and root_info.st_gid==uid
    def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    def sha(value):return hashlib.sha256(encode(value)).hexdigest()
    allowed={'jobs','sqlite_sequence','coordinator_identity','nodes','viewer_tasks','viewer_files','viewer_incarnations',
        'reference_jobs','reference_acceptances','reference_outbox','reference_claims','reference_worker_receipts',
        'reference_transfer_grants','reference_worker_failures','reference_terminal_acceptances','owned_maintenance_schedules'}
    files=[]
    for path in sorted(root.rglob('*')):
        relative=path.relative_to(root).as_posix();info=path.lstat()
        assert info.st_uid==uid and info.st_gid==uid and not stat.S_ISLNK(info.st_mode)
        # The reviewed baseline contains system metadata and empty native dirs.
        assert relative in ('jobs.sqlite3','coordinator.lock','viewer-tasks','viewer-spool')
        if stat.S_ISDIR(info.st_mode):files.append(dict(path=relative,kind='directory',mode=stat.S_IMODE(info.st_mode)))
        else:
            assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=67108864
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
            try:
                before=os.fstat(fd);assert (before.st_dev,before.st_ino)==(info.st_dev,info.st_ino)
                h=hashlib.sha256();size=0
                while True:
                    data=os.read(fd,1048576)
                    if not data:break
                    size+=len(data);assert size<=info.st_size;h.update(data)
                after=path.lstat()
                assert size==info.st_size and (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)
            finally:os.close(fd)
            files.append(dict(path=relative,kind='file',mode=stat.S_IMODE(info.st_mode),bytes=size,sha256=h.hexdigest()))
    assert any(item['path']=='jobs.sqlite3' for item in files)
    uri='file:'+str(root/'jobs.sqlite3')+'?mode=ro'+('&immutable=1' if offline else '')
    db=sqlite3.connect(uri,uri=True,timeout=3)
    try:
        db.execute('BEGIN')
        assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        objects=[list(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]
        names={row[1] for row in objects if row[0]=='table'}
        assert not names-allowed and {'jobs','coordinator_identity','nodes','reference_jobs','viewer_tasks'}<=names
        assert not any(row[0] in ('trigger','view') for row in objects)
        assert all(row[0]!='index' or row[1].startswith('sqlite_autoindex_') or row[1]=='viewer_engine_uuid' for row in objects)
        tables={}
        for name in sorted(names):
            assert name in allowed
            columns=[list(row) for row in db.execute('PRAGMA table_info("'+name+'")')]
            rows=[]
            for row in db.execute('SELECT * FROM "'+name+'"'):
                rows.append([{'blob':value.hex()} if isinstance(value,bytes) else value for value in row])
            rows.sort(key=encode)
            tables[name]=dict(count=len(rows),sha256=sha(rows),columns=columns)
        authority=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]
        active=db.execute("SELECT COUNT(*) FROM jobs WHERE status NOT IN ('succeeded','failed')").fetchone()[0]
        assert active==0
        for name in names:
            if name.startswith(('reference_','viewer_')) or name=='owned_maintenance_schedules':assert tables[name]['count']==0
        node_columns=[row[1] for row in db.execute('PRAGMA table_info(nodes)')]
        nodes={row[0]:dict(zip(node_columns,row)) for row in db.execute('SELECT * FROM nodes ORDER BY id')}
        return dict(authorityId=authority,schema=objects,tables=tables,files=files,nodeRows=nodes,
            rootMode=stat.S_IMODE(root_info.st_mode),sqliteQuickCheck='ok')
    finally:db.close()


def owned_schema():
    import sqlite3
    db=sqlite3.connect(':memory:')
    try:
        db.execute('''CREATE TABLE owned_maintenance_schedules (
            request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)''')
        return [list(row) for row in db.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name')]
    finally:db.close()


def normalized_schema(rows):
    return [[*row[:3],re.sub(r'\s+',' ',row[3]).strip() if row[3] is not None else None] for row in rows]


def compatible(observed, baseline, *, ignore_nodes=False, allow_owned=False):
    """Only one reviewed empty additive table can cross the old-binary fence."""
    a,b=json.loads(canonical(observed)),json.loads(canonical(baseline))
    for value in (a,b):
        if value['authorityId']!=AUTHORITY or value['sqliteQuickCheck']!='ok':raise Stopped('census_authority_or_integrity_changed')
    if allow_owned and 'owned_maintenance_schedules' not in b['tables'] and 'owned_maintenance_schedules' in a['tables']:
        additions=[row for row in a['schema'] if row[2]=='owned_maintenance_schedules']
        if (normalized_schema(additions)!=normalized_schema(owned_schema())
                or a['tables']['owned_maintenance_schedules']['count']!=0):raise Stopped('unreviewed_owned_schema_or_receipts')
        a['schema']=[row for row in a['schema'] if row[2]!='owned_maintenance_schedules']
        a['tables'].pop('owned_maintenance_schedules')
    for value in (a,b):
        # SQLite page layout changes during our own maintenance; all logical
        # bytes remain bound separately, while the raw file hash is retained.
        value['files']=[{key:item for key,item in entry.items() if entry['path']!='jobs.sqlite3' or key not in ('sha256','bytes')}
            for entry in value['files']]
        if ignore_nodes:value['tables'].pop('nodes',None);value.pop('nodeRows',None)
    if a!=b:raise Stopped('full_census_changed')


TLS_CENSUS = """import hashlib,json,os,stat
from pathlib import Path
root=Path('/run/processing-reference');result=[]
rs=root.lstat();assert stat.S_ISDIR(rs.st_mode) and rs.st_uid==10001 and rs.st_gid==10001 and stat.S_IMODE(rs.st_mode)==0o700
for p in sorted(root.rglob('*')):
 s=p.lstat();assert s.st_uid==10001 and s.st_gid==10001 and not stat.S_ISLNK(s.st_mode)
 assert stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_size<=1048576
 fd=os.open(p,os.O_RDONLY|os.O_NOFOLLOW)
 try:
  a=os.fstat(fd);assert (a.st_dev,a.st_ino)==(s.st_dev,s.st_ino)
  h=hashlib.sha256();n=0
  while True:
   b=os.read(fd,65536)
   if not b:break
   n+=len(b);assert n<=s.st_size;h.update(b)
  z=p.lstat();assert (z.st_dev,z.st_ino,z.st_size,z.st_mtime_ns)==(s.st_dev,s.st_ino,s.st_size,s.st_mtime_ns)
  result.append(dict(path=p.relative_to(root).as_posix(),uid=s.st_uid,gid=s.st_gid,mode=stat.S_IMODE(s.st_mode),dev=s.st_dev,ino=s.st_ino,bytes=n,sha256=h.hexdigest()))
 finally:os.close(fd)
assert result
print(json.dumps(dict(root=dict(dev=rs.st_dev,ino=rs.st_ino,uid=rs.st_uid,gid=rs.st_gid,mode=stat.S_IMODE(rs.st_mode)),files=result)))
"""


def application_environment(transport, item):
    env=transport.environment()
    defaults=dict(value.split('=',1) for value in transport.image(item['Image'])['Config'].get('Env',[]))
    observed=dict(value.split('=',1) for value in item['Config']['Env'])
    application=dict(env,PROCESSING_REFERENCE_CONFIG=legacy.CONFIG_PATH)
    if observed!=dict(defaults,**application):raise Stopped('reference_application_environment_changed')
    return digest(application)


def owned_api_probe(operator):
    """Only the new authenticated missing-receipt response proves API support."""
    if operator.origin!='http://127.0.0.1:18080' or operator.node:raise Stopped('fixed_operator_probe_required')
    connection=http.client.HTTPConnection(operator.parsed.hostname,operator.parsed.port,timeout=10)
    try:
        connection.request('GET','/v1/prototype/nodes/cluster/schedule-owned/00000000-0000-4000-8000-000000000001',
            headers={'Authorization':'Bearer '+operator.token,'Accept':'application/json'})
        response=connection.getresponse();raw=response.read(4097)
        if (response.status!=404 or len(raw)>4096 or json.loads(raw)!=dict(error='owned_schedule_not_found')):
            raise Stopped('owned_schedule_api_not_installed')
    finally:connection.close()
    return dict(operatorLookupVerified=True)


def backup_script(*,incremental_profile=False,pilot_reservations=False,retained_viewer=False):
    if type(retained_viewer) is not bool:
        raise Stopped('explicit_retained_backup_profile_required')
    if retained_viewer:
        if RETAINED_VIEWER_BACKUP_QUALIFIED is not True or not incremental_profile or pilot_reservations:
            raise Stopped('retained_backup_not_qualified')
        # This profile is deliberately absent from all production call sites.
        # Its caller must provide an independent offline writer fence. The
        # historical backup still refuses queued, running and held jobs.
        census=incremental_census_source()
        preamble=census+"\nretained_before=retained_viewer_inventory('/source')\n"
        old="  for table in ('viewer_tasks','viewer_files','viewer_incarnations'):\n   if table in tables:assert db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]==0\n"
        if old not in legacy.BACKUP:
            raise Stopped('retained_backup_template_changed')
        body=legacy.BACKUP.replace(old,'')
        tail="assert retained_viewer_inventory(source)==retained_before\nassert retained_viewer_inventory(target)==retained_before\nprint(json.dumps(counts))"
        return preamble+body.replace('print(json.dumps(counts))',tail)
    if type(pilot_reservations) is not bool or (pilot_reservations and not incremental_profile):
        raise Stopped('explicit_pilot_incremental_backup_required')
    if incremental_profile:
        pilot=',pilot_reservations='+repr(pilot_reservations)
        preamble=incremental_census_source()+"\nvolume_census('/source',authority="+repr(AUTHORITY)+pilot+")\n"
        body=legacy.BACKUP.replace('for entry in sorted(src.iterdir(),key=lambda p:p.name):','for entry in sorted(_inventory(src),key=lambda p:p.name):')
        extra='''
before=volume_census(source,authority='''+repr(AUTHORITY)+pilot+''');copied=volume_census(target,authority='''+repr(AUTHORITY)+pilot+''')
assert copied.pop('rootMode')==0o700
comparison=dict(before);comparison.pop('rootMode');assert comparison==copied
counts['fullCensusSha256']=digest(before)
print(json.dumps(counts))
'''
        return preamble+body.replace('print(json.dumps(counts))',extra)
    extra=inspect.getsource(volume_census)+'''
before=volume_census(source,offline=True); copied=volume_census(target,offline=True)
assert copied.pop('rootMode')==0o700
comparison=dict(before);comparison.pop('rootMode');assert comparison==copied
counts['fullCensusSha256']=hashlib.sha256(json.dumps(before,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
print(json.dumps(counts))
'''
    return legacy.BACKUP.replace('print(json.dumps(counts))',extra)


def copy_backup(transport,root,state):
    destination=root/'original-state-backup'
    if destination.exists() or destination.is_symlink():raise Stopped('new_backup_destination_required')
    installer='''import os,stat,sys
from pathlib import Path
p=Path(sys.argv[1]);assert p.is_absolute() and p.name=='original-state-backup'
assert p.parent.resolve()==p.parent and not any(q.is_symlink() for q in p.parents)
s=p.parent.stat();assert s.st_uid==1000 and not stat.S_IMODE(s.st_mode)&0o077
os.mkdir(p,0o700);os.chown(p,10001,10001)
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
'''
    transport.command(['sudo','-n','python3','-c',installer,str(destination)])
    info=destination.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid!=10001 or info.st_gid!=10001 or stat.S_IMODE(info.st_mode)!=0o700):
        raise Stopped('private_numeric_uid_backup_required')
    return json.loads(transport.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
        '--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount','type=volume,src='+VOLUME+',dst=/source,readonly',
        '--mount','type=bind,src='+str(destination)+',dst=/backup','--entrypoint','python',state['image'],'-c',backup_script(incremental_profile='incrementalBinding' in state),timeout=120))


def cluster_backend_idle(transport,state):
    """Fixed existing private backend: no token/command arguments in proof."""
    from processing.container_update import ComposeNode
    item=transport.inspected('node-odx-gpu')
    labels=item['Config'].get('Labels') or {}
    bindings=item.get('NetworkSettings',{}).get('Ports',{}).get('3000/tcp')
    if (item['Id']!=state['clusterContainerId'] or item['Name']!='/node-odx-gpu'
            or item['Image']!=state['engineImages']['cluster'] or item['State']['Running'] is not True
            or labels.get('com.docker.compose.project')!='ledgetop-cluster-node-private'
            or labels.get('com.docker.compose.service')!='nodeodm'
            or bindings!=[{'HostIp':'127.0.0.1','HostPort':'13001'}]):raise Stopped('fixed_cluster_backend_changed')
    token=transport.environment().get('LOCAL_NODE_TOKEN')
    if type(token) is not str or not token:raise Stopped('private_cluster_credential_unavailable')
    deadline=time.monotonic()+10
    info=cluster_http_json('/info',token,deadline);tasks=cluster_http_json('/task/list',token,deadline)
    if (type(info) is not dict or info.get('engine')!='odm' or type(info.get('taskQueueCount')) is not int
            or info['taskQueueCount']!=0 or type(tasks) is not list or tasks!=[]):raise Stopped('cluster_backend_tasks_not_idle')
    # These two existing inspectors perform only the fixed upload-directory
    # and engine-child census. They cannot run Compose replacement actions.
    inspector=object.__new__(ComposeNode)
    def docker(command,**kwargs):
        if not command or command[0]!='docker':raise Stopped('fixed_local_inspection_required')
        return transport.docker(*command[1:],**kwargs)
    inspector.docker=docker
    if not inspector.uploads_idle(item['Id']) or not inspector.processing_idle(item['Id']):
        raise Stopped('cluster_upload_or_child_process_not_idle')

class ReferenceCoordinatorUpdate:
    def __init__(self,root,*,gate_factory,transport=legacy,cluster_probe=None,incremental_binding=None,helper_source_commit=None):
        self.root=Path(root).absolute();self.t=transport;self.gate_factory=gate_factory
        if incremental_binding is not None and type(incremental_binding) is not incremental.IncrementalBinding:raise Stopped('concrete_incremental_binding_required')
        self.incremental_binding=incremental_binding
        self.old_id=incremental_binding.predecessor_container if incremental_binding else OLD_ID
        self.old_image=incremental_binding.predecessor_image if incremental_binding else OLD_IMAGE
        self.old_source=incremental_binding.predecessor_source if incremental_binding else OLD_SOURCE
        self.helper_source_commit=helper_source_commit
        self._profile_identity=(self.old_id,self.old_image,self.old_source,helper_source_commit)
        if incremental_binding:
            from ops.coordinator_gate_bridge import incremental_helper_digest
            if type(helper_source_commit) is not str or not re.fullmatch('[a-f0-9]{40}',helper_source_commit) or getattr(gate_factory,'incremental_binding',None)!=incremental_binding:raise Stopped('incremental_helper_and_gate_binding_required')
            self._incremental_sha=digest(asdict(incremental_binding));self._helper_sha=incremental_helper_digest()
        elif helper_source_commit is not None:raise Stopped('helper_source_requires_incremental_profile')
        self.cluster_probe=cluster_probe or self.cluster_idle
        if self.root.resolve()!=self.root or any(path.is_symlink() for path in (self.root,*self.root.parents)):
            raise Stopped('private_update_root_required')
        info=self.root.stat()
        if info.st_uid!=os.geteuid() or not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:raise Stopped('private_update_root_required')
        if not callable(gate_factory) or not isinstance(getattr(gate_factory,'binding',None),dict):raise Stopped('live_gate_factory_required')

    def save(self,state,phase=None,**fields):
        if phase:state['phase']=phase
        state.update(fields);self.t.save(self.root,state)

    def load(self):
        state=json.loads(self.t.private_file(self.root/'checkpoint.json').read_text())
        self.validate(state);return state

    def validate(self,state):
        if self.incremental_binding:
            from ops.coordinator_gate_bridge import incremental_helper_digest
            if (digest(asdict(self.incremental_binding))!=self._incremental_sha or incremental_helper_digest()!=self._helper_sha
                    or getattr(self.gate_factory,'incremental_binding',None) is not self.incremental_binding
                    or (self.old_id,self.old_image,self.old_source,self.helper_source_commit)!=self._profile_identity
                    or state.get('incrementalBinding')!=asdict(self.incremental_binding) or state.get('helperSourceCommit')!=self.helper_source_commit
                    or state.get('helperSha256')!=self._helper_sha or state.get('sourceCommit')!=self.incremental_binding.candidate_source
                    or state.get('engineImages')!={'main':self.incremental_binding.main_image,'cluster':self.incremental_binding.cluster_image}
                    or state.get('clusterContainerId')!=self.incremental_binding.cluster_container):raise Stopped('incremental_checkpoint_or_helper_changed')
        elif any(key in state for key in ('incrementalBinding','helperSourceCommit','helperSha256')):raise Stopped('explicit_incremental_profile_required')
        if (state.get('schemaVersion')!=(3 if self.incremental_binding else 2) or state.get('mode')!=(INCREMENTAL_MODE if self.incremental_binding else MODE) or state.get('authorityId')!=AUTHORITY
                or state.get('old',{}).get('containerId')!=self.old_id or state['old'].get('image')!=self.old_image
                or state.get('gateBinding')!=self.gate_factory.binding):raise Stopped('reference_update_checkpoint_binding_changed')
        if (type(state.get('image')) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',state['image'])
                or type(state.get('sourceCommit')) is not str or not re.fullmatch(r'[a-f0-9]{40}',state['sourceCommit'])
                or state.get('oldSourceCommit')!=self.old_source or type(state.get('operation')) is not str
                or not re.fullmatch(r'[a-f0-9]{32}',state['operation'])
                or state.get('rollbackName')!=NAME+'-pre-reference-update-'+state['operation']
                or state.get('failedName')!=NAME+'-failed-reference-update-'+state['operation']
                or type(state.get('engineImages')) is not dict or set(state['engineImages'])!={'main','cluster'}
                or any(type(value) is not str or not re.fullmatch(r'sha256:[a-f0-9]{64}',value) for value in state['engineImages'].values())
                or type(state.get('clusterContainerId')) is not str or not re.fullmatch(r'[a-f0-9]{64}',state['clusterContainerId'])
                or ('newContainerId' in state and (type(state['newContainerId']) is not str or not re.fullmatch(r'[a-f0-9]{64}',state['newContainerId'])))):
            raise Stopped('immutable_checkpoint_identity_changed')

    def census(self,state,*,offline=False,container=None,pilot_reservations=False):
        if type(pilot_reservations) is not bool or (pilot_reservations and not self.incremental_binding):
            raise Stopped('explicit_pilot_incremental_census_required')
        path='/source' if offline else '/prototype-state'
        code=(incremental_census_source()+'\nprint(json.dumps(volume_census('+repr(path)+',authority='+repr(AUTHORITY)+',pilot_reservations='+repr(pilot_reservations)+')))') if self.incremental_binding else inspect.getsource(volume_census)+'\nimport json\nprint(json.dumps(volume_census('+repr(path)+',offline='+repr(offline)+')))'
        if offline:
            if self.t.docker('ps','--filter','volume='+VOLUME,'--format','{{.ID}}').strip():raise Stopped('offline_volume_has_owner')
            raw=self.t.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
                '--cap-drop','ALL','--security-opt','no-new-privileges','--mount','type=volume,src='+VOLUME+',dst=/source,readonly',
                '--entrypoint','python',state['image'],'-c',code)
        else:raw=self.t.docker('exec','--user','10001:10001',container or state['old']['containerId'],'python','-c',code)
        result=json.loads(raw)
        if result['authorityId']!=AUTHORITY or result['tables']['jobs']['count']!=12:raise Stopped('reviewed_idle_history_required')
        return result

    def compare(self,state,observed,baseline,**legacy_flags):
        if not self.incremental_binding:return compatible(observed,baseline,**legacy_flags)
        plans=tuple(value['ownedRow'] for value in state['plans'].values())
        cancels=tuple(dict(nodeId=node,**value['cancelIntent']) for node,value in state['plans'].items() if value.get('cancelIntent'))
        reopened=tuple(node for node,value in state['plans'].items() if value.get('cancelIntent') and observed['nodeRows'][node]['state']=='active')
        try:incremental.compare_transition(observed,state['baseline'],binding=self.incremental_binding,plans=plans,cancel_intents=cancels,reopened=reopened)
        except Exception:raise Stopped('incremental_full_census_changed') from None

    def tls(self,state):
        raw=self.t.docker('run','--rm','--pull=never','--network','none','--read-only','--user','10001:10001',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--mount',
            'type=bind,src='+str(legacy.TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '--entrypoint','python',state['image'],'-c',TLS_CENSUS)
        value=json.loads(raw)
        if self.incremental_binding:
            files=value.get('files') if type(value) is dict else None
            expected=Path(legacy.CONFIG_PATH).relative_to('/run/processing-reference').as_posix()
            if type(files) is not list or [row.get('sha256') for row in files if row.get('path')==expected]!=[self.incremental_binding.private_config_sha256]:raise Stopped('incremental_private_configuration_changed')
        return value

    def cluster_idle(self,state):return cluster_backend_idle(self.t,state)

    def original(self,state=None):
        item=self.t.inspected(NAME)
        if (item['Id']!=self.old_id or item['Image']!=self.old_image or not item['State']['Running']
                or item['State'].get('Health',{}).get('Status')!='healthy'):raise Stopped('exact_current_reference_primary_required')
        facts=self.t.facts(item,reference=True);self.t.sole_owner(self.old_id)
        if self.t.image(self.old_image)['Config'].get('Labels',{}).get('org.opencontainers.image.revision')!=self.old_source:
            raise Stopped('predecessor_source_changed')
        if state and (facts!=state['old'] or application_environment(self.t,item)!=state['applicationEnvironmentSha256']):
            raise Stopped('prepared_reference_primary_changed')
        return item,facts

    def activity(self,state,api):
        if self.t.activity(api)!=state['history']:raise Stopped('public_history_or_native_activity_changed')

    def prepare(self,target,source_commit,main_image):
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',target) or not re.fullmatch(r'[0-9a-f]{40}',source_commit):raise Stopped('immutable_candidate_required')
        if target==self.old_image:raise Stopped('new_reference_image_required')
        if (self.root/'checkpoint.json').exists():raise Stopped('new_private_operation_required')
        old,original=self.original();application=application_environment(self.t,old)
        if self.incremental_binding and (source_commit!=self.incremental_binding.candidate_source or main_image!=self.incremental_binding.main_image or application!=self.incremental_binding.application_environment_sha256):raise Stopped('incremental_candidate_or_environment_changed')
        candidate=self.t.image(target)
        if (candidate['Id']!=target or candidate['Config'].get('Labels',{}).get('org.opencontainers.image.revision')!=source_commit
                or any(candidate['Config'].get(key)!=old['Config'].get(key) for key in ('User','WorkingDir','Cmd','Entrypoint','Healthcheck'))):
            raise Stopped('candidate_recipe_or_source_not_verified')
        _env,api,_operator=self.t.local_clients();history=self.t.activity(api)
        if history['count']!=12:raise Stopped('reviewed_twelve_terminal_jobs_required')
        nodes={row['id']:row for row in api.json('GET','/v1/prototype/nodes')['nodes']}
        if set(nodes)!={'main','cluster'} or any(row['state']!='active' for row in nodes.values()):raise Stopped('other_maintenance_present')
        cluster=self.t.inspected('node-odx-gpu')
        if (not cluster['State']['Running'] or not re.fullmatch(r'[a-f0-9]{64}',cluster['Id'])
                or not re.fullmatch(r'sha256:[0-9a-f]{64}',main_image)):raise Stopped('verified_processing_images_required')
        operation=uuid.uuid4().hex
        state=dict(schemaVersion=2,mode=MODE,operation=operation,phase='preparing',image=target,sourceCommit=source_commit,
            oldSourceCommit=self.old_source,old=original,authorityId=AUTHORITY,history=history,plans={},
            applicationEnvironmentSha256=application,gateBinding=self.gate_factory.binding,
            engineImages=dict(main=main_image,cluster=cluster['Image']),clusterContainerId=cluster['Id'],
            rollbackName=NAME+'-pre-reference-update-'+operation,failedName=NAME+'-failed-reference-update-'+operation)
        if self.incremental_binding:
            state.update(schemaVersion=3,mode=INCREMENTAL_MODE,incrementalBinding=asdict(self.incremental_binding),helperSourceCommit=self.helper_source_commit,helperSha256=self._helper_sha)
        self.validate(state)
        self.cluster_probe(state)
        for name in (state['rollbackName'],state['failedName']):
            if self.t.docker('ps','--all','--filter','name=^/'+name+'$','--format','{{.ID}}').strip():raise Stopped('retention_name_exists')
        self.save(state);self.t.candidate_loader(target,operation)
        self.save(state,'prepared',baseline=self.census(state),tls=self.tls(state))
        if self.incremental_binding:self.incremental_binding.check_baseline(state['baseline'])
        return state

    def check(self,state,gates):
        self.validate(state);gates.check()
        if self.tls(state)!=state['tls']:raise Stopped('tls_or_private_configuration_changed')
        self.cluster_probe(state)
        if self.incremental_binding and 'baseline' in state:
            owners=self.t.docker('ps','--no-trunc','--filter','volume='+VOLUME,'--format','{{.ID}}').split()
            if len(owners)>1 or any(value not in (self.old_id,state.get('newContainerId')) for value in owners):raise Stopped('incremental_volume_owner_changed')
            if not state.get('pendingPlan'):
                observed=self.census(state,container=owners[0]) if owners else self.census(state,offline=True)
                self.compare(state,observed,state['baseline'])
        # TLS/Docker reads can take time. Never act on the earlier gate proof.
        gates.check()

    def mutation(self,state,gates,*args,**kwargs):
        self.check(state,gates);return self.t.docker(*args,**kwargs)

    def candidate(self,state):
        identifier=state.get('newContainerId')
        if not identifier:
            matches=self.t.docker('ps','--all','--filter','label='+LABEL+'='+state['operation'],'--format','{{.ID}}').split()
            if len(matches)>1:raise Stopped('ambiguous_candidate_identity')
            if not matches:return None
            identifier=matches[0]
        item=self.t.inspected(identifier)
        if (not re.fullmatch(r'[a-f0-9]{64}',item['Id']) or item['Image']!=state['image']
                or item['Config'].get('Labels',{}).get(LABEL)!=state['operation']
                or item['Name'] not in ('/'+NAME,'/'+state['failedName'])):raise Stopped('foreign_candidate_refused')
        self.t.facts(item,reference=True)
        if application_environment(self.t,item)!=state['applicationEnvironmentSha256']:raise Stopped('candidate_application_environment_changed')
        self.save(state,newContainerId=item['Id']);return item

    def plans(self,state,api):
        for identifier,plan in state['plans'].items():
            current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if (current['state']!='draining' or current['planId']!=plan['planId']
                    or current!=plan['acknowledgedNode']):raise Stopped('owned_draining_plan_changed')

    def owned_schedule(self,state,gates,node,original,payload,api,operator,*,lookup_only=False):
        route='/v1/prototype/nodes/'+node+'/schedule-owned'
        if lookup_only:reply=operator.json('GET',route+'/'+payload['requestId'])
        else:
            try:reply=operator.json('POST',route,payload)
            except Exception:
                # Missing/uncertain GET holds. Never repeat POST or rotate ID.
                reply=operator.json('GET',route+'/'+payload['requestId'])
        if type(reply) is not dict or set(reply)!={'receipt'}:raise Stopped('exact_owned_schedule_receipt_required')
        row=dict(request_id=payload['requestId'],node_id=node,authority_id=AUTHORITY,
            request_sha256=digest(dict(nodeId=node,request=payload)),request_json=canonical(payload).decode(),receipt_json=canonical(reply['receipt']).decode())
        try:incremental.owned_row(row,AUTHORITY)
        except Exception:raise Stopped('owned_schedule_response_binding_changed') from None
        receipt=reply['receipt'];expected=dict(original,state='draining',revision=receipt['revision'],planId=receipt['planId'],drainAt=receipt['drainAt'],notBefore=receipt['notBefore'],afterJobId=None,targetImage=receipt['targetImage'],result=None)
        current=api.json('GET','/v1/prototype/nodes/'+node)['node']
        if current!=expected:raise Stopped('owned_schedule_current_cas_changed')
        state['plans'][node]=dict(planId=current['planId'],targetImage=current['targetImage'],payload=payload,acknowledgedNode=current,ownedRow=row)
        state.pop('pendingPlan',None);self.save(state)
        self.check(state,gates)
        return current

    def reconcile_schedule(self,state):
        """Explicit receipt-only reconciliation before any Docker transition."""
        self.validate(state)
        if not self.incremental_binding or state['phase']!='needs_inspection' or not state.get('pendingPlan') or any(key in state for key in ('drained','newContainerId','backup','reopeningStarted')):raise Stopped('pending_owned_schedule_only')
        self.original(state);_env,api,operator=self.t.local_clients();self.activity(state,api)
        intent=state['pendingPlan']
        if set(intent)!={'nodeId','payload','originalNode'}:raise Stopped('full_owned_schedule_intent_required')
        node=intent['nodeId'];payload=intent['payload'];expected=intent['originalNode']
        with self.gate_factory() as gates:
            self.check(state,gates);self.owned_schedule(state,gates,node,expected,payload,api,operator,lookup_only=True)
            self.save(state,'prepared')
        return state

    def node_census(self,state,observed,*,reopening=False):
        if self.incremental_binding:
            self.compare(state,observed,state['baseline']);return
        baseline=state['baseline']['nodeRows']
        if set(baseline)!=set(observed['nodeRows']) or set(state['plans'])!={'main','cluster'}:raise Stopped('node_inventory_changed')
        for identifier,original in baseline.items():
            plan=state['plans'][identifier];wire=plan['payload']
            if wire['expectedRevision']!=original['revision']:raise Stopped('node_revision_not_prepared')
            expected=dict(original,state='draining',revision=wire['expectedRevision']+1,plan_id=plan['planId'],
                drain_at=wire['drainAt'],not_before=wire['notBefore'],after_job_id=None,target_image=wire['targetImage'],
                result=None,update_hash=None,finish_status=None)
            # Only a FULL-saved exact cancel intent may account for a reopened
            # row. All original/private node columns remain compared.
            intent=plan.get('cancelIntent')
            if reopening and intent:
                if intent!={'planId':plan['planId'],'expectedRevision':expected['revision']}:raise Stopped('cancel_census_binding_changed')
                if observed['nodeRows'][identifier]['state']=='active':
                    expected.update(state='active',revision=expected['revision']+1)
            if observed['nodeRows'][identifier]!=expected:raise Stopped('unowned_node_row_transition')

    def create_args(self,state):
        return ('create','--name',NAME,'--pull=never','--init','--restart','unless-stopped','--network','bridge',
            '--read-only','--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--pids-limit','64','--cpus','0.5','--memory','256m','--memory-swap','512m','--ipc','private','--shm-size','64m',
            '--log-driver','json-file','--workdir','/app','--env-file',str(legacy.NATIVE_ENV),'-e','PROCESSING_REFERENCE_CONFIG='+legacy.CONFIG_PATH,
            '--mount','type=volume,src='+VOLUME+',dst=/prototype-state',
            '--mount','type=bind,src='+str(legacy.TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '-p','127.0.0.1:18080:8080','-p','192.168.50.89:3005:8080','-p','192.168.50.89:18443:8443',
            '--label',LABEL+'='+state['operation'],state['image'])

    def quarantine_create_args(self, state):
        """Create the same candidate without any ingress or restart on reboot.

        The private state/TLS mounts remain necessary for real startup; callers
        must already own and fence those resources before this function is used.
        No promotion path is implemented here.
        """
        args=list(self.create_args(state))
        try:
            index=args.index('--restart')
            if args[index+1]!='unless-stopped':raise Stopped('quarantine_create_template_changed')
            args[index+1]='no'
            index=args.index('--network')
            if args[index+1]!='bridge':raise Stopped('quarantine_create_template_changed')
            args[index+1]='none'
            bindings=[]
            while '-p' in args:
                index=args.index('-p');bindings.append(args[index+1]);del args[index:index+2]
            if bindings!=['127.0.0.1:18080:8080','192.168.50.89:3005:8080','192.168.50.89:18443:8443']:
                raise Stopped('quarantine_create_template_changed')
        except (ValueError,IndexError):
            raise Stopped('quarantine_create_template_changed') from None
        return tuple(args)

    def verify_quarantined_candidate(self,state):
        """Inspect the exact isolated candidate, then probe its native health internally."""
        import copy
        identifier=state.get('newContainerId')
        if type(identifier) is not str or not re.fullmatch('[a-f0-9]{64}',identifier):
            raise Stopped('quarantine_candidate_identity_required')
        item=self.t.inspected(identifier)
        host=item['HostConfig'];network=item.get('NetworkSettings',{})
        if (item['Id']!=identifier or item['Name']!='/'+NAME or item['Image']!=state['image']
                or item['Config'].get('Labels',{}).get(LABEL)!=state['operation']
                or item['State'].get('Running') is not True
                or host.get('NetworkMode')!='none' or host.get('PortBindings') not in (None,{})
                or host.get('RestartPolicy',{}).get('Name')!='no'
                or host.get('RestartPolicy',{}).get('MaximumRetryCount',0)!=0
                or network.get('Ports') not in (None,{})
                or set(network.get('Networks',{})) - {'none'}):
            raise Stopped('quarantine_candidate_not_isolated')
        # Apply the existing immutable security/mount policy to a copy only,
        # substituting the three intentional quarantine differences.
        proxy=copy.deepcopy(item)
        proxy['HostConfig']['NetworkMode']='bridge'
        proxy['HostConfig']['RestartPolicy']['Name']='unless-stopped'
        proxy['HostConfig']['PortBindings']=legacy.REFERENCE_PORTS
        self.t.facts(proxy,reference=True)
        if application_environment(self.t,item)!=state['applicationEnvironmentSha256']:
            raise Stopped('candidate_application_environment_changed')
        code=('import json,urllib.request; '
              'r=urllib.request.urlopen("http://127.0.0.1:8080/healthz",timeout=3); '
              'v=json.load(r); print(json.dumps({"status":v.get("status"),'
              '"viewerEnabled":v.get("viewerEnabled"),'
              '"syntheticPrototypeEnabled":v.get("syntheticPrototypeEnabled")}))')
        value=json.loads(self.t.docker('exec','--user','10001:10001',identifier,'python','-c',code,timeout=10))
        if value!={'status':'ok','viewerEnabled':True,'syntheticPrototypeEnabled':True}:
            raise Stopped('quarantine_health_unavailable')
        # Health says both app listeners started, not that external TLS or
        # backend integration has been validated.
        return {'isolated':True,'restartDisabled':True,'internalHealth':'ok'}

    def backup(self,state,gates):
        self.check(state,gates)
        # Historical copier hashes every byte and fsyncs files/directories. Its
        # old count check is supplemented by our full logical/content manifest.
        before=self.census(state,offline=True)
        self.compare(state,before,state['drained'])
        result=copy_backup(self.t,self.root,state)
        if result.get('fullCensusSha256')!=digest(before):raise Stopped('backup_full_content_proof_changed')
        self.check(state,gates)
        after=self.census(state,offline=True);self.compare(state,after,before)
        return dict(**result,sourceCensusSha256=digest(before),sourceRawFiles=before['files'])

    def reopen(self,state,gates,api,operator):
        if state.get('sourceCommit')==QUARANTINED_A75_SOURCE:raise Stopped('a75_reopen_not_qualified')
        if state.get('pendingPlan'):raise Stopped('unknown_schedule_outcome_never_adopted')
        for identifier,plan in state['plans'].items():
            self.check(state,gates)
            if not state.get('reopeningStarted'):
                self.activity(state,api)
                observed=self.census(state,container=state.get('newContainerId') or self.old_id)
                self.node_census(state,observed)
                self.compare(state,observed,state['drained'],allow_owned=True)
            current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if current['planId']!=plan['planId'] or current['targetImage']!=state['engineImages'][identifier]:raise Stopped('cannot_reopen_peer_plan')
            intent=plan.get('cancelIntent')
            if current['state']=='active' and intent and current['revision']==intent['expectedRevision']+1:
                expected=dict(plan['acknowledgedNode'],state='active',revision=intent['expectedRevision']+1)
                if current!=expected:raise Stopped('reopen_readback_changed')
                self.check(state,gates);plan['cancelled']=True;self.save(state);continue
            if current!=plan['acknowledgedNode'] or intent:raise Stopped('reopen_outcome_requires_inspection')
            payload=dict(planId=plan['planId'],expectedRevision=current['revision'])
            plan['cancelIntent']=payload
            self.save(state,'reopening',reopeningStarted=True)
            self.check(state,gates)
            try:operator.json('POST','/v1/prototype/nodes/'+identifier+'/cancel',payload)
            except Exception:
                after=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
                if (after['state']!='active' or after['planId']!=plan['planId'] or after['revision']!=payload['expectedRevision']+1
                        or after['targetImage']!=state['engineImages'][identifier]):raise Stopped('reopen_outcome_requires_inspection') from None
            after=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
            if after!=dict(plan['acknowledgedNode'],state='active',revision=payload['expectedRevision']+1):raise Stopped('reopen_readback_changed')
            self.check(state,gates)
            plan['cancelled']=True;self.save(state)

    def activate(self,state):
        self.validate(state)
        quarantined = state.get('sourceCommit')==QUARANTINED_A75_SOURCE
        if quarantined and QUARANTINED_A75_REVIEWED is not True:
            raise Stopped('a75_quarantine_activation_not_qualified')
        if quarantined and self.incremental_binding is None:
            raise Stopped('a75_incremental_profile_required')
        if self.incremental_binding and not incremental_release_reviewed(self.incremental_binding,state['image'],state['sourceCommit']):raise Stopped('incremental_application_migration_review_required')
        if state['phase']!='prepared':raise Stopped('prepared_operation_only_no_replay')
        self.original(state);_env,api,operator=self.t.local_clients();self.activity(state,api)
        self.compare(state,self.census(state),state['baseline'])
        with self.gate_factory() as gates:
            self.check(state,gates)
            try:
                for identifier in ('cluster','main'):
                    current=api.json('GET','/v1/prototype/nodes/'+identifier)['node']
                    if self.incremental_binding and identifier in state['plans']:
                        if current!=state['plans'][identifier]['acknowledgedNode']:raise Stopped('prepared_owned_node_changed')
                        continue
                    if current['state']!='active':raise Stopped('node_no_longer_active')
                    stamp=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
                    payload=dict(expectedRevision=current['revision'],targetImage=state['engineImages'][identifier],drainAt=stamp,notBefore=stamp,afterJobId=None)
                    if self.incremental_binding:
                        payload.update(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=AUTHORITY)
                    pending=dict(nodeId=identifier,payload=payload)
                    if self.incremental_binding:pending['originalNode']=current
                    self.save(state,'schedule_pending',pendingPlan=pending)
                    self.check(state,gates)
                    if self.incremental_binding:
                        plan=self.owned_schedule(state,gates,identifier,current,payload,api,operator)
                    else:
                        # Old e2 has no durable caller correlation. Any lost ACK
                        # is held, even when a target happens to match readback.
                        plan=operator.json('POST','/v1/prototype/nodes/'+identifier+'/schedule',payload)['node']
                    if (plan['id']!=identifier or plan['state']!='draining' or plan['targetImage']!=payload['targetImage']
                            or type(plan['planId']) is not str or str(uuid.UUID(plan['planId']))!=plan['planId']):raise Stopped('exact_schedule_ack_required')
                    if plan['revision']!=payload['expectedRevision']+1:raise Stopped('schedule_revision_changed')
                    if not self.incremental_binding:state['plans'][identifier]=dict(planId=plan['planId'],targetImage=plan['targetImage'],payload=payload,acknowledgedNode=plan)
                    state.pop('pendingPlan',None);self.save(state)
                self.plans(state,api);self.activity(state,api)
                drained=self.census(state);self.node_census(state,drained)
                self.compare(state,drained,state['baseline'],ignore_nodes=True)
                self.save(state,'stop_old_pending',drained=drained)
                self.mutation(state,gates,'stop','--time','-1',self.old_id,timeout=60)
                if self.t.inspected(self.old_id)['State']['Running']:raise Stopped('old_not_confirmed_stopped')
                self.compare(state,self.census(state,offline=True),drained)
                self.save(state,'backup_pending');backed=self.backup(state,gates)
                self.save(state,'rename_old_pending',backup=backed)
                self.mutation(state,gates,'rename',self.old_id,state['rollbackName'])
                self.save(state,'create_pending')
                identifier=self.mutation(state,gates,*(self.quarantine_create_args(state) if quarantined else self.create_args(state))).strip()
                if not re.fullmatch(r'[a-f0-9]{64}',identifier):raise Stopped('create_response_unknown')
                self.save(state,'created',newContainerId=identifier)
                if not quarantined:self.candidate(state)
                self.save(state,'start_pending')
                self.mutation(state,gates,'start',identifier)
                self.save(state,'started',replacementStarted=True)
                if quarantined:
                    gates.check()
                    evidence=self.verify_quarantined_candidate(state)
                    gates.check()
                    self.save(state,'quarantine_verified',quarantineEvidence=evidence)
                    # No exposed-port promotion or old-version rollback has
                    # been qualified. Preserve the exact isolated candidate.
                    raise Stopped('quarantine_promotion_not_qualified')
                self.t.health(api,container_id=identifier);self.t.sole_owner(identifier)
                self.check(state,gates);self.activity(state,api);self.plans(state,api)
                self.compare(state,self.census(state,container=identifier),drained,allow_owned=True)
                verified=self.t.verify_reference();verified.update(owned_api_probe(operator))
                self.save(state,'verified',verification=verified)
                self.reopen(state,gates,api,operator)
                self.check(state,gates)
                self.save(state,'completed',sameAuthority=True,sameStateVolume=True,databaseRestored=False)
            except Exception:
                # No automatic rollback. It is a separately invoked guarded
                # operation; this also avoids replay after unknown stop/start.
                self.save(state,'needs_inspection');raise
        return state

    def retain(self,state,gates,item):
        if state.get('sourceCommit')==QUARANTINED_A75_SOURCE:raise Stopped('a75_retention_not_qualified')
        self.check(state,gates)
        current=self.candidate(state)
        if current is None or current['Id']!=item['Id'] or current['Name']!='/'+NAME:raise Stopped('canonical_candidate_retention_only')
        if not current['State']['Running']:
            if self.t.docker('ps','--filter','volume='+VOLUME,'--format','{{.ID}}').strip():raise Stopped('retention_volume_has_other_owner')
            self.save(state,'retain_start_pending')
            self.mutation(state,gates,'start',item['Id'])
        else:self.t.sole_owner(item['Id'])
        _env,api,_operator=self.t.local_clients()
        self.t.health(api,container_id=item['Id']);self.t.sole_owner(item['Id'])
        self.save(state,'needs_inspection',downgradeRefused=True,replacementRetained=True)

    def finish_rollback(self,state,gates):
        if state.get('sourceCommit')==QUARANTINED_A75_SOURCE:raise Stopped('a75_rollback_not_qualified')
        self.check(state,gates);self.original(state)
        _env,api,operator=self.t.local_clients();self.t.health(api,container_id=self.old_id)
        self.t.sole_owner(self.old_id);self.activity(state,api)
        self.compare(state,self.census(state,container=self.old_id),state['drained'],allow_owned=True)
        self.t.verify_reference()
        state.pop('newContainerId',None)
        self.reopen(state,gates,api,operator)
        self.check(state,gates)
        self.save(state,'rolled_back',databaseRestored=False,referenceTLSRetained=True)
        return state

    def rollback(self,state):
        if state.get('sourceCommit')==QUARANTINED_A75_SOURCE:raise Stopped('a75_rollback_not_qualified')
        self.validate(state)
        if self.incremental_binding and not incremental_release_reviewed(self.incremental_binding,state['image'],state['sourceCommit']):raise Stopped('incremental_application_migration_review_required')
        if state.get('reopeningStarted') or state.get('pendingPlan'):raise Stopped('reopen_or_schedule_uncertainty_prevents_rollback')
        with self.gate_factory() as gates:
            self.check(state,gates);item=self.candidate(state)
            old=self.t.inspected(self.old_id)
            if old['State']['Running']:
                if state['phase']!='rollback_start_old_pending' or old['Name']!='/'+NAME:
                    raise Stopped('unexpected_running_predecessor')
                # Exact old-start intent + observed sole restored owner proves
                # the lost ACK without an offline helper or another start.
                return self.finish_rollback(state,gates)
            if item is not None and item['State']['Running']:
                _env,api,_operator=self.t.local_clients();self.activity(state,api)
                self.compare(state,self.census(state,container=item['Id']),state['drained'],allow_owned=True)
                self.save(state,'rollback_stop_pending')
                self.mutation(state,gates,'stop','--time','-1',item['Id'],timeout=60)
                if self.t.inspected(item['Id'])['State']['Running']:raise Stopped('candidate_not_confirmed_stopped')
            try:self.compare(state,self.census(state,offline=True),state['drained'],allow_owned=True)
            except Exception:
                if item is not None:self.retain(state,gates,item)
                raise Stopped('offline_rollback_refused') from None
            old=self.t.inspected(self.old_id)
            if old['Image']!=self.old_image or old['Name'] not in ('/'+NAME,'/'+state['rollbackName']):raise Stopped('exact_old_reference_rollback_only')
            self.t.facts(old,reference=True)
            if application_environment(self.t,old)!=state['applicationEnvironmentSha256']:raise Stopped('rollback_application_environment_changed')
            if item is not None and item['Name']=='/'+NAME:
                self.save(state,'rollback_rename_candidate_pending');self.mutation(state,gates,'rename',item['Id'],state['failedName'])
            if old['Name']!='/'+NAME:
                self.save(state,'rollback_rename_old_pending');self.mutation(state,gates,'rename',self.old_id,NAME)
            if not old['State']['Running']:
                self.save(state,'rollback_start_old_pending');self.mutation(state,gates,'start',self.old_id)
            self.finish_rollback(state,gates)
        return state


class GateFactory:
    """Cluster executor half of the reviewed RootRelay transport.

    The root relay launches authenticated, host-key-pinned main/cluster SSH
    processes. This executor's stdin/stdout are exclusively bridge frames.
    """
    def __init__(self,config,*,operation_id=None,transport=legacy,incremental_binding=None):
        from ops.coordinator_gate_bridge import BridgeBinding,LocalGate,BridgeGateClient,GateSet,helper_digest,incremental_helper_digest
        if incremental_binding is not None and type(incremental_binding) is not incremental.IncrementalBinding:raise Stopped('concrete_incremental_gate_binding_required')
        self.incremental_binding=incremental_binding
        self._incremental_binding=incremental_binding
        expected_helper=incremental_helper_digest() if incremental_binding else helper_digest()
        if (type(config) is not dict or set(config)!={'schemaVersion','cluster','main','sessionNonce','timeoutSeconds'}
                or type(config['schemaVersion']) is not int or config['schemaVersion']!=1
                or type(config['sessionNonce']) is not str or not re.fullmatch(r'[a-f0-9]{64}',config['sessionNonce'])
                or type(config['timeoutSeconds']) is not int or not 1<=config['timeoutSeconds']<=30):raise Stopped('closed_bridge_configuration_required')
        bindings={}
        fields={'engineId','nodeId','authorityId','coordinatorOrigin','generation','helperSha256'}
        for name in ('cluster','main'):
            row=config[name]
            if (type(row) is not dict or set(row)!=fields or row['nodeId']!=name or row['authorityId']!=AUTHORITY
                    or row['coordinatorOrigin']!='http://127.0.0.1:18080' or row['helperSha256']!=expected_helper):
                raise Stopped('fixed_live_gate_binding_required')
            bindings[name]=BridgeBinding(engine_id=row['engineId'],node_id=name,authority_id=row['authorityId'],
                coordinator_origin=row['coordinatorOrigin'],gate_generation=row['generation'],helper_sha256=row['helperSha256'])
        self.binding={name:item.public() for name,item in bindings.items()}
        self.bindings=bindings;self.operation_id=operation_id;self.config=config;self.t=transport
        self.classes=(LocalGate,BridgeGateClient,GateSet)

    def __call__(self):
        import sys
        if self.incremental_binding is not self._incremental_binding:raise Stopped('incremental_gate_profile_changed')
        if not self.operation_id:raise Stopped('prepared_bridge_operation_required')
        LocalGate,BridgeGateClient,GateSet=self.classes
        def engine_probe():
            if self.incremental_binding is not self._incremental_binding:raise Stopped('incremental_gate_profile_changed')
            engine=self.t.docker('info','--format','{{.ID}}').strip()
            if self.incremental_binding:
                cluster_backend_idle(self.t,dict(clusterContainerId=self.incremental_binding.cluster_container,engineImages=dict(cluster=self.incremental_binding.cluster_image)))
                if self.t.docker('info','--format','{{.ID}}').strip()!=engine:raise Stopped('cluster_engine_identity_changed')
            return engine
        local=LocalGate(self.bindings['cluster'],engine_probe=engine_probe)
        remote=BridgeGateClient(sys.stdin.buffer,sys.stdout.buffer,binding=self.bindings['main'],
            operation_id=self.operation_id,session_nonce=self.config['sessionNonce'],timeout_seconds=self.config['timeoutSeconds'])
        return GateSet(local,remote)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('prepare','activate','rollback','reconcile-schedule'))
    parser.add_argument('--image',required=True);parser.add_argument('--source-commit',required=True)
    parser.add_argument('--receipt-dir',required=True);parser.add_argument('--verified-main-image',required=True)
    parser.add_argument('--gate-config',required=True,help='private reviewed live dual-host gate bridge configuration')
    parser.add_argument('--incremental-profile',help='closed explicit incremental binding JSON; no credentials')
    parser.add_argument('--helper-source-commit')
    args=parser.parse_args(argv)
    try:
        if os.name!='posix':raise Stopped('linux_reference_update_required')
        import pwd
        if (os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name!='bkoltz'
                or socket.gethostname().split('.')[0].lower()!='cluster'):raise Stopped('fixed_cluster_account_required')
        with legacy.operation_lock():
            root=Path(args.receipt_dir)
            if not root.is_absolute() or root.resolve()!=root or any(path.is_symlink() for path in (root,*root.parents)):
                raise Stopped('absolute_private_operation_required')
            if args.action=='prepare':
                parent=root.parent.stat()
                if parent.st_uid!=os.geteuid() or parent.st_mode & 0o077:raise Stopped('private_operation_parent_required')
                root.mkdir(mode=0o700)
            state=None if args.action=='prepare' else json.loads(legacy.private_file(root/'checkpoint.json').read_text())
            profile=incremental.IncrementalBinding(**json.loads(args.incremental_profile)) if args.incremental_profile else None
            factory=GateFactory(json.loads(legacy.private_file(args.gate_config).read_text()),operation_id=state['operation'] if state else None,incremental_binding=profile)
            updater=ReferenceCoordinatorUpdate(root,gate_factory=factory,incremental_binding=profile,helper_source_commit=args.helper_source_commit)
            if args.action=='prepare':state=updater.prepare(args.image,args.source_commit,args.verified_main_image)
            else:
                if not ACTIVATION_REVIEWED or (profile and args.action!='reconcile-schedule' and not incremental_release_reviewed(profile,args.image,args.source_commit)):raise Stopped('source_review_required_before_activation')
                state=updater.load()
                if (state['image'],state['sourceCommit'],state['engineImages']['main'])!=(args.image,args.source_commit,args.verified_main_image):raise Stopped('prepared_argument_binding_changed')
                state=updater.activate(state) if args.action=='activate' else updater.reconcile_schedule(state) if args.action=='reconcile-schedule' else updater.rollback(state)
        # Activation stdout belongs to the root relay until process EOF.
        print(json.dumps(dict(operation=state['operation'],phase=state['phase'],authorityId=AUTHORITY)),
            file=__import__('sys').stdout if args.action=='prepare' else __import__('sys').stderr)
        return 0
    except Exception:
        print('Reference coordinator update held; retain private evidence and inspect locally.',file=__import__('sys').stderr)
        return 2


if __name__=='__main__':raise SystemExit(main())
