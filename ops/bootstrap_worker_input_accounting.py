"""Explicit fresh-worker input accounting bootstrap; never runs on import.

No engine, network, NAS, credential creation or cleanup interface exists here.
The fixed claim is permanent until a separately qualified retirement procedure.
Unknown preexisting state is retained and refused, never adopted or repaired.
"""
from contextlib import closing
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import sys
import uuid
from urllib.parse import urlsplit

from processing.scratch_policy import ScratchLedger, INPUT_SCHEMA, INPUT_TABLES

DIRECTORY = 'input-copy-bootstrap'
CLAIM = '.input-copy-bootstrap.json'
LIMIT = 16384
BUSINESS = ('job_environments','reservations','results','objects','receipts','pins',
            'cleanup_intents','control_charges')


def _require(value):
    if not value:
        raise ValueError('worker input accounting bootstrap unavailable')


def _encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def _sha(value):
    return hashlib.sha256(_encoded(value)).hexdigest()


def input_profile(config):
    if 'inputCopyAccounting' not in config:
        return False
    value=config['inputCopyAccounting']
    _require(type(value) is dict and type(value.get('schemaVersion')) is int)
    if value['schemaVersion']==1:
        _require(set(value)=={'schemaVersion'})
    else:
        _require(value['schemaVersion']==2 and set(value)=={'schemaVersion','fetchJournal'})
        fetch=value['fetchJournal']
        _require(type(fetch) is dict and set(fetch)=={'schemaVersion','policyId'}
                 and type(fetch['schemaVersion']) is int and fetch['schemaVersion']==1
                 and fetch['policyId']=='indexed-worker-fetch-v1')
        routing=config.get('executionRouting')
        _require(routing is None or type(routing) is dict and routing.get('treedar') is None)
    return True


def indexed_profile(config):
    """A closed v2 composition request, never an authority to initialize state."""
    return input_profile(config) and config['inputCopyAccounting']['schemaVersion']==2


def _indexed_descriptor():
    from processing.reference_indexed_fetch import compiled_profile_descriptor
    value=compiled_profile_descriptor()
    _require(type(value) is dict and set(value)=={'schemaVersion','protocol','policyId','policySha256','schemaSha256','limits'}
             and type(value['schemaVersion']) is int and value['schemaVersion']==1
             and value['protocol']=='ltds-indexed-worker-fetch-v1' and value['policyId']=='indexed-worker-fetch-v1'
             and type(value['limits']) is dict and value['limits'])
    for key in ('policySha256','schemaSha256'):
        _require(type(value[key]) is str and re.fullmatch('[a-f0-9]{64}',value[key]) is not None)
    _require(all(type(k) is str and type(v) is int and 0<=v<=2**53-1 for k,v in value['limits'].items()))
    return json.loads(_encoded(value))


def _identity(info):
    return [info.st_dev,info.st_ino]


def _directory(path):
    path=Path(path)
    _require(path.is_absolute() and path.resolve()==path
             and not any(p.is_symlink() for p in (path,*path.parents)))
    info=path.lstat()
    _require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid()
             and stat.S_IMODE(info.st_mode)==0o700)
    return _identity(info)


def _file(path):
    path=Path(path);_require(path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)))
    before=path.lstat()
    _require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_uid==os.geteuid()
             and stat.S_IMODE(before.st_mode)==0o600 and before.st_size<=LIMIT)
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        opened=os.fstat(fd);_require(_identity(opened)==_identity(before))
        raw=os.read(fd,LIMIT+1);after=os.fstat(fd);visible=path.lstat()
        facts=lambda i:(i.st_dev,i.st_ino,i.st_mode,i.st_nlink,i.st_uid,i.st_size,i.st_mtime_ns,i.st_ctime_ns)
        _require(facts(before)==facts(opened)==facts(after)==facts(visible) and len(raw)==before.st_size)
    finally:
        os.close(fd)
    value=json.loads(raw)
    _require(_encoded(value)==raw)
    return value,_identity(before),hashlib.sha256(raw).hexdigest()


def _sync(path):
    fd=os.open(path,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    try:os.fsync(fd)
    finally:os.close(fd)


def _write_record(path,value,owner):
    owner.check()
    _directory(path.parent)
    raw=_encoded(value);_require(len(raw)<=LIMIT)
    fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    try:
        _require(os.write(fd,raw)==len(raw));os.fsync(fd)
    finally:
        os.close(fd)
    _sync(path.parent);owner.check()
    return _file(path)


def _origin(value,schemes):
    _require(type(value) is str and 1<=len(value)<=4096 and not any(ord(c)<=32 for c in value))
    parsed=urlsplit(value)
    _require(parsed.scheme in schemes and parsed.hostname and parsed.username is None
             and parsed.password is None and parsed.path in ('','/') and not parsed.query
             and not parsed.fragment and parsed.port!=0 and '\\' not in value)
    return value.rstrip('/')


def _binding(config):
    root,scratch=Path(config['stateRoot']),Path(config['scratchRoot'])
    root_id,scratch_id=_directory(root),_directory(scratch)
    _require(root!=scratch and root not in scratch.parents and scratch not in root.parents)
    identity={key:config[key] for key in ('nodeId','workerId','workerInstanceId','authorityId','engineId','gateGeneration')}
    for key in ('nodeId','workerId','workerInstanceId','authorityId','engineId'):
        _require(type(identity[key]) is str and 1<=len(identity[key])<=128 and not any(ord(c)<=32 for c in identity[key]))
    _require(type(identity['gateGeneration']) is int and identity['gateGeneration']>=1)
    policy={key:config[key] for key in ('outputBudget','transferTtl','safetyFloor','cleanupGrace')}
    _require(all(type(v) is int and 0<=v<=2**53-1 for v in policy.values()))
    policy['environments']=sorted(set(config.get('allowedEnvironments',['production','staging'])))
    _require(policy['environments'] and set(policy['environments'])<={'production','staging'})
    value=dict(schemaVersion=1,stateRoot=str(root),stateRootIdentity=root_id,scratchRoot=str(scratch),
                scratchRootIdentity=scratch_id,physical=identity,
                origins={key:_origin(config[key],('https',) if key in ('coordinatorOrigin','brokerOrigin') else ('http','https'))
                         for key in ('coordinatorOrigin','brokerOrigin','gateCoordinatorOrigin','engineOrigin')},
                policy=policy,inputCopyAccounting=dict(schemaVersion=1))
    if indexed_profile(config):
        value['inputCopyAccounting']=json.loads(_encoded(config['inputCopyAccounting']))
        value['indexedFetchJournal']=_indexed_descriptor()
    _require(ipaddress.ip_address(urlsplit(value['origins']['engineOrigin']).hostname).is_loopback)
    serialized=_encoded(value)
    _require(len(serialized)<=LIMIT-1024)
    for key in ('engineToken','workerBearer','resultReadBearer'):
        secret=config.get(key)
        _require(not secret or type(secret) is str and secret.encode() not in serialized)
    return value


def _names(path,allowed):
    with os.scandir(path) as entries:
        found=set()
        for entry in entries:
            _require(entry.name in allowed and entry.name not in found)
            found.add(entry.name)
    return found


def _fresh_namespace(root,scratch,*,ledger=False,claim=False,indexed=False):
    allowed={'worker-daemon.lock',DIRECTORY}|({'scratch.sqlite'} if ledger else set())|({'driver.sqlite'} if indexed else set())
    _names(root,allowed)
    _names(scratch,{CLAIM} if claim else set())


def _ledger(path,binding,*,fresh=False,expected_identity=None):
    _require(path.resolve()==path and not any(p.is_symlink() for p in (path,*path.parents)))
    before=path.lstat()
    _require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_uid==os.geteuid()
             and stat.S_IMODE(before.st_mode)==0o600 and before.st_size<=1024**3
             and not any(os.path.lexists(str(path)+suffix) for suffix in ('-wal','-shm','-journal')))
    if expected_identity is not None:_require(_identity(before)==expected_identity)
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
        db.set_progress_handler(lambda:1 if __import__('time').monotonic()>deadline else 0,1000)
        deadline=__import__('time').monotonic()+3
        _require(db.execute('PRAGMA quick_check').fetchone()==('ok',))
        rows=db.execute("SELECT value FROM metadata WHERE key='config' AND typeof(value)='text' AND length(CAST(value AS BLOB))<=8192").fetchall()
        _require(len(rows)==1)
        config=json.loads(rows[0][0])
        base={'root':binding['scratchRoot'],'floor':binding['policy']['safetyFloor'],'grace':binding['policy']['cleanupGrace']}
        _require(set(config)<=set(base)|{'accountingVersion','inputCopyAccountingVersion'}
                 and _encoded({k:config.get(k) for k in base})==_encoded(base))
        for marker in ('accountingVersion','inputCopyAccountingVersion'):
            if marker in config:_require(type(config[marker]) is int and config[marker]==1)
        tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        _require(set(BUSINESS)|{'metadata'}<=tables)
        enabled='inputCopyAccountingVersion' in config
        _require((set(INPUT_TABLES)&tables)==(set(INPUT_TABLES) if enabled else set()))
        if enabled:
            _require(db.execute('SELECT singleton,version FROM input_accounting').fetchall()==[(1,1)])
            with closing(sqlite3.connect(':memory:')) as expected:
                for statement in INPUT_SCHEMA:expected.execute(statement)
                query="SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN (%s) ORDER BY type,name" % ','.join("'"+t+"'" for t in INPUT_TABLES)
                _require(db.execute(query).fetchall()==expected.execute(query).fetchall())
        control=bool(db.execute('SELECT 1 FROM control_charges LIMIT 1').fetchone())
        _require(control==('accountingVersion' in config))
        if fresh:
            _require(tables==set(BUSINESS)|{'metadata'}|(set(INPUT_TABLES) if enabled else set()))
            _require(not any(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() for table in BUSINESS))
            if enabled:
                _require(not any(db.execute('SELECT 1 FROM '+table+' LIMIT 1').fetchone() for table in INPUT_TABLES if table!='input_accounting'))
            _require({row[0] for row in db.execute('SELECT key FROM metadata')}=={'config','clock'})
    after=path.lstat()
    _require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)==
             (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
    return {'identity':_identity(after),'enabled':enabled}


def _read_intent(directory,binding):
    intent,_,intent_sha=_file(directory/'intent.json')
    _require(set(intent)=={'schemaVersion','purpose','bootstrapId','binding','bindingSha256'}
             and intent['schemaVersion']==1 and type(intent['schemaVersion']) is int
             and intent['purpose']=='fresh-worker-input-copy-accounting'
             and intent['binding']==binding and intent['bindingSha256']==_sha(binding)
             and str(uuid.UUID(intent['bootstrapId']))==intent['bootstrapId'])
    return intent,intent_sha


def _claim_value(intent,binding):
    value=dict(schemaVersion=1,purpose='exclusive-input-copy-scratch-root',bootstrapId=intent['bootstrapId'],
        bindingSha256=intent['bindingSha256'],stateRootIdentity=binding['stateRootIdentity'],scratchRootIdentity=binding['scratchRootIdentity'])
    if 'indexedFetchJournal' in binding:
        value.update(schemaVersion=2,purpose='exclusive-indexed-fetch-scratch-root',indexedFetchJournal=binding['indexedFetchJournal'])
    return value


def _indexed_context(root,intent,owner):
    from processing.reference_indexed_fetch import read_only_profile,IndexedFetchContext
    context=read_only_profile(root/'driver.sqlite',bootstrap_id=intent['bootstrapId'],binding=intent['binding'],owner=owner)
    _require(type(context) is IndexedFetchContext and context.bootstrap_id==intent['bootstrapId']
             and context.binding_sha256==intent['bindingSha256']
             and list(context.root_identity)==intent['binding']['stateRootIdentity'])
    return context


def _indexed_record(root,directory,intent,intent_sha,created_sha,owner,*,fresh=False,external_only=False):
    if external_only:
        from processing.reference_indexed_fetch import _file as indexed_file
        info=indexed_file(root/'driver.sqlite')
        from types import SimpleNamespace
        context=SimpleNamespace(driver_identity=_identity(info),root_identity=_directory(root),
                                binding_sha256=intent['bindingSha256'])
    else:
        context=_indexed_context(root,intent,owner)
    if fresh:
        with closing(sqlite3.connect((root/'driver.sqlite').as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            _require(db.execute('SELECT 1 FROM driver_binding LIMIT 1').fetchone() is None)
    expected=dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,
        ledgerCreatedSha256=created_sha,descriptorSha256=_sha(intent['binding']['indexedFetchJournal']),
        driverIdentity=list(context.driver_identity),rootIdentity=list(context.root_identity),bindingSha256=context.binding_sha256)
    actual,_,record_sha=_file(directory/'indexed-created.json')
    _require(_encoded(actual)==_encoded(expected))
    return context,record_sha


def _records(config,owner,*,completed,external_indexed=False):
    owner.check(Path(config['stateRoot']))
    binding=_binding(config);root=Path(binding['stateRoot']);directory=root/DIRECTORY
    _directory(directory)
    indexed='indexedFetchJournal' in binding
    allowed={'intent.json','ledger-created.json','completed.json'} if completed else {'intent.json','ledger-created.json'}
    _names(directory,allowed|({'indexed-created.json'} if indexed else set()))
    intent,intent_sha=_read_intent(directory,binding)
    claim,claim_id,claim_sha=_file(Path(binding['scratchRoot'])/CLAIM)
    _require(type(claim.get('schemaVersion')) is int)
    _require(_encoded(claim)==_encoded(_claim_value(intent,binding)))
    created,_,created_sha=_file(directory/'ledger-created.json')
    _require(set(created)=={'schemaVersion','bootstrapId','intentSha256','claimSha256','claimIdentity','ledgerIdentity'}
             and created['schemaVersion']==1 and type(created['schemaVersion']) is int
             and created['bootstrapId']==intent['bootstrapId'] and created['intentSha256']==intent_sha
             and created['claimSha256']==claim_sha and created['claimIdentity']==claim_id)
    ledger=_ledger(root/'scratch.sqlite',binding,fresh=not completed,expected_identity=created['ledgerIdentity'])
    indexed_sha=None
    if indexed:
        if os.path.lexists(directory/'indexed-created.json'):
            _,indexed_sha=_indexed_record(root,directory,intent,intent_sha,created_sha,owner,fresh=not completed,external_only=external_indexed)
        else:
            _require(not completed and not os.path.lexists(root/'driver.sqlite'))
    if completed:
        receipt,_,_=_file(directory/'completed.json')
        _require(type(receipt.get('schemaVersion')) is int and type(receipt.get('inputCopyAccountingVersion')) is int)
        expected=dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,
                      createdSha256=created_sha,inputCopyAccountingVersion=1)
        if indexed:expected.update(schemaVersion=2,indexedCreatedSha256=indexed_sha)
        _require(_encoded(receipt)==_encoded(expected)
                 and ledger['enabled'])
    owner.check(root)
    return intent,created,ledger


def completed_indexed_recovery_authority(config,owner):
    """External completed-v2 authority; no driver SQLite open or clock observation."""
    _require(indexed_profile(config))
    intent,created,ledger=_records(config,owner,completed=True,external_indexed=True)
    _require(ledger['enabled'])
    return intent


def _driver_settings(stored,bound,*,indexed):
    accounting=stored.get('inputCopyAccounting')
    _require(type(accounting) is dict and set(accounting)=={'schemaVersion'}
             and type(accounting['schemaVersion']) is int and accounting['schemaVersion']==1)
    if indexed:
        _require(_encoded(stored.get('indexedFetchJournal'))==_encoded(bound['indexedFetchJournal']))
    else:
        _require('indexedFetchJournal' not in stored)
    physical=bound['physical'];origins=bound['origins'];policy=bound['policy']
    expected={**physical,'origin':origins['coordinatorOrigin'],'brokerOrigin':origins['brokerOrigin'],
              'engineOrigin':origins['engineOrigin'],'gateOrigin':origins['gateCoordinatorOrigin'],
              'outputBudget':policy['outputBudget'],'transferTtl':policy['transferTtl'],
              'safetyFloor':policy['safetyFloor'],'environments':policy['environments']}
    _require(_encoded({key:stored.get(key) for key in expected})==_encoded(expected))


def require_worker_input_profile(config,owner):
    """Normal startup validates existing authority; it never creates/latches it."""
    enabled=input_profile(config);root=Path(config['stateRoot']);scratch=Path(config['scratchRoot'])
    indexed=indexed_profile(config) if enabled else False
    owner.check(root)
    if not indexed:
        # This must precede even a legacy ledger clock observation. The normal
        # driver constructor's later footprint guard is too late for the factory.
        from processing.reference_indexed_fetch import reject_indexed_footprint
        reject_indexed_footprint(root/'driver.sqlite')
    if not enabled:
        _require(not os.path.lexists(root/DIRECTORY) and not os.path.lexists(scratch/CLAIM))
        return False
    intent,_,_=_records(config,owner,completed=True)
    # An opted-in daemon may not silently adopt even an empty legacy driver.
    driver=root/'driver.sqlite'
    if os.path.lexists(driver):
        info=driver.lstat()
        _require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid()
                 and stat.S_IMODE(info.st_mode)==0o600 and info.st_size<=1024**3 and not driver.is_symlink()
                 and not any(os.path.lexists(str(driver)+s) for s in ('-wal','-shm','-journal')))
        with closing(sqlite3.connect(driver.as_uri()+'?mode=ro&immutable=1',uri=True)) as db:
            deadline=__import__('time').monotonic()+3
            db.set_progress_handler(lambda:1 if __import__('time').monotonic()>deadline else 0,1000)
            rows=db.execute("SELECT binding FROM driver_binding WHERE singleton=1 AND typeof(binding)='text' AND length(CAST(binding AS BLOB))<=16384").fetchall()
            if indexed and not rows:
                # The concrete indexed context already proved all indexed/base
                # business tables empty. Never extend this to a legacy journal.
                _require(db.execute('SELECT 1 FROM driver_jobs LIMIT 1').fetchone() is None)
            else:
                _require(len(rows)==1)
                _driver_settings(json.loads(rows[0][0]),intent['binding'],indexed=indexed)
        after=driver.lstat()
        _require((info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns)==
                 (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns))
    owner.check(root)
    return True


def require_worker_indexed_context(config,owner):
    """Return only the actual prebootstrapped v2 context before component opens."""
    if not indexed_profile(config):return None
    intent,_,_=_records(config,owner,completed=True)
    return _indexed_context(Path(config['stateRoot']),intent,owner)


def bootstrap_input_copy_accounting(config):
    """One explicitly invoked fresh-state operation; known checkpoints resume."""
    _require(sys.platform.startswith('linux') and input_profile(config))
    from ops.reference_worker_daemon import DaemonStateLock
    from ops.private_worker_directory import private_worker_directory
    with DaemonStateLock(config['stateRoot']) as owner:
        root=Path(config['stateRoot']);directory=root/DIRECTORY;scratch=Path(config['scratchRoot'])
        if not os.path.lexists(directory):
            _names(root,{'worker-daemon.lock'})
            private_worker_directory(scratch);_fresh_namespace(root,scratch)
            binding=_binding(config);owner.check(root)
            os.mkdir(directory,0o700);_sync(root)
            intent=dict(schemaVersion=1,purpose='fresh-worker-input-copy-accounting',bootstrapId=str(uuid.uuid4()),binding=binding,bindingSha256=_sha(binding))
            _write_record(directory/'intent.json',intent,owner)
        else:
            _directory(directory)
            # Read and compare the actual intent before any new file or DB.
            binding=_binding(config);intent,_=_read_intent(directory,binding)
        if os.path.lexists(directory/'completed.json'):
            require_worker_input_profile(config,owner)
            return {'state':'already_enabled','bootstrapId':intent['bootstrapId']}
        indexed=indexed_profile(config)
        _names(directory,{'intent.json','ledger-created.json'}|({'indexed-created.json'} if indexed else set()))
        if indexed and os.path.lexists(directory/'indexed-created.json'):
            _require(os.path.lexists(directory/'ledger-created.json'))
        if os.path.lexists(directory/'ledger-created.json'):
            # A created checkpoint means the claim and ledger already existed.
            # Do not recreate either when that known phase is incomplete.
            _records(config,owner,completed=False)
        ledger_path=root/'scratch.sqlite';claim_path=scratch/CLAIM
        has_indexed=indexed and os.path.lexists(directory/'indexed-created.json')
        _fresh_namespace(root,scratch,ledger=os.path.lexists(directory/'ledger-created.json'),claim=os.path.lexists(claim_path),indexed=has_indexed)
        claim=_claim_value(intent,binding)
        if not os.path.lexists(claim_path):_write_record(claim_path,claim,owner)
        actual_claim,claim_id,claim_sha=_file(claim_path);_require(actual_claim==claim)
        if not os.path.lexists(directory/'ledger-created.json'):
            _require(not os.path.lexists(ledger_path));owner.check(root)
            _read_intent(directory,_binding(config))
            ledger=ScratchLedger(ledger_path,scratch,safety_floor_bytes=config['safetyFloor'],cleanup_grace_seconds=config['cleanupGrace'],input_copy_accounting=True)
            snapshot=_ledger(ledger_path,binding,fresh=True);_require(not snapshot['enabled'])
            _,_,intent_sha=_file(directory/'intent.json')
            _write_record(directory/'ledger-created.json',dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,claimSha256=claim_sha,claimIdentity=claim_id,ledgerIdentity=snapshot['identity']),owner)
        intent,created,snapshot=_records(config,owner,completed=False)
        if indexed and not os.path.lexists(directory/'indexed-created.json'):
            from processing.reference_indexed_fetch import initialize_fresh_profile,IndexedFetchContext
            _fresh_namespace(root,scratch,ledger=True,claim=True);owner.check(root)
            _require(not os.path.lexists(root/'driver.sqlite'))
            _read_intent(directory,_binding(config))
            context=initialize_fresh_profile(root/'driver.sqlite',bootstrap_id=intent['bootstrapId'],binding=binding,owner=owner)
            _require(type(context) is IndexedFetchContext and context.bootstrap_id==intent['bootstrapId']
                     and context.binding_sha256==intent['bindingSha256'] and list(context.root_identity)==binding['stateRootIdentity'])
            _,_,intent_sha=_file(directory/'intent.json');_,_,created_sha=_file(directory/'ledger-created.json')
            _write_record(directory/'indexed-created.json',dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],
                intentSha256=intent_sha,ledgerCreatedSha256=created_sha,descriptorSha256=_sha(binding['indexedFetchJournal']),
                driverIdentity=list(context.driver_identity),rootIdentity=list(context.root_identity),bindingSha256=context.binding_sha256),owner)
        _fresh_namespace(root,scratch,ledger=True,claim=True,indexed=indexed);owner.check(root)
        if not snapshot['enabled']:
            ledger=ScratchLedger(ledger_path,scratch,safety_floor_bytes=config['safetyFloor'],cleanup_grace_seconds=config['cleanupGrace'],input_copy_accounting=True)
            _records(config,owner,completed=False)
            _fresh_namespace(root,scratch,ledger=True,claim=True,indexed=indexed);owner.check(root)
            ledger.enable_input_copy_accounting()
        intent,created,snapshot=_records(config,owner,completed=False);_require(snapshot['enabled'])
        _,_,intent_sha=_file(directory/'intent.json');_,_,created_sha=_file(directory/'ledger-created.json')
        completion=dict(schemaVersion=1,bootstrapId=intent['bootstrapId'],intentSha256=intent_sha,createdSha256=created_sha,inputCopyAccountingVersion=1)
        if indexed:
            _,_,indexed_sha=_file(directory/'indexed-created.json')
            completion.update(schemaVersion=2,indexedCreatedSha256=indexed_sha)
        _write_record(directory/'completed.json',completion,owner)
        require_worker_input_profile(config,owner)
        return {'state':'enabled','bootstrapId':intent['bootstrapId']}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--config',required=True)
    args=parser.parse_args(argv)
    try:
        from ops.reference_worker_daemon import load_config
        print(json.dumps(bootstrap_input_copy_accounting(load_config(args.config))));return 0
    except Exception:
        print('Worker input accounting bootstrap held; retain existing state for inspection.',file=sys.stderr);return 2


if __name__=='__main__':raise SystemExit(main())
