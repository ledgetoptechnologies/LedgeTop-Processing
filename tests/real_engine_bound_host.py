"""Owned synthetic NodeODM image rehearsal; reads trusted archive from stdin.

Run only in an isolated test context. No host mounts, published ports, engine
commit, GPU or client data. On identity or cleanup uncertainty resources remain.
"""
import base64
import hashlib
import io
import json
import os
import re
import selectors
import subprocess
import sys
import tarfile
import time
import uuid

ENGINE='sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6'
HELPER='sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530'
SOURCE_COMMIT='f0646ab2e1b92e079234f1ff960ca82dd80ca0f7'
ARCHIVE_SHA='a92e9ee7130d6ca2e0218698ae82d5ba4cdebfdc728f2f2e76fcf77da6168993'
PROBE_SHA='63b033dd9f48152cd3959e01f2b252c5c82b87f4f8750d1a851e694c88fb304e'
ENGINE_LAUNCH="umask 077; printf '%s' '{\"logger\":{\"logDirectory\":\"/var/www/logs\"}}' > /tmp/lt-probe-config.json && exec /usr/bin/node /var/www/index.js --config /tmp/lt-probe-config.json"
env={k:v for k,v in os.environ.items() if k.upper() not in ('DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH')}

def run(args,*,data=None,timeout=15,check=True):
    proc=subprocess.Popen(args,stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                          stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
    selector=selectors.DefaultSelector();output={'out':bytearray(),'err':bytearray()};offset=0;deadline=time.monotonic()+timeout
    try:
        for name,stream in (('out',proc.stdout),('err',proc.stderr)):
            os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,name)
        if data is not None:
            os.set_blocking(proc.stdin.fileno(),False);selector.register(proc.stdin,selectors.EVENT_WRITE,'input')
        while selector.get_map():
            remaining=deadline-time.monotonic()
            if remaining<=0:raise RuntimeError('owned command timeout')
            events=selector.select(remaining)
            if not events:raise RuntimeError('owned command timeout')
            for key,_ in events:
                stream=key.fileobj
                if key.data=='input':
                    if offset>=len(data):selector.unregister(stream);stream.close();continue
                    try:count=os.write(stream.fileno(),data[offset:offset+65536])
                    except BlockingIOError:continue
                    except BrokenPipeError:selector.unregister(stream);stream.close();continue
                    offset+=count
                else:
                    try:chunk=os.read(stream.fileno(),8192)
                    except BlockingIOError:continue
                    if not chunk:selector.unregister(stream);stream.close();continue
                    output[key.data].extend(chunk)
                    if len(output[key.data])>200000:raise RuntimeError('bounded command output exceeded')
        remaining=deadline-time.monotonic()
        if remaining<=0:raise RuntimeError('owned command timeout')
        code=proc.wait(timeout=remaining)
        if data is not None and offset!=len(data):raise RuntimeError('owned command input incomplete')
        result=subprocess.CompletedProcess(args,code,bytes(output['out']),bytes(output['err']))
    finally:
        selector.close()
        for stream in (proc.stdin,proc.stdout,proc.stderr):
            if stream is not None and not stream.closed:stream.close()
        if proc.poll() is None:
            proc.kill()
            try:proc.wait(timeout=2)
            except subprocess.TimeoutExpired:raise RuntimeError('owned CLI reap uncertain') from None
    if check and result.returncode:
        text=(result.stdout+result.stderr).decode('utf-8','replace').lower()
        markers=('assertionerror','admissionerror','jsondecodeerror','permissionerror','filenotfounderror',
                 'syntaxerror','modulenotfounderror','brokenpipeerror','typeerror','referenceerror','timeout','eacces','erofs','enoent')
        raise RuntimeError('owned command failed: '+args[1]+'; categories='+','.join(x for x in markers if x in text))
    return result

def docker(*args,**kw):return run(['docker',*args],**kw)
def daemon():return docker('info','--format','{{.ID}}').stdout.strip()
def decode_json(label,data):
    import math
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('duplicate key')
            result[key]=value
        return result
    def number(value):
        result=float(value)
        if not math.isfinite(result):raise ValueError('nonfinite number')
        return result
    def bad_constant(_):raise ValueError('nonfinite number')
    def parse(raw):return json.loads(raw,object_pairs_hook=pairs,parse_constant=bad_constant,parse_float=number)
    try:return parse(data)
    except (ValueError,UnicodeError):
        if label=='stored object':
            lines=data.splitlines();matched=0
            if len(lines)<=16:
                for line in lines:
                    try:value=parse(line)
                    except (ValueError,UnicodeError):continue
                    if type(value) is dict and set(value)=={'name','bytes','sha256','matchingCount'}:matched+=1
            else:matched=-1
            raise RuntimeError('stored object invalid JSON; byte_count='+str(len(data))+'; lines='+str(len(lines))+'; expected_objects='+str(matched)) from None
        raise RuntimeError(label+' invalid JSON; byte_count='+str(len(data))) from None
def inspect(cid):return decode_json('container inspect',docker('inspect',cid).stdout)[0]

def safe_reason(error):
    if type(error) is not RuntimeError:return None
    text=str(error)
    match=re.fullmatch(r'owned command failed: (exec|cp); categories=([a-z,]*)',text)
    if match:
        categories=match[2].split(',') if match[2] else []
        allowed={'assertionerror','admissionerror','jsondecodeerror','permissionerror','filenotfounderror',
                 'syntaxerror','modulenotfounderror','brokenpipeerror','typeerror','referenceerror','timeout','eacces','erofs','enoent'}
        if len(categories)==len(set(categories)) and all(item in allowed for item in categories):
            return {'code':match[1]+'_failed','categories':categories}
    if text=='synthetic stored object differs':return {'code':'stored_object_mismatch'}
    fixed={'stored_length_mismatch','stored_hash_mismatch',
           'stored_directory_tar_invalid','stored_directory_structure','stored_duplicate_or_missing'}
    if text in fixed:return {'code':text}
    match=re.fullmatch(r'stored object invalid JSON; byte_count=([0-9]{1,6}); lines=([0-9]{1,6}); expected_objects=(-1|[0-9]{1,2})',text)
    if match:return {'code':'stored_object_invalid_json','byteCount':int(match[1]),'lineCount':int(match[2]),'expectedJsonObjects':int(match[3])}
    return None

def verify_stored_tar(directory_tar,identifier,expected):
    if (not re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',identifier)
        or type(expected) is not dict or set(expected)!={'bytes','sha256'} or type(expected['bytes']) is not int
        or not 0<expected['bytes']<=4096 or not re.fullmatch('[0-9a-f]{64}',expected['sha256'])):
        raise RuntimeError('synthetic stored identity invalid')
    try:
        with tarfile.open(fileobj=io.BytesIO(directory_tar),mode='r:') as archive:
            members=archive.getmembers()
            if not 1<=len(members)<=17:raise RuntimeError('stored_directory_structure')
            names=[];target=None;root_count=0
            for item in members:
                parts=item.name.split('/')
                if item.name==identifier and item.isdir():root_count+=1;continue
                if len(parts)!=2 or parts[0]!=identifier or not parts[1] or parts[1] in ('.','..') or not item.isfile() or item.size>4096:raise RuntimeError('stored_directory_structure')
                names.append(parts[1])
                if parts[1]=='synthetic.jpg':target=item
            if root_count!=1 or len(names)!=len(set(names)) or sum(bool(re.fullmatch(r'synthetic_*\.jpg',name)) for name in names)!=1 or target is None:raise RuntimeError('stored_duplicate_or_missing')
            if target.size!=expected['bytes']:raise RuntimeError('stored_length_mismatch')
            content=archive.extractfile(target).read(expected['bytes']+1)
            if len(content)!=expected['bytes']:raise RuntimeError('stored_length_mismatch')
            if hashlib.sha256(content).hexdigest()!=expected['sha256']:raise RuntimeError('stored_hash_mismatch')
    except (tarfile.TarError,OSError,ValueError,AttributeError):
        raise RuntimeError('stored_directory_tar_invalid') from None
    return {'name':'synthetic.jpg','bytes':expected['bytes'],'sha256':expected['sha256'],'matchingCount':1}

try:
    nonce=uuid.uuid4().hex;label='ledgetop.synthetic.bound-probe';base='lt-bound-probe-'+nonce
    payload=json.loads(sys.stdin.buffer.readline(20_000_000));archive=base64.b64decode(payload['archive'],validate=True);probe=base64.b64decode(payload['probe'],validate=True)
    if (len(archive)>16_000_000 or len(probe)>100_000 or payload['sourceCommit']!=SOURCE_COMMIT
        or payload['archiveSha256']!=ARCHIVE_SHA or payload['probeSha256']!=PROBE_SHA
        or hashlib.sha256(archive).hexdigest()!=ARCHIVE_SHA or hashlib.sha256(probe).hexdigest()!=PROBE_SHA):
        raise RuntimeError('input size or digest mismatch')
    initial_daemon=daemon();owned=[];cleanup_uncertain=False;creation_uncertain=False;report=None;diagnostic=None;probe_summary=None;phase='image_verification'
    if not initial_daemon:raise RuntimeError('daemon identity unavailable')
    for image in (ENGINE,HELPER):
        actual=docker('image','inspect',image,'--format','{{.Id}}').stdout.decode().strip()
        if actual!=image:raise RuntimeError('exact local image unavailable')

    def same_daemon():
        if daemon()!=initial_daemon:raise RuntimeError('daemon changed; preserve resources')

    def verify(cid,image,network):
        item=inspect(cid);cfg=item['Config'];host=item['HostConfig']
        expected_tmpfs=({'/var/www/data':'rw,size=128m,mode=1777','/var/www/tmp':'rw,size=32m,mode=1777',
                         '/var/www/logs':'rw,size=16m,mode=1777','/tmp':'rw,size=32m,mode=1777'} if image==ENGINE
                        else {'/work':'rw,size=64m,mode=1777','/tmp':'rw,size=16m,mode=1777'})
        if (item['Id']!=cid or item['Image']!=image or (cfg.get('Labels') or {}).get(label)!=nonce
            or host['NetworkMode']!=network or host.get('Privileged') or not host.get('ReadonlyRootfs')
            or host.get('Binds') or host.get('Devices') or host.get('DeviceRequests') or host.get('PortBindings') or host.get('Runtime')!='runc'
            or host.get('Tmpfs')!=expected_tmpfs or host.get('NanoCpus')!=1000000000
            or host.get('Memory')!=1073741824 or host.get('PidsLimit')!=256
            or 'ALL' not in (host.get('CapDrop') or [])
            or not any(str(v).startswith('no-new-privileges') for v in (host.get('SecurityOpt') or []))
            or (host.get('RestartPolicy') or {}).get('Name')!='no'
            or any(m.get('Type')!='tmpfs' for m in item.get('Mounts',[]))):
            raise RuntimeError('owned container identity mismatch')
        if image==ENGINE and (cfg.get('User')!='odm' or cfg.get('Entrypoint')!=['/bin/sh'] or cfg.get('Cmd')!=['-c',ENGINE_LAUNCH]):
            raise RuntimeError('engine launch identity changed')
        if image==HELPER and (cfg.get('User')!='65534:65534' or cfg.get('Entrypoint')!=['python3'] or cfg.get('Cmd')!=['-c','import time;time.sleep(120)']):
            raise RuntimeError('helper launch identity changed')
        return item

    def create(image,network,extra):
        global creation_uncertain
        creation_uncertain=True
        same_daemon();out=docker('create','--pull=never','--name',base+'-'+('engine' if image==ENGINE else 'helper'),
            '--label',label+'='+nonce,'--network',network,'--read-only','--cap-drop=ALL','--security-opt=no-new-privileges',
            '--runtime=runc','--cpus=1','--memory=1g','--pids-limit=256',*extra,image,
            *(['-c',ENGINE_LAUNCH] if image==ENGINE else ['-c','import time;time.sleep(120)'])).stdout.decode().strip()
        if not re.fullmatch('[0-9a-f]{64}',out):raise RuntimeError('invalid created container id')
        owned.append((out,image,network,False));verify(out,image,network);owned[-1]=(out,image,network,True);creation_uncertain=False
        return out

    def startup_diagnostic(engine):
        same_daemon();state=verify(engine,ENGINE,'none')['State']
        result={'phase':'engine_start','running':bool(state['Running']),'exitCode':state.get('ExitCode'),'logCategories':[]}
        try:
            logs=docker('logs','--tail','80',engine,timeout=5,check=False)
            text=(logs.stdout+logs.stderr).decode('utf-8','replace').lower()
            categories={'eacces':'EACCES','erofs':'EROFS','enoent':'ENOENT','module_not_found':'MODULE_NOT_FOUND',
                        'permission denied':'permission_denied','out of memory':'out_of_memory',
                        'cuda':'cuda','address already in use':'address_in_use'}
            result['logCategories']=[value for key,value in categories.items() if key in text]
            if logs.returncode:result['logRead']='unavailable'
        except Exception:result['logRead']='unavailable'
        return result

    try:
        phase='engine_create'
        engine=create(ENGINE,'none',['--tmpfs','/var/www/data:rw,size=128m,mode=1777','--tmpfs','/var/www/tmp:rw,size=32m,mode=1777','--tmpfs','/var/www/logs:rw,size=16m,mode=1777','--tmpfs','/tmp:rw,size=32m,mode=1777','--entrypoint','/bin/sh'])
        phase='engine_start'
        same_daemon();verify(engine,ENGINE,'none');docker('start',engine)
        phase='helper_create'
        helper=create(HELPER,'container:'+engine,['--user','65534:65534','--tmpfs','/work:rw,size=64m,mode=1777','--tmpfs','/tmp:rw,size=16m,mode=1777','--entrypoint','python3'])
        if inspect(helper)['Config']['User']!='65534:65534':raise RuntimeError('helper user changed')
        phase='helper_start'
        same_daemon();verify(helper,HELPER,'container:'+engine);docker('start',helper)
        phase='engine_readiness'
        deadline=time.monotonic()+30
        while True:
            same_daemon();engine_state=verify(engine,ENGINE,'none')['State']
            if not engine_state['Running']:
                diagnostic=startup_diagnostic(engine)
                raise RuntimeError('isolated engine exited before readiness')
            result=docker('exec',helper,'python3','-c','import socket;s=socket.create_connection(("127.0.0.1",3000),1);s.close()',check=False,timeout=3)
            if result.returncode==0:break
            if time.monotonic()>=deadline:
                diagnostic=startup_diagnostic(engine)
                raise RuntimeError('isolated engine startup timeout')
            time.sleep(.3)
        extract='''import hashlib,io,os,sys,tarfile\ndata=sys.stdin.buffer.read(16000001)\nassert len(data)<=16000000 and hashlib.sha256(data).hexdigest()==sys.argv[1]\nos.mkdir('/work/src')\nwith tarfile.open(fileobj=io.BytesIO(data),mode='r:') as t:\n m=t.getmembers();assert len(m)<10000 and all((x.isfile() or x.isdir()) and not x.name.startswith('/') and '..' not in x.name.split('/') and (x.name=='processing' or x.name.startswith('processing/')) and not x.mode&0o002 for x in m)\n for x in m:x.mode=0o755 if x.isdir() or x.mode&0o111 else 0o644\n t.extractall('/work/src',filter='data')\n'''
        phase='source_extract';same_daemon();verify(engine,ENGINE,'none');verify(helper,HELPER,'container:'+engine)
        docker('exec','-i',helper,'python3','-c',extract,payload['archiveSha256'],data=archive,timeout=20)
        invoke="import base64,hashlib,sys;b=base64.b64decode(sys.stdin.buffer.read(),validate=True);assert hashlib.sha256(b).hexdigest()==sys.argv[1];sys.path.insert(0,'/work/src');exec(compile(b,'<owned-synthetic-probe>','exec'))"
        phase='probe';same_daemon();verify(engine,ENGINE,'none');verify(helper,HELPER,'container:'+engine)
        result=docker('exec','-i',helper,'python3','-c',invoke,PROBE_SHA,data=base64.b64encode(probe),timeout=45)
        phase='probe_result';outcome=decode_json('probe result',result.stdout);identifier=outcome['engineUuid']
        if not re.fullmatch('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}',identifier):raise RuntimeError('unexpected synthetic task identity')
        if (set(outcome)!={'engineUuid','filename','bytes','sha256','init','upload','partial','duplicate'}
            or outcome['filename']!='synthetic.jpg' or outcome['bytes']!=31
            or outcome['sha256']!='dbf597c06188325763901c9687203c7662967feb9599ae116dd1fc284da18b4b'
            or any(outcome[key]!=value for key,value in (('init','accepted'),('upload','accepted'),('partial','held'),('duplicate','refused')))):
            raise RuntimeError('synthetic probe outcome changed')
        probe_summary={'init':'accepted','upload':'accepted','partial':'held','duplicate':'refused',
                       'syntheticInputBytes':31,'syntheticInputSha256':outcome['sha256']}
        phase='stored_tar_exec';same_daemon();verify(engine,ENGINE,'none');verify(helper,HELPER,'container:'+engine)
        directory_tar=docker('exec',engine,'/bin/tar','-C','/var/www/tmp','-cf','-',identifier,timeout=5).stdout
        phase='stored_tar_verify'
        stored=verify_stored_tar(directory_tar,identifier,{'bytes':outcome['bytes'],'sha256':outcome['sha256']})
        phase='stored_compare'
        if stored!={'name':outcome['filename'],'bytes':outcome['bytes'],'sha256':outcome['sha256'],'matchingCount':1}:
            raise RuntimeError('synthetic stored object differs')
        report={'outcome':outcome,'stored':stored,'engineImage':ENGINE,'helperImage':HELPER,'sourceCommit':SOURCE_COMMIT,'archiveSha256':ARCHIVE_SHA,'probeSha256':PROBE_SHA}
    except Exception as error:
        detail={'phase':phase,'category':type(error).__name__}
        reason=safe_reason(error)
        if reason is not None:detail['reason']=reason
        if probe_summary is not None:detail['probeAssertions']=probe_summary
        print(json.dumps({'failure':detail},sort_keys=True),file=sys.stderr,flush=True)
        raise
    finally:
        cleanup_uncertain=creation_uncertain
        for cid,image,network,verified in reversed(owned):
            if not verified or creation_uncertain:
                cleanup_uncertain=True;continue
            try:
                same_daemon();item=verify(cid,image,network)
                if image==HELPER and item['Config']['User']!='65534:65534':raise RuntimeError('helper identity changed')
                if item['State']['Running']:
                    same_daemon();verify(cid,image,network);docker('stop','--time','3',cid,timeout=10)
                same_daemon();verify(cid,image,network);docker('rm',cid,timeout=10)
                same_daemon()
                listing=docker('ps','-a','--no-trunc','--format','{{.ID}}').stdout.decode('ascii').splitlines()
                same_daemon()
                if any(not re.fullmatch('[0-9a-f]{64}',item) for item in listing) or cid in listing:
                    raise RuntimeError('container removal unconfirmed')
            except Exception:
                cleanup_uncertain=True
        if cleanup_uncertain:
            print(json.dumps({'cleanup':'uncertain','nonce':nonce,'containerIds':[row[0] for row in owned]}),file=sys.stderr,flush=True)
            raise RuntimeError('owned cleanup uncertain; preserve evidence')
        if diagnostic is not None:
            print(json.dumps({'diagnostic':diagnostic,'cleanup':'verified_removed'},sort_keys=True),file=sys.stderr,flush=True)
    if report is not None:
        report['cleanup']='verified_removed';print(json.dumps(report,sort_keys=True),flush=True)

except BaseException as error:
    print(json.dumps({"failure":{"phase":"unhandled","category":type(error).__name__}},sort_keys=True),file=sys.stderr,flush=True)
    raise SystemExit(1) from None
