"""Windows-owned outer watchdog; retained resources, no deletion."""
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import uuid

from tests.paired_owned_windows import run_owned

IMAGE='sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668'
LABEL='ledgetop.synthetic.partial-drop'

def docker(*args,timeout=12):
    code,out=run_owned(['docker',*args],timeout=timeout)
    if code:raise RuntimeError('docker inspection unavailable')
    return out.decode('utf8').strip()

def main():
    if os.name!='nt':raise RuntimeError('Windows Job Object required')
    root=Path(tempfile.mkdtemp(prefix='lt-partial-drop-outer-')).resolve(strict=True)
    run_id=uuid.uuid4().hex;name='lt-partial-drop-'+run_id;volume='lt-partial-volume-'+run_id
    print('RETAINED_OUTER_ROOT',root,'RUN_ID',run_id,flush=True)
    os.environ.update(LT_PARTIAL_RUN_ID=run_id,LT_PARTIAL_ROOT=str(root))
    failure=None;output=b''
    try:
        code,output=run_owned([sys.executable,'-m','tests.paired_partial_drop_launcher'],timeout=160)
        sys.stdout.buffer.write(output);sys.stdout.flush()
        if code:failure='owned launcher failed'
    except Exception as error:
        failure='owned launcher outcome uncertain: '+type(error).__name__
        print(failure,flush=True)
    finally:
        # Only a valid cidfile and independently inspected exact identity permit
        # stopping. If any ownership check is unavailable, retain and report.
        volume_verified=False
        try:
            info=json.loads(docker('volume','inspect',volume))[0]
            if info['Name']!=volume or info.get('Driver')!='local' or (info.get('Labels') or {}).get(LABEL)!=run_id:raise ValueError('volume identity mismatch')
            volume_verified=True
            print('RETAINED_VOLUME',volume,flush=True)
        except Exception as error:
            print('VOLUME_OWNERSHIP_UNCERTAIN',volume,type(error).__name__,flush=True)
            failure=failure or 'volume ownership uncertain'
        cidfile=root/'container.cid'
        try:
            cid=cidfile.read_text(encoding='ascii').strip()
            if not re.fullmatch(r'[a-f0-9]{64}',cid):raise ValueError('invalid cidfile')
            info=json.loads(docker('inspect','--format','{{json .}}',cid))
            if (not volume_verified or info['Id']!=cid or info['Name']!='/'+name or info['Image']!=IMAGE or
                (info['Config'].get('Labels') or {}).get(LABEL)!=run_id or
                not any(m.get('Type')=='volume' and m.get('Name')==volume and m.get('Destination')=='/owned' for m in info.get('Mounts',[]))):
                raise ValueError('container ownership mismatch')
            if info['State']['Running']:
                docker('stop','--time','3',cid,timeout=15)
                info=json.loads(docker('inspect','--format','{{json .}}',cid))
            if info['Id']!=cid or info['Name']!='/'+name or info['State']['Running']:raise RuntimeError('owned container exit uncertain')
            print('RETAINED_CONTAINER',name,cid,'EXIT',info['State']['ExitCode'],flush=True)
        except Exception as error:
            print('CONTAINER_OWNERSHIP_OR_EXIT_UNCERTAIN',name,type(error).__name__,flush=True)
            failure=failure or 'container ownership or exit uncertain'
    if failure:raise RuntimeError(failure)

if __name__=='__main__':main()
