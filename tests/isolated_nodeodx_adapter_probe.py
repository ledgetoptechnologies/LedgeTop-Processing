"""Run only inside an isolated, network-none NodeODX container; never commits."""
import hashlib,os,tempfile,time,urllib.request,uuid
from processing import reference_protocol as p
from processing.direct_output_slot import _identity,open_slot
from processing.direct_outbox_contract import direct_pipeline_digest
from processing.viewer_locked_upload import upload_locked
from processing.viewer_service import NativeEngine
from processing.viewer_strict_mutations import initialize

def main():
    url='http://127.0.0.1:3000'
    for _ in range(50):
        try:
            with urllib.request.urlopen(url+'/info',timeout=1) as response:response.read()
            break
        except Exception:time.sleep(.2)
    else:raise RuntimeError('synthetic engine unavailable')
    identifier=str(uuid.uuid4());plan={'options':[],'outputs':None};name='synthetic-adapter-probe'
    engine=NativeEngine(url)
    initialize(engine,identifier,plan,direct_pipeline_digest(plan['options'],plan['outputs']),name,p.digest(name))
    data=b'synthetic-not-a-photo';root=tempfile.mkdtemp(prefix='adapter-probe-',dir='/tmp');os.chmod(root,0o700)
    fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY)
    try:root_id=_identity(fd)
    finally:os.close(fd)
    slot=open_slot(root,root_id,'a'*64,len(data),max_budget=4096,create=True)
    try:
        slot.write_at(0,data);slot.sync()
        identity=dict(root=_identity(slot._root),allocation=_identity(slot._fd),metadata=_identity(slot._meta))
        item=dict(name='synthetic.jpg',bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
        upload_locked(engine,identifier,item,slot,identity)
    finally:slot.close()
    print('strict init and locked upload succeeded; no commit or processing',flush=True)
if __name__=='__main__':main()
