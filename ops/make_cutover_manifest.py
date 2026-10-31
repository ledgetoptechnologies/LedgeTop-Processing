"""Create a bounded deterministic source manifest from an exact Git commit."""
import argparse,hashlib,io,json,os,re,subprocess,tarfile,threading
from pathlib import Path,PurePosixPath

def manifest_from_archive(raw):
    if len(raw)>67108864:raise ValueError('source archive too large')
    result={};total=0;members=0
    with tarfile.open(fileobj=io.BytesIO(raw),mode='r:') as archive:
        for member in archive:
            members+=1
            if members>8192:raise ValueError('source tree limit')
            name=member.name.rstrip('/');parts=PurePosixPath(name).parts
            if not parts or parts[0] not in ('ops','processing') or name.startswith('/') or str(PurePosixPath(name))!=name or any(p in ('.','..') for p in parts):raise ValueError('source path invalid')
            if member.isdir():continue
            if not member.isfile() or name in result or len(result)>=4096 or member.size>4194304:raise ValueError('source entry invalid')
            stream=archive.extractfile(member);data=stream.read(4194305)
            if len(data)!=member.size:raise ValueError('source entry truncated')
            total+=len(data)
            if total>67108864:raise ValueError('source bytes exceed limit')
            result[name]=hashlib.sha256(data).hexdigest()
    if not result:raise ValueError('empty source manifest')
    return json.dumps(result,sort_keys=True,separators=(',',':')).encode()

def manifest_for_commit(commit):
    if not re.fullmatch('[a-f0-9]{40}',commit):raise ValueError('exact commit required')
    process=subprocess.Popen(['git','archive','--format=tar',commit,'ops','processing'],cwd=Path(__file__).resolve().parents[1],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
    raw=bytearray();errors=[]
    def read():
        try:
            while True:
                chunk=process.stdout.read(65536)
                if not chunk:break
                raw.extend(chunk)
                if len(raw)>67108864:errors.append('archive too large');process.kill();break
        except Exception:errors.append('archive read failed')
    thread=threading.Thread(target=read,daemon=True)
    try:
        thread.start();process.wait(timeout=30);thread.join(2)
        if thread.is_alive() or errors or process.returncode!=0:raise RuntimeError('source archive unavailable')
    finally:
        if process.poll() is None:
            process.kill()
            try:process.wait(timeout=2)
            except subprocess.TimeoutExpired:pass
        if thread.ident is not None:thread.join(2)
        if not thread.is_alive():process.stdout.close()
    return manifest_from_archive(raw)

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('commit');parser.add_argument('output');args=parser.parse_args(argv)
    data=manifest_for_commit(args.commit)
    fd=os.open(args.output,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        if os.write(fd,data)!=len(data):raise OSError('short manifest write')
        os.fsync(fd)
    finally:os.close(fd)
    print(hashlib.sha256(data).hexdigest())

if __name__=='__main__':main()
