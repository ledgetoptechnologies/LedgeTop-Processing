"""UNREVIEWED Windows watchdog for owned joined rehearsal. DO NOT EXECUTE."""
import ctypes
from ctypes import wintypes
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid

run_id=uuid.uuid4().hex
root=Path(tempfile.mkdtemp(prefix='ledgetop-acceptance-outer-'))
print('RETAINED_ROOT',root,'REHEARSAL',run_id,flush=True)
label='ledgetop.synthetic.rehearsal'
names=['synthetic-joined-python-'+run_id,'synthetic-joined-viewer-'+run_id]
volume_name='synthetic-joined-models-'+run_id
env=os.environ.copy();env.update(LT_SYNTHETIC_RUN_ID=run_id,LT_SYNTHETIC_ROOT=str(root))
k=ctypes.WinDLL('kernel32',use_last_error=True); n=ctypes.WinDLL('ntdll',use_last_error=True)
k.CreateJobObjectW.argtypes=(ctypes.c_void_p,wintypes.LPCWSTR);k.CreateJobObjectW.restype=wintypes.HANDLE
k.SetInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD);k.SetInformationJobObject.restype=wintypes.BOOL
k.AssignProcessToJobObject.argtypes=(wintypes.HANDLE,wintypes.HANDLE);k.AssignProcessToJobObject.restype=wintypes.BOOL
k.TerminateJobObject.argtypes=(wintypes.HANDLE,wintypes.UINT);k.TerminateJobObject.restype=wintypes.BOOL
k.CloseHandle.argtypes=(wintypes.HANDLE,);k.CloseHandle.restype=wintypes.BOOL
k.QueryInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD,ctypes.c_void_p);k.QueryInformationJobObject.restype=wintypes.BOOL
n.NtResumeProcess.argtypes=(wintypes.HANDLE,);n.NtResumeProcess.restype=wintypes.LONG
class Accounting(ctypes.Structure):
    _fields_=[(name,ctypes.c_longlong) for name in ('user','kernel','period_user','period_kernel')]+[(name,wintypes.DWORD) for name in ('faults','total','active','terminated')]
class Basic(ctypes.Structure):
    _fields_=[('a',ctypes.c_longlong),('b',ctypes.c_longlong),('flags',wintypes.DWORD),('c',ctypes.c_size_t),('d',ctypes.c_size_t),('e',wintypes.DWORD),('f',ctypes.c_size_t),('g',wintypes.DWORD),('h',wintypes.DWORD)]
class Io(ctypes.Structure): _fields_=[(f'v{i}',ctypes.c_ulonglong) for i in range(6)]
class Extended(ctypes.Structure):
    _fields_=[('basic',Basic),('io',Io),('a',ctypes.c_size_t),('b',ctypes.c_size_t),('c',ctypes.c_size_t),('d',ctypes.c_size_t)]

def docker(*args):
    return subprocess.run(['docker',*args],capture_output=True,timeout=8,check=True).stdout.decode().strip()

def inspect_owned(name):
    value=json.loads(docker('inspect',name))[0]
    if (value['Name'].lstrip('/')!=name or value['Config']['Labels'].get(label)!=run_id
        or len(value['Id'])!=64): raise RuntimeError('owned identity mismatch')
    return value

def reconcile(name):
    # Only exact unique name plus exact run label authorizes action.
    value=inspect_owned(name); ident=value['Id']
    if value['State']['Running']:
        docker('kill','--signal','KILL',ident)
        until=time.monotonic()+8
        while time.monotonic()<until:
            value=inspect_owned(name)
            if value['Id']!=ident:raise RuntimeError('owned identity changed')
            if not value['State']['Running']:break
            time.sleep(.2)
    if value['State']['Running']:raise RuntimeError('owned container still running')
    print('OUTER_OWNED_EXIT',name,ident,value['State']['ExitCode'],flush=True)

job=k.CreateJobObjectW(None,None)
if not job:raise ctypes.WinError(ctypes.get_last_error())
child=None;assigned=False;failure=None
try:
    limits=Extended();limits.basic.flags=0x2000
    if not k.SetInformationJobObject(job,9,ctypes.byref(limits),ctypes.sizeof(limits)):raise ctypes.WinError(ctypes.get_last_error())
    child=subprocess.Popen([sys.executable,str(Path(__file__).with_name('joined_worker_viewer_runner.py'))],
        env=env,creationflags=4,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
    if not k.AssignProcessToJobObject(job,int(child._handle)):raise ctypes.WinError(ctypes.get_last_error())
    assigned=True
    if n.NtResumeProcess(int(child._handle))!=0:raise RuntimeError('cannot resume owned runner')
    try: output,_=child.communicate(timeout=260)
    except subprocess.TimeoutExpired:
        if not k.TerminateJobObject(job,1):raise RuntimeError('owned runner job termination failed')
        output,_=child.communicate(timeout=8)
        failure='runner exceeded outer deadline'
    sys.stdout.buffer.write(output);sys.stdout.flush()
    if child.returncode!=0:failure=failure or 'runner exited unsuccessfully'
finally:
    if child is not None and child.poll() is None:
        try:
            if assigned and not k.TerminateJobObject(job,1):failure='owned runner job termination failed'
            if child.poll() is None:child.kill()
            child.communicate(timeout=8)
        except Exception:failure='owned runner reaping uncertain'
    accounting=Accounting()
    if not k.QueryInformationJobObject(job,1,ctypes.byref(accounting),ctypes.sizeof(accounting),None):
        failure='owned job status unavailable'
    elif accounting.active:
        if not k.TerminateJobObject(job,1):failure='owned job termination failed'
        until=time.monotonic()+8
        while time.monotonic()<until:
            if not k.QueryInformationJobObject(job,1,ctypes.byref(accounting),ctypes.sizeof(accounting),None):
                failure='owned job status unavailable';break
            if not accounting.active:break
            time.sleep(.2)
        if accounting.active:failure='owned job still active'
    print('OUTER_JOB_ACTIVE',accounting.active,flush=True)
    if not k.CloseHandle(job):failure='owned job close failed'
    for name in reversed(names):
        try: reconcile(name)
        except subprocess.CalledProcessError:
            # Inspect failure can mean absent or unavailable: do not claim absence.
            print('OUTER_UNCERTAIN_NAME',name,flush=True);failure=failure or 'container state unavailable'
        except Exception as error:
            print('OUTER_UNCERTAIN_NAME',name,type(error).__name__,flush=True);failure=failure or 'container state unavailable'
    try:
        volume=json.loads(docker('volume','inspect',volume_name))[0]
        if (volume['Name']!=volume_name or (volume.get('Labels') or {}).get(label)!=run_id
                or volume.get('Driver')!='local'):raise RuntimeError('volume identity mismatch')
        # Matching identity does not settle an ambiguous create or establish
        # exclusive creation if another actor raced the initial absence check.
        print('OUTER_MATCHING_VOLUME_RETAINED',volume_name,flush=True)
    except Exception as error:
        print('OUTER_UNCERTAIN_VOLUME',volume_name,type(error).__name__,flush=True)
        failure=failure or 'volume identity unavailable'
    if failure:raise RuntimeError(failure)
print('OUTER_CONTAINER_AND_JOB_EXIT_VERIFIED',flush=True)
