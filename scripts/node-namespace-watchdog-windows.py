"""REVIEW CANDIDATE ONLY: Windows Job Object namespace-test watchdog.

No Docker resource deletion. Child starts suspended, enters an owned Job before
resume, and Docker resources are reconciled only by exact per-run identities.
A failed/timeout run remains inconclusive even after reconciliation.
"""
import argparse
import ctypes
from ctypes import wintypes
import importlib.util
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
SPEC=importlib.util.spec_from_file_location('namespace_rehearsal',HERE/'node-namespace-rehearsal.py')
runner=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(runner)

class Accounting(ctypes.Structure):
    _fields_=[(n,ctypes.c_longlong) for n in ('user','kernel','period_user','period_kernel')]+[(n,wintypes.DWORD) for n in ('faults','total','active','terminated')]
class Basic(ctypes.Structure):
    _fields_=[('a',ctypes.c_longlong),('b',ctypes.c_longlong),('flags',wintypes.DWORD),('c',ctypes.c_size_t),('d',ctypes.c_size_t),('e',wintypes.DWORD),('f',ctypes.c_size_t),('g',wintypes.DWORD),('h',wintypes.DWORD)]
class Io(ctypes.Structure):_fields_=[(f'v{i}',ctypes.c_ulonglong) for i in range(6)]
class Extended(ctypes.Structure):
    _fields_=[('basic',Basic),('io',Io),('a',ctypes.c_size_t),('b',ctypes.c_size_t),('c',ctypes.c_size_t),('d',ctypes.c_size_t)]


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--image-id',required=True);args=parser.parse_args()
    if os.name!='nt':parser.error('Windows required')
    if not runner.IMAGE_ID.fullmatch(args.image_id):parser.error('full immutable image ID required')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True);native=ctypes.WinDLL('ntdll',use_last_error=True)
    kernel.CreateJobObjectW.argtypes=(ctypes.c_void_p,wintypes.LPCWSTR);kernel.CreateJobObjectW.restype=wintypes.HANDLE
    kernel.SetInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD);kernel.SetInformationJobObject.restype=wintypes.BOOL
    kernel.AssignProcessToJobObject.argtypes=(wintypes.HANDLE,wintypes.HANDLE);kernel.AssignProcessToJobObject.restype=wintypes.BOOL
    kernel.TerminateJobObject.argtypes=(wintypes.HANDLE,wintypes.UINT);kernel.TerminateJobObject.restype=wintypes.BOOL
    kernel.QueryInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD,ctypes.c_void_p);kernel.QueryInformationJobObject.restype=wintypes.BOOL
    kernel.CloseHandle.argtypes=(wintypes.HANDLE,);kernel.CloseHandle.restype=wintypes.BOOL
    native.NtResumeProcess.argtypes=(wintypes.HANDLE,);native.NtResumeProcess.restype=wintypes.LONG
    run=secrets.token_hex(12);prefix='lt-ns-'+run
    print('Windows owned rehearsal run:',run,flush=True)
    job=kernel.CreateJobObjectW(None,None)
    if not job:raise ctypes.WinError(ctypes.get_last_error())
    child=None;assigned=False;failure=None;output=b'';active=None
    try:
        limits=Extended();limits.basic.flags=0x2000 # KILL_ON_JOB_CLOSE
        if not kernel.SetInformationJobObject(job,9,ctypes.byref(limits),ctypes.sizeof(limits)):
            raise ctypes.WinError(ctypes.get_last_error())
        child=subprocess.Popen([sys.executable,str(HERE/'node-namespace-rehearsal.py'),
                                '--run-id',run,'--image-id',args.image_id],
                               creationflags=0x00000004|subprocess.CREATE_NO_WINDOW,
                               stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        if not kernel.AssignProcessToJobObject(job,int(child._handle)):
            raise ctypes.WinError(ctypes.get_last_error())
        assigned=True
        if native.NtResumeProcess(int(child._handle))!=0:
            raise RuntimeError('owned runner could not resume')
        try:output,_=child.communicate(timeout=90)
        except subprocess.TimeoutExpired:
            failure='runner timeout; Docker create may finish late'
            if not kernel.TerminateJobObject(job,1):failure='owned Job termination failed'
            try:output,_=child.communicate(timeout=8)
            except subprocess.TimeoutExpired:failure='owned runner exit unconfirmed'
        if child.returncode!=0:failure=failure or 'runner exited unsuccessfully'
    except BaseException as error:
        failure=failure or ('watchdog setup failed: '+type(error).__name__)
    finally:
        if child is not None and child.poll() is None:
            try:
                if assigned:
                    if not kernel.TerminateJobObject(job,1):failure='owned Job termination failed'
                else:
                    child.kill() # exact owned suspended Popen, not an arbitrary process
                child.communicate(timeout=8)
            except Exception:failure='owned runner reaping uncertain'
        if assigned:
            accounting=Accounting()
            if not kernel.QueryInformationJobObject(job,1,ctypes.byref(accounting),ctypes.sizeof(accounting),None):
                failure='owned Job accounting unavailable'
            else:
                if accounting.active:
                    failure=failure or 'owned Job had remaining processes'
                    if not kernel.TerminateJobObject(job,1):failure='owned Job termination failed'
                    until=time.monotonic()+8
                    while time.monotonic()<until:
                        if not kernel.QueryInformationJobObject(job,1,ctypes.byref(accounting),ctypes.sizeof(accounting),None):
                            failure='owned Job accounting unavailable';break
                        if not accounting.active:break
                        time.sleep(.2)
                active=accounting.active
                if active:failure='owned Job processes remain'
        if not kernel.CloseHandle(job):failure='owned Job handle close failed'
    if active is not None:print('owned Job active processes:',active,flush=True)
    # This reconciliation is intentionally exact-name/full-ID/label based.
    # A create may finish after reconciliation; a failure is never called clean.
    for suffix in ('bridge-probe','none-probe','engine','broker'):
        name=prefix+'-'+suffix
        try:
            obj,observed,label,_,_,_=runner.inspect_container(name)
            if not runner.FULL_ID.fullmatch(obj) or observed!='/'+name or label!=run:
                raise RuntimeError('container identity differs')
            state=runner.verified_container(obj,name,run)
            if state=='running':runner.docker('stop','--time','2',obj,timeout=8)
            if runner.verified_container(obj,name,run) not in ('created','exited'):
                raise RuntimeError('container stop unconfirmed')
            print('verified stopped and retained:',obj,flush=True)
        except Exception as error:
            failure=failure or 'container identity or state unconfirmed'
            print('uncertain container:',name,type(error).__name__,flush=True)
    try:
        net=runner.network_inspect(prefix+'-bridge')
        if not runner.FULL_ID.fullmatch(net[0]) or net[1]!=prefix+'-bridge' or net[2]!=run or net[3] is not True:
            raise RuntimeError('network identity differs')
        print('verified retained network:',net[0],flush=True)
    except Exception as error:
        failure=failure or 'network identity unconfirmed'
        print('uncertain network:',prefix+'-bridge',type(error).__name__,flush=True)
    if output:
        # Emit the inner pass only after Job and resource checks both succeed.
        if failure:output=output.replace(b'PASS:',b'UNVERIFIED:')
        sys.stdout.buffer.write(output);sys.stdout.flush()
    if failure:
        print('INCONCLUSIVE:',failure,'; late daemon creates or untracked processes may remain',file=sys.stderr,flush=True)
        return 1
    print('Windows watchdog: owned Job exited and resources verified stopped; retained for review.',flush=True)
    return 0


if __name__=='__main__':raise SystemExit(main())
