"""Windows Job Object watchdog for one owned synthetic Node child.

No external process enumeration, names or PID-based termination. The child is
created suspended and assigned before execution; only this unique job and the
unassigned suspended child are ever terminated. Windows-only fixture helper.
"""
import ctypes
from ctypes import wintypes
import subprocess
import time

class Accounting(ctypes.Structure):
    _fields_=[(name,ctypes.c_longlong) for name in ('user','kernel','period_user','period_kernel')]+[(name,wintypes.DWORD) for name in ('faults','total','active','terminated')]
class Basic(ctypes.Structure):
    _fields_=[('a',ctypes.c_longlong),('b',ctypes.c_longlong),('flags',wintypes.DWORD),('c',ctypes.c_size_t),('d',ctypes.c_size_t),('e',wintypes.DWORD),('f',ctypes.c_size_t),('g',wintypes.DWORD),('h',wintypes.DWORD)]
class Io(ctypes.Structure): _fields_=[(f'v{i}',ctypes.c_ulonglong) for i in range(6)]
class Extended(ctypes.Structure):
    _fields_=[('basic',Basic),('io',Io),('a',ctypes.c_size_t),('b',ctypes.c_size_t),('c',ctypes.c_size_t),('d',ctypes.c_size_t)]

def run_owned(argv, *, timeout=20):
    k=ctypes.WinDLL('kernel32',use_last_error=True);n=ctypes.WinDLL('ntdll',use_last_error=True)
    k.CreateJobObjectW.argtypes=(ctypes.c_void_p,wintypes.LPCWSTR);k.CreateJobObjectW.restype=wintypes.HANDLE
    k.SetInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD);k.SetInformationJobObject.restype=wintypes.BOOL
    k.AssignProcessToJobObject.argtypes=(wintypes.HANDLE,wintypes.HANDLE);k.AssignProcessToJobObject.restype=wintypes.BOOL
    k.TerminateJobObject.argtypes=(wintypes.HANDLE,wintypes.UINT);k.TerminateJobObject.restype=wintypes.BOOL
    k.CloseHandle.argtypes=(wintypes.HANDLE,);k.CloseHandle.restype=wintypes.BOOL
    k.QueryInformationJobObject.argtypes=(wintypes.HANDLE,ctypes.c_int,ctypes.c_void_p,wintypes.DWORD,ctypes.c_void_p);k.QueryInformationJobObject.restype=wintypes.BOOL
    n.NtResumeProcess.argtypes=(wintypes.HANDLE,);n.NtResumeProcess.restype=wintypes.LONG
    job=k.CreateJobObjectW(None,None)
    if not job:raise ctypes.WinError(ctypes.get_last_error())
    child=None;assigned=False;failure=None;output=b'';exitcode=None
    try:
        limits=Extended();limits.basic.flags=0x2000
        if not k.SetInformationJobObject(job,9,ctypes.byref(limits),ctypes.sizeof(limits)):raise ctypes.WinError(ctypes.get_last_error())
        child=subprocess.Popen(argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,creationflags=4)
        if not k.AssignProcessToJobObject(job,int(child._handle)):raise ctypes.WinError(ctypes.get_last_error())
        assigned=True
        if n.NtResumeProcess(int(child._handle))!=0:raise RuntimeError('owned child resume failed')
        try:output,_=child.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            if not k.TerminateJobObject(job,1):raise RuntimeError('owned job termination failed')
            output,_=child.communicate(timeout=5);failure='owned child deadline exceeded'
        exitcode=child.returncode
    finally:
        if child is not None and child.poll() is None:
            try:
                if assigned:
                    if not k.TerminateJobObject(job,1):failure='owned job termination failed'
                else:child.kill()  # exact owned suspended Popen child only
                child.communicate(timeout=5)
            except Exception:failure='owned child exit/reap uncertain'
        info=Accounting()
        if not k.QueryInformationJobObject(job,1,ctypes.byref(info),ctypes.sizeof(info),None):
            failure='owned job status unavailable'
        elif info.active:
            if not k.TerminateJobObject(job,1):failure='owned job termination failed'
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                if not k.QueryInformationJobObject(job,1,ctypes.byref(info),ctypes.sizeof(info),None):failure='owned job status unavailable';break
                if not info.active:break
                time.sleep(.05)
            if info.active:failure='owned job descendants remain'
        if not k.CloseHandle(job):failure='owned job close failed'
    if failure:raise RuntimeError(failure+'; owned output: '+output.decode('utf8','replace')[-4000:])
    return exitcode,output
