"""Owned outer Windows Job Object for the reviewed synthetic tests only."""
import ctypes
import os
from ctypes import wintypes
import subprocess
import sys
import time

k = ctypes.WinDLL('kernel32', use_last_error=True)
n = ctypes.WinDLL('ntdll', use_last_error=True)
k.CreateJobObjectW.argtypes = (ctypes.c_void_p, wintypes.LPCWSTR)
k.CreateJobObjectW.restype = wintypes.HANDLE
k.AssignProcessToJobObject.argtypes = (wintypes.HANDLE, wintypes.HANDLE)
k.AssignProcessToJobObject.restype = wintypes.BOOL
k.TerminateJobObject.argtypes = (wintypes.HANDLE, wintypes.UINT)
k.TerminateJobObject.restype = wintypes.BOOL
k.CloseHandle.argtypes = (wintypes.HANDLE,)
k.CloseHandle.restype = wintypes.BOOL
k.SetInformationJobObject.argtypes = (wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD)
k.SetInformationJobObject.restype = wintypes.BOOL
n.NtResumeProcess.argtypes = (wintypes.HANDLE,)
n.NtResumeProcess.restype = wintypes.LONG

class Accounting(ctypes.Structure):
    _fields_ = [(name, ctypes.c_longlong) for name in ('TotalUserTime','TotalKernelTime','ThisPeriodTotalUserTime','ThisPeriodTotalKernelTime')] + [(name, wintypes.DWORD) for name in ('TotalPageFaultCount','TotalProcesses','ActiveProcesses','TotalTerminatedProcesses')]

k.QueryInformationJobObject.argtypes = (wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p)
k.QueryInformationJobObject.restype = wintypes.BOOL
class BasicLimits(ctypes.Structure):
    _fields_ = [('PerProcessUserTimeLimit', ctypes.c_longlong), ('PerJobUserTimeLimit', ctypes.c_longlong),
                ('LimitFlags', wintypes.DWORD), ('MinimumWorkingSetSize', ctypes.c_size_t),
                ('MaximumWorkingSetSize', ctypes.c_size_t), ('ActiveProcessLimit', wintypes.DWORD),
                ('Affinity', ctypes.c_size_t), ('PriorityClass', wintypes.DWORD),
                ('SchedulingClass', wintypes.DWORD)]

class IoCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in ('ReadOperationCount','WriteOperationCount',
                 'OtherOperationCount','ReadTransferCount','WriteTransferCount','OtherTransferCount')]

class ExtendedLimits(ctypes.Structure):
    _fields_ = [('BasicLimitInformation', BasicLimits), ('IoInfo', IoCounters),
                ('ProcessMemoryLimit', ctypes.c_size_t), ('JobMemoryLimit', ctypes.c_size_t),
                ('PeakProcessMemoryUsed', ctypes.c_size_t), ('PeakJobMemoryUsed', ctypes.c_size_t)]

job = k.CreateJobObjectW(None, None)
if not job: raise ctypes.WinError(ctypes.get_last_error())
child = None
assigned = False
# Kept strongly if the OS cannot confirm termination/reaping.
UNREAPED = []
try:
    limits = ExtendedLimits()
    limits.BasicLimitInformation.LimitFlags = 0x00002000 # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not k.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
        raise ctypes.WinError(ctypes.get_last_error())
    test_env = os.environ.copy()
    test_env['PATH'] = r'C:\Program Files\Git\usr\bin' + os.pathsep + test_env.get('PATH', '')
    child = subprocess.Popen([sys.executable, '-m', 'unittest', '-v',
                              'tests.test_control_subprocess_latch',
                              'tests.test_control_subprocess_deadline'],
                             creationflags=0x00000004, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT, env=test_env)
    if not k.AssignProcessToJobObject(job, int(child._handle)):
        raise ctypes.WinError(ctypes.get_last_error())
    assigned = True
    if n.NtResumeProcess(int(child._handle)) != 0:
        k.TerminateJobObject(job, 1); child.wait(timeout=5)
        raise RuntimeError('cannot resume owned test process')
    try:
        output, _ = child.communicate(timeout=45)
    except subprocess.TimeoutExpired:
        k.TerminateJobObject(job, 1)
        child.communicate(timeout=5)
        raise RuntimeError('owned test job timed out') from None
    sys.stdout.buffer.write(output)
    accounting = Accounting()
    if not k.QueryInformationJobObject(job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None):
        raise ctypes.WinError(ctypes.get_last_error())
    print('outer_test_pid=', child.pid, 'job_active_processes=', accounting.ActiveProcesses, flush=True)
    if accounting.ActiveProcesses:
        k.TerminateJobObject(job, 1)
        raise RuntimeError('owned test job left active children')
    sys.exit(child.returncode)
finally:
    # Only the handle created above and its assigned Job may be terminated.
    # Close the job last; KILL_ON_JOB_CLOSE is a final containment backstop.
    failure = None
    if child is not None and child.poll() is None:
        if assigned and not k.TerminateJobObject(job, 1):
            failure = 'owned job termination failed'
        try:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)
        except BaseException:
            UNREAPED.append(child)
            failure = 'owned test process reaping uncertain'
    if assigned:
        accounting = Accounting()
        if not k.QueryInformationJobObject(job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None):
            failure = 'owned job status unavailable'
        elif accounting.ActiveProcesses:
            if not k.TerminateJobObject(job, 1):
                failure = 'owned job termination failed'
            until = time.monotonic() + 5
            while time.monotonic() < until:
                if not k.QueryInformationJobObject(job, 1, ctypes.byref(accounting), ctypes.sizeof(accounting), None):
                    failure = 'owned job status unavailable'
                    break
                if not accounting.ActiveProcesses: break
                time.sleep(.05)
            if accounting.ActiveProcesses:
                failure = 'owned job still has active processes'
    if not k.CloseHandle(job):
        failure = 'owned job close failed'
    if failure:
        print(failure, 'pid=', child.pid if child is not None else None, file=sys.stderr, flush=True)
        raise RuntimeError(failure)
