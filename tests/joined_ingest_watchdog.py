"""INCOMPLETE COPY, NOT A JOINED WATCHDOG. DO NOT EXECUTE.

Run only as the sole process in a disposable, retained, network-isolated
container with an init process. Never run against an existing service.
"""
import os
import signal
import subprocess
import sys
import time

child=subprocess.Popen([sys.executable,'-m','tests.joined_ingest_supervisor'],
                       start_new_session=True)
print('OWNED_FIXTURE_PID_AND_PGID',child.pid,flush=True)
try:
    try:code=child.wait(timeout=145)
    except subprocess.TimeoutExpired:
        # Exact process group created by start_new_session, and only while the
        # original direct child handle still reports it alive.
        if child.poll() is None:os.killpg(child.pid,signal.SIGKILL)
        child.wait(timeout=8)
        raise RuntimeError('owned fixture exceeded deadline') from None
    until=time.monotonic()+5
    while time.monotonic()<until:
        try:os.killpg(child.pid,0)
        except ProcessLookupError:break
        time.sleep(.1)
    else:
        print('OWNED_PROCESS_GROUP_UNCERTAIN',child.pid,flush=True)
        raise RuntimeError('owned process group remains')
    print('OWNED_PROCESS_GROUP_EXITED',child.pid,'code',code,flush=True)
    if code:raise RuntimeError('synthetic fixture failed')
    print('INNER_PROCESS_GROUP_CHECK_PASSED',flush=True)
finally:
    if child.poll() is None:
        os.killpg(child.pid,signal.SIGKILL)
        try:child.wait(timeout=8)
        except subprocess.TimeoutExpired:
            print('OWNED_FIXTURE_UNREAPED',child.pid,flush=True)
