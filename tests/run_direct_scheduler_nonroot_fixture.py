"""Root-supervised, disposable scheduler fixture; never touches installed services.

Run in a task-owned Linux container as root with /tmp mounted separately.
The child runs as 10001; only this test supervisor creates its root enrollment.
"""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile


def main():
    if os.geteuid()!=0:raise RuntimeError('root test supervisor required')
    with tempfile.TemporaryDirectory(dir='/run',prefix='scheduler-test-') as directory:
        root=Path(directory);root.chmod(0o755)
        service=root/'service';service.mkdir(mode=0o700);os.chown(service,10001,10001)
        env=dict(os.environ,LTDS_SECURE_TEST_ROOT=str(service),LTDS_TEST_ROOT_ENROLLMENT_HANDOFF='1')
        def drop_privileges():os.setgroups([]);os.setgid(10001);os.setuid(10001)
        tests={'scheduler':'tests.test_direct_preparation_scheduler.SchedulerTest.test_real_worker_preparation_through_enrolled_mutual_tls',
               'transfer':'tests.test_direct_enrolled_sender.EnrolledSenderTests.test_pinned_sender_grants_and_streams_to_actual_mtls_boundary'}
        mode=sys.argv[1] if len(sys.argv)==2 else 'scheduler'
        if mode not in tests:raise ValueError('unknown test fixture')
        child=subprocess.Popen([sys.executable,'-m','unittest',tests[mode],'-v'],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env,preexec_fn=drop_privileges)
        try:
            if not select.select([child.stdout],[],[],20)[0]:raise RuntimeError('fixture enrollment handshake timed out')
            line=child.stdout.readline()
            if not line.startswith('TEST_ENROLLMENT ') or len(line)>1000000:raise RuntimeError('fixture enrollment handshake failed')
            value=json.loads(line[len('TEST_ENROLLMENT '):])
            target=root/'scheduler-enrollment.json';target.write_text(json.dumps(value));target.chmod(0o644)
            _,errors=child.communicate(str(target)+'\n',timeout=30)
            sys.stderr.write(errors)
            if child.returncode:raise RuntimeError('unprivileged scheduler fixture failed')
        finally:
            if child.poll() is None:
                child.kill();child.communicate(timeout=5)


if __name__=='__main__':main()
