"""REVIEW CANDIDATE ONLY: owned POSIX watchdog for namespace rehearsal.

Starts one child in a fresh process group. On timeout it signals only that
owned group, then reports/attempts to stop only exact-name and label verified
containers. It does not delete resources and cannot rule out a Docker daemon
create completing after its final reconciliation. Review before execution.
"""
import argparse
import importlib.util
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location('namespace_rehearsal', HERE / 'node-namespace-rehearsal.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--image-id', required=True)
    args = parser.parse_args()
    if os.name != 'posix':
        parser.error('watchdog requires POSIX process groups')
    if not runner.IMAGE_ID.fullmatch(args.image_id):
        parser.error('an immutable full sha256 image ID is required')
    run = secrets.token_hex(12)
    print('watchdog run:', run, flush=True)
    child = subprocess.Popen([sys.executable, str(HERE / 'node-namespace-rehearsal.py'),
                              '--run-id', run, '--image-id', args.image_id], start_new_session=True)
    try:
        return child.wait(timeout=90)
    except subprocess.TimeoutExpired:
        print('INCONCLUSIVE: owned child exceeded watchdog deadline', file=sys.stderr, flush=True)
        # Signal only the process group created by this Popen while its leader
        # is still alive. No unrelated process or container is targeted.
        if child.poll() is None:
            try:os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:pass
        try:child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if child.poll() is None:
                try:os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:pass
            try:child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print('owned child exit unconfirmed', file=sys.stderr, flush=True)
        # A reaped leader is not evidence that its descendants exited. Never
        # signal or inspect a numeric group ID after the leader has exited: it
        # may have been reused, and descendants could have changed session.
        print('owned child descendants are unverified; no further group signals',
              file=sys.stderr, flush=True)
        prefix='lt-ns-'+run
        for suffix in ('bridge-probe','none-probe','engine','broker'):
            name=prefix+'-'+suffix
            try:
                obj,observed_name,label,_,_,_=runner.inspect_container(name)
                if not runner.FULL_ID.fullmatch(obj) or observed_name!='/'+name or label!=run:
                    raise RuntimeError('identity differs')
                state=runner.verified_container(obj,name,run)
                if state=='running':runner.docker('stop','--time','2',obj,timeout=8)
                if runner.verified_container(obj,name,run) not in ('created','exited'):
                    raise RuntimeError('stop unconfirmed')
                print('watchdog verified stopped and retained:',obj,flush=True)
            except Exception as error:
                print('watchdog resource absent or uncertain:',name,type(error).__name__,file=sys.stderr,flush=True)
        try:
            net=runner.network_inspect(prefix+'-bridge')
            if (not runner.FULL_ID.fullmatch(net[0]) or net[1]!=prefix+'-bridge' or net[2]!=run or net[3] is not True):
                raise RuntimeError('network identity differs')
            print('watchdog retained network:',net[0],flush=True)
        except Exception as error:
            print('watchdog network absent or uncertain:',type(error).__name__,file=sys.stderr,flush=True)
        print('INCONCLUSIVE: Docker create may complete after reconciliation, and owned descendants may survive or escape the group; inspect exact names and process ownership.',file=sys.stderr,flush=True)
        return 1


if __name__=='__main__':
    raise SystemExit(main())
