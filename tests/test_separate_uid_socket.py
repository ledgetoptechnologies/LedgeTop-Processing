import json,os,signal,subprocess,sys,time,unittest
from pathlib import Path

class Tests(unittest.TestCase):
    def test_distinct_uid_cannot_connect_owner_socket(self):
        helper=Path(__file__).with_name('separate_uid_socket_fixture.py')
        child=subprocess.Popen([sys.executable,str(helper)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                               text=True,start_new_session=True)
        try:
            deadline=time.monotonic()+12
            while os.waitid(os.P_PID,child.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT) is None:
                if time.monotonic()>=deadline: raise TimeoutError('fixture timeout')
                time.sleep(.02)
            try: os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            out,err=child.communicate(timeout=3)
            self.assertEqual(child.returncode,0,err)
            self.assertEqual(json.loads(out),{'separateUid':True,'ownerSocketDenied':True,'ownerLaunchDenied':True})
        finally:
            if child.returncode is None:
                try: os.killpg(child.pid,signal.SIGKILL)
                except ProcessLookupError: pass
                child.communicate(timeout=3)

if __name__=='__main__': unittest.main()
