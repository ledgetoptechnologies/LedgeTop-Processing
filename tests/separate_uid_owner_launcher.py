"""Trusted harness child: unshare own user namespace, await owner map, exec."""
import ctypes,os,sys
ready,go=map(int,sys.argv[1:3]); command=sys.argv[3:]
libc=ctypes.CDLL(None,use_errno=True)
if libc.unshare(0x10000000)!=0: raise OSError(ctypes.get_errno(),'unshare user failed')
os.write(ready,b'R'); os.close(ready)
if os.read(go,1)!=b'G': raise SystemExit('mapping not granted')
os.close(go)
os.execvp(command[0],command)
