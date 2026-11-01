"""Bounded SCM_RIGHTS parser for trusted synthetic socketpair fixtures only."""
import array,contextlib,json,os,socket,struct,time

class BootstrapError(ValueError): pass


def read_bootstrap(control,*,timeout=3):
    deadline=time.monotonic()+timeout
    rights=[]
    def remaining():
        value=deadline-time.monotonic()
        if value<=0: raise TimeoutError('bootstrap deadline')
        control.settimeout(value)
    def read_exact(n):
        result=bytearray()
        while len(result)<n:
            remaining(); block=control.recv(n-len(result))
            if not block: raise EOFError('bootstrap closed')
            result.extend(block)
        return bytes(result)
    try:
        remaining()
        head,ancillary,flags,_=control.recvmsg(4,socket.CMSG_SPACE(2*array.array('i').itemsize),socket.MSG_CMSG_CLOEXEC)
        unexpected=False
        for level,kind,data in ancillary:
            if level==socket.SOL_SOCKET and kind==socket.SCM_RIGHTS:
                values=array.array('i')
                if len(data)%values.itemsize: unexpected=True
                values.frombytes(data[:len(data)-(len(data)%values.itemsize)])
                rights.extend(values.tolist())
            else: unexpected=True
        if (flags & socket.MSG_CTRUNC or unexpected or len(rights) not in (0,2)
                or len(ancillary)!=(1 if rights else 0)):
            raise BootstrapError('invalid ancillary data')
        if len(head)<4: head+=read_exact(4-len(head))
        length=struct.unpack('!I',head)[0]
        if not 0<length<=1024: raise BootstrapError('invalid frame length')
        payload=json.loads(read_exact(length))
        if (not isinstance(payload,dict) or set(payload)!={'bootstrap'}
                or not isinstance(payload['bootstrap'],str) or len(payload['bootstrap'])<32):
            raise BootstrapError('invalid bootstrap')
        return payload['bootstrap'],rights
    except BaseException:
        for fd in rights:
            try: os.close(fd)
            except OSError: pass
        raise


@contextlib.contextmanager
def bootstrap_descriptors(control,*,timeout=3):
    token,rights=read_bootstrap(control,timeout=timeout)
    try:
        yield token,rights
    finally:
        for fd in rights:
            try: os.close(fd)
            except OSError: pass
