"""Fixed literal-IP, nonblocking, bounded JSON reads; no authority or retry."""
import errno,ipaddress,re,socket,select,ssl,time
from urllib.parse import urlsplit
from .reference_protocol import decode_submission

def read_json(url, headers, *, context, timeout, max_bytes=16384):
    """Literal-IP nonblocking TCP/TLS/HTTP under ONE absolute deadline.

    Like the existing gate bridge's fixed local HTTP parser, bounds cover
    headers as well as body; verified TLS and fixed IP remove DNS ambiguity.
    No timeout thread, redirects, proxies, decompression or chunked replay.
    """
    if type(max_bytes) is not int or not 1<=max_bytes<=262144:raise ValueError("bounded JSON limit required")
    connection=None;connected=False;deadline=time.monotonic()+timeout
    try:
        parsed=urlsplit(url);address=ipaddress.ip_address(parsed.hostname)
        connection=socket.socket(socket.AF_INET6 if address.version==6 else socket.AF_INET,socket.SOCK_STREAM)
        connection.setblocking(False)
        def wait(read):
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError()
            readable,writable,_=select.select([connection] if read else [],[] if read else [connection],[],remaining)
            if not readable and not writable or time.monotonic()>=deadline:raise TimeoutError()
        port=parsed.port or (443 if parsed.scheme=='https' else 80)
        result=connection.connect_ex((str(address),port))
        if result not in (0,errno.EISCONN):
            if result not in (errno.EINPROGRESS,errno.EWOULDBLOCK,errno.EALREADY):raise OSError(result,'connect refused')
            wait(False);result=connection.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
            if result:raise OSError(result,'connect refused')
        connected=True
        def io(operation,read):
            while True:
                if time.monotonic()>=deadline:raise TimeoutError()
                try:return operation()
                except ssl.SSLWantReadError:wait(True)
                except ssl.SSLWantWriteError:wait(False)
                except BlockingIOError:wait(read)
        if parsed.scheme=='https':
            connection=context.wrap_socket(connection,server_hostname=str(address),do_handshake_on_connect=False)
            connection.setblocking(False);io(connection.do_handshake,True)
        host='['+str(address)+']' if address.version==6 else str(address)
        route=parsed.path or '/'
        if parsed.query:route+='?'+parsed.query
        lines=['GET '+route+' HTTP/1.1','Host: '+host+':'+str(port),'Connection: close']
        lines.extend(key+': '+value for key,value in headers.items())
        request=('\r\n'.join(lines)+'\r\n\r\n').encode('ascii')
        while request:
            sent=io(lambda:connection.send(request),False)
            if sent<=0:raise ValueError()
            request=request[sent:]
        data=bytearray()
        while b'\r\n\r\n' not in data:
            chunk=io(lambda:connection.recv(1024),True)
            if not chunk:raise ValueError()
            data.extend(chunk)
            if len(data)>8192+max_bytes or b'\r\n\r\n' not in data and len(data)>8192:raise ValueError()
        head,body=bytes(data).split(b'\r\n\r\n',1)
        if len(head)>8192:raise ValueError()
        rows=head.decode('ascii').split('\r\n')
        if re.fullmatch(r'HTTP/1\.[01] [1-5][0-9]{2}(?: [^\r\n]*)?',rows[0]) is None:raise ValueError()
        code=int(rows[0].split(' ')[1]);fields={}
        for row in rows[1:]:
            key,value=row.split(':',1);key=key.lower()
            if key in fields or re.fullmatch('[a-z0-9-]+',key) is None:raise ValueError()
            fields[key]=value.strip()
        if (fields.get('content-type','').split(';')[0].strip().lower()!='application/json'
                or fields.get('content-encoding','identity').lower()!='identity' or 'transfer-encoding' in fields
                or re.fullmatch('[0-9]{1,8}',fields.get('content-length','')) is None):raise ValueError()
        length=int(fields['content-length'])
        if length>max_bytes or len(body)>length:raise ValueError()
        while len(body)<length:
            chunk=io(lambda:connection.recv(min(4096,length-len(body))),True)
            if not chunk:raise ValueError()
            body+=chunk
        if time.monotonic()>=deadline:raise TimeoutError()
        return code,decode_submission(body)
    except (ssl.SSLError,ValueError):return 'unknown',None
    except OSError as error:
        unavailable=not connected and (isinstance(error,TimeoutError) or error.errno in
            (errno.ECONNREFUSED,errno.EHOSTUNREACH,errno.ENETUNREACH,errno.ECONNRESET,errno.ETIMEDOUT))
        return ('unavailable' if unavailable else 'unknown'),None
    except Exception:return 'unknown',None
    finally:
        if connection is not None:connection.close()
