"""Unqualified Linux marked upstream socket for isolated fixture use only.

This is not an authorization boundary: network namespace membership, dropped
client capabilities, firewall rules and Docker lifecycle must be enforced by a
privileged supervisor. Production enablement is intentionally prohibited.
"""
import fcntl
import http.client
import ipaddress
import os
import socket

PRODUCTION_QUALIFIED = False
MARK = 0x42

class MarkedConnection(http.client.HTTPConnection):
    def __init__(self, host, port, *, timeout, namespace_fd, upstream, fixture=False):
        if fixture is not True or PRODUCTION_QUALIFIED:
            raise ValueError('marked connection not qualified for runtime')
        if (type(upstream) is not tuple or len(upstream) != 2 or (host, port) != upstream
                or type(port) is not int or not 0 < port < 65536):
            raise ValueError('exact upstream required')
        try:
            if ipaddress.ip_address(host) != ipaddress.ip_address('127.0.0.1'): raise ValueError()
        except ValueError:
            raise ValueError('numeric loopback upstream required') from None
        if fcntl.ioctl(namespace_fd, 0xb703) != 0x40000000:
            raise ValueError('network namespace descriptor required')
        info = os.fstat(namespace_fd)
        self.namespace = (info.st_dev, info.st_ino)
        self._check_namespace()
        super().__init__(host, port, timeout=timeout)

    def _check_namespace(self):
        info = os.stat('/proc/self/ns/net')
        if (info.st_dev, info.st_ino) != self.namespace:
            raise OSError('network namespace changed')

    def connect(self):
        self._check_namespace()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_MARK, MARK)
            if sock.getsockopt(socket.SOL_SOCKET, socket.SO_MARK) != MARK:
                raise OSError('socket mark unavailable')
            sock.connect((self.host, self.port))
            self._check_namespace()
            self.sock = sock
        except BaseException:
            sock.close()
            raise
