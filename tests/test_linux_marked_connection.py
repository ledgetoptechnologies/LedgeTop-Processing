import os
import unittest
from ops.linux_marked_connection import MarkedConnection

class Tests(unittest.TestCase):
    def test_default_disabled_and_wrong_origin_or_namespace_refused(self):
        fd = os.open('/proc/self/ns/net', os.O_RDONLY | os.O_CLOEXEC)
        other = os.open('/dev/null', os.O_RDONLY | os.O_CLOEXEC)
        try:
            with self.assertRaises(ValueError):
                MarkedConnection('127.0.0.1', 13001, timeout=2, namespace_fd=fd, upstream=('127.0.0.1', 13001))
            with self.assertRaises(ValueError):
                MarkedConnection('127.0.0.1', 13001, timeout=2, namespace_fd=fd, upstream=('127.0.0.1', 13002), fixture=True)
            with self.assertRaises((ValueError, OSError)):
                MarkedConnection('127.0.0.1', 13001, timeout=2, namespace_fd=other, upstream=('127.0.0.1', 13001), fixture=True)
        finally:
            os.close(other); os.close(fd)

if __name__ == '__main__': unittest.main()
