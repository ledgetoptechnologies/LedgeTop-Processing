"""Optional standalone certificate context for the authenticated Viewer API."""
import os
from pathlib import Path
import ssl
import stat


def _checked_file(value, *, private):
    if type(value) is not str or not value or not Path(value).is_absolute():
        raise ValueError('Viewer TLS files require absolute paths')
    path = Path(value).absolute()
    if path.resolve() != path or any(part.is_symlink() for part in (path, *path.parents)):
        raise ValueError('Viewer TLS file paths cannot contain symlinks')
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise ValueError('Viewer TLS files must be single-link regular files')
    if private and os.name != 'nt' and (info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) & 0o077):
        raise ValueError('Viewer TLS private key must be owner-private')
    return path, info


def standalone_viewer_tls_context(certificate, private_key):
    """Validate both paths and load the server key before starting the queue."""
    descriptors = []
    try:
        cert, before_cert = _checked_file(certificate, private=False)
        key, before_key = _checked_file(private_key, private=True)
        for path, expected in ((cert, before_cert), (key, before_key)):
            fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            descriptors.append(fd)
            opened = os.fstat(fd)
            if (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino):
                raise ValueError('Viewer TLS file identity changed')
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        # Linux procfs keeps OpenSSL on the checked file descriptors even if a
        # path is replaced; other platforms retain the before/after checks.
        if os.name == 'posix' and Path('/proc/self/fd').is_dir():
            cert_load, key_load = (f'/proc/self/fd/{fd}' for fd in descriptors)
        else:
            cert_load, key_load = str(cert), str(key)
        context.load_cert_chain(cert_load, key_load, password=lambda: '')
        for path, before, private in ((cert, before_cert, False), (key, before_key, True)):
            _, after = _checked_file(str(path), private=private)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError('Viewer TLS file changed while loading')
        return context
    except (OSError, ValueError, ssl.SSLError):
        # Certificate paths, key contents and OpenSSL details stay out of logs.
        raise ValueError('Viewer TLS certificate or key is invalid') from None
    finally:
        for fd in descriptors:
            os.close(fd)
