"""Opt-in physical reservations for one Viewer input spool/copy."""
import os
import threading

# Serializes check/allocation inside this interpreter only; other writers may race.
_reservation_lock = threading.Lock()

class InputSpaceUnavailable(OSError):
    """A single input cannot be reserved while retaining the configured floor."""

def reserve_input(fd, size, floor, offset=0):
    if type(size) is not int or size<1 or type(floor) is not int or floor<0 or type(offset) is not int or offset<0:
        raise ValueError('invalid input reservation')
    if os.name!='posix' or not hasattr(os,'posix_fallocate') or not hasattr(os,'fstatvfs'):
        raise InputSpaceUnavailable('input reservation unavailable')
    try:
        with _reservation_lock:
            space=os.fstatvfs(fd)
            if space.f_bavail*space.f_frsize < size+floor:
                raise InputSpaceUnavailable('input space unavailable')
            os.posix_fallocate(fd,offset,size)
    except InputSpaceUnavailable:raise
    except OSError as exc:raise InputSpaceUnavailable('input reservation unavailable') from exc
