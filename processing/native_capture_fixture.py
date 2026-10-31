"""Loopback-only synthetic NativeEngine capture adapter; never wired to runtime.

The caller supplies an already allocated slot and a fixture binding. This
module does not authenticate that binding or establish task/gate ownership.
"""
import ipaddress
from urllib.error import HTTPError
from urllib.parse import urlsplit

from .direct_outbox_contract import validate_binding
from .direct_slot_capture import begin_capture
from .viewer_service import NativeEngine


class FixtureCaptureHeld(RuntimeError):pass


def capture_native_fixture(engine,root,root_identity,binding,*,max_budget):
    """GET one fixture archive into a sealed slot; never acknowledge/cleanup."""
    validate_binding(binding)
    if type(engine) is not NativeEngine:raise FixtureCaptureHeld('fixture_engine_required')
    try:
        origin=urlsplit(engine.origin)
        if (origin.scheme!='http' or not origin.hostname or origin.username is not None
                or origin.password is not None or origin.path not in ('','/') or origin.query or origin.fragment
                or not ipaddress.ip_address(origin.hostname).is_loopback or not origin.port):
            raise ValueError()
    except ValueError:raise FixtureCaptureHeld('fixture_loopback_required') from None
    capture=None
    try:
        with engine.download(binding['engineTaskId']) as response:
            if response.status!=200:raise FixtureCaptureHeld('fixture_response_invalid')
            lengths=response.headers.get_all('Content-Length',[])
            encodings=response.headers.get_all('Content-Encoding',[])
            types=response.headers.get_all('Content-Type',[])
            transfers=response.headers.get_all('Transfer-Encoding',[])
            if (len(lengths)>1 or len(encodings)>1 or len(types)>1 or len(transfers)>1
                    or (transfers and (transfers[0].lower()!='chunked' or lengths))
                    or (encodings and encodings[0].lower()!='identity')
                    or (types and types[0].split(';',1)[0].strip().lower() not in
                        ('application/zip','application/octet-stream','application/x-zip-compressed'))):
                raise FixtureCaptureHeld('fixture_response_invalid')
            declared=None
            if lengths:
                length=lengths[0]
                if not length.isascii() or not length.isdecimal() or len(length)>16:
                    raise FixtureCaptureHeld('fixture_response_invalid')
                declared=int(length)
                if not 1<=declared<=binding['outputBudgetBytes']:raise FixtureCaptureHeld('fixture_response_invalid')
            capture=begin_capture(root,root_identity,binding,max_budget=max_budget,declared_length=declared)
            return capture.consume(response)
    except HTTPError as error:
        error.close();raise FixtureCaptureHeld('fixture_download_failed') from None
    except Exception:
        raise FixtureCaptureHeld('fixture_capture_requires_inspection') from None
    finally:
        if capture is not None:capture.close()
