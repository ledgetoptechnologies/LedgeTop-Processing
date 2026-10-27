"""Read-only probes of fixed ODM-family processing nodes."""

import json
import re
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler

REQUIRED_VIEWER_OUTPUTS = frozenset(("pc-ept", "gltf", "3d-tiles"))


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, newurl):
        return None


def _get_json(origin: str, route: str, max_bytes: int, token: str = ''):
    if token:
        route += '?' + urlencode({'token': token})
    request = Request(f"{origin}{route}", headers={"Accept": "application/json"})
    with build_opener(ProxyHandler({}), _NoRedirect).open(request, timeout=3) as response:
        if "json" not in response.headers.get("Content-Type", "").lower():
            raise ValueError("unexpected content type")
        body = response.read(max_bytes + 1)
        if len(body) > max_bytes:
            raise ValueError("provider response too large")
        return json.loads(body)


def _viewer_type(info: dict) -> str | None:
    if not isinstance(info, dict):
        return None
    if not (isinstance(info.get("version"), str)
            and re.fullmatch(r"\d+\.\d+(?:\.\d+)?(?:[-+][0-9A-Za-z.-]+)?", info["version"])
            and isinstance(info.get("engine"), str) and 1 <= len(info["engine"]) <= 120
            and isinstance(info.get("engineVersion"), str) and 1 <= len(info["engineVersion"]) <= 120
            and type(info.get("taskQueueCount")) is int
            and info["taskQueueCount"] >= 0
            and "maxImages" in info):
        return None
    max_images = info.get("maxImages")
    if max_images is not None and (type(max_images) is not int or max_images < 1):
        return None
    sentinel = (info.get("totalMemory") == 99999999999
                and info.get("availableMemory") == 99999999999
                and info.get("cpuCores") == 99999999999)
    if info["version"].startswith("1.") and sentinel:
        return "clusterodm"
    if info["version"].startswith("2.") and not sentinel:
        return "nodeodm"
    return None


def probe_node(role: str, origin: str, token: str = '') -> dict:
    if not origin:
        return {"role": role, "configured": False, "ready": False, "reason": "not_configured"}
    try:
        info = _get_json(origin, "/info", 65536, token)
        options = _get_json(origin, "/options", 262144, token)
        if not isinstance(options, list):
            raise ValueError("options must be an array")
        option_names = {item.get("name") for item in options if isinstance(item, dict) and isinstance(item.get("name"), str)}
        missing = sorted(REQUIRED_VIEWER_OUTPUTS - option_names)
        viewer_type = _viewer_type(info)
        if not viewer_type:
            reason = "viewer_provider_signature_unsupported"
        elif missing:
            reason = "viewer_outputs_missing"
        else:
            reason = None
        return {
            "role": role, "configured": True, "ready": reason is None,
            "reason": reason, "viewerProviderType": viewer_type,
            "engine": info.get("engine") if isinstance(info, dict) else None,
            "maxImages": info.get("maxImages") if isinstance(info, dict) else None,
            "taskQueueCount": info.get("taskQueueCount") if isinstance(info, dict) else None,
            "missingViewerOutputs": missing,
        }
    except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError):
        return {"role": role, "configured": True, "ready": False, "reason": "probe_failed"}
