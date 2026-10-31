"""Private worker result daemon adapter, independent of coordinator SQLite.

Read credentials must be separate from claim/Viewer credentials. A configured
sharing policy makes the actual decision; Viewer acceptance is never required
before serving the bytes needed to obtain that acceptance.
"""
import copy
import hmac
import re
from types import MappingProxyType

from . import reference_protocol as protocol
from .reference_api import Response
from .reference_binary import BinaryResponse
from .reference_results import ResultAuthorization, ResultUnavailable

PREFIX = "/ltds/worker/v1"


class ReferenceResultAPI:
    def __init__(self, results, *, bearer_readers):
        """Readers map bearer -> node_id/worker_id/environments/share_policy.

        share_policy(path-free persisted metadata) must return exactly True to
        permit sharing. It is configured trusted code, never a request field.
        """
        if type(bearer_readers) is not dict or not bearer_readers:
            raise ValueError("explicit result reader configuration required")
        readers = {}
        for token, config in bearer_readers.items():
            if (type(token) is not str or not 16 <= len(token) <= 4096
                    or not all(0x21 <= ord(char) <= 0x7e for char in token)):
                raise ValueError("invalid result reader credential")
            protocol._closed(config, ("node_id", "worker_id", "environments", "share_policy"), "result reader")
            if (config["node_id"] != results.worker.node_id or config["worker_id"] != results.worker.worker_id
                    or type(config["environments"]) not in (tuple, list, frozenset)
                    or not config["environments"] or not callable(config["share_policy"])):
                raise ValueError("reader must bind the fixed worker and sharing policy")
            for environment in config["environments"]:
                protocol._environment(environment)
            readers[token] = MappingProxyType(dict(config, environments=frozenset(config["environments"])))
        self.results = results
        self._readers = MappingProxyType(readers)

    @staticmethod
    def _values(headers, name):
        if not hasattr(headers, "items"):
            return []
        return [value for key, value in headers.items() if type(key) is str and key.lower() == name.lower()]

    def _authenticate(self, headers):
        values = self._values(headers, "Authorization")
        if len(values) != 1 or type(values[0]) is not str or not values[0].startswith("Bearer "):
            return None
        token = values[0][7:]
        if not 16 <= len(token) <= 4096 or not token.isascii():
            return None
        reader = None
        for configured, candidate in self._readers.items():
            if hmac.compare_digest(token, configured):
                reader = candidate
        return reader

    @staticmethod
    def _error(status, code):
        return Response(status, {"error": code})

    def handle(self, method, path, headers, body=b"", *, protected_transport=False):
        if protected_transport is not True:
            return self._error(503, "protected_transport_required")
        reader = self._authenticate(headers)
        if reader is None:
            return self._error(401, "unauthorized")
        try:
            match = re.fullmatch(re.escape(PREFIX) + r"/jobs/([A-Za-z0-9][A-Za-z0-9_-]{0,127})/results/([A-Za-z0-9][A-Za-z0-9_-]{0,127})", path) if type(path) is str else None
            if method != "GET" or not match:
                return self._error(404, "not_found")
            if body not in (b"", ""):
                return self._error(400, "invalid_request")
            required = {}
            for name in ("X-Result-Generation", "X-Result-Sha256", "X-Result-Manifest-Sha256"):
                values = self._values(headers, name)
                if len(values) != 1 or type(values[0]) is not str:
                    return self._error(400, "invalid_result_binding")
                required[name] = values[0]
            text = required["X-Result-Generation"]
            if not re.fullmatch(r"[1-9][0-9]{0,15}", text):
                return self._error(400, "invalid_result_binding")
            generation = int(text)
            protocol._integer(generation, "generation")
            protocol._hash(required["X-Result-Sha256"], "object hash")
            protocol._hash(required["X-Result-Manifest-Sha256"], "result manifest hash")
            job_id, object_id = match.groups()
            metadata = self.results.metadata(job_id)
            if metadata["environment"] not in reader["environments"]:
                return self._error(404, "not_found")
            obj = next((item for item in metadata["objects"] if item["id"] == object_id), None)
            if (obj is None or metadata["generation"] != generation
                    or metadata["resultManifestSha256"] != required["X-Result-Manifest-Sha256"]
                    or obj["sha256"] != required["X-Result-Sha256"]):
                return self._error(409, "result_binding_mismatch")
            policy_metadata = dict(copy.deepcopy(metadata), requestedObjectId=object_id)
            if reader["share_policy"](policy_metadata) is not True:
                return self._error(403, "result_not_shared")
            ranges = self._values(headers, "Range")
            if len(ranges) > 1:
                return self._error(400, "invalid_range_header")
            authorization = ResultAuthorization(node_id=reader["node_id"], worker_id=reader["worker_id"],
                job_id=job_id, generation=generation, environment=metadata["environment"],
                result_manifest_sha256=metadata["resultManifestSha256"], object_ids=(object_id,), share_approved=True)
            stream = self.results.open(job_id, generation=generation, object_id=object_id,
                result_manifest_sha256=metadata["resultManifestSha256"], object_sha256=obj["sha256"],
                authorization=authorization, range_header=ranges[0] if ranges else None)
            return BinaryResponse(stream=stream, status=stream.status, headers=stream.headers)
        except ResultUnavailable as error:
            response = self._error(error.status, error.code)
            response.headers.update(getattr(error, "headers", {}))
            return response
        except (ValueError, TypeError):
            return self._error(400, "invalid_result_binding")
        except Exception:
            return self._error(500, "internal_error")
