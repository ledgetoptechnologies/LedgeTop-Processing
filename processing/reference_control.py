"""Explicit assembly of public and private reference routes; no autostart."""
from .reference_api import ReferenceAPI
from .reference_delivery import DeliveryReceiver
from .reference_worker_api import ReferenceWorkerAPI, PREFIX as WORKER_PREFIX


class ReferenceControlAPI:
    def __init__(self, store, *, viewer_bearers, worker_bearers,
                 capability_fingerprint, options_fingerprint,
                 allowed_odm_options=(), treedar_available=False, result_proxy=None,
                 recovery_bindings=None, viewer_receipt_keys=None, source_spool=None,
                 source_attestation_keys=None, authority_id=None, source_release_service=None,
                 source_bearer_principals=None):
        self.coordinator_queue = store.queue
        if set(viewer_bearers) & set(worker_bearers):
            raise ValueError('Viewer and worker credentials must be separate')
        self.viewer = ReferenceAPI(store, bearer_tokens=viewer_bearers,
            capability_fingerprint=capability_fingerprint, options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, treedar_available=treedar_available,
            result_proxy=result_proxy, viewer_receipt_keys=viewer_receipt_keys,
            source_spool=source_spool, source_attestation_keys=source_attestation_keys,
            authority_id=authority_id,source_release_service=source_release_service,
            source_bearer_principals=source_bearer_principals)
        self.worker = ReferenceWorkerAPI(store, DeliveryReceiver(store), bearer_workers=worker_bearers,
            recovery_bindings=recovery_bindings, source_spool=source_spool)

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        from .reference_transfer import OBJECT_PREFIX
        from .source_range_transfer import OBJECT_PREFIX as SOURCE_RANGE_OBJECT_PREFIX, RENEWAL_PATH, RENEWAL_ACK_PATH
        api = self.worker if (isinstance(path, str) and
            (path.startswith(WORKER_PREFIX + '/') or path.startswith(OBJECT_PREFIX)
             or path.startswith(SOURCE_RANGE_OBJECT_PREFIX) or path in (RENEWAL_PATH, RENEWAL_ACK_PATH))) else self.viewer
        return api.handle(method, path, headers, body, protected_transport=protected_transport)

    def stream_source_object(self, path, headers, reader, content_length, *, protected_transport=False):
        return self.viewer.stream_source_object(path, headers, reader, content_length,
            protected_transport=protected_transport)
