"""Explicit assembly of public and private reference routes; no autostart."""
from .reference_api import ReferenceAPI
from .reference_delivery import DeliveryReceiver
from .reference_worker_api import ReferenceWorkerAPI, PREFIX as WORKER_PREFIX


class ReferenceControlAPI:
    def __init__(self, store, *, viewer_bearers, worker_bearers,
                 capability_fingerprint, options_fingerprint,
                 allowed_odm_options=(), treedar_available=False, result_proxy=None,
                 recovery_bindings=None, viewer_receipt_keys=None, source_spool=None,
                 source_attestation_keys=None, authority_id=None):
        if set(viewer_bearers) & set(worker_bearers):
            raise ValueError('Viewer and worker credentials must be separate')
        self.viewer = ReferenceAPI(store, bearer_tokens=viewer_bearers,
            capability_fingerprint=capability_fingerprint, options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, treedar_available=treedar_available,
            result_proxy=result_proxy, viewer_receipt_keys=viewer_receipt_keys,
            source_spool=source_spool, source_attestation_keys=source_attestation_keys,
            authority_id=authority_id)
        self.worker = ReferenceWorkerAPI(store, DeliveryReceiver(store), bearer_workers=worker_bearers,
            recovery_bindings=recovery_bindings)

    def handle(self, method, path, headers, body=b'', *, protected_transport=False):
        api = self.worker if isinstance(path, str) and path.startswith(WORKER_PREFIX + '/') else self.viewer
        return api.handle(method, path, headers, body, protected_transport=protected_transport)
