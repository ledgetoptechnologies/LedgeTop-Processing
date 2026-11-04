"""Inactive ODM v2 proof journal; no network, execution or cleanup authority."""
from .viewer_import_confirmation import ImportConfirmationRejected
from .viewer_import_confirmation_store import ImportConfirmationStore, ImportConfirmationStoreHeld
from .viewer_odm_import_confirmation_v2 import verify_odm_import_confirmation


class OdmImportConfirmationStoreV2(ImportConfirmationStore):
    """Separate proof-version table; no migration or cross-version acceptance.

    Trusted loader, issuer-key registry, transaction and source-binding duties
    are identical to the base store; they are not supplied by this candidate.
    """
    _table = 'viewer_odm_import_confirmations_candidate_v2'

    @staticmethod
    def _verify(confirmation, context, prior=None):
        try:
            return verify_odm_import_confirmation(confirmation,
                trusted_import_keys=context['trusted_import_keys'],
                trusted_copy_keys=context['trusted_copy_keys'], expected=context['expected'],
                result_manifest=context['result_manifest'], accepted_envelopes=context['accepted_envelopes'],
                prior_confirmation_sha256=prior)
        except (ImportConfirmationRejected, TypeError, ValueError):
            raise ImportConfirmationStoreHeld('confirmation_unverified_or_changed') from None
