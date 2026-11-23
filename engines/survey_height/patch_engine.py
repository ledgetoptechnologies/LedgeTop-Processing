"""Add the opt-in hook to a copy of the pinned engine reader.

The engine is AGPL-3.0-or-later. Its source is not included here: this inserts a
few lines into a hash-verified copy and checks the resulting hash.
"""
import hashlib

ORIGINAL_SHA256 = '337e0df0a7da404d2dffcd816cbb1dbe28733c248a05cbf9ed10f8f278992583'
LOCATION_SHA256 = 'd366ef27f819c0a9b8df4129500db0ff6e0838dbf1634a35466190255c6a9f70'
PATCHED_SHA256 = 'fd75fbbb3ea9ce368b09c995b132402eaa847ef1afae49d610365b229c05f242'
ANCHOR = """            if lines:
                self.raw_srs = lines[0] # SRS
"""
HOOKED = """            if lines:
                # LedgeTop 2026-11-23: opt-in compound survey input (AGPL-3.0-or-later).
                # Runs before any CRS parsing; every other header is unchanged.
                if lines[0].upper().startswith('SURVEY3D'):
                    from survey_height import import_handoff
                    import_handoff.import_survey_gcp(
                        self.gcp_path, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    raise RuntimeError('survey_import_returned_without_handoff')
                self.raw_srs = lines[0] # SRS
"""


def patch_gcp_source(original):
    if hashlib.sha256(original).hexdigest() != ORIGINAL_SHA256:
        raise ValueError('engine_reader_not_pinned')
    text = original.decode('utf-8')
    if text.count(ANCHOR) != 1:
        raise ValueError('engine_reader_anchor_mismatch')
    patched = text.replace(ANCHOR, HOOKED).encode('utf-8')
    if hashlib.sha256(patched).hexdigest() != PATCHED_SHA256:
        raise ValueError('patched_reader_digest_mismatch')
    return patched
