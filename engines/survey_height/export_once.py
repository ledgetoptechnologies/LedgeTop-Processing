"""Convert a synthetic Float64 DEM to NAVD88 metres exactly once, at export."""
from .import_handoff import verify_contract
from .survey_frame import SurveyRefused


class ExportLedger:
    """Refuses a source already converted here, and any output made here."""

    def __init__(self):
        self._seen = set()

    def check(self, source_sha256):
        if source_sha256 in self._seen:
            raise SurveyRefused('second_conversion')

    def record(self, source_sha256, output_sha256):
        self._seen.update((source_sha256, output_sha256))


def export_dem(source, destination, *, source_sha256, grid_path, contract, ledger,
               block_size=128, convert=None):
    """`contract` is the declared working frame; it does not prove the DEM's frame."""
    verify_contract(contract)
    if contract['source']['roles'] != 'SYNTHETIC':
        raise SurveyRefused('synthetic_dem_only')
    ledger.check(source_sha256)
    if convert is None:
        from .convert_dem_navd88 import convert
    try:
        receipt = convert(source, destination, source_sha256=source_sha256,
                          grid_path=grid_path, block_size=block_size)
    except (ValueError, FileNotFoundError) as error:
        code = str(error) if isinstance(error, ValueError) else 'input_file_missing'
        raise SurveyRefused(code) from error
    ledger.record(source_sha256, receipt['output_sha256'])
    return {**receipt, 'working_frame_sha256': contract['sha256'],
            'vertical_application_count': 1}
