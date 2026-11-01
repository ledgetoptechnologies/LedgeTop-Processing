"""Offline backup stage for a stopped nonempty coordinator; no activation.

The caller must externally exclude every writer for the entire operation and
authenticate both census and schema inputs. This module does not establish
those prerequisites or bind them to a candidate image.
"""
from ops import archive_state_preflight, stopped_viewer_backup


def prepare(source, destination, lease_directory, authority_id, key, baseline,
            qualified_schema, *, stopped_writers_confirmed=False,
            independent_destination_confirmed=False):
    if not stopped_writers_confirmed or not independent_destination_confirmed:
        raise stopped_viewer_backup.BackupRefused('external_prerequisites_required')
    observed = archive_state_preflight.inspect(source, key, baseline, qualified_schema)
    if observed['database']['authorityId'] != authority_id:
        raise stopped_viewer_backup.BackupRefused('authority_identity_mismatch')
    # No reference-work quiescence contract is integrated yet. Do not infer
    # reference idleness from the separate native jobs table.
    if any(count for name, count in observed['database']['rowCounts'].items()
           if name.startswith('reference_')):
        raise stopped_viewer_backup.BackupRefused('reference_history_requires_inspection')
    # backup() reacquires both advisory locks and compares a fresh census to
    # this exact document before creating destination. It leaves incomplete
    # destinations in place for inspection rather than deleting them.
    return stopped_viewer_backup.backup(
        source, destination, lease_directory, authority_id, key,
        stopped_writers_confirmed=True, independent_destination_confirmed=True,
        expected_source_inventory=observed)
