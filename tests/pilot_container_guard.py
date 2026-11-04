"""Verify a recorded pilot container is stopped; not a filesystem writer fence."""
import re


def stopped_pilot(item, expected):
    """Validate identity and mount before inspecting an owned test database.

    This only proves the inspected container is stopped at this instant.
    It does not prove that it cannot restart, that another process/container
    cannot write the mount, or that the database is checkpointed.
    """
    required = {'id', 'name', 'image', 'owner', 'mountSource'}
    if type(expected) is not dict or set(expected) != required:
        raise ValueError('invalid expected pilot identity')
    if (not re.fullmatch(r'[0-9a-f]{64}', str(expected['id']))
            or not re.fullmatch(r'[0-9a-f]{32}', str(expected['owner']))
            or expected['name'] != 'ltds-pilot-fake-' + expected['owner']
            or not re.fullmatch(r'sha256:[0-9a-f]{64}', str(expected['image']))
            or type(expected['mountSource']) is not str or not expected['mountSource']):
        raise ValueError('invalid expected pilot identity')
    try:
        labels = item['Config']['Labels'] or {}
        matches = [mount for mount in item['Mounts'] if mount['Destination'] == '/pilot']
        if (item['Id'] != expected['id'] or item['Name'] != '/' + expected['name']
                or item['Image'] != expected['image']
                or labels.get('ltds.fixture.owner') != expected['owner']
                or item['State']['Running'] is not False
                or item['State']['Restarting'] is not False
                or len(matches) != 1 or matches[0]['Type'] != 'bind'
                or matches[0]['Source'] != expected['mountSource']
                or matches[0]['RW'] is not True):
            raise ValueError('pilot container state or identity changed')
    except (KeyError, TypeError):
        raise ValueError('pilot container state or identity unavailable') from None
    return {'containerStopped': True, 'otherWritersNotVerified': True,
            'restartNotPrevented': True, 'databaseCheckpointNotVerified': True}
