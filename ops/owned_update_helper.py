"""Owned read-only updater helper container lifecycle."""
import re
import uuid


def run_owned_helper(transport, args, *, timeout=60, error_type=ValueError):
    """Run an updater helper and reconcile its daemon-side lifetime.

    A timeout of the Docker CLI does not stop the container. Never remove a
    container without checking both its operation-unique name and label.
    """
    operation = uuid.uuid4().hex
    name = 'ledgetop-update-helper-' + operation
    label = 'io.ledgetop.update-helper=' + operation
    def listed():
        raw = transport.docker('ps', '-a', '--no-trunc', '--filter', 'label=' + label,
                               '--format', '{{.ID}}', timeout=30)
        ids = raw.splitlines()
        if len(ids) > 1 or any(not re.fullmatch(r'[a-f0-9]{64}', value) for value in ids):
            raise error_type('helper_identity_ambiguous')
        return ids
    if listed():
        raise error_type('helper_identity_already_exists')
    try:
        # Split create from start: if the create client times out, a late
        # daemon-side create may leave a stopped container, but cannot start a
        # late untracked helper after our cleanup check.
        created = transport.docker('create', '--name', name, '--label', label, *args, timeout=30).strip()
        if not re.fullmatch(r'[a-f0-9]{64}', created) or listed() != [created]:
            raise error_type('helper_create_unconfirmed')
        item = transport.inspected(created, timeout=30)
        if (item.get('Id') != created or item.get('Name') != '/' + name
                or (item.get('Config', {}).get('Labels') or {}).get('io.ledgetop.update-helper') != operation
                or item.get('State', {}).get('Running') is not False):
            raise error_type('helper_identity_ambiguous')
        return transport.docker('start', '--attach', created, timeout=timeout)
    finally:
        ids = listed()
        if ids:
            try:
                item = transport.inspected(ids[0], timeout=30)
            except Exception:
                # --rm may finish between listing and inspecting. Only treat
                # that race as resolved if a fresh daemon listing is empty.
                if listed():
                    raise error_type('helper_identity_unconfirmed') from None
                item = None
            if item is not None and (item.get('Id') != ids[0] or item.get('Name') != '/' + name
                    or (item.get('Config', {}).get('Labels') or {}).get('io.ledgetop.update-helper') != operation):
                raise error_type('helper_identity_ambiguous')
            if item is not None:
                try:
                    transport.docker('rm', '-f', ids[0], timeout=30)
                except Exception:
                    # The --rm helper can finish between inspect and removal.
                    if listed():
                        raise error_type('helper_cleanup_unconfirmed') from None
                if listed():
                    raise error_type('helper_cleanup_unconfirmed')

