"""Narrow observer-only queue projection; no paths, names, credentials or result bodies."""
from datetime import datetime, timezone
import re
from urllib.parse import parse_qs

_ALLOWED_STATUS = {'queued', 'running', 'held', 'succeeded', 'failed', 'cancelled'}
_ALLOWED_ENV = {'production', 'staging'}
_PHASES = frozenset(('uploading', 'queued', 'waiting_node', 'claimed', 'init_pending', 'initialized',
                     'upload_pending', 'uploaded', 'commit_pending', 'awaiting_engine_start',
                     'processing', 'complete', 'cancel_pending', 'cancelled', 'held'))
_HELD_REASONS = frozenset(('operator', 'coordinator_restart'))

def safe_known(value, allowed):
    if value is None:
        return None
    return value if isinstance(value, str) and value in allowed else 'other'

def unavailable():
    return {'state': 'unavailable', 'reason': 'not_reported', 'observedAt': None}

_SQLITE_UTC = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?$')
_ISO_OFFSET = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$')

def utc_wire(value):
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError('invalid stored timestamp')
    try:
        if _SQLITE_UTC.fullmatch(value):
            parsed = datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        elif _ISO_OFFSET.fullmatch(value):
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        else:
            raise ValueError('invalid stored timestamp')
        return parsed.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    except (ValueError, OverflowError):
        raise ValueError('invalid stored timestamp') from None

def parse_query(query):
    try:
        q = parse_qs(query, keep_blank_values=True, strict_parsing=True, max_num_fields=4)
    except ValueError:
        raise ValueError('invalid query') from None
    if set(q) - {'after', 'limit', 'status', 'environment'} or any(len(v) != 1 for v in q.values()):
        raise ValueError('invalid query')
    after, limit = q.get('after', ['0'])[0], q.get('limit', ['50'])[0]
    if not after.isascii() or not after.isdigit() or len(after) > 15 or not limit.isascii() or not limit.isdigit() or len(limit) > 3:
        raise ValueError('invalid query')
    after, limit = int(after), int(limit)
    if not 1 <= limit <= 100:
        raise ValueError('invalid query')
    status, environment = q.get('status', [None])[0], q.get('environment', [None])[0]
    if status is not None and status not in _ALLOWED_STATUS or environment is not None and environment not in _ALLOWED_ENV:
        raise ValueError('invalid query')
    return after, limit, status, environment

def snapshot(queue, viewer_enabled, after, limit, status, environment, *, configured_nodes=None):
    # Empty configuration is an explicit unknown: the scheduler has a legacy
    # fallback and the dashboard must not invent its limits from defaults.
    policy = None
    if configured_nodes:
        ids = [getattr(node, 'node_id', None) for node in configured_nodes]
        if ids != list(queue.node_ids) or len(ids) != len(set(ids)):
            raise ValueError('configured node order differs from queue')
        policy = {}
        for node in configured_nodes:
            limit_value = getattr(node, 'max_photos', 'missing')
            if limit_value is not None and (type(limit_value) is not int or not 1 <= limit_value <= 1_000_000):
                raise ValueError('invalid configured node limit')
            policy[node.node_id] = limit_value

    conditions, values = ['j.seq > ?'], [after]
    if status:
        conditions.append('j.status = ?'); values.append(status)
    if environment:
        conditions.append('j.environment = ?'); values.append(environment)
    join = 'LEFT JOIN viewer_tasks v ON v.job_id=j.id AND v.removed=0' if viewer_enabled else ''
    projection = 'v.uuid provider_uuid, v.engine_uuid, v.phase, v.upload_cursor, v.progress, v.error, v.updated_at viewer_updated_at' if viewer_enabled else 'NULL provider_uuid, NULL engine_uuid, NULL phase, NULL upload_cursor, NULL progress, NULL error, NULL viewer_updated_at'
    with queue._db() as db:
        db.execute('BEGIN')
        rows = db.execute(f'''SELECT j.seq, j.id, j.environment, j.profile, j.status, j.node_id,
            j.held_reason, j.created_at, j.updated_at, {projection} FROM jobs j {join}
            WHERE {' AND '.join(conditions)} ORDER BY j.seq LIMIT ?''', (*values, limit + 1)).fetchall()
        nodes = db.execute('SELECT id,state,revision,drain_at,not_before FROM nodes ORDER BY id').fetchall()
        observed = datetime.now(timezone.utc).isoformat()
    more = len(rows) > limit
    rows = rows[:limit]
    jobs = [{'jobId': r['id'], 'providerTaskId': r['provider_uuid'], 'engineTaskId': r['engine_uuid'],
             'environment': r['environment'], 'profile': r['profile'], 'status': r['status'],
             'nodeId': r['node_id'], 'phase': safe_known(r['phase'], _PHASES), 'uploadedFiles': r['upload_cursor'],
             'totalFiles': None,
             'progress': r['progress'], 'errorCode': None if r['error'] is None else 'other',
             'heldReason': safe_known(r['held_reason'], _HELD_REASONS),
             'createdAt': utc_wire(r['created_at']), 'queueUpdatedAt': utc_wire(r['updated_at']),
             'viewerUpdatedAt': utc_wire(r['viewer_updated_at'])} for r in rows]
    ordered_nodes = {r['id']: r for r in nodes}
    if any(n not in ordered_nodes for n in queue.node_ids):
        raise ValueError('configured node missing from state')
    nodes = [ordered_nodes[n] for n in queue.node_ids]
    return {'schemaVersion': 1, 'observedAt': observed,
            'logs': unavailable(), 'consistency': 'per_page_database_snapshot',
            'pagination': 'creation_sequence_not_change_feed',
            'refresh': 'restart_at_zero_and_read_all_pages', 'jobs': jobs,
            'nodes': [{'nodeId': r['id'], 'preferenceIndex': index,
                       'maintenanceState': r['state'], 'revision': r['revision'],
                       'drainAt': utc_wire(r['drain_at']), 'notBefore': utc_wire(r['not_before']),
                       'configuredPhotoLimit': ({'state': 'unavailable', 'source': None, 'maxPhotos': None}
                           if policy is None else {'state': 'no_configured_cap' if policy[r['id']] is None else 'configured',
                                                   'source': 'configuration', 'maxPhotos': policy[r['id']]}),
                       'effectivePhotoLimit': unavailable(),
                       'engine': unavailable(), 'container': unavailable(), 'resources': unavailable()}
                      for index, r in enumerate(nodes)],
            'nextAfter': rows[-1]['seq'] if more and rows else None}
