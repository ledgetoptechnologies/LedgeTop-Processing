"""Narrow observer-only queue projection; no paths, names, credentials or result bodies."""
from datetime import datetime, timezone
from urllib.parse import parse_qs

_ALLOWED_STATUS = {'queued', 'running', 'held', 'succeeded', 'failed', 'cancelled'}
_ALLOWED_ENV = {'production', 'staging'}

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

def snapshot(queue, viewer_enabled, after, limit, status, environment):
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
             'nodeId': r['node_id'], 'phase': r['phase'], 'uploadedFiles': r['upload_cursor'],
             'totalFiles': None,
             'progress': r['progress'], 'errorCode': r['error'], 'heldReason': r['held_reason'],
             'createdAt': r['created_at'], 'queueUpdatedAt': r['updated_at'],
             'viewerUpdatedAt': r['viewer_updated_at']} for r in rows]
    ordered_nodes = {r['id']: r for r in nodes}
    nodes = [ordered_nodes[n] for n in queue.node_ids if n in ordered_nodes]
    return {'observedAt': observed, 'consistency': 'per_page_database_snapshot',
            'pagination': 'creation_sequence_not_change_feed',
            'refresh': 'restart_at_zero_and_read_all_pages', 'jobs': jobs,
            'nodes': [{'nodeId': r['id'], 'maintenanceState': r['state'], 'revision': r['revision'],
                       'drainAt': r['drain_at'], 'notBefore': r['not_before']} for r in nodes],
            'nextAfter': rows[-1]['seq'] if more and rows else None}
