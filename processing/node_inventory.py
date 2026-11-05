"""Startup-only inventory pins; never migrate worker gates or delete history.

Ordering/capacity changes need no handover. Removing or rebinding a node needs
its executions, update ownership, readers and durable result handoffs closed.
Legacy results without a verified worker observation fail closed for review.
"""
from dataclasses import asdict
import json

from .maintenance import Conflict
from .config import normalized_node_origin
from . import reference_protocol as p


def _table(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _closed_node(db, node):
    if db.execute("SELECT 1 FROM jobs WHERE node_id=? AND status IN ('running','held') LIMIT 1", (node,)).fetchone():
        raise Conflict('inventory change has an occupied or held node')
    if _table(db, 'nodes'):
        row = db.execute('SELECT state FROM nodes WHERE id=?', (node,)).fetchone()
        if row and row['state'] != 'active':
            raise Conflict('inventory change has unresolved maintenance ownership')
    if _table(db, 'viewer_tasks') and db.execute('SELECT 1 FROM viewer_tasks WHERE node_id=? LIMIT 1', (node,)).fetchone():
        raise Conflict('legacy Viewer node history requires explicit retirement review')
    jobs = []
    if _table(db, 'reference_jobs'):
        jobs = db.execute('SELECT job_id,phase,result_json FROM reference_jobs WHERE node_id=?', (node,)).fetchall()
        for row in jobs:
            if (row['phase'] != 'completed' or not row['result_json']
                    or not _table(db, 'reference_worker_acceptance_observations')
                    or not db.execute('SELECT 1 FROM reference_worker_acceptance_observations WHERE job_id=?',
                                      (row['job_id'],)).fetchone()):
                raise Conflict('inventory change has an unobserved result or unresolved execution')
    if _table(db, 'source_reader_pins'):
        if db.execute("""SELECT 1 FROM source_reader_pins p JOIN jobs j ON j.id=p.job_id
            WHERE j.node_id=? AND p.state IN ('active','uncertain') LIMIT 1""", (node,)).fetchone():
            raise Conflict('inventory change has unresolved source readers')
    return [row['job_id'] for row in jobs]


def validate_removed_nodes(db, proposed):
    """Pre-overlay queue guard: preserve original IDs and pending ownership."""
    if _table(db, 'processing_node_inventory'):
        saved = db.execute('SELECT inventory_json FROM processing_node_inventory ORDER BY revision DESC LIMIT 1').fetchone()
        previous = set(json.loads(saved[0])['nodes']) if saved else set()
    elif _table(db, 'nodes'):
        previous = {row[0] for row in db.execute('SELECT id FROM nodes')}
    else:
        previous = set()
    for node in previous - set(proposed):
        _closed_node(db, node)


def pin_reference_inventory(store, config, settings):
    """Pin authenticated coordinator configuration, without inferring hardware.

    Existing recovery installations must first start once with unchanged
    recovery bindings: the old digest alone cannot reconstruct removed nodes.
    Worker gate/journal identity still requires its separate owner handover.
    """
    recovery = settings.recovery_bindings
    inventory = {}
    for node in config.node_endpoints:
        worker = settings.workers[node.id]
        inventory[node.id] = dict(engineOrigin=list(normalized_node_origin(node.url)),
            physical=asdict(recovery[node.id]) if recovery else None,
            workerId=worker['workerId'], instanceId=worker['instanceId'], resultOrigin=worker['resultOrigin'])
    document = dict(nodeOrder=list(config.prototype_node_ids), nodes=inventory)
    wire = p.canonical_json(document).decode()
    with store.queue._db() as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute('''CREATE TABLE IF NOT EXISTS processing_node_inventory (
            revision INTEGER PRIMARY KEY,inventory_json TEXT NOT NULL,
            inventory_sha256 TEXT NOT NULL,created_at TEXT DEFAULT CURRENT_TIMESTAMP)''')
        old = db.execute('SELECT * FROM processing_node_inventory ORDER BY revision DESC LIMIT 1').fetchone()
        if old:
            previous = json.loads(old['inventory_json'])
            if p.canonical_json(previous).decode() != old['inventory_json'] or p.digest(previous) != old['inventory_sha256']:
                raise Conflict('persisted inventory pin changed')
            previous = previous['nodes']
            affected = {node for node in previous if node not in inventory or previous[node] != inventory[node]}
        else:
            if _table(db, 'reference_recovery_startup'):
                legacy = db.execute('SELECT binding_sha256 FROM reference_recovery_startup WHERE singleton=1').fetchone()
                expected = p.digest({node: asdict(value) for node, value in recovery.items()}) if recovery else None
                if legacy and legacy[0] != expected:
                    raise Conflict('first inventory pin requires unchanged recovery bindings')
            affected = set()
            # The legacy recovery digest pins engine bindings, but not worker
            # IDs. Existing live claim ownership supplies that missing fence.
            if _table(db, 'reference_claims'):
                for claim in db.execute("""SELECT c.node_id,c.worker_id,c.instance_id
                    FROM reference_claims c JOIN jobs j ON j.id=c.job_id
                    WHERE j.status IN ('running','held')"""):
                    proposed = inventory.get(claim['node_id'])
                    if (proposed is None or proposed['workerId'] != claim['worker_id']
                            or proposed['instanceId'] != claim['instance_id']):
                        raise Conflict('first inventory pin differs from live worker ownership')
        for node in affected:
            for job_id in _closed_node(db, node):
                from .reference_acceptance_observed import lookup
                if lookup(store, job_id) is None:
                    raise Conflict('inventory retirement requires verified worker observation')
        if old is None or old['inventory_json'] != wire:
            db.execute('INSERT INTO processing_node_inventory(revision,inventory_json,inventory_sha256) VALUES(?,?,?)',
                       (old['revision'] + 1 if old else 1, wire, p.digest(document)))
