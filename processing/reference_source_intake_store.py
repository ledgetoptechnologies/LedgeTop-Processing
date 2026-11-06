"""Durable source intake and verified-spool operations for ReferenceStore."""
import hashlib
import hmac
import json
import math
import secrets
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone

from .maintenance import Conflict
from . import reference_protocol as protocol

PROFILE = "reference-pipeline-v1"
SOURCE_RANGE_GRANT_TTL_SECONDS = 6 * 60 * 60

_SOURCE_INTAKE_FIELDS = {"schemaVersion", "submissionId", "providerTaskId", "sourceManifest",
                         "viewerTaskId", "derivedManifest", "manifest", "pipeline", "capabilityFingerprint",
                         "sourceAttestation",
                         "environment", "requestDigest", "manifestSha256", "sourceManifestSha256",
                         "derivedManifestSha256", "pipelineDigest"}


def _json(value):
    return protocol.canonical_json(value).decode("utf-8")


def _metadata_field_bytes(*values):
    """Count bounded SQLite text/integer metadata using UTF-8 field lengths."""
    return sum(len(value) if isinstance(value, bytes) else len(str(value).encode('utf-8'))
               for value in values if value is not None)


def _open_pinned_source(source_spool, job_id, generation, object_id, expected):
    """Open one pinned spool inode without hashing it again for each range."""
    import os
    import stat
    source_spool._assert_mount()
    key = source_spool._key(job_id, generation, object_id)
    target = source_spool.root / key
    if source_spool._directory(job_id, generation) != target.parent:
        raise Conflict('source spool directory changed')
    fd = os.open(target, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_BINARY', 0))
    try:
        info = os.fstat(fd)
        identity = (info.st_dev, info.st_ino, info.st_size, info.st_mode, info.st_nlink,
                    info.st_uid, info.st_mtime_ns, info.st_ctime_ns)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or (os.name != 'nt' and info.st_uid != os.geteuid())
                or info.st_dev != source_spool._mount_identity[0]
                or (info.st_size, str(info.st_dev), str(info.st_ino), info.st_mode, info.st_nlink,
                    info.st_uid, info.st_mtime_ns, info.st_ctime_ns)
                    != (expected['byte_size'], str(expected['device']), str(expected['inode']), expected['mode'],
                        expected['link_count'], expected['owner_uid'], expected['mtime_ns'], expected['ctime_ns'])):
            raise Conflict('source object pin changed')
        return os.fdopen(fd, 'rb'), identity
    except Exception:
        os.close(fd)
        raise


def _ensure_source_range_schema(queue):
    """Additive broker journal tables; legacy grant rows remain unchanged."""
    if getattr(queue,'_source_range_schema_ready',False):
        return
    with queue._db() as db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS reference_source_grant_renewals (
              job_id TEXT NOT NULL REFERENCES reference_jobs(job_id), sequence INTEGER NOT NULL,
              renewal_id TEXT NOT NULL, request_json TEXT NOT NULL, request_sha256 TEXT NOT NULL,
              previous_grant_sha256 TEXT NOT NULL, successor_grant_json TEXT NOT NULL,
              successor_sha256 TEXT NOT NULL, acked INTEGER NOT NULL DEFAULT 0 CHECK(acked IN (0,1)),
              created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY(job_id,sequence), UNIQUE(renewal_id));
            CREATE TABLE IF NOT EXISTS reference_source_range_usage (
              job_id TEXT NOT NULL REFERENCES reference_jobs(job_id), generation INTEGER NOT NULL,
              reservation_id TEXT NOT NULL, requests_used INTEGER NOT NULL DEFAULT 0,
              bytes_used INTEGER NOT NULL DEFAULT 0, attempts_used INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(job_id,generation));
            CREATE TABLE IF NOT EXISTS reference_source_range_requests (
              job_id TEXT NOT NULL, generation INTEGER NOT NULL, request_id TEXT NOT NULL,
              object_id TEXT NOT NULL, range_start INTEGER NOT NULL, range_length INTEGER NOT NULL,
              attempts_used INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(job_id,generation,request_id));
            CREATE TABLE IF NOT EXISTS reference_source_object_pins (
              job_id TEXT NOT NULL REFERENCES reference_jobs(job_id), generation INTEGER NOT NULL,
              object_id TEXT NOT NULL, byte_size INTEGER NOT NULL, sha256 TEXT NOT NULL,
              mount_id TEXT NOT NULL, device TEXT NOT NULL, inode TEXT NOT NULL,
              mode INTEGER NOT NULL, link_count INTEGER NOT NULL, owner_uid INTEGER NOT NULL,
              mtime_ns INTEGER NOT NULL, ctime_ns INTEGER NOT NULL,
              PRIMARY KEY(job_id,generation,object_id));
        ''')
    queue._source_range_schema_ready=True


class ReferenceSourceIntakeMethods:
    @contextmanager
    def source_operation(self, job_id, *, generation, operation, receiving_only=False):
        with self.queue.coordinator_operation('source-io'):
            with self._source_operation(job_id,generation=generation,operation=operation,
                    receiving_only=receiving_only):
                yield

    @contextmanager
    def _source_operation(self, job_id, *, generation, operation, receiving_only=False):
        """Pin one synchronous source writer/auditor in the existing read lease.

        The owning call must close its internal descriptors before returning.
        Fences may win during I/O; the pin drains before release can proceed,
        and a final authority check prevents acknowledging work past the fence.
        """
        if operation not in ('source-put', 'source-receipt', 'source-complete', 'source-audit'):
            raise ValueError('invalid source operation')
        def authorize(db):
            row = self._row(db, job_id)
            from .source_intake_mode import require as require_source_mode
            require_source_mode(db, job_id, 'spool-v1')
            allowed = ('receiving',) if receiving_only else ('receiving', 'accepted')
            if (row['generation'] != generation or row['source_state'] not in allowed
                    or row['derived_state'] not in allowed):
                raise Conflict('source operation custody changed')
        readers = self.source_readers()
        handle = readers.reserve(job_id, generation, operation,
            authorize=authorize, quota=self._quota, operation=operation)
        try:
            with readers.operation_scope(handle):
                handle.check_authority(authorize=authorize)
                yield
                handle.check_authority(authorize=authorize)
        except BaseException as error:
            from .source_spool import SourceSpoolCloseUncertain
            if isinstance(error, SourceSpoolCloseUncertain):
                handle.uncertain = True
            raise
        finally:
            if not handle.uncertain:
                handle.close()

    def source_readers(self):
        """One Linux source-reader lease per existing coordinator authority."""
        from .reference_source_readers import SourceReaders, SourceReaderError
        import os
        if self.queue._creator_pid != os.getpid():
            raise SourceReaderError('source_reader_owner_process_required')
        with self.queue._lifecycle_lock:
            if self.queue._lock.closed:
                raise SourceReaderError('source_reader_coordinator_closed')
            readers = getattr(self.queue, '_source_reader_registry', None)
            if readers is None or readers._closed:
                readers = SourceReaders(self).start()
                self.queue._source_reader_registry = readers
            readers.check()
            return readers

    @staticmethod
    def _source_reader_unfenced(db, job_id, generation):
        from .reference_source_readers import SourceReaders
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='source_reader_fences'").fetchone():
            SourceReaders._unfenced(db, job_id, generation)

    def _source_read_authorizer(self, job_id, generation, token_sha, grant_json, *, range_only=False, clock=None):
        expiry = datetime.fromisoformat(json.loads(grant_json)['expiresAt'].replace('Z', '+00:00')).timestamp()
        previous_now = None
        def authorize(db):
            nonlocal previous_now
            now = (clock or time.time)()
            if (type(now) not in (int, float) or not math.isfinite(now) or now >= expiry
                    or (previous_now is not None and now < previous_now)):
                raise Conflict('source grant expired before read admission')
            previous_now = now
            job = self._row(db, job_id)
            grant = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
            token = db.execute('SELECT job_id,generation FROM reference_source_grant_tokens WHERE token_sha256=?', (token_sha,)).fetchone()
            if (job['generation'] != generation or job['status'] != 'running'
                    or job['phase'] not in (('fetching',) if range_only else ('fetching', 'processing'))
                    or job['source_state'] != 'accepted' or job['derived_state'] != 'accepted'
                    or not grant or grant['grant_json'] != grant_json
                    or not token or (token['job_id'], token['generation']) != (job_id, generation)):
                raise Conflict('source grant changed before read admission')
        return authorize

    def _registered_source_reader(self, *, job_id, generation, object_id, authorize, open_reader):
        handle = self.source_readers().reserve(job_id, generation, object_id, authorize=authorize, quota=self._quota)
        try:
            reader, metadata = open_reader()
            return handle.attach(reader, authorize=authorize), metadata
        except Exception as error:
            from .source_spool import SourceSpoolCloseUncertain
            if isinstance(error, SourceSpoolCloseUncertain):
                handle.uncertain = True
            if not handle.closed and not handle.uncertain:
                handle.close()
            raise

    def _registered_source_check(self, *, job_id, generation, object_id, authorize, check):
        """Pin one synchronous NAS check that owns its descriptor from open to close.

        Nothing escapes the call, so the pin covers the whole check and the claim
        is reauthorized once it returns. An unknown close keeps the pin.
        """
        from .source_spool import SourceSpoolCloseUncertain
        handle = self.source_readers().reserve(job_id, generation, object_id, authorize=authorize, quota=self._quota)
        try:
            result = check()
            handle.check_authority(authorize=authorize)
            return result
        except BaseException as error:
            if isinstance(error, SourceSpoolCloseUncertain):
                handle.uncertain = True
            raise
        finally:
            if not handle.uncertain:
                handle.close()

    def _source_range_metadata_bytes(self, db):
        """Account every persisted source-range journal field in the queue quota."""
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        queries = {
            'reference_source_grant_renewals': '''SELECT COALESCE(SUM(
                length(CAST(job_id AS BLOB))+length(CAST(sequence AS TEXT))+length(CAST(renewal_id AS BLOB))+
                length(CAST(request_json AS BLOB))+length(CAST(request_sha256 AS BLOB))+
                length(CAST(previous_grant_sha256 AS BLOB))+length(CAST(successor_grant_json AS BLOB))+
                length(CAST(successor_sha256 AS BLOB))+length(CAST(acked AS TEXT))+length(CAST(created_at AS BLOB))),0)
                FROM reference_source_grant_renewals''',
            'reference_source_range_usage': '''SELECT COALESCE(SUM(length(CAST(job_id AS BLOB))+
                length(CAST(generation AS TEXT))+length(CAST(reservation_id AS BLOB))+
                length(CAST(requests_used AS TEXT))+length(CAST(bytes_used AS TEXT))+
                length(CAST(attempts_used AS TEXT))),0) FROM reference_source_range_usage''',
            'reference_source_range_requests': '''SELECT COALESCE(SUM(length(CAST(job_id AS BLOB))+
                length(CAST(generation AS TEXT))+length(CAST(request_id AS BLOB))+
                length(CAST(object_id AS BLOB))+length(CAST(range_start AS TEXT))+
                length(CAST(range_length AS TEXT))+length(CAST(attempts_used AS TEXT))),0)
                FROM reference_source_range_requests''',
            'reference_source_object_pins': '''SELECT COALESCE(SUM(length(CAST(job_id AS BLOB))+
                length(CAST(generation AS TEXT))+length(CAST(object_id AS BLOB))+
                length(CAST(byte_size AS TEXT))+length(CAST(sha256 AS BLOB))+length(CAST(mount_id AS BLOB))+
                length(CAST(device AS BLOB))+length(CAST(inode AS BLOB))+length(CAST(mode AS TEXT))+
                length(CAST(link_count AS TEXT))+length(CAST(owner_uid AS TEXT))+
                length(CAST(mtime_ns AS TEXT))+length(CAST(ctime_ns AS TEXT))),0)
                FROM reference_source_object_pins''',
            'reference_source_grant_tokens': '''SELECT COALESCE(SUM(length(CAST(token_sha256 AS BLOB))+
                length(CAST(job_id AS BLOB))+length(CAST(generation AS TEXT))+length(CAST(created_at AS BLOB))),0)
                FROM reference_source_grant_tokens''',
        }
        return sum(db.execute(query).fetchone()[0] for table, query in queries.items() if table in tables)

    @staticmethod
    def _source_renewal_node_ready(db, node_id):
        """Renewal continues an exact existing claim, so a drain allows it.

        Maintenance waits for active transfers and work, and a drained node
        still admits no new claim. An update or a hold issues nothing.
        """
        node = db.execute('SELECT state FROM nodes WHERE id=?', (node_id,)).fetchone()
        if node is None or node['state'] not in ('active', 'scheduled', 'draining'):
            raise Conflict('renewal_authority_unavailable')
        return node['state']

    @staticmethod
    def _source_claim_custody(db, job_id):
        """Claim fields that must hold across NAS I/O; cheap enough per object."""
        state = db.execute('''SELECT r.generation,r.phase,r.node_id,r.environment,r.manifest_sha,
            j.status,j.node_id AS queue_node,j.worker_id AS queue_worker,j.claim_hash,
            c.request_id,c.node_id AS claim_node,c.worker_id AS claim_worker,c.instance_id,c.token,
            s.source_state,s.derived_state,g.digest
            FROM reference_jobs r JOIN jobs j ON j.id=r.job_id
            LEFT JOIN reference_claims c ON c.job_id=r.job_id
            LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
            LEFT JOIN reference_transfer_grants g ON g.job_id=r.job_id
            WHERE r.job_id=?''', (job_id,)).fetchone()
        return dict(state) if state else None

    def _source_claim_still_owned(self, job_id, node_id, claim_custody, changed, *, first_grant_may_land=False):
        """Reader authorizer: the claim snapshotted before NAS I/O still holds.

        An initial mint can lose its CAS to an identical verifier. That winner's
        grant is the one custody field allowed to appear, and only from nothing;
        the mint's own commit then checks it binding by binding.
        """
        def still_owned(db):
            self._source_renewal_node_ready(db, node_id)
            current = self._source_claim_custody(db, job_id)
            if first_grant_may_land and current and claim_custody['digest'] is None:
                current = {**current, 'digest': None}
            if current != claim_custody:
                raise Conflict(changed)
        return still_owned

    @staticmethod
    def _source_range_ledger(db, job_id, generation):
        """Aggregate the durable range journal; one row per range would be unbounded."""
        usage = db.execute('''SELECT reservation_id,requests_used,bytes_used,attempts_used
            FROM reference_source_range_usage WHERE job_id=? AND generation=?''',
            (job_id, generation)).fetchone()
        totals = db.execute('''SELECT COUNT(*),COALESCE(SUM(range_length),0),COALESCE(SUM(attempts_used),0)
            FROM reference_source_range_requests WHERE job_id=? AND generation=?''',
            (job_id, generation)).fetchone()
        objects = db.execute('''SELECT DISTINCT object_id FROM reference_source_range_requests
            WHERE job_id=? AND generation=? ORDER BY object_id''', (job_id, generation)).fetchall()
        return {'usage': dict(usage) if usage else None, 'totals': list(totals),
                'objects': [item[0] for item in objects]}

    @staticmethod
    def _reconcile_source_range_ledger(ledger, grant, *, next_attempt=True):
        """Refuse unless served ranges fit the original budget, with room for one more.

        ``next_attempt=False`` asks only that the ledger is sound: a finished
        transfer may have spent every attempt and still needs nothing further.
        """
        usage = ledger['usage']
        if usage is None:
            if ledger['totals'] != [0, 0, 0] or ledger['objects']:
                raise Conflict('source renewal range ledger changed')
            return
        if (usage['reservation_id'] != grant.identity.reservation_id
                or ledger['totals'] != [usage['requests_used'], usage['bytes_used'], usage['attempts_used']]
                or not set(ledger['objects']).issubset(item.id for item in grant.objects)):
            raise Conflict('source renewal range ledger changed')
        if (usage['requests_used'] > grant.request_budget or usage['bytes_used'] > grant.byte_budget
                or usage['attempts_used'] > grant.attempt_budget - (1 if next_attempt else 0)):
            raise Conflict('source renewal budget exhausted')

    @staticmethod
    def _source_retention_bound(row, job_id):
        """Both retention receipts, digest-exact and bound to this job and attempt."""
        source_receipt=json.loads(row['source_receipt_json'])
        derived_receipt=json.loads(row['derived_receipt_json'])
        if (type(source_receipt) is not dict or type(derived_receipt) is not dict
                or protocol.digest(source_receipt)!=row['source_receipt_sha']
                or protocol.digest(derived_receipt)!=row['derived_receipt_sha']
                or source_receipt.get('receiptType')!='source_retention'
                or derived_receipt.get('receiptType')!='attempt_derived_input'
                or (source_receipt.get('jobId'),source_receipt.get('attemptId'),source_receipt.get('generation'),
                    source_receipt.get('environment'),source_receipt.get('pipelineManifestSha256'),
                    source_receipt.get('sourceManifestSha256')) !=
                   (job_id,row['submission_id'],row['generation'],row['environment'],
                    row['pipeline_manifest_sha'],row['source_manifest_sha'])
                or (derived_receipt.get('jobId'),derived_receipt.get('attemptId'),derived_receipt.get('generation'),
                    derived_receipt.get('environment'),derived_receipt.get('derivedManifestSha256')) !=
                   (job_id,row['submission_id'],row['generation'],row['environment'],row['derived_manifest_sha'])):
            raise Conflict('source renewal retention receipts changed')

    def _source_custody_open(self, db, row, job_id):
        """Accepted, retained custody with no fence or release against this attempt."""
        self._source_reader_unfenced(db, job_id, row['generation'])
        if (db.execute("SELECT 1 FROM sqlite_master WHERE name='source_releases'").fetchone()
                and db.execute('SELECT 1 FROM source_releases WHERE job_id=? LIMIT 1', (job_id,)).fetchone()):
            raise Conflict('source release recorded')
        if (row['source_state'], row['derived_state']) != ('accepted', 'accepted'):
            raise Conflict('source custody not accepted')
        self._source_retention_bound(row, job_id)
        self._source_accepted_custody(db, row, job_id)

    def _source_preflight_closure(self, db, job_id, *, clock=time.time):
        """The closure of a transfer that has not begun, or Conflict.

        Nothing has been served, so there are no renewals and no range usage. With
        no grant the reservation must still be live; minting re-verifies the bytes.
        A grant, even one the worker never heard about, takes the full closure.
        """
        row = self._row(db, job_id)
        generation = row['generation']
        # These tables first appear at a mint, so a preflight read must not need them.
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='reference_source_range_usage'").fetchone():
            if db.execute('SELECT 1 FROM reference_source_grant_renewals WHERE job_id=? LIMIT 1', (job_id,)).fetchone():
                raise Conflict('source transfer already renewed')
            ranges = self._source_range_ledger(db, job_id, generation)
            if ranges['usage'] is not None or ranges['totals'] != [0, 0, 0] or ranges['objects']:
                raise Conflict('source transfer already began')
        stored = db.execute('SELECT digest FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
        if stored is not None:
            if row['transfer_json'] is None:
                raise Conflict('source grant without reservation')
            return self._source_fetch_closure(db, job_id)
        self._source_custody_open(db, row, job_id)
        if row['transfer_json'] is None:
            for table in ('reference_source_grant_tokens','reference_source_object_pins',
                          'reference_source_grant_renewals','reference_source_range_usage','reference_source_range_requests'):
                if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
                        and db.execute(f'SELECT 1 FROM {table} WHERE job_id=? LIMIT 1',(job_id,)).fetchone()):
                    raise Conflict('source effects without reservation')
        transfer = json.loads(row['transfer_json']) if row['transfer_json'] is not None else None
        if transfer is not None and datetime.fromisoformat(transfer['expiresAt'].replace('Z', '+00:00')).timestamp() <= clock():
            raise Conflict('source reservation expired')
        receipts = [dict(item) for item in db.execute('''SELECT generation,object_id,byte_size,sha256,mount_id,
            storage_key,receipt_json FROM reference_source_object_receipts WHERE job_id=? ORDER BY object_id''', (job_id,))]
        return {'closureSha256': protocol.digest(dict(transfer=row['transfer_json'], grant=None,
                    source=row['source_manifest_sha'], derived=row['derived_manifest_sha'],
                    retention=[row['source_receipt_sha'], row['derived_receipt_sha']], receipts=receipts)),
                'grantSha256': None, 'predecessorGrantSha256': None, 'renewalSequence': 0, 'renewalPending': False,
                'requestsAdmitted': 0, 'bytesAdmitted': 0, 'attemptsAdmitted': 0, 'objectCount': len(receipts)}

    def _source_accepted_custody(self, db, row, job_id):
        """Every retained object against the accepted manifests, granted or not.

        Stored SHA fields prove nothing alone, so each manifest is recomputed from
        its JSON and tied to the signed proof and retention receipts. A receipt
        must be the exact spool receipt for its manifest entry, which covers the
        raw inputs a grant excludes. Metadata only: no NAS read happens here.
        """
        from .source_spool import SourceSpool
        manifests = (('source', json.loads(row['source_manifest_json']), row['source_manifest_sha']),
                     ('derived', json.loads(row['derived_manifest_json']), row['derived_manifest_sha']))
        for _kind, manifest, accepted in manifests:
            if manifest.get('sha256') != accepted or protocol.manifest_sha256(manifest) != accepted:
                raise Conflict('source manifest differs from accepted custody')
        self._source_acceptance_binding(row)
        receipts = [dict(item) for item in db.execute('''SELECT generation,object_id,object_kind,byte_size,sha256,
            mount_id,storage_key,receipt_json FROM reference_source_object_receipts WHERE job_id=?''', (job_id,))]
        self._source_completion_ack(row, receipts)
        if len({item['mount_id'] for item in receipts}) != 1:
            raise Conflict('source custody storage identity changed')
        objects = {item['id']: item for _kind, manifest, _sha in manifests for item in manifest['objects']}
        for receipt in receipts:
            item = objects[receipt['object_id']]
            expected = dict(schemaVersion=1, jobId=job_id, generation=row['generation'], objectId=item['id'],
                storageKey=SourceSpool.storage_key(job_id, row['generation'], item['id']), byteSize=item['byteSize'],
                sha256=item['sha256'], mountId=receipt['mount_id'])
            if (receipt['generation'] != row['generation'] or receipt['storage_key'] != expected['storageKey']
                    or receipt['receipt_json'] != _json(expected)):
                raise Conflict('source custody receipt changed')

    def _source_fetch_closure(self, db, job_id):
        """The exact persisted transfer a restarted coordinator may resume, or Conflict.

        Metadata only, inside the caller's transaction: no NAS I/O, because every
        range read rechecks its object anyway. Old renewals are checked through
        their hash links; only the newest row's bodies are re-read, since each
        body repeats the whole object list and there can be 1000 of them.
        """
        from .source_range_transfer import MAX_RENEWAL_SEQUENCE, validate_source_grant
        row = self._row(db, job_id)
        generation = row['generation']
        self._source_custody_open(db, row, job_id)
        stored = db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?', (job_id,)).fetchone()
        if stored is None:
            raise Conflict('source grant unavailable')
        grant = json.loads(stored['grant_json'])
        if stored['digest'] != protocol.digest(grant):
            raise Conflict('source grant changed')
        checked = validate_source_grant(grant, job=self._record(row, True), allow_expired=True, past_reservation=True)
        if not db.execute('SELECT 1 FROM reference_source_grant_tokens WHERE token_sha256=? AND job_id=? AND generation=?',
                (hashlib.sha256(grant['token'].encode('ascii')).hexdigest(), job_id, generation)).fetchone():
            raise Conflict('source grant token unavailable')

        chain = db.execute('''SELECT sequence,previous_grant_sha256,successor_sha256,acked
            FROM reference_source_grant_renewals WHERE job_id=? ORDER BY sequence LIMIT ?''',
            (job_id, MAX_RENEWAL_SEQUENCE + 1)).fetchall()
        if len(chain) > MAX_RENEWAL_SEQUENCE:
            raise Conflict('source renewal chain exceeds its bound')
        for index, link in enumerate(chain):
            if (link['sequence'] != index + 1
                    or index and link['previous_grant_sha256'] != chain[index - 1]['successor_sha256']
                    or index + 1 < len(chain) and not link['acked']):
                raise Conflict('source renewal chain broken')
        pending = bool(chain) and not chain[-1]['acked']
        if chain:
            newest = db.execute('''SELECT request_json,request_sha256,successor_grant_json,successor_sha256
                FROM reference_source_grant_renewals WHERE job_id=? AND sequence=?''',
                (job_id, chain[-1]['sequence'])).fetchone()
            if (protocol.digest(json.loads(newest['request_json'])) != newest['request_sha256']
                    or protocol.digest(json.loads(newest['successor_grant_json'])) != newest['successor_sha256']
                    or stored['digest'] != chain[-1]['previous_grant_sha256' if pending else 'successor_sha256']):
                raise Conflict('source renewal chain broken')

        ranges = self._source_range_ledger(db, job_id, generation)
        self._reconcile_source_range_ledger(ranges, checked, next_attempt=False)
        expected = {item['id'] for name in ('source_manifest_json', 'derived_manifest_json')
                    for item in json.loads(row[name])['objects']}
        receipts = [dict(item) for item in db.execute('''SELECT generation,object_id,byte_size,sha256,mount_id,
            storage_key,receipt_json FROM reference_source_object_receipts WHERE job_id=? ORDER BY object_id''', (job_id,))]
        pins = [dict(item) for item in db.execute('''SELECT generation,object_id,byte_size,sha256,mount_id,device,inode,
            mode,link_count,owner_uid,mtime_ns,ctime_ns FROM reference_source_object_pins
            WHERE job_id=? ORDER BY object_id''', (job_id,))]
        by_id = {item['object_id']: item for item in receipts}
        pinned = {item['object_id']: item for item in pins}
        if (len(by_id) != len(receipts) or set(by_id) != expected or len(pinned) != len(pins)
                or set(pinned) != {item.id for item in checked.objects}):
            raise Conflict('source custody closure changed')
        for item in checked.objects:
            receipt, pin = by_id[item.id], pinned[item.id]
            stored_receipt = json.loads(receipt['receipt_json'])
            if ((receipt['generation'], receipt['byte_size'], receipt['sha256']) != (generation, item.byte_size, item.sha256)
                    or (pin['generation'], pin['byte_size'], pin['sha256'], pin['mount_id']) !=
                       (generation, item.byte_size, item.sha256, receipt['mount_id'])
                    or (stored_receipt.get('jobId'), stored_receipt.get('generation'), stored_receipt.get('objectId'),
                        stored_receipt.get('byteSize'), stored_receipt.get('sha256'), stored_receipt.get('mountId'),
                        stored_receipt.get('storageKey')) !=
                       (job_id, generation, item.id, item.byte_size, item.sha256, receipt['mount_id'], receipt['storage_key'])):
                raise Conflict('source custody binding changed')
        usage = ranges['usage'] or dict(requests_used=0, bytes_used=0, attempts_used=0)
        return {'closureSha256': protocol.digest(dict(transfer=row['transfer_json'], grant=stored['digest'],
                    source=row['source_manifest_sha'], derived=row['derived_manifest_sha'],
                    retention=[row['source_receipt_sha'], row['derived_receipt_sha']],
                    chain=[list(item) for item in chain], ranges=ranges, receipts=receipts, pins=pins)),
                'grantSha256': stored['digest'],
                'predecessorGrantSha256': chain[-1]['previous_grant_sha256'] if chain else None,
                'renewalSequence': len(chain), 'renewalPending': pending,
                'requestsAdmitted': usage['requests_used'], 'bytesAdmitted': usage['bytes_used'],
                'attemptsAdmitted': usage['attempts_used'], 'objectCount': len(checked.objects)}

    def renew_source_transfer_grant(self, request, *, source_spool, worker=None, clock=None):
        """Create or replay one durable, claim-bound V2 grant successor.

        ``worker`` is the authenticated identity plus its claim token. The
        expired grant only names the journal step; every issue or replay needs
        the exact owner of the current claim. The reservation deadline caps a
        successor while it is open. Afterwards the claim still owns the
        transfer, so a successor gets one grant lifetime and no more.
        """
        _ensure_source_range_schema(self.queue)
        import hashlib
        import hmac
        import math
        from datetime import datetime, timezone
        from .coordinator_freeze import CoordinatorFrozen
        from .source_range_transfer import MAX_RENEWAL_SEQUENCE, validate_source_grant

        if source_spool is None or type(request) is not dict:
            raise Conflict('renewal_authority_unavailable')
        if type(worker) is not dict or set(worker) != {'node_id', 'worker_id', 'worker_instance_id', 'claim_token'}:
            raise Conflict('source renewal worker required')
        protocol._closed(request, ('protocol','renewalId','sequence','previousGrantSha256','previousGrant'),
                         'source range renewal')
        if request.get('protocol') != 'source-range-v2-renewal':
            raise Conflict('source renewal request invalid')
        protocol._slug(request.get('renewalId'), 'renewal ID')
        sequence = request.get('sequence')
        if type(sequence) is not int or not 1 <= sequence <= MAX_RENEWAL_SEQUENCE:
            raise Conflict('source renewal sequence invalid')
        previous = request.get('previousGrant')
        if type(previous) is not dict:
            raise Conflict('source renewal grant invalid')
        protocol._hash(request.get('previousGrantSha256'),'previous grant digest')
        previous_sha = hashlib.sha256(protocol.canonical_json(previous)).hexdigest()
        if not hmac.compare_digest(previous_sha, request.get('previousGrantSha256', '')):
            raise Conflict('source renewal grant digest mismatch')
        request_encoded = _json(request)
        request_sha = protocol.digest(request)
        read_clock = time.time if clock is None else clock
        previous_now = None

        def now():
            nonlocal previous_now
            value = read_clock()
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise Conflict('invalid source grant clock')
            if previous_now is not None and value < previous_now:
                raise Conflict('source grant clock moved backwards')
            previous_now = value
            return value

        def capture(db, job_id):
            row = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, row['generation'])
            claim = db.execute('''SELECT request_id,job_id,node_id,worker_id,instance_id,token
                FROM reference_claims WHERE job_id=?''', (job_id,)).fetchone()
            receipts = db.execute('''SELECT generation,object_id,object_kind,byte_size,sha256,mount_id,storage_key,receipt_json
                FROM reference_source_object_receipts WHERE job_id=? ORDER BY generation,object_id''',
                (job_id,)).fetchall()
            grant = db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',
                               (job_id,)).fetchone()
            pins = db.execute('''SELECT generation,object_id,byte_size,sha256,mount_id,device,inode,mode,
                link_count,owner_uid,mtime_ns,ctime_ns FROM reference_source_object_pins
                WHERE job_id=? ORDER BY generation,object_id''',(job_id,)).fetchall()
            return (row, dict(claim) if claim else None, [dict(x) for x in receipts],
                    dict(grant) if grant else None, [dict(x) for x in pins])

        def authorize(db, job_id):
            """The exact claim owner, for the exact job and generation it holds."""
            binding = self._claim_snapshot(db, job_id, **worker)
            if ((previous.get('generation'), previous.get('environment'), previous.get('manifestSha256'))
                    != (binding['generation'], binding['environment'], binding['manifestSha256'])):
                raise Conflict('source renewal authority unavailable')
            self._source_renewal_node_ready(db, worker['node_id'])
            return {'binding': binding,
                    'ranges': self._source_range_ledger(db, job_id, binding['generation'])}

        def replay(db, job_id, job):
            # Exact retries, including retries after a lost response, get the
            # same already-minted successor. Never mint on a changed request.
            prior = db.execute('''SELECT request_json,request_sha256,previous_grant_sha256,
                successor_grant_json,successor_sha256 FROM reference_source_grant_renewals
                WHERE job_id=? AND (sequence=? OR renewal_id=?)''',
                               (job_id, sequence, request['renewalId'])).fetchall()
            if not prior:
                return None
            if (len(prior) != 1 or prior[0]['request_json'] != request_encoded
                    or prior[0]['request_sha256'] != request_sha
                    or prior[0]['previous_grant_sha256'] != previous_sha
                    or prior[0]['successor_sha256'] != protocol.digest(json.loads(prior[0]['successor_grant_json']))):
                raise Conflict('source renewal replay conflict')
            if job['status'] != 'running' or job['phase'] != 'fetching':
                raise Conflict('source renewal authority unavailable')
            successor = json.loads(prior[0]['successor_grant_json'])
            return {'protocol':'source-range-v2-renewal','renewalId':request['renewalId'],
                'sequence':sequence,'previousGrantSha256':previous_sha,'grant':successor}

        job_id = previous.get('jobId')
        protocol._slug(job_id, 'job ID')
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current_job = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, current_job['generation'])
            authority = authorize(db, job_id)
            replayed = replay(db, job_id, current_job)
            if replayed is not None:
                return replayed
            row, claim, receipt_rows, grant_row, pin_rows = capture(db, job_id)
            latest = db.execute('SELECT COALESCE(MAX(sequence),0) FROM reference_source_grant_renewals WHERE job_id=?',
                                (job_id,)).fetchone()[0]
            if latest + 1 != sequence:
                raise Conflict('source renewal sequence conflict')
            snapshot = {'row': dict(row), 'claim': claim, 'receipts': receipt_rows,
                        'grant': grant_row, 'pins': pin_rows, 'latest': latest, 'authority': authority}
            claim_custody = self._source_claim_custody(db, job_id)
            db.commit()

        request_now = now()
        if (row['status'] != 'running' or row['phase'] != 'fetching'
                or claim is None or claim['token'] != row['private_claim_token']
                or not row['claim_hash']
                or not hmac.compare_digest(hashlib.sha256(claim['token'].encode()).hexdigest(), row['claim_hash'])):
            raise Conflict('source renewal authority unavailable')
        if (grant_row is None or grant_row['digest'] != protocol.digest(previous)
                or json.loads(grant_row['grant_json']) != previous):
            raise Conflict('source renewal previous grant changed')
        private = self._record(row, True)
        try:
            self._source_retention_bound(row, job_id)
            reservation_expiry = datetime.fromisoformat(
                private['transferRequest']['expiresAt'].replace('Z','+00:00')).timestamp()
            if not math.isfinite(reservation_expiry):
                raise Conflict('renewal_authority_unavailable')
            old = validate_source_grant(previous, job=private, clock=lambda: request_now,
                                        allow_expired=True, past_reservation=True)
            if old.expires > request_now:
                raise Conflict('grant_not_expired')
            self._reconcile_source_range_ledger(authority['ranges'], old)
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            expected = {item['id']: item for item in source['objects']}
            expected.update({item['id']: item for item in derived['objects']})
            granted_ids = {item.id for item in old.objects}
            if not granted_ids or not granted_ids.issubset(expected) or len(receipt_rows) != len(expected):
                raise Conflict('source renewal custody closure changed')
            by_id = {item['object_id']: item for item in receipt_rows}
            if set(by_id) != set(expected):
                raise Conflict('source renewal custody receipt missing')
            pins_by_id = {pin['object_id']:pin for pin in pin_rows if pin['generation']==row['generation']}
            if len(pins_by_id)!=len(pin_rows) or set(pins_by_id)!=granted_ids:
                raise Conflict('source renewal pins unavailable')

            still_owned = self._source_claim_still_owned(job_id, worker['node_id'], claim_custody,
                'source renewal authority changed during verification')
            for item in old.objects:
                stored = by_id[item.id]
                if (stored['generation'] != row['generation'] or stored['byte_size'] != item.byte_size
                        or stored['sha256'] != item.sha256 or stored['mount_id'] != source_spool.mount_id
                        or stored['storage_key'] != source_spool.storage_key(job_id,row['generation'],item.id)):
                    raise Conflict('source renewal custody binding changed')
                receipt = json.loads(stored['receipt_json'])
                if ((receipt.get('jobId'),receipt.get('generation'),receipt.get('objectId'),
                     receipt.get('byteSize'),receipt.get('sha256'),receipt.get('mountId'),receipt.get('storageKey'))
                        != (job_id,row['generation'],item.id,item.byte_size,item.sha256,
                            source_spool.mount_id,stored['storage_key'])):
                    raise Conflict('source renewal receipt changed')
                # Each NAS descriptor is a registered read, so freeze, release
                # and Queue.close wait for it and an unknown close keeps its pin.
                handle,_metadata = self._registered_source_reader(job_id=job_id, generation=row['generation'],
                    object_id=item.id, authorize=still_owned,
                    open_reader=lambda item=item: (_open_pinned_source(source_spool,job_id,row['generation'],
                        item.id,pins_by_id[item.id])[0], None))
                handle.close()
            verified_at = now()
            expiry = verified_at + SOURCE_RANGE_GRANT_TTL_SECONDS
            if reservation_expiry > verified_at:
                expiry = min(expiry, reservation_expiry)
            if expiry <= max(verified_at, old.expires):
                raise Conflict('renewal_authority_unavailable')
            grant_expiry = datetime.fromtimestamp(expiry, timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
            successor = dict(previous)
            successor.update(expiresAt=grant_expiry, token=secrets.token_urlsafe(32))
            validate_source_grant(successor, job=private, clock=lambda: verified_at, past_reservation=True)
        except (Conflict, CoordinatorFrozen):
            raise
        except Exception:
            raise Conflict('renewal_authority_unavailable') from None

        response = {'protocol':'source-range-v2-renewal','renewalId':request['renewalId'],
            'sequence':sequence,'previousGrantSha256':previous_sha,'grant':successor}
        successor_encoded = _json(successor)
        successor_sha = protocol.digest(successor)
        commit_now = now()
        if expiry <= commit_now or expiry <= old.expires:
            raise Conflict('renewal_authority_unavailable')
        validate_source_grant(successor,job=private,clock=lambda:commit_now,past_reservation=True)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current, current_claim, current_receipts, current_grant, current_pins = capture(db, job_id)
            current_authority = authorize(db, job_id)
            # A retry can overtake the slow original; the one that lost the
            # race answers with the winner's successor instead of refusing.
            replayed = replay(db, job_id, current)
            if replayed is not None:
                return replayed
            current_latest = db.execute('SELECT COALESCE(MAX(sequence),0) FROM reference_source_grant_renewals WHERE job_id=?',
                                        (job_id,)).fetchone()[0]
            if (_json({'row':dict(current),'claim':current_claim,'receipts':current_receipts,
                       'grant':current_grant,'pins':current_pins,'latest':current_latest,
                       'authority':current_authority})
                    != _json(snapshot)):
                raise Conflict('source renewal authority changed during verification')
            latest = db.execute('SELECT COALESCE(MAX(sequence),0) FROM reference_source_grant_renewals WHERE job_id=?',
                                (job_id,)).fetchone()[0]
            if latest + 1 != sequence:
                raise Conflict('source renewal sequence conflict')
            renewal_metadata = _metadata_field_bytes(job_id, sequence, request['renewalId'], request_encoded,
                request_sha, previous_sha, successor_encoded, successor_sha, 0, '0000-00-00 00:00:00')
            self._quota(db, renewal_metadata)
            db.execute('''INSERT INTO reference_source_grant_renewals
                (job_id,sequence,renewal_id,request_json,request_sha256,previous_grant_sha256,
                 successor_grant_json,successor_sha256) VALUES(?,?,?,?,?,?,?,?)''',
                (job_id,sequence,request['renewalId'],request_encoded,request_sha,previous_sha,
                 successor_encoded,successor_sha))
        return response

    def acknowledge_source_transfer_grant(self, request, *, worker=None, clock=None):
        """Install a previously returned successor only after its exact ACK.

        A successor can expire while its ACK is outstanding, for instance across
        a coordinator outage. Installing it keeps the journal chain intact and
        gives no access; the worker then renews from it as usual.
        """
        _ensure_source_range_schema(self.queue)
        import hashlib
        import hmac
        from .source_range_transfer import validate_source_grant
        if type(worker) is not dict or set(worker) != {'node_id', 'worker_id', 'worker_instance_id'}:
            raise Conflict('source renewal worker required')
        protocol._closed(request, ('protocol','renewalId','sequence','grantSha256','token'), 'source renewal ACK')
        if request.get('protocol') != 'source-range-v2-renewal-ack':
            raise Conflict('source renewal ACK invalid')
        protocol._slug(request.get('renewalId'), 'renewal ID')
        protocol._hash(request.get('grantSha256'), 'source grant digest')
        if type(request.get('sequence')) is not int or request['sequence'] < 1:
            raise Conflict('source renewal ACK invalid')
        token = request.get('token')
        if type(token) is not str or not 1 <= len(token) <= 4096 or not token.isascii():
            raise Conflict('source renewal ACK invalid')
        token_sha = hashlib.sha256(token.encode('ascii')).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('''SELECT job_id,sequence,successor_grant_json,successor_sha256,acked
                FROM reference_source_grant_renewals WHERE renewal_id=? AND sequence=?''',
                (request['renewalId'],request['sequence'])).fetchone()
            if row is None:
                raise Conflict('source renewal ACK unavailable')
            job_id = row['job_id']; successor = json.loads(row['successor_grant_json'])
            if (row['successor_sha256'] != request['grantSha256']
                    or row['successor_sha256'] != protocol.digest(successor)
                    or not hmac.compare_digest(successor.get('token',''),token)):
                raise Conflict('source renewal ACK binding changed')
            response = {'protocol':'source-range-v2-renewal-ack','renewalId':request['renewalId'],
                'sequence':request['sequence'],'grantSha256':request['grantSha256']}
            current = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, current['generation'])
            claim = db.execute('SELECT node_id,worker_id,instance_id,token FROM reference_claims WHERE job_id=?',
                               (job_id,)).fetchone()
            if (claim is None or (claim['node_id'],claim['worker_id'],claim['instance_id']) !=
                    (worker['node_id'],worker['worker_id'],worker['worker_instance_id'])):
                raise Conflict('source renewal authority unavailable')
            if row['acked']:
                return response
            self._source_renewal_node_ready(db, worker['node_id'])
            if (current['status'] != 'running' or current['phase'] != 'fetching'
                    or claim is None or claim['token'] != current['private_claim_token']
                    or not current['claim_hash']
                    or not hmac.compare_digest(hashlib.sha256(claim['token'].encode()).hexdigest(),current['claim_hash'])):
                raise Conflict('source renewal authority unavailable')
            private = self._record(current, True)
            validate_source_grant(successor, job=private, clock=clock or time.time,
                                  allow_expired=True, past_reservation=True)
            current_grant = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?',(job_id,)).fetchone()
            renew = db.execute('SELECT previous_grant_sha256 FROM reference_source_grant_renewals WHERE job_id=? AND sequence=?',
                               (job_id,request['sequence'])).fetchone()
            if current_grant is None or protocol.digest(json.loads(current_grant['grant_json'])) != renew['previous_grant_sha256']:
                raise Conflict('source renewal predecessor changed')
            successor_encoded = _json(successor)
            token_metadata = _metadata_field_bytes(token_sha, job_id, current['generation'],
                '0000-00-00 00:00:00')
            self._quota(db, max(0, len(successor_encoded.encode('utf-8'))-
                                len(current_grant['grant_json'].encode('utf-8'))) + token_metadata)
            db.execute('UPDATE reference_transfer_grants SET grant_json=?,digest=? WHERE job_id=?',
                       (successor_encoded,row['successor_sha256'],job_id))
            db.execute('INSERT INTO reference_source_grant_tokens(token_sha256,job_id,generation) VALUES(?,?,?)',
                       (token_sha,job_id,current['generation']))
            db.execute('UPDATE reference_source_grant_renewals SET acked=1 WHERE job_id=? AND sequence=?',
                       (job_id,request['sequence']))
            return response

    def source_range_for_grant(self, token, object_id, *, range_start, range_length,
                               request_id, if_match, source_spool, clock=None):
        """Authorize and open one canonical V2 range; client verifies the full object digest."""
        _ensure_source_range_schema(self.queue)
        import hashlib
        import os
        import stat
        from .source_range_transfer import CHUNK_BYTES, _request_id, SourceIdentity, validate_source_grant
        if type(token) is not str or not token or not token.isascii() or source_spool is None:
            raise Conflict('source grant unavailable')
        token_sha = hashlib.sha256(token.encode('ascii')).hexdigest()
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            token_row = db.execute('SELECT job_id,generation FROM reference_source_grant_tokens WHERE token_sha256=?',(token_sha,)).fetchone()
            if token_row is None:
                raise Conflict('source grant unavailable')
            job = self._row(db,token_row['job_id'])
            claim = db.execute('SELECT token FROM reference_claims WHERE job_id=?',(token_row['job_id'],)).fetchone()
            grant_row = db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',(token_row['job_id'],)).fetchone()
            if (grant_row is None or token_row['generation'] != job['generation'] or job['status'] != 'running'
                    or job['phase'] != 'fetching' or job['source_state'] != 'accepted' or job['derived_state'] != 'accepted'
                    or claim is None or claim['token'] != job['private_claim_token'] or not job['claim_hash']
                    or not hmac.compare_digest(hashlib.sha256(claim['token'].encode('ascii')).hexdigest(),job['claim_hash'])):
                raise Conflict('source grant execution is not active')
            grant = json.loads(grant_row['grant_json'])
            if grant_row['digest'] != protocol.digest(grant) or grant.get('token') != token:
                raise Conflict('source grant unavailable')
            private = self._record(job,True)
            renewed = db.execute('''SELECT 1 FROM reference_source_grant_renewals
                WHERE job_id=? AND successor_sha256=? AND acked=1''',
                (token_row['job_id'],grant_row['digest'])).fetchone()
            checked = validate_source_grant(grant,job=private,clock=clock or time.time,
                                            past_reservation=renewed is not None)
            item = next((entry for entry in checked.objects if entry.id == object_id),None)
            if item is None or type(range_start) is not int or type(range_length) is not int:
                raise Conflict('source range unavailable')
            expected_length = min(CHUNK_BYTES,item.byte_size-range_start)
            if (range_start < 0 or range_start >= item.byte_size or range_start % CHUNK_BYTES
                    or range_length != expected_length or if_match != '"sha256-'+item.sha256+'"'):
                raise Conflict('source range identity mismatch')
            identity = SourceIdentity(grant['environment'],grant['jobId'],grant['reservationId'],
                grant['generation'],grant['manifestSha256'],grant['sourceVersion'])
            identity_json = json.dumps(identity.canonical(),sort_keys=True,separators=(',',':'))
            if request_id != _request_id(identity_json,item,range_start,range_length):
                raise Conflict('source range request identity mismatch')
            usage = db.execute('SELECT requests_used,bytes_used,attempts_used,reservation_id FROM reference_source_range_usage WHERE job_id=? AND generation=?',
                               (token_row['job_id'],job['generation'])).fetchone()
            usage_exists = usage is not None
            used = tuple(usage) if usage_exists else (0,0,0,grant['reservationId'])
            if used[3] != grant['reservationId']:
                raise Conflict('source range reservation changed')
            prior = db.execute('SELECT object_id,range_start,range_length,attempts_used FROM reference_source_range_requests WHERE job_id=? AND generation=? AND request_id=?',
                               (token_row['job_id'],job['generation'],request_id)).fetchone()
            if prior:
                if (prior['object_id'],prior['range_start'],prior['range_length']) != (object_id,range_start,range_length):
                    raise Conflict('source range replay conflict')
                attempts = prior['attempts_used']+1
            else:
                if used[0]+1 > grant['requestBudget'] or used[1]+range_length > grant['byteBudget']:
                    raise Conflict('source range budget exhausted')
                attempts = 1
            if used[2]+1 > grant['attemptBudget']:
                raise Conflict('source range attempt budget exhausted')
            next_requests = used[0] + (0 if prior else 1)
            next_bytes = used[1] + (0 if prior else range_length)
            next_attempts = used[2] + 1
            if usage_exists:
                usage_growth = (_metadata_field_bytes(next_requests, next_bytes, next_attempts)
                                - _metadata_field_bytes(*used[:3]))
            else:
                usage_growth = _metadata_field_bytes(token_row['job_id'], job['generation'],
                    grant['reservationId'], next_requests, next_bytes, next_attempts)
            if prior:
                request_growth = _metadata_field_bytes(attempts)-_metadata_field_bytes(prior['attempts_used'])
            else:
                request_growth = _metadata_field_bytes(token_row['job_id'],job['generation'],request_id,
                    object_id,range_start,range_length,attempts)
            self._quota(db, max(0,usage_growth) + max(0,request_growth))
            if not usage_exists:
                db.execute('INSERT INTO reference_source_range_usage(job_id,generation,reservation_id,requests_used,bytes_used,attempts_used) VALUES(?,?,?,?,?,?)',
                    (token_row['job_id'],job['generation'],grant['reservationId'],next_requests,next_bytes,next_attempts))
            elif not prior:
                db.execute('UPDATE reference_source_range_usage SET requests_used=?,bytes_used=?,attempts_used=? WHERE job_id=? AND generation=?',
                    (next_requests,next_bytes,next_attempts,token_row['job_id'],job['generation']))
            else:
                db.execute('UPDATE reference_source_range_usage SET attempts_used=? WHERE job_id=? AND generation=?',
                    (next_attempts,token_row['job_id'],job['generation']))
            if not prior:
                db.execute('INSERT INTO reference_source_range_requests(job_id,generation,request_id,object_id,range_start,range_length,attempts_used) VALUES(?,?,?,?,?,?,?)',
                           (token_row['job_id'],job['generation'],request_id,object_id,range_start,range_length,attempts))
            else:
                db.execute('UPDATE reference_source_range_requests SET attempts_used=? WHERE job_id=? AND generation=? AND request_id=?',
                    (attempts,token_row['job_id'],job['generation'],request_id))
            receipt_row = db.execute('''SELECT byte_size,sha256,mount_id,storage_key,receipt_json FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? AND object_id=?''',(token_row['job_id'],job['generation'],object_id)).fetchone()
            pin = db.execute('''SELECT byte_size,sha256,mount_id,device,inode,mode,link_count,owner_uid,mtime_ns,ctime_ns
                FROM reference_source_object_pins WHERE job_id=? AND generation=? AND object_id=?''',
                (token_row['job_id'],job['generation'],object_id)).fetchone()
            if (receipt_row is None or (receipt_row['byte_size'],receipt_row['sha256'],receipt_row['mount_id'])
                    != (item.byte_size,item.sha256,source_spool.mount_id)
                    or receipt_row['storage_key'] != source_spool.storage_key(token_row['job_id'],job['generation'],object_id)
                    or pin is None or (pin['byte_size'],pin['sha256'],pin['mount_id']) !=
                        (item.byte_size,item.sha256,source_spool.mount_id)):
                raise Conflict('source object receipt changed')
            expected_receipt={'schemaVersion':1,'jobId':token_row['job_id'],'generation':job['generation'],
                'objectId':object_id,'storageKey':receipt_row['storage_key'],'byteSize':item.byte_size,
                'sha256':item.sha256,'mountId':source_spool.mount_id}
            if receipt_row['receipt_json'] != _json(expected_receipt):
                raise Conflict('source object receipt changed')
            pin_row=dict(pin)
        # The accepted source receipt was fully hashed before initial grant
        # minting, and the worker verifies the assembled full-object digest
        # before use. Here enforce no-follow, regular inode, owner, mount, link,
        # and size checks without rereading a 50GB object for every 8MB range.
        def open_reader():
            reader,file_identity=_open_pinned_source(source_spool,token_row['job_id'],job['generation'],object_id,pin_row)
            import os
            try:
                os.lseek(reader.fileno(),range_start,os.SEEK_SET)
            except Exception:
                reader.close()
                raise
            return reader, {'byteSize':item.byte_size,'rangeStart':range_start,'rangeLength':range_length,
                'sha256':item.sha256,'fileIdentity':file_identity}
        try:
            return self._registered_source_reader(job_id=token_row['job_id'], generation=job['generation'],
                object_id=object_id, authorize=self._source_read_authorizer(token_row['job_id'],
                    job['generation'], token_sha, grant_row['grant_json'], range_only=True, clock=clock), open_reader=open_reader)
        except Exception:
            raise Conflict('source_storage_unavailable') from None

    def _source_completion_ack(self, row, receipt_rows):
        """Project the immutable intake completion acknowledgement from durable receipts.

        This is the historical response consumed by Viewer upload recovery. It
        deliberately excludes live queue status, phase, node, and result data;
        GET job continues to expose those current values.
        """
        source = json.loads(row['source_manifest_json'])
        derived = json.loads(row['derived_manifest_json'])
        source_receipt = json.loads(row['source_receipt_json'])
        derived_receipt = json.loads(row['derived_receipt_json'])
        source_fields = {'schemaVersion', 'receiptType', 'receiptId', 'storageReceiptId', 'jobId',
            'attemptId', 'generation', 'environment', 'pipelineManifestSha256', 'sourceManifestSha256',
            'pipelineDigest', 'objects', 'retainedAt', 'retentionGuarantee'}
        derived_fields = {'schemaVersion', 'receiptType', 'receiptId', 'jobId', 'attemptId',
            'generation', 'environment', 'derivedManifestSha256', 'objects', 'retainedAt'}
        if (type(source_receipt) is not dict or type(derived_receipt) is not dict
                or set(source_receipt) != source_fields or set(derived_receipt) != derived_fields
                or protocol.digest(source_receipt) != row['source_receipt_sha']
                or protocol.digest(derived_receipt) != row['derived_receipt_sha']):
            raise Conflict('persisted source completion receipts are unavailable')
        receipt_ids = (source_receipt['receiptId'], source_receipt['storageReceiptId'],
                       derived_receipt['receiptId'])
        try:
            canonical_ids = tuple(str(uuid.UUID(value)) for value in receipt_ids)
        except (ValueError, TypeError, AttributeError):
            raise Conflict('persisted source completion receipt identity changed') from None
        if (canonical_ids != receipt_ids or len(set(receipt_ids)) != len(receipt_ids)
                or type(source_receipt['schemaVersion']) is not int or source_receipt['schemaVersion'] != 2
                or source_receipt['receiptType'] != 'source_retention'
                or source_receipt['retentionGuarantee'] != 'until_explicit_source_release_v1'
                or type(derived_receipt['schemaVersion']) is not int or derived_receipt['schemaVersion'] != 1
                or type(source_receipt['generation']) is not int
                or type(derived_receipt['generation']) is not int
                or derived_receipt['receiptType'] != 'attempt_derived_input'
                or source_receipt['retainedAt'] != derived_receipt['retainedAt']):
            raise Conflict('persisted source completion receipt identity changed')
        expected_source_receipt_objects = [{key: obj[key] for key in (
            'id', 'byteSize', 'sha256', 'sourceType', 'retentionClass', 'retentionHoldId')}
            for obj in source['objects']]
        expected_derived_receipt_objects = [{key: obj[key] for key in (
            'id', 'name', 'byteSize', 'sha256', 'snapshotAttemptId', 'snapshotContentSha256',
            'snapshotEvidenceSha256', 'sourceType', 'retentionHoldId', 'retentionClass')}
            for obj in derived['objects']]
        if (source_receipt.get('objects') != expected_source_receipt_objects
                or derived_receipt.get('objects') != expected_derived_receipt_objects
                or (source_receipt.get('jobId'), source_receipt.get('attemptId'),
                    source_receipt.get('generation'), source_receipt.get('environment'),
                    source_receipt.get('pipelineManifestSha256'), source_receipt.get('sourceManifestSha256'),
                    source_receipt.get('pipelineDigest')) !=
                   (row['job_id'], row['submission_id'], row['generation'], row['environment'],
                    row['pipeline_manifest_sha'], row['source_manifest_sha'], row['pipeline_digest'])
                or (derived_receipt.get('jobId'), derived_receipt.get('attemptId'),
                    derived_receipt.get('generation'), derived_receipt.get('environment'),
                    derived_receipt.get('derivedManifestSha256')) !=
                   (row['job_id'], row['submission_id'], row['generation'], row['environment'],
                    row['derived_manifest_sha'])):
            raise Conflict('persisted source completion receipt binding changed')
        by_id = {item['object_id']: item for item in receipt_rows
                 if item['generation'] == row['generation']}
        expected = {obj['id'] for obj in source['objects'] + derived['objects']}
        if len(by_id) != len(receipt_rows) or set(by_id) != expected:
            raise Conflict('persisted source completion receipt inventory changed')

        def acknowledgements(manifest, kind, manifest_sha):
            result = []
            for obj in manifest['objects']:
                stored = by_id.get(obj['id'])
                if (stored is None or stored['object_kind'] != kind
                        or stored['byte_size'] != obj['byteSize'] or stored['sha256'] != obj['sha256']):
                    raise Conflict('persisted source completion object acknowledgement changed')
                result.append({'objectId': obj['id'], 'objectKind': kind,
                    'byteSize': obj['byteSize'], 'sha256': obj['sha256'],
                    'generation': row['generation'], 'manifestSha256': manifest_sha})
            return result

        return {
            'jobId': row['job_id'], 'attemptId': row['submission_id'],
            'providerTaskId': row['provider_task_id'], 'generation': row['generation'],
            'environment': row['environment'], 'status': 'queued', 'phase': 'queued',
            'sourceState': 'retained', 'derivedState': 'retained',
            'pipelineManifestSha256': row['pipeline_manifest_sha'],
            'sourceManifestSha256': row['source_manifest_sha'],
            'derivedManifestSha256': row['derived_manifest_sha'],
            'pipelineDigest': row['pipeline_digest'],
            'sourceReceipt': source_receipt, 'sourceReceiptSha256': row['source_receipt_sha'],
            'derivedReceipt': derived_receipt, 'derivedReceiptSha256': row['derived_receipt_sha'],
            'sourceObjectReceipts': acknowledgements(source, 'source', row['source_manifest_sha']),
            'derivedObjectReceipts': acknowledgements(derived, 'derived', row['derived_manifest_sha']),
        }

    def _source_acceptance_binding(self, row):
        """Project only the authenticated, persisted V2 source/attempt identity."""
        from .source_acceptance_binding import validate_source_acceptance_binding
        from .viewer_source_attestation import PROOF_FIELDS, PROTOCOL as SOURCE_PROTOCOL, proof_digest
        try:
            if row['source_state'] != 'accepted' or row['derived_state'] != 'accepted':
                raise ValueError('source receipts are not complete')
            attestation = json.loads(row['source_attestation_json'])
            protocol._closed(attestation, ('proof', 'signature', 'attestationSha256'), 'persisted source proof')
            proof = attestation['proof']
            protocol._closed(proof, PROOF_FIELDS, 'persisted source proof body')
            if (type(proof['schemaVersion']) is not int or proof['schemaVersion'] != 2
                    or proof['protocol'] != SOURCE_PROTOCOL
                    or attestation['attestationSha256'] != row['source_attestation_sha']
                    or proof_digest(proof) != row['source_attestation_sha']):
                raise ValueError('source proof changed')
            expected_proof = dict(authorityId=self.queue.authority_id, environment=row['environment'],
                submissionId=row['submission_id'], viewerAttemptId=row['submission_id'],
                viewerTaskId=row['viewer_task_id'], providerTaskId=row['provider_task_id'],
                sourceManifestSha256=row['source_manifest_sha'], derivedManifestSha256=row['derived_manifest_sha'],
                pipelineManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'])
            if any(proof[key] != value for key, value in expected_proof.items()):
                raise ValueError('persisted source identity changed')
            source = json.loads(row['source_receipt_json'])
            derived = json.loads(row['derived_receipt_json'])
            if protocol.digest(source) != row['source_receipt_sha'] or protocol.digest(derived) != row['derived_receipt_sha']:
                raise ValueError('source receipts changed')
            common = dict(jobId=row['job_id'], attemptId=row['submission_id'],
                          generation=row['generation'], environment=row['environment'])
            if (type(source.get('schemaVersion')) is not int or source['schemaVersion'] != 2
                    or source.get('receiptType') != 'source_retention'
                    or source.get('retentionGuarantee') != 'until_explicit_source_release_v1'
                    or type(derived.get('schemaVersion')) is not int or derived['schemaVersion'] != 1
                    or derived.get('receiptType') != 'attempt_derived_input'
                    or any(source[key] != value or derived[key] != value for key, value in common.items())
                    or source['sourceManifestSha256'] != row['source_manifest_sha']
                    or source['pipelineManifestSha256'] != row['manifest_sha']
                    or source['pipelineDigest'] != row['pipeline_digest']
                    or derived['derivedManifestSha256'] != row['derived_manifest_sha']):
                raise ValueError('source receipt identity changed')
            binding = dict(coordinatorAuthorityId=self.queue.authority_id,
                viewerPrincipalId=proof['principalId'], submissionId=row['submission_id'],
                activationId=proof['activationId'], viewerTaskId=row['viewer_task_id'],
                viewerAttemptId=row['submission_id'], providerTaskId=row['provider_task_id'],
                preparedEnvelopeSha256=proof['preparedEnvelopeSha256'], jobId=row['job_id'],
                generation=row['generation'], environment=row['environment'],
                inputManifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                sourceManifestSha256=row['source_manifest_sha'], derivedManifestSha256=row['derived_manifest_sha'],
                sourceReceiptSha256=row['source_receipt_sha'], derivedReceiptSha256=row['derived_receipt_sha'])
            return validate_source_acceptance_binding(binding, dict(sourceIntakeVersion=2,
                jobId=row['job_id'], generation=row['generation'], environment=row['environment'],
                manifestSha256=row['manifest_sha'], pipelineDigest=row['pipeline_digest'],
                authorityId=self.queue.authority_id, attemptId=row['submission_id']))
        except (KeyError, TypeError, ValueError):
            raise Conflict('persisted V2 source acceptance binding unavailable') from None

    def submit_source_intake(self, validated, *, capability_fingerprint,
                             options_fingerprint, allowed_odm_options,
                             source_spool, trusted_source_keys, authority_id,
                             available_engines=('odm', 'treedar'), ingress_mode='spool-v1',
                             authenticated_principal=None):
        """Atomically queue a v2 source envelope in a non-claimable state.

        This records metadata only. A separate verified spool writer must
        publish every object and durable receipt before a future finalizer can
        mark either closure accepted. Until then this job remains unclaimable.
        """
        from .reference_source_intake import validate_source_intake
        if type(validated) is not dict or set(validated) != _SOURCE_INTAKE_FIELDS:
            raise ValueError('expected a normalized source-intake submission')
        raw = {key: validated[key] for key in (
            'schemaVersion', 'submissionId', 'providerTaskId', 'viewerTaskId', 'sourceManifest',
            'derivedManifest', 'manifest', 'pipeline', 'capabilityFingerprint', 'sourceAttestation')}
        checked = validate_source_intake(raw, environment=validated['environment'],
            capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, available_engines=available_engines,
            trusted_source_keys=trusted_source_keys, authority_id=authority_id)
        if _json(checked) != _json({key: validated[key] for key in checked}):
            raise ValueError('normalized source-intake identity or digest mismatch')
        if ingress_mode not in ('spool-v1','direct-v2'):
            raise ValueError('closed source ingress mode required')
        if ingress_mode == 'direct-v2':
            protocol._slug(authenticated_principal, 'authenticated source principal')
            if checked['sourceAttestation']['proof']['principalId'] != authenticated_principal:
                raise Conflict('authenticated source principal changed')
        if ingress_mode == 'spool-v1' and (source_spool is None or not callable(getattr(source_spool, 'check_capacity', None))):
            raise ValueError('configured source spool required for source-intake admission')
        pipeline_raw = {key: raw[key] for key in (
            'schemaVersion', 'submissionId', 'manifest', 'pipeline', 'capabilityFingerprint')}
        pipeline_raw['schemaVersion'] = 1
        from .reference_limits import V2_SOURCE_INTAKE_BYTES
        submission = protocol.validate_submission(pipeline_raw,
            environment=validated['environment'],
            capability_fingerprint=capability_fingerprint,
            options_fingerprint=options_fingerprint,
            allowed_odm_options=allowed_odm_options, available_engines=available_engines,
            max_body_bytes=V2_SOURCE_INTAKE_BYTES)
        from .reference_source_intake_canonical import pipeline_digest as v2_pipeline_digest
        submission['pipelineDigest'] = v2_pipeline_digest(submission['pipeline'])
        if submission['pipelineDigest'] != validated['pipelineDigest']:
            raise Conflict('source-intake pipeline digest changed')
        submission['sourceIntakeVersion'] = 2
        encoded = _json(submission)
        source_encoded = _json(validated['sourceManifest'])
        derived_encoded = _json(validated['derivedManifest'])
        attestation_encoded = _json(validated['sourceAttestation'])
        reserved_bytes = validated['sourceManifest']['totalBytes'] + sum(
            item['byteSize'] for item in validated['derivedManifest']['objects'])
        if ingress_mode == 'direct-v2':
            reserved_bytes = 0
        protocol._integer(reserved_bytes, 'source spool reservation', allow_zero=True)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('''SELECT r.job_id,r.environment,s.request_digest
                FROM reference_jobs r LEFT JOIN reference_source_intakes s ON s.job_id=r.job_id
                WHERE r.submission_id=?''', (submission['submissionId'],)).fetchone()
            if old:
                if (old['environment'] != submission['environment']
                        or old['request_digest'] != validated['requestDigest']):
                    raise Conflict('submissionId belongs to a different reference request')
                from .source_intake_mode import require as require_source_mode
                require_source_mode(db,old['job_id'],ingress_mode)
                return self._public(self._row(db, old['job_id'])), False
            pending = db.execute("SELECT COUNT(*) FROM reference_jobs WHERE phase NOT IN ('completed','cancelled')").fetchone()[0]
            if pending >= self.max_queued_jobs:
                raise Conflict('reference pending job quota reached')
            existing_reservations = db.execute("""SELECT COALESCE(SUM(reserved_bytes),0)
                FROM reference_source_intakes""").fetchone()[0]
            if ingress_mode == 'spool-v1':
                source_spool.check_capacity(reserved_bytes, reserved_bytes=existing_reservations)
            job_id = validated['providerTaskId'] or str(uuid.uuid4())
            if db.execute('SELECT 1 FROM jobs WHERE id=?', (job_id,)).fetchone():
                raise Conflict('providerTaskId already belongs to another queue job')
            self._quota(db, len(encoded.encode('utf-8')) + len(source_encoded.encode('utf-8'))
                        + len(derived_encoded.encode('utf-8')) + len(attestation_encoded.encode('utf-8'))
                        + len(job_id.encode('utf-8')) + len(ingress_mode))
            status='held' if ingress_mode=='direct-v2' else 'queued'
            reason='direct_admission_pending' if ingress_mode=='direct-v2' else None
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,held_reason) VALUES(?,?,?,?,?,?)",
                (job_id, 'reference:' + submission['submissionId'], submission['environment'], PROFILE,status,reason))
            db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
                manifest_sha,pipeline_digest,phase) VALUES(?,?,?,?,?,?,?,'queued')""",
                (job_id, submission['submissionId'], submission['requestDigest'], submission['environment'],
                 encoded, submission['manifestSha256'], submission['pipelineDigest']))
            db.execute("""INSERT INTO reference_source_intakes(job_id,request_digest,viewer_task_id,provider_task_id,
                source_attestation_json,source_attestation_sha,source_manifest_json,
                derived_manifest_json,source_manifest_sha,derived_manifest_sha,pipeline_manifest_sha,reserved_bytes,
                source_state,derived_state) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'receiving','receiving')""",
                (job_id, validated['requestDigest'], validated['viewerTaskId'], validated['providerTaskId'],
                 attestation_encoded, validated['sourceAttestation']['attestationSha256'],
                 source_encoded, derived_encoded,
                 validated['sourceManifestSha256'], validated['derivedManifestSha256'],
                 validated['manifestSha256'], reserved_bytes))
            from .source_intake_mode import claim as claim_source_mode
            claim_source_mode(db, job_id, ingress_mode)
            return self._public(self._row(db, job_id)), True

    def source_capacity(self, source_spool):
        """Return current admission limits with existing byte reservations deducted."""
        if source_spool is None:
            return {"enabled": False}
        with self.queue._db() as db:
            reserved = db.execute('SELECT COALESCE(SUM(reserved_bytes),0) FROM reference_source_intakes').fetchone()[0]
        available = source_spool.available_bytes()
        maximum = max(0, available - source_spool.safety_floor_bytes - reserved)
        from .reference_limits import V2_SOURCE_INTAKE_BYTES
        return {"enabled": True, "maxObjects": protocol.MAX_OBJECTS,
            "maxManifestBytes": V2_SOURCE_INTAKE_BYTES,
            "maxObjectBytes": source_spool.max_object_bytes,
            "maxSourceBytes": maximum, "chunkBytes": source_spool.chunk_bytes}

    def has_source_intakes(self):
        with self.queue._db() as db:
            return db.execute('SELECT 1 FROM reference_source_intakes LIMIT 1').fetchone() is not None

    def _hold_source_intake(self, job_id):
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('''UPDATE reference_source_intakes SET source_state='held',derived_state='held',
                updated_at=CURRENT_TIMESTAMP WHERE job_id=?''', (job_id,))
            db.execute('''UPDATE jobs SET status='held',held_reason='source_receipt_held',
                updated_at=CURRENT_TIMESTAMP WHERE id=? AND profile=?''', (job_id, PROFILE))

    def reconcile_source_spool(self, source_spool):
        """Reverify files at startup, adopt exact files lacking a DB ACK, and hold drift."""
        with self.queue._db() as db:
            rows = db.execute('''SELECT r.job_id,r.generation,s.source_manifest_json,s.derived_manifest_json,
                s.source_state,s.derived_state,s.source_manifest_sha,s.derived_manifest_sha,
                s.pipeline_manifest_sha FROM reference_jobs r JOIN reference_source_intakes s
                ON s.job_id=r.job_id JOIN reference_source_intake_modes m
                ON m.job_id=s.job_id AND m.mode='spool-v1' ORDER BY r.created_at,r.job_id''').fetchall()
            recorded = {row['job_id']: db.execute('''SELECT generation,object_id,object_kind,
                byte_size,sha256,mount_id,storage_key FROM reference_source_object_receipts
                WHERE job_id=? AND generation=?''', (row['job_id'], row['generation'])).fetchall()
                for row in rows}
        if source_spool is None:
            if rows:
                raise RuntimeError('source spool required for persisted source intakes')
            return {"jobs": 0, "adopted": 0, "held": 0}
        source_spool.audit_root({row['job_id'] for row in rows})
        adopted = held = 0
        for row in rows:
            job_id, generation = row['job_id'], row['generation']
            from .source_release import reconcile_source_release_custody
            try:
                if reconcile_source_release_custody(self,source_spool,job_id,generation):
                    held += int(row['source_state']=='held' or row['derived_state']=='held')
                    continue
            except Exception as error:
                from .source_spool import SourceSpoolCloseUncertain
                from .coordinator_freeze import CoordinatorFrozen
                if isinstance(error,(SourceSpoolCloseUncertain,CoordinatorFrozen)):raise
                self._hold_source_intake(job_id)
                held += 1
                continue
            if row['source_state'] == 'held' or row['derived_state'] == 'held':
                held += 1
                continue
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            objects = [{"id": obj['id'], "byteSize": obj['byteSize'], "sha256": obj['sha256']}
                       for obj in source['objects'] + derived['objects']]
            try:
                with self.source_operation(job_id, generation=generation, operation='source-audit'):
                    found = source_spool.audit_job(job_id=job_id, generation=generation,
                        expected_objects=objects)
                    by_id = {item['object_id']: item for item in recorded[job_id]}
                    if len(by_id) != len(recorded[job_id]):
                        raise Conflict('duplicate source receipt')
                    if any(object_id not in found for object_id in by_id):
                        raise Conflict('durable source receipt has no exact file')
                    for object_id, receipt in found.items():
                        old = by_id.get(object_id)
                        if old is not None and (old['generation'] != receipt['generation']
                                or old['byte_size'] != receipt['byteSize'] or old['sha256'] != receipt['sha256']
                                or old['mount_id'] != receipt['mountId'] or old['storage_key'] != receipt['storageKey']):
                            raise Conflict('durable source receipt changed')
                        if old is None:
                            self.record_source_object_receipt(job_id, receipt, source_spool=source_spool)
                            adopted += 1
                    expected_count = len(objects)
                    if (row['source_state'] == 'accepted' or row['derived_state'] == 'accepted') and len(found) != expected_count:
                        raise Conflict('retained source closure is incomplete')
            except Exception as error:
                from .source_spool import SourceSpoolError
                from .reference_source_readers import SourceReaderError
                from .source_spool import SourceSpoolCloseUncertain
                if isinstance(error, SourceSpoolCloseUncertain):
                    raise
                if isinstance(error, SourceReaderError):
                    if str(error) not in ('source_release_fenced', 'source_operation_busy'):
                        raise
                    # An authorized future release owns custody after its fence.
                    # Do not touch files or overwrite that custody with audit drift.
                    held += 1
                    continue
                if isinstance(error, SourceSpoolError) and error.code in (
                        'spool_mount_unavailable', 'spool_mount_changed'):
                    raise RuntimeError('source spool mount unavailable during reconciliation') from None
                self._hold_source_intake(job_id)
                held += 1
        return {"jobs": len(rows), "adopted": adopted, "held": held}

    def source_object_expectation(self, job_id, *, generation, object_id):
        """Return one closed manifest object for a streaming source PUT."""
        protocol._integer(generation, 'generation')
        protocol._slug(object_id, 'source object ID')
        with self.queue._db() as db:
            row = self._row(db, job_id)
            if row['generation'] != generation:
                raise Conflict('source generation changed')
            if row['source_state'] != 'receiving' or row['derived_state'] != 'receiving':
                raise Conflict('source intake is not receiving')
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            matches = [(obj, 'source') for obj in source['objects'] if obj['id'] == object_id]
            matches.extend((obj, 'derived') for obj in derived['objects'] if obj['id'] == object_id)
            if len(matches) != 1:
                raise Conflict('unknown source object')
            obj, kind = matches[0]
            total_reserved = db.execute('SELECT COALESCE(SUM(reserved_bytes),0) '
                'FROM reference_source_intakes').fetchone()[0]
            already_received = db.execute('''SELECT 1 FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? AND object_id=?''',
                (job_id, generation, object_id)).fetchone() is not None
            current_reservation = 0 if already_received else obj['byteSize']
            if not already_received and row['reserved_bytes'] < current_reservation:
                raise Conflict('source byte reservation is unavailable')
            if total_reserved < current_reservation:
                raise Conflict('source reservation ledger is inconsistent')
            return {"jobId": job_id, "generation": generation, "objectId": object_id,
                    "objectKind": kind, "byteSize": obj['byteSize'], "sha256": obj['sha256'],
                    "environment": row['environment'],
                    "otherReservedBytes": total_reserved - current_reservation}

    def record_source_object_receipt(self, job_id, receipt, *, source_spool):
        if type(receipt) is not dict:
            raise ValueError('invalid source object receipt')
        with self.source_operation(job_id, generation=receipt.get('generation'),
                operation='source-receipt', receiving_only=True):
            return self._record_source_object_receipt(job_id, receipt, source_spool=source_spool)

    def _record_source_object_receipt(self, job_id, receipt, *, source_spool):
        """Persist a spool receipt after re-opening and rehashing its object."""
        fields = {"schemaVersion", "jobId", "generation", "objectId", "storageKey",
                  "byteSize", "sha256", "mountId"}
        if (type(receipt) is not dict or set(receipt) != fields
                or type(receipt['schemaVersion']) is not int or receipt['schemaVersion'] != 1):
            raise ValueError('invalid source object receipt')
        protocol._slug(receipt['jobId'], 'job ID')
        if receipt['jobId'] != job_id:
            raise Conflict('source receipt job binding changed')
        protocol._integer(receipt['generation'], 'generation')
        protocol._slug(receipt['objectId'], 'source object ID')
        protocol._integer(receipt['byteSize'], 'source object size', allow_zero=True)
        protocol._hash(receipt['sha256'], 'source object digest')
        if (source_spool is None or receipt['mountId'] != source_spool.mount_id
                or receipt['storageKey'] != source_spool.storage_key(
                    receipt['jobId'], receipt['generation'], receipt['objectId'])):
            raise Conflict('source storage binding changed')
        source_spool.verify_object(job_id=receipt['jobId'], generation=receipt['generation'],
            object_id=receipt['objectId'], byte_size=receipt['byteSize'], sha256=receipt['sha256'])
        encoded = _json(receipt)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, receipt['generation'])
            expected_matches = []
            for obj in json.loads(row['source_manifest_json'])['objects']:
                if obj['id'] == receipt['objectId']:
                    expected_matches.append((obj, 'source'))
            for obj in json.loads(row['derived_manifest_json'])['objects']:
                if obj['id'] == receipt['objectId']:
                    expected_matches.append((obj, 'derived'))
            if (row['generation'] != receipt['generation'] or row['source_state'] != 'receiving'
                    or row['derived_state'] != 'receiving' or len(expected_matches) != 1):
                raise Conflict('source receipt is stale or outside intake closure')
            expected, kind = expected_matches[0]
            if (expected['byteSize'], expected['sha256']) != (receipt['byteSize'], receipt['sha256']):
                raise Conflict('source receipt differs from intake manifest')
            existing = db.execute('''SELECT receipt_json FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? AND object_id=?''',
                (job_id, receipt['generation'], receipt['objectId'])).fetchone()
            if existing:
                if existing['receipt_json'] != encoded:
                    raise Conflict('source object identity has a different durable receipt')
                return False
            if row['reserved_bytes'] < receipt['byteSize']:
                raise Conflict('source byte reservation is unavailable')
            self._quota(db, len(encoded.encode('utf-8')))
            db.execute('''INSERT INTO reference_source_object_receipts(job_id,generation,object_id,
                object_kind,storage_key,byte_size,sha256,mount_id,receipt_json)
                VALUES(?,?,?,?,?,?,?,?,?)''', (job_id, receipt['generation'], receipt['objectId'],
                kind, receipt['storageKey'], receipt['byteSize'], receipt['sha256'], receipt['mountId'], encoded))
            db.execute('''UPDATE reference_source_intakes SET reserved_bytes=reserved_bytes-?,
                updated_at=CURRENT_TIMESTAMP WHERE job_id=?''', (receipt['byteSize'], job_id))
            return True

    def complete_source_intake(self, job_id, *, generation, source_manifest_sha256,
                               derived_manifest_sha256, pipeline_manifest_sha256, source_spool):
        with self.source_operation(job_id, generation=generation, operation='source-complete'):
            return self._complete_source_intake(job_id, generation=generation,
                source_manifest_sha256=source_manifest_sha256,
                derived_manifest_sha256=derived_manifest_sha256,
                pipeline_manifest_sha256=pipeline_manifest_sha256, source_spool=source_spool)

    def _complete_source_intake(self, job_id, *, generation, source_manifest_sha256,
                               derived_manifest_sha256, pipeline_manifest_sha256, source_spool):
        """Retain both closed manifests only after every persisted object re-verifies."""
        protocol._integer(generation, 'generation')
        for digest in (source_manifest_sha256, derived_manifest_sha256, pipeline_manifest_sha256):
            protocol._hash(digest, 'source completion digest')
        if source_spool is None:
            raise ValueError('configured source spool required')
        with self.queue._db() as db:
            row = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, generation)
            if row['generation'] != generation:
                raise Conflict('source generation changed')
            if (row['source_manifest_sha'] != source_manifest_sha256
                    or row['derived_manifest_sha'] != derived_manifest_sha256
                    or row['pipeline_manifest_sha'] != pipeline_manifest_sha256):
                raise Conflict('source completion manifest binding changed')
            if row['source_state'] == 'accepted' and row['derived_state'] == 'accepted':
                source = json.loads(row['source_manifest_json'])
                derived = json.loads(row['derived_manifest_json'])
            elif row['source_state'] != 'receiving' or row['derived_state'] != 'receiving':
                raise Conflict('source intake is not receiving')
            else:
                source = json.loads(row['source_manifest_json'])
                derived = json.loads(row['derived_manifest_json'])
            expected = [(obj, 'source') for obj in source['objects']]
            expected.extend((obj, 'derived') for obj in derived['objects'])
            receipts = db.execute('''SELECT * FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? ORDER BY object_kind,object_id''',
                (job_id, generation)).fetchall()
            if len(receipts) != len(expected) or row['reserved_bytes'] != 0:
                raise Conflict('source intake is incomplete')
            completion_binding = {key: row[key] for key in ('source_manifest_json',
                'derived_manifest_json', 'source_manifest_sha', 'derived_manifest_sha',
                'pipeline_manifest_sha')}
            receipt_binding = _json([dict(receipt) for receipt in receipts])
            by_id = {receipt['object_id']: receipt for receipt in receipts}
            if len(by_id) != len(receipts):
                raise Conflict('source object receipt set is invalid')
        verified = []
        for obj, kind in expected:
            receipt_row = by_id.get(obj['id'])
            if (receipt_row is None or receipt_row['object_kind'] != kind
                    or receipt_row['byte_size'] != obj['byteSize']
                    or receipt_row['sha256'] != obj['sha256']
                    or receipt_row['mount_id'] != source_spool.mount_id):
                raise Conflict('source receipt set differs from manifest')
            receipt = json.loads(receipt_row['receipt_json'])
            expected_receipt = source_spool.verify_object(job_id=job_id,
                generation=generation, object_id=obj['id'], byte_size=obj['byteSize'],
                sha256=obj['sha256'])
            if _json(receipt) != _json(expected_receipt) or receipt_row['storage_key'] != expected_receipt['storageKey']:
                raise Conflict('source object receipt no longer verifies')
            verified.append(receipt['objectId'])
        from datetime import datetime, timezone
        retained_at = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        source_receipt = {
            'schemaVersion': 2, 'receiptType': 'source_retention',
            'receiptId': str(uuid.uuid4()), 'storageReceiptId': str(uuid.uuid4()),
            'jobId': job_id, 'attemptId': row['submission_id'], 'generation': generation,
            'environment': row['environment'], 'pipelineManifestSha256': pipeline_manifest_sha256,
            'sourceManifestSha256': source_manifest_sha256, 'pipelineDigest': row['pipeline_digest'],
            'objects': [{key: obj[key] for key in ('id', 'byteSize', 'sha256', 'sourceType',
                'retentionClass', 'retentionHoldId')} for obj in source['objects']],
            'retainedAt': retained_at,
            'retentionGuarantee': 'until_explicit_source_release_v1'}
        derived_receipt = {
            'schemaVersion': 1, 'receiptType': 'attempt_derived_input',
            'receiptId': str(uuid.uuid4()), 'jobId': job_id, 'attemptId': row['submission_id'],
            'generation': generation, 'environment': row['environment'],
            'derivedManifestSha256': derived_manifest_sha256,
            'objects': [{key: obj[key] for key in ('id', 'name', 'byteSize', 'sha256',
                'snapshotAttemptId', 'snapshotContentSha256', 'snapshotEvidenceSha256',
                'sourceType', 'retentionHoldId', 'retentionClass')} for obj in derived['objects']],
            'retainedAt': retained_at}
        source_receipt_json, derived_receipt_json = _json(source_receipt), _json(derived_receipt)
        source_receipt_sha, derived_receipt_sha = protocol.digest(source_receipt), protocol.digest(derived_receipt)
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, generation)
            if (row['generation'] != generation or row['source_state'] not in ('receiving', 'accepted')
                    or row['derived_state'] not in ('receiving', 'accepted') or row['reserved_bytes'] != 0):
                raise Conflict('source intake changed during verification')
            current_receipts = db.execute('''SELECT * FROM reference_source_object_receipts
                WHERE job_id=? AND generation=? ORDER BY object_kind,object_id''',
                (job_id, generation)).fetchall()
            if (any(row[key] != value for key, value in completion_binding.items())
                    or _json([dict(receipt) for receipt in current_receipts]) != receipt_binding):
                raise Conflict('source receipt set changed during verification')
            if row['source_receipt_json'] or row['derived_receipt_json']:
                if (row['source_receipt_json'] != source_receipt_json
                        or row['derived_receipt_json'] != derived_receipt_json):
                    # The first completion response may have been lost. Its exact
                    # persisted receipts, not newly generated IDs/timestamps, win.
                    if (row['source_state'] == 'accepted' and row['derived_state'] == 'accepted'
                            and row['source_receipt_json'] and row['derived_receipt_json']):
                        return self._source_completion_ack(row, receipts), False
                    raise Conflict('source completion receipt binding changed')
                return self._source_completion_ack(row, receipts), False
            self._quota(db, len(source_receipt_json.encode('utf-8'))
                + len(derived_receipt_json.encode('utf-8')))
            db.execute('''UPDATE reference_source_intakes SET source_state='accepted',
                derived_state='accepted',source_receipt_json=?,source_receipt_sha=?,
                derived_receipt_json=?,derived_receipt_sha=?,updated_at=CURRENT_TIMESTAMP WHERE job_id=?''',
                (source_receipt_json, source_receipt_sha, derived_receipt_json, derived_receipt_sha, job_id))
            result = self._source_completion_ack(self._row(db, job_id), receipts)
        return result, True

    def mint_source_transfer_grant(self, job_id, *, source_spool, clock=None):
        """Seal a claim-bound broker grant only after durable source intake acceptance."""
        _ensure_source_range_schema(self.queue)
        from datetime import datetime
        from .reference_transfer import validate_grant
        from .source_range_transfer import validate_source_grant
        read_clock = time.time if clock is None else clock
        previous_clock = None

        def checked_now():
            nonlocal previous_clock
            value = read_clock()
            if (isinstance(value, bool) or not isinstance(value, (int, float))
                    or not math.isfinite(value)):
                raise Conflict('invalid source grant clock')
            if previous_clock is not None and value < previous_clock:
                raise Conflict('source grant clock moved backwards')
            previous_clock = value
            return value

        def capture(db):
            row = self._row(db, job_id)
            self._source_reader_unfenced(db, job_id, row['generation'])
            claim = db.execute('''SELECT request_id,job_id,node_id,worker_id,instance_id,token
                FROM reference_claims WHERE job_id=?''', (job_id,)).fetchone()
            receipts = db.execute('''SELECT generation,object_id,object_kind,byte_size,sha256,mount_id,storage_key,receipt_json
                FROM reference_source_object_receipts WHERE job_id=? ORDER BY generation,object_id''',
                (job_id,)).fetchall()
            grant_row = db.execute('SELECT grant_json,digest FROM reference_transfer_grants WHERE job_id=?',
                                   (job_id,)).fetchone()
            token_rows = db.execute('''SELECT token_sha256,job_id,generation FROM reference_source_grant_tokens
                WHERE job_id=? ORDER BY token_sha256''', (job_id,)).fetchall()
            pins = db.execute('''SELECT generation,object_id,byte_size,sha256,mount_id,device,inode,mode,
                link_count,owner_uid,mtime_ns,ctime_ns FROM reference_source_object_pins
                WHERE job_id=? ORDER BY generation,object_id''',(job_id,)).fetchall()
            claim_value = dict(claim) if claim is not None else None
            snapshot = {
                'row': {key: row[key] for key in (
                    'job_id','submission_id','request_digest','environment','input_json','manifest_sha',
                    'pipeline_digest','generation','phase','node_id','transfer_json','status','claim_hash',
                    'held_reason','private_claim_token','source_request_digest','viewer_task_id','provider_task_id',
                    'source_attestation_json','source_attestation_sha','source_manifest_json','derived_manifest_json',
                    'source_manifest_sha','derived_manifest_sha','pipeline_manifest_sha','source_state',
                    'derived_state','reserved_bytes','source_receipt_json','source_receipt_sha',
                    'derived_receipt_json','derived_receipt_sha')},
                'claim': claim_value,
                'receipts': [dict(item) for item in receipts],
                'grant': dict(grant_row) if grant_row is not None else None,
                'tokens': [dict(item) for item in token_rows],
                'pins': [dict(item) for item in pins],
            }
            return row, claim_value, snapshot

        # Read a consistent, immutable metadata snapshot briefly. In particular,
        # leave Queue._db (and its lifecycle lock) before touching the NAS.
        with self.queue._db() as db:
            db.execute('BEGIN')
            row, claim, snapshot = capture(db)
            claim_custody = self._source_claim_custody(db, job_id)
            db.commit()

        now = checked_now()
        if snapshot['grant'] is None and snapshot['tokens']:
            raise Conflict('orphan source grant token history')
        if row['status'] != 'running' or row['phase'] != 'awaiting_transfer':
            raise Conflict('source grant requires a running transfer reservation')
        if (row['source_state'] != 'accepted' or row['derived_state'] != 'accepted'
                or not row['source_receipt_json'] or not row['derived_receipt_json']):
            raise Conflict('source retention receipts are not accepted')
        if (claim is None or claim['token'] != row['private_claim_token']
                or not row['claim_hash']
                or not hmac.compare_digest(hashlib.sha256(claim['token'].encode('utf-8')).hexdigest(), row['claim_hash'])):
            raise Conflict('active source grant claim is unavailable')
        request = json.loads(row['transfer_json']) if row['transfer_json'] else None
        if not isinstance(request, dict):
            raise Conflict('worker reservation is unavailable')
        source_range_v2 = request.get('protocol') == 'source-range-v2'
        try:
            verified_pins = []
            expiry = datetime.fromisoformat(request['expiresAt'].replace('Z', '+00:00')).timestamp()
        except (KeyError, ValueError, AttributeError, OverflowError, OSError):
            raise Conflict('worker reservation expiry is invalid') from None
        if not math.isfinite(expiry) or expiry <= now:
            raise Conflict('worker reservation expired')
        source_receipt = json.loads(row['source_receipt_json'])
        derived_receipt = json.loads(row['derived_receipt_json'])
        if (type(source_receipt) is not dict or type(derived_receipt) is not dict
                or protocol.digest(source_receipt) != row['source_receipt_sha']
                or protocol.digest(derived_receipt) != row['derived_receipt_sha']
                or source_receipt.get('receiptType') != 'source_retention'
                or derived_receipt.get('receiptType') != 'attempt_derived_input'
                or (source_receipt.get('jobId'), source_receipt.get('attemptId'),
                    source_receipt.get('generation'), source_receipt.get('environment'),
                    source_receipt.get('pipelineManifestSha256'), source_receipt.get('sourceManifestSha256')) !=
                   (job_id, row['submission_id'], row['generation'], row['environment'],
                    row['pipeline_manifest_sha'], row['source_manifest_sha'])
                or (derived_receipt.get('jobId'), derived_receipt.get('attemptId'),
                    derived_receipt.get('generation'), derived_receipt.get('environment'),
                    derived_receipt.get('derivedManifestSha256')) !=
                   (job_id, row['submission_id'], row['generation'], row['environment'],
                    row['derived_manifest_sha'])):
            raise Conflict('source retention receipt binding changed')
        try:
            source = json.loads(row['source_manifest_json'])
            derived = json.loads(row['derived_manifest_json'])
            submission = json.loads(row['input_json'])
            objects = submission['manifest']['objects']
            expected = {item['id']: (item, 'source') for item in source['objects']}
            expected.update({item['id']: (item, 'derived') for item in derived['objects']})
            receipt_rows = snapshot['receipts']
            if len(receipt_rows) != len(expected):
                raise Conflict('accepted source receipt closure is incomplete')
            by_id = {item['object_id']: item for item in receipt_rows
                     if item['generation'] == row['generation']}
            if len(by_id) != len(receipt_rows) or set(by_id) != set(expected):
                raise Conflict('accepted source receipt closure changed')
            still_owned = self._source_claim_still_owned(job_id, claim['node_id'], claim_custody,
                'source grant authority changed during verification', first_grant_may_land=True)
            for obj in objects:
                stored = by_id.get(obj['id'])
                retained = expected.get(obj['id'])
                if (stored is None or retained is None
                        or (stored['byte_size'], stored['sha256'], stored['object_kind']) !=
                           (obj['byteSize'], obj['sha256'], retained[1])
                        or stored['mount_id'] != source_spool.mount_id
                        or stored['storage_key'] != source_spool.storage_key(
                            job_id, row['generation'], obj['id'])):
                    raise Conflict('pipeline source receipt binding changed')
                receipt = json.loads(stored['receipt_json'])
                if source_range_v2:
                    # Each NAS descriptor is a registered read, so freeze, release
                    # and Queue.close wait for it and an unknown close keeps its pin.
                    handle,actual = self._registered_source_reader(job_id=job_id, generation=row['generation'],
                        object_id=obj['id'], authorize=still_owned,
                        open_reader=lambda obj=obj: source_spool.open_verified_object(job_id=job_id,
                            generation=row['generation'],object_id=obj['id'],byte_size=obj['byteSize'],
                            sha256=obj['sha256']))
                    try:
                        import stat
                        identity=handle.file_identity
                        if not stat.S_ISREG(identity[3]) or identity[4]!=1:
                            raise Conflict('source object inode is not privately retained')
                        verified_pins.append((obj['id'],obj['byteSize'],obj['sha256'],source_spool.mount_id,
                            str(identity[0]),str(identity[1]),identity[3],identity[4],identity[5],identity[6],identity[7]))
                    finally:
                        handle.close()
                else:
                    # verify_object keeps its full hash and stability check; the
                    # pin gives that descriptor the same custody as a reader.
                    actual = self._registered_source_check(job_id=job_id, generation=row['generation'],
                        object_id=obj['id'], authorize=still_owned,
                        check=lambda obj=obj: source_spool.verify_object(job_id=job_id,
                            generation=row['generation'], object_id=obj['id'], byte_size=obj['byteSize'],
                            sha256=obj['sha256']))
                if _json(actual) != _json(receipt):
                    raise Conflict('pipeline source object is no longer retained')
        except (KeyError, TypeError, ValueError):
            raise Conflict('source transfer grant could not be validated') from None

        verified_at = checked_now()
        if expiry <= verified_at:
            raise Conflict('worker reservation expired during source verification')

        # Hashing has finished. Take the write lock, re-read every authority and
        # receipt binding, then mint or reconcile only against this exact state.
        with self.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current, current_claim, current_snapshot = capture(db)
            before_cas = {key: value for key, value in snapshot.items() if key not in ('grant', 'tokens', 'pins')}
            after_cas = {key: value for key, value in current_snapshot.items() if key not in ('grant', 'tokens', 'pins')}
            if _json(after_cas) != _json(before_cas):
                raise Conflict('source grant authority changed during verification')
            if (current['status'] != 'running' or current['phase'] != 'awaiting_transfer'
                    or current_claim is None or current_claim['token'] != current['private_claim_token']
                    or current['generation'] != row['generation']
                    or current['transfer_json'] != row['transfer_json']):
                raise Conflict('source grant reservation changed during verification')
            if current_snapshot['grant'] is None and current_snapshot['tokens']:
                raise Conflict('orphan source grant token history')
            # Another identical verifier may have won the CAS while this thread
            # was hashing. Return only that exact persisted, still-live grant.
            if current_snapshot['grant'] is not None:
                persisted = json.loads(current_snapshot['grant']['grant_json'])
                token_hash = hashlib.sha256(persisted['token'].encode('ascii')).hexdigest()
                if (current_snapshot['grant']['digest'] != protocol.digest(persisted)
                        or len(current_snapshot['tokens']) != 1
                        or current_snapshot['tokens'][0] != {
                            'token_sha256': token_hash, 'job_id': job_id,
                            'generation': current['generation']}
                        or any(persisted.get(key) != request.get(key) for key in (
                            ('protocol','reservationId','generation','manifestSha256','byteBudget','nonce',
                             'sourceVersion','objectIds','chunkBytes','requestBudget','attemptBudget')
                            if source_range_v2 else
                             ('reservationId','generation','expiresAt','manifestSha256','byteBudget','nonce')))
                        or persisted.get('jobId') != job_id
                        or persisted.get('environment') != current['environment']
                        or persisted.get('objectIds') != [item['id'] for item in objects]):
                    raise Conflict('persisted source grant binding changed')
                if source_range_v2:
                    expected_pins = [{
                        'generation':current['generation'],'object_id':item[0], 'byte_size':item[1],
                        'sha256':item[2], 'mount_id':item[3], 'device':item[4], 'inode':item[5],
                        'mode':item[6], 'link_count':item[7], 'owner_uid':item[8],
                        'mtime_ns':item[9], 'ctime_ns':item[10]} for item in verified_pins]
                    expected_pins.sort(key=lambda pin:(pin['generation'],pin['object_id']))
                    if _json(expected_pins) != _json(current_snapshot['pins']):
                        raise Conflict('persisted source grant pins changed')
                commit_now = checked_now()
                if source_range_v2:
                    validate_source_grant(persisted, job=self._record(current, True), clock=lambda: commit_now)
                else:
                    validate_grant(persisted, job=self._record(current, True), clock=lambda: commit_now)
                return persisted
            if source_range_v2:
                if snapshot['pins']:
                    raise Conflict('source range pins exist before initial grant')
            commit_now = checked_now()
            if commit_now >= expiry:
                raise Conflict('worker reservation expired during source verification')
            if source_range_v2:
                from datetime import datetime, timezone
                reservation_expiry = datetime.fromisoformat(request['expiresAt'].replace('Z','+00:00')).timestamp()
                grant_expiry = min(commit_now + SOURCE_RANGE_GRANT_TTL_SECONDS, reservation_expiry)
                if grant_expiry <= commit_now:
                    raise Conflict('worker reservation expired during source verification')
                grant = {key: request[key] for key in ('protocol','reservationId','generation','manifestSha256',
                    'sourceVersion','objectIds','chunkBytes','requestBudget','byteBudget','nonce','attemptBudget')}
                grant.update(expiresAt=datetime.fromtimestamp(grant_expiry,timezone.utc).isoformat(
                    timespec='milliseconds').replace('+00:00','Z'), jobId=job_id,
                    environment=current['environment'], token=secrets.token_urlsafe(32))
                validate_source_grant(grant, job=self._record(current, True), clock=lambda: commit_now)
                for item in verified_pins:
                    db.execute('''INSERT INTO reference_source_object_pins
                        (job_id,generation,object_id,byte_size,sha256,mount_id,device,inode,mode,
                         link_count,owner_uid,mtime_ns,ctime_ns) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                        (job_id,row['generation'],*item))
            else:
                grant = {key: request[key] for key in
                         ('reservationId', 'generation', 'expiresAt', 'manifestSha256', 'byteBudget', 'nonce')}
                grant.update(jobId=job_id, environment=current['environment'],
                             objectIds=[item['id'] for item in objects], token=secrets.token_urlsafe(32))
                validate_grant(grant, job=self._record(current, True), clock=lambda: commit_now)
            encoded = _json(grant)
            digest = protocol.digest(grant)
            token_sha = hashlib.sha256(grant['token'].encode('ascii')).hexdigest()
            token_metadata = _metadata_field_bytes(token_sha, job_id, current['generation'],
                '0000-00-00 00:00:00')
            self._quota(db, len(encoded.encode('utf-8')) + token_metadata)
            db.execute('INSERT INTO reference_transfer_grants VALUES(?,?,?)', (job_id, encoded, digest))
            db.execute('INSERT INTO reference_source_grant_tokens(token_sha256,job_id,generation) VALUES(?,?,?)',
                       (token_sha, job_id, current['generation']))
            return grant

    def source_object_for_grant(self, token, object_id, *, source_spool, clock=None):
        """Authorize one exact pipeline input and return its already verified file handle."""
        from .reference_transfer import validate_grant
        if type(token) is not str or not token:
            raise Conflict('source grant unavailable')
        token_sha = hashlib.sha256(token.encode('utf-8')).hexdigest()
        with self.queue._db() as db:
            row = db.execute('''SELECT job_id,generation FROM reference_source_grant_tokens
                WHERE token_sha256=?''', (token_sha,)).fetchone()
            if row is None:
                raise Conflict('source grant unavailable')
            job = self._row(db, row['job_id'])
            grant_row = db.execute('SELECT grant_json FROM reference_transfer_grants WHERE job_id=?',
                                    (row['job_id'],)).fetchone()
            if grant_row is None:
                raise Conflict('source grant unavailable')
            grant = json.loads(grant_row['grant_json'])
            private = self._record(job, True)
            if (row['generation'] != job['generation'] or job['status'] != 'running'
                    or job['phase'] not in ('fetching', 'processing')
                    or job['source_state'] != 'accepted' or job['derived_state'] != 'accepted'):
                raise Conflict('source grant execution is not active')
            if (type(grant) is not dict or type(grant.get('token')) is not str
                    or not hmac.compare_digest(token, grant['token'])):
                raise Conflict('source grant unavailable')
            checked = validate_grant(grant, job=private, clock=clock or time.time)
            item = next((entry for entry in checked.objects if entry.id == object_id), None)
            if item is None:
                raise Conflict('source object outside pipeline grant')
            source_rows = db.execute('''SELECT object_id,object_kind,byte_size,sha256,mount_id,storage_key
                FROM reference_source_object_receipts WHERE job_id=? AND generation=? AND object_id=?''',
                (row['job_id'], row['generation'], object_id)).fetchall()
            if len(source_rows) != 1:
                raise Conflict('source object receipt unavailable')
            receipt = source_rows[0]
            if (receipt['byte_size'], receipt['sha256'], receipt['mount_id']) != (
                    item.byte_size, item.sha256, source_spool.mount_id):
                raise Conflict('source object receipt changed')
        return self._registered_source_reader(job_id=row['job_id'], generation=row['generation'],
            object_id=object_id, authorize=self._source_read_authorizer(row['job_id'],
                row['generation'], token_sha, grant_row['grant_json'], clock=clock),
            open_reader=lambda: source_spool.open_verified_object(job_id=row['job_id'], generation=row['generation'],
                object_id=object_id, byte_size=item.byte_size, sha256=item.sha256))


