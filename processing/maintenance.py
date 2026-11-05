"""Transactional container-maintenance admission; never performs host actions."""

from datetime import datetime, timezone
import hashlib
import hmac
import json
import re
import secrets
import uuid


class Conflict(Exception):
    pass


STAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)\Z")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}\Z")
ENGINE_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,79}\Z")
SCHEDULE_FIELDS = ('expectedRevision','drainAt','notBefore','afterJobId','targetImage')


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def canonical_uuid(value):
    try:
        if type(value) is not str or str(uuid.UUID(value))!=value:raise ValueError()
    except (ValueError,TypeError,AttributeError):
        raise ValueError('canonical UUID required') from None
    return value


def utc_stamp(value):
    if not isinstance(value, str) or not STAMP.fullmatch(value):
        raise ValueError("timestamp must be ISO 8601 with explicit UTC Z or +00:00")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed, parsed.isoformat().replace("+00:00", "Z")


class Maintenance:
    def _now(self):
        now = self._clock()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return now.astimezone(timezone.utc)

    def _init_maintenance(self, db):
        db.execute("""CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY, state TEXT NOT NULL DEFAULT 'active',
            revision INTEGER NOT NULL DEFAULT 0, plan_id TEXT, drain_at TEXT,
            not_before TEXT, after_job_id TEXT, target_image TEXT, result TEXT,
            update_hash TEXT, finish_status TEXT)""")
        columns = {row['name'] for row in db.execute('PRAGMA table_info(nodes)')}
        if 'target_engine_version' not in columns:
            # Historical scheduled/updating plans remain unbound. Never infer
            # their target from whichever container happens to answer /info.
            db.execute('ALTER TABLE nodes ADD COLUMN target_engine_version TEXT')
        for node in self.node_ids:
            db.execute("INSERT OR IGNORE INTO nodes(id) VALUES(?)", (node,))
        db.execute("""UPDATE nodes SET state='held', revision=revision+1,
            update_hash=NULL, finish_status=NULL WHERE state='updating'""")
        db.execute('''CREATE TABLE IF NOT EXISTS owned_maintenance_schedules (
            request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL,
            request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)''')

    @staticmethod
    def _node_public(row):
        return {"id": row["id"], "state": row["state"], "revision": row["revision"],
                "planId": row["plan_id"], "drainAt": row["drain_at"],
                "notBefore": row["not_before"], "afterJobId": row["after_job_id"],
                "targetImage": row["target_image"],
                "targetEngineVersion": row["target_engine_version"],
                "result": json.loads(row["result"]) if row["result"] else None}

    def _known_node(self, identifier):
        self.slug(identifier)
        if identifier not in self.node_ids:
            raise ValueError("unknown configured node")

    def _refresh_nodes(self, db):
        now = self._now()
        for row in db.execute("SELECT id,drain_at FROM nodes WHERE state='scheduled'").fetchall():
            if utc_stamp(row["drain_at"])[0] <= now:
                db.execute("UPDATE nodes SET state='draining',revision=revision+1 WHERE id=?", (row["id"],))

    def _node(self, db, identifier):
        self._known_node(identifier)
        return db.execute("SELECT * FROM nodes WHERE id=?", (identifier,)).fetchone()

    @staticmethod
    def _cas(row, payload, plan=False):
        revision = payload["expectedRevision"]
        if type(revision) is not int or revision < 0:
            raise ValueError("expectedRevision must be a nonnegative integer")
        if revision != row["revision"] or (plan and payload["planId"] != row["plan_id"]):
            raise Conflict("maintenance revision or plan changed")

    def get_node(self, identifier):
        self._known_node(identifier)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._refresh_nodes(db)
            return self._node_public(self._node(db, identifier))

    def list_nodes(self):
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._refresh_nodes(db)
            return [self._node_public(self._node(db, node)) for node in self.node_ids]

    @staticmethod
    def _validate_schedule(payload):
        if not isinstance(payload, dict):
            raise ValueError('maintenance schedule must be an object')
        drain, drain_text = utc_stamp(payload["drainAt"])
        earliest, earliest_text = utc_stamp(payload["notBefore"])
        if earliest < drain:
            raise ValueError("notBefore cannot precede drainAt")
        if not isinstance(payload["targetImage"], str) or not IMAGE.fullmatch(payload["targetImage"]):
            raise ValueError("targetImage must be an immutable sha256 image ID")
        version = payload.get('targetEngineVersion')
        if version is not None and (type(version) is not str or not ENGINE_VERSION.fullmatch(version)):
            raise ValueError('targetEngineVersion must be an exact engine version pin')
        after = payload["afterJobId"]
        if after is not None and not isinstance(after, str):
            raise ValueError("afterJobId must be null or an exact running job ID")
        return drain,drain_text,earliest_text,after,version

    def _schedule_node(self,db,identifier,payload,validated):
        drain,drain_text,earliest_text,after,version=validated
        self._refresh_nodes(db)
        row = self._node(db, identifier)
        self._cas(row, payload)
        if row["state"] != "active":
            raise Conflict("node already has maintenance or needs inspection")
        if after is not None and not db.execute(
                "SELECT 1 FROM jobs WHERE id=? AND node_id=? AND status='running'", (after, identifier)).fetchone():
            raise Conflict("afterJobId is not currently running on this node")
        state = "draining" if after is not None or drain <= self._now() else "scheduled"
        db.execute("""UPDATE nodes SET state=?,revision=revision+1,plan_id=?,drain_at=?,
            not_before=?,after_job_id=?,target_image=?,target_engine_version=?,result=NULL,update_hash=NULL,
            finish_status=NULL WHERE id=?""", (state, str(uuid.uuid4()), drain_text,
            earliest_text, after, payload["targetImage"], version, identifier))
        return self._node_public(self._node(db, identifier))

    def schedule_node(self, identifier, payload):
        expected = set(SCHEDULE_FIELDS) | ({'targetEngineVersion'} if isinstance(payload, dict) and 'targetEngineVersion' in payload else set())
        self.fields(payload, expected)
        validated=self._validate_schedule(payload)
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            return self._schedule_node(db,identifier,payload,validated)

    def schedule_node_owned(self,identifier,payload):
        self._known_node(identifier)
        expected = ('schemaVersion','requestId','authorityId',*SCHEDULE_FIELDS)
        if isinstance(payload, dict) and 'targetEngineVersion' in payload:
            expected = (*expected, 'targetEngineVersion')
        self.fields(payload,expected)
        if type(payload['schemaVersion']) is not int or payload['schemaVersion']!=1:
            raise ValueError('owned schedule schemaVersion must be 1')
        canonical_uuid(payload['requestId']);canonical_uuid(payload['authorityId'])
        if payload['authorityId']!=self.authority_id:raise Conflict('coordinator authority changed')
        if type(payload['expectedRevision']) is not int or not 0<=payload['expectedRevision']<=2**53-1:
            raise ValueError('expectedRevision must be a nonnegative safe integer')
        if payload['afterJobId'] is not None:canonical_uuid(payload['afterJobId'])
        validated=self._validate_schedule(payload)
        request_json=canonical(payload)
        request_sha=hashlib.sha256(canonical(dict(nodeId=identifier,request=payload)).encode()).hexdigest()
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            old=db.execute('SELECT * FROM owned_maintenance_schedules WHERE request_id=?',(payload['requestId'],)).fetchone()
            if old:
                if (old['node_id']!=identifier or old['authority_id']!=self.authority_id
                        or old['request_sha256']!=request_sha or old['request_json']!=request_json):
                    raise Conflict('owned schedule requestId is already bound')
                return json.loads(old['receipt_json'])
            node=self._schedule_node(db,identifier,payload,validated)
            receipt=dict(schemaVersion=1,requestId=payload['requestId'],authorityId=self.authority_id,nodeId=identifier,
                requestSha256=request_sha,expectedRevision=payload['expectedRevision'],revision=node['revision'],
                planId=node['planId'],state=node['state'],drainAt=node['drainAt'],notBefore=node['notBefore'],
                afterJobId=node['afterJobId'],targetImage=node['targetImage'])
            if 'targetEngineVersion' in payload:
                receipt['targetEngineVersion'] = node['targetEngineVersion']
            receipt['receiptSha256']=hashlib.sha256(canonical(receipt).encode()).hexdigest()
            db.execute('INSERT INTO owned_maintenance_schedules VALUES(?,?,?,?,?,?)',
                (payload['requestId'],identifier,self.authority_id,request_sha,request_json,canonical(receipt)))
            return receipt

    def owned_schedule_receipt(self,identifier,request_id):
        """Read only an exact historical receipt; never refresh or adopt a node."""
        self._known_node(identifier);canonical_uuid(request_id)
        with self._db() as db:
            row=db.execute('SELECT * FROM owned_maintenance_schedules WHERE request_id=? AND node_id=?',
                           (request_id,identifier)).fetchone()
            if row is None:return None
            if row['authority_id']!=self.authority_id:raise Conflict('coordinator authority changed')
            return json.loads(row['receipt_json'])

    def cancel_node(self, identifier, payload):
        self.fields(payload, ("expectedRevision", "planId"))
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._refresh_nodes(db)
            row = self._node(db, identifier)
            self._cas(row, payload, plan=True)
            if row["state"] not in ("scheduled", "draining"):
                raise Conflict("maintenance can no longer be canceled")
            db.execute("UPDATE nodes SET state='active',revision=revision+1 WHERE id=?", (identifier,))
            return self._node_public(self._node(db, identifier))

    def begin_node_update(self, identifier, payload):
        self.fields(payload, ("expectedRevision", "planId"))
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            self._refresh_nodes(db)
            row = self._node(db, identifier)
            self._cas(row, payload, plan=True)
            if row["state"] != "draining" or self._now() < utc_stamp(row["not_before"])[0]:
                raise Conflict("node is not ready for its update")
            if db.execute("SELECT 1 FROM jobs WHERE node_id=? AND status IN ('running','held')", (identifier,)).fetchone():
                raise Conflict("node still owns running or held work")
            token = secrets.token_urlsafe(32)
            db.execute("""UPDATE nodes SET state='updating',revision=revision+1,update_hash=?,
                finish_status=NULL WHERE id=?""", (hashlib.sha256(token.encode()).hexdigest(), identifier))
            return {"node": self._node_public(self._node(db, identifier)), "updateToken": token}

    def finish_node_update(self, identifier, payload):
        self.fields(payload, ("planId", "updateToken", "status", "result"))
        if (not isinstance(payload["updateToken"], str) or not 1 <= len(payload["updateToken"]) <= 128
                or payload["status"] not in ("succeeded", "failed") or not isinstance(payload["result"], dict)):
            raise ValueError("invalid maintenance completion")
        if payload["status"] == "succeeded":
            if payload["result"].get("healthy") is not True:
                raise ValueError("successful update requires healthy:true")
        result = json.dumps(payload["result"], sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(result.encode()) > 4096:
            raise ValueError("maintenance result exceeds limit")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._node(db, identifier)
            row_version = row['target_engine_version']
            if payload['status'] == 'succeeded' and (
                    type(row_version) is not str or not ENGINE_VERSION.fullmatch(row_version)
                    or payload['result'].get('engineVersion') != row_version):
                raise Conflict('target_engine_version_unbound_or_mismatched')
            hashed = hashlib.sha256(payload["updateToken"].encode()).hexdigest()
            if (payload["planId"] != row["plan_id"] or not row["update_hash"]
                    or not hmac.compare_digest(hashed, row["update_hash"])):
                raise Conflict("invalid maintenance claim")
            if row["finish_status"]:
                if row["finish_status"] == payload["status"] and row["result"] == result:
                    return self._node_public(row)
                raise Conflict("maintenance completion differs")
            if row["state"] != "updating":
                raise Conflict("maintenance is held for operator inspection")
            state = "active" if payload["status"] == "succeeded" else "failed"
            db.execute("""UPDATE nodes SET state=?,revision=revision+1,result=?,finish_status=?
                WHERE id=?""", (state, result, payload["status"], identifier))
            return self._node_public(self._node(db, identifier))

    def hold_node(self, identifier, payload):
        """Fence an uncertain updater; does not stop containers or restore health."""
        self.fields(payload, ("expectedRevision", "planId", "note"))
        if (not isinstance(payload["note"], str) or not payload["note"].strip()
                or len(payload["note"]) > 256):
            raise ValueError("operator inspection note required")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._node(db, identifier)
            self._cas(row, payload, plan=True)
            if row["state"] != "updating":
                raise Conflict("only an updating node can be held")
            result = json.dumps({"operatorAction": "hold", "note": payload["note"]})
            db.execute("""UPDATE nodes SET state='held',revision=revision+1,result=?,
                update_hash=NULL,finish_status=NULL WHERE id=?""", (result, identifier))
            return self._node_public(self._node(db, identifier))

    def resume_node(self, identifier, payload):
        self.fields(payload, ("expectedRevision", "planId", "note", "workerStopped", "healthy"))
        if (payload["workerStopped"] is not True or payload['healthy'] is not True or not isinstance(payload["note"], str)
                or not payload["note"].strip() or len(payload["note"]) > 256):
            raise ValueError("operator note, healthy:true and workerStopped:true required")
        with self._db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = self._node(db, identifier)
            self._cas(row, payload, plan=True)
            if row["state"] not in ("held", "failed"):
                raise Conflict("only held or failed maintenance can be resumed")
            if db.execute("SELECT 1 FROM jobs WHERE node_id=? AND status IN ('running','held')", (identifier,)).fetchone():
                raise Conflict("node still owns running or held work")
            result = json.dumps({"operatorAction": "resume", "note": payload["note"], "workerStopped": True, 'healthy': True})
            db.execute("""UPDATE nodes SET state='active',revision=revision+1,result=?,
                update_hash=NULL,finish_status=NULL WHERE id=?""", (result, identifier))
            return self._node_public(self._node(db, identifier))
