"""Disabled local CAS-intent journal; no custody or stream authority."""
import json
from . import reference_v2_remote_anchor_candidate as remote

QUALIFIED=False
DDL=('CREATE TABLE anchor_intents (operation TEXT PRIMARY KEY, request TEXT NOT NULL, '
     "state TEXT NOT NULL CHECK(state IN ('pending','recorded')), receipt TEXT)")

def install(db):
    if QUALIFIED is not True:raise remote.AnchorHeld('unqualified client journal')
    if db.execute("SELECT 1 FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' LIMIT 1").fetchone():raise remote.AnchorHeld('new isolated journal required')
    db.execute(DDL)

def _attest(db):
    row=db.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='anchor_intents'").fetchone()
    if row is None or row[0]!=DDL:raise remote.AnchorHeld('client journal schema changed')

def prepare(db,request):
    """Caller owns transaction and durable commit before any remote mutation."""
    if QUALIFIED is not True:raise remote.AnchorHeld('unqualified client journal')
    _attest(db)
    if type(request) is not dict or set(request)!={'domain','ledger','epoch','operation','previousRevision','previousDigest','nextRevision','nextDigest'}:raise remote.AnchorHeld('invalid intent')
    raw=json.dumps(request,sort_keys=True,separators=(',',':'),allow_nan=False)
    if len(raw)>4096 or type(request['operation']) is not str:raise remote.AnchorHeld('invalid intent')
    db.execute("INSERT OR IGNORE INTO anchor_intents VALUES(?,?,'pending',NULL)",(request['operation'],raw))
    row=db.execute('SELECT request,state FROM anchor_intents WHERE operation=?',(request['operation'],)).fetchone()
    if row is None or row[0]!=raw:raise remote.AnchorHeld('intent changed')
    return row[1]

def complete(db,request,receipt):
    """Record only the exact authenticated-session result; not freshness proof."""
    if QUALIFIED is not True:raise remote.AnchorHeld('unqualified client journal')
    _attest(db)
    raw=json.dumps(request,sort_keys=True,separators=(',',':'),allow_nan=False)
    expected=dict(domain=request['domain'],ledger=request['ledger'],epoch=request['epoch'],operation=request['operation'],revision=request['nextRevision'],digest=request['nextDigest'])
    if type(receipt) is not dict or receipt!=expected:raise remote.AnchorHeld('receipt changed')
    encoded=json.dumps(receipt,sort_keys=True,separators=(',',':'),allow_nan=False)
    row=db.execute('SELECT request,state,receipt FROM anchor_intents WHERE operation=?',(request['operation'],)).fetchone()
    if row is None or row[0]!=raw:raise remote.AnchorHeld('intent changed')
    if row[1]=='recorded':
        if row[2]!=encoded:raise remote.AnchorHeld('receipt changed')
        return
    if row[1]!='pending' or row[2] is not None:raise remote.AnchorHeld('intent state changed')
    if db.execute("UPDATE anchor_intents SET state='recorded',receipt=? WHERE operation=? AND request=? AND state='pending'",(encoded,request['operation'],raw)).rowcount!=1:raise remote.AnchorHeld('intent race')
