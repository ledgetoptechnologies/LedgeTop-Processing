"""Fixture-only immutable v2 geometry registry; never transfer or reserve bytes.

Not a protocol activation or a cross-version fence against older binaries.
"""
import sqlite3
from . import reference_protocol as p
from .viewer_archive_spool import ViewerArchiveSpool,SpoolHeld
from .viewer_chunk_spool import SyntheticChunkSpool

DDL='CREATE TABLE viewer_chunk_v2_geometry (job_id TEXT PRIMARY KEY, binding TEXT NOT NULL)'
MAX_BYTES=4*1024*1024*1024
MAX_CHUNK_BYTES=1024*1024
MAX_CHUNKS=4096

def geometry(archive_bytes,chunk_bytes,version):
    if type(version) is not int or version!=2:raise SpoolHeld('v2_version_required')
    if type(archive_bytes) is not int or not 1<=archive_bytes<=MAX_BYTES:raise SpoolHeld('v2_archive_limit')
    if type(chunk_bytes) is not int or not 1<=chunk_bytes<=MAX_CHUNK_BYTES:raise SpoolHeld('v2_chunk_limit')
    count=(archive_bytes+chunk_bytes-1)//chunk_bytes
    if count>MAX_CHUNKS:raise SpoolHeld('v2_count_limit')
    return dict(version=2,archiveBytes=archive_bytes,chunkBytes=chunk_bytes,chunkCount=count)

class SyntheticV2Geometry:
    def __init__(self,spool,*,synthetic_fixture=False):
        if synthetic_fixture is not True or type(spool) is not ViewerArchiveSpool:raise SpoolHeld('synthetic_fixture_required')
        self.spool=spool

    def negotiate(self,job,identity,token,*,version,chunk_bytes):
        """Persist exact authenticated binding; return held, not dispatch authority."""
        p._uuid(job,'job ID')
        with self.spool.claims.queue._db() as db:
            db.execute('BEGIN IMMEDIATE')
            saved,_=self.spool.claims._authorize_inventory(db,job,identity,token)
            metadata=self.spool._metadata(db,job,identity,token)
            SyntheticChunkSpool._admit_operation(db,job)
            if db.execute('SELECT 1 FROM viewer_worker_chunk_sessions WHERE job_id=?',(job,)).fetchone() or db.execute('SELECT 1 FROM viewer_worker_spool WHERE job_id=?',(job,)).fetchone():
                raise SpoolHeld('v1_or_existing_spool_requires_inspection')
            item=geometry(metadata['archiveBytes'],chunk_bytes,version)
            binding=dict(kind='synthetic-v2-geometry-held',jobId=job,claimSha256=p.digest(saved),metadataSha256=p.digest(metadata),geometry=item)
            encoded=p.canonical_json(binding).decode()
            objects=db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='viewer_chunk_v2_geometry' ORDER BY type,name").fetchall()
            if not objects:
                db.execute(DDL)
            expected=sqlite3.connect(':memory:')
            try:
                expected.execute(DDL)
                schema=lambda conn:[tuple(x) for x in conn.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name='viewer_chunk_v2_geometry' ORDER BY type,name")]
                if schema(db)!=schema(expected):raise SpoolHeld('v2_schema_requires_inspection')
            finally:expected.close()
            old=db.execute('SELECT binding FROM viewer_chunk_v2_geometry WHERE job_id=?',(job,)).fetchone()
            if old and old['binding']!=encoded:raise SpoolHeld('v2_geometry_changed')
            if not old:db.execute('INSERT INTO viewer_chunk_v2_geometry VALUES (?,?)',(job,encoded))
            return dict(state='held_no_transfer',binding=binding)
