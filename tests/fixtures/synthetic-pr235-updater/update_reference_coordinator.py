"""Synthetic-only copy of PR235 updater census; no update methods.

The volume_census function is copied unchanged. This fixture cannot activate,
stop, restart, restore or modify an operational coordinator.
"""

if __name__ == "__main__":
    raise SystemExit("synthetic census fixture cannot execute updates")

def volume_census(root, *, uid=10001, offline=False):
    """Pure read-only census; also serialized into the network-none helper.

    Job/receipt contents never leave this function. Only the two system node
    rows are retained privately to prove exact operation-owned CAS transitions.
    """
    import hashlib, json, os, re, sqlite3, stat
    from pathlib import Path
    root=Path(root)
    root_info=root.lstat()
    assert stat.S_ISDIR(root_info.st_mode) and root_info.st_uid==uid and root_info.st_gid==uid
    def encode(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    def sha(value):return hashlib.sha256(encode(value)).hexdigest()
    allowed={'jobs','sqlite_sequence','coordinator_identity','nodes','viewer_tasks','viewer_files','viewer_incarnations',
        'reference_jobs','reference_acceptances','reference_outbox','reference_claims','reference_worker_receipts',
        'reference_transfer_grants','reference_worker_failures','reference_terminal_acceptances','owned_maintenance_schedules',
        'viewer_reservations','viewer_option_evidence'}
    files=[]
    for path in sorted(root.rglob('*')):
        relative=path.relative_to(root).as_posix();info=path.lstat()
        assert info.st_uid==uid and info.st_gid==uid and not stat.S_ISLNK(info.st_mode)
        # The reviewed baseline contains system metadata and empty native dirs.
        assert relative in ('jobs.sqlite3','coordinator.lock','viewer-tasks','viewer-spool')
        if stat.S_ISDIR(info.st_mode):files.append(dict(path=relative,kind='directory',mode=stat.S_IMODE(info.st_mode)))
        else:
            assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_size<=67108864
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW)
            try:
                before=os.fstat(fd);assert (before.st_dev,before.st_ino)==(info.st_dev,info.st_ino)
                h=hashlib.sha256();size=0
                while True:
                    data=os.read(fd,1048576)
                    if not data:break
                    size+=len(data);assert size<=info.st_size;h.update(data)
                after=path.lstat()
                assert size==info.st_size and (after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns)==(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns)
            finally:os.close(fd)
            files.append(dict(path=relative,kind='file',mode=stat.S_IMODE(info.st_mode),bytes=size,sha256=h.hexdigest()))
    assert any(item['path']=='jobs.sqlite3' for item in files)
    uri='file:'+str(root/'jobs.sqlite3')+'?mode=ro'+('&immutable=1' if offline else '')
    db=sqlite3.connect(uri,uri=True,timeout=3)
    try:
        db.execute('BEGIN')
        assert db.execute('PRAGMA quick_check').fetchone()[0]=='ok'
        objects=[list(row) for row in db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name")]
        names={row[1] for row in objects if row[0]=='table'}
        if names-allowed or not {'jobs','coordinator_identity','nodes','reference_jobs','viewer_tasks'}<=names:
            raise AssertionError('unreviewed database tables')
        # These tables are introduced together by the current Viewer store.
        # A partial or modified schema is not a safe compatibility signal.
        viewer_extra={'viewer_reservations','viewer_option_evidence'}
        if names & viewer_extra:
            if not viewer_extra <= names: raise AssertionError('partial Viewer schema')
            expected={
                'viewer_reservations':"CREATE TABLE viewer_reservations ( uuid TEXT PRIMARY KEY, environment TEXT NOT NULL, capability_hash TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('reserved','cancelled')))",
                'viewer_option_evidence':"CREATE TABLE viewer_option_evidence ( uuid TEXT NOT NULL, generation INTEGER NOT NULL, definitions TEXT NOT NULL, sha256 TEXT NOT NULL, PRIMARY KEY(uuid,generation))",
            }
            for name in viewer_extra:
                entries=[row for row in objects if row[2]==name]
                table=[row for row in entries if row[0]=='table']
                indexes=[row for row in entries if row[0]=='index']
                if (len(table)!=1 or not isinstance(table[0][3],str)
                        or re.sub(r'\s+',' ',table[0][3]).strip()!=expected[name]
                        or indexes!=[['index','sqlite_autoindex_'+name+'_1',name,None]]
                        or len(entries)!=2):raise AssertionError('unreviewed Viewer schema')
        if any(row[0] in ('trigger','view') for row in objects):raise AssertionError('unreviewed database object')
        if not all(row[0]!='index' or row[1].startswith('sqlite_autoindex_') or row[1]=='viewer_engine_uuid' for row in objects):raise AssertionError('unreviewed database index')
        tables={}
        for name in sorted(names):
            assert name in allowed
            columns=[list(row) for row in db.execute('PRAGMA table_info("'+name+'")')]
            rows=[]
            for row in db.execute('SELECT * FROM "'+name+'"'):
                rows.append([{'blob':value.hex()} if isinstance(value,bytes) else value for value in row])
            rows.sort(key=encode)
            tables[name]=dict(count=len(rows),sha256=sha(rows),columns=columns)
        authority=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]
        active=db.execute("SELECT COUNT(*) FROM jobs WHERE status NOT IN ('succeeded','failed')").fetchone()[0]
        assert active==0
        for name in names:
            if (name.startswith(('reference_','viewer_')) or name=='owned_maintenance_schedules') and tables[name]['count']!=0:
                raise AssertionError('nonempty guarded table')
        node_columns=[row[1] for row in db.execute('PRAGMA table_info(nodes)')]
        nodes={row[0]:dict(zip(node_columns,row)) for row in db.execute('SELECT * FROM nodes ORDER BY id')}
        return dict(authorityId=authority,schema=objects,tables=tables,files=files,nodeRows=nodes,
            rootMode=stat.S_IMODE(root_info.st_mode),sqliteQuickCheck='ok')
    finally:db.close()
