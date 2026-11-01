"""Unwired bounded indexed inventory experiment; not authority or recovery.

Checks metadata-to-name correspondence in O(n log n) with private in-memory
SQLite. The caller must fence writers and perform independent content/identity
verification; this module does not make a directory listing immutable.
"""
import re
import sqlite3

MAX_FILES = 100000
MAX_ENTRIES = 200000
_NAME = re.compile(r'input-[0-9a-f]{32}\Z')

class InventoryHeld(ValueError):
    pass

def compare(metadata, entries, *, max_files=MAX_FILES, max_entries=MAX_ENTRIES):
    """Compare (name_key, path) pairs with (entry_name, is_regular) pairs.

    Both iterables may be lazy. Stops at a fixed count and stores at most that
    count; never opens, changes, follows, or deletes directory entries.
    """
    if type(max_files) is not int or not 1<=max_files<=MAX_FILES or type(max_entries) is not int or not 1<=max_entries<=MAX_ENTRIES:
        raise ValueError('invalid inventory bound')
    db=sqlite3.connect(':memory:')
    try:
        db.execute('PRAGMA temp_store=MEMORY')
        db.execute('CREATE TABLE expected(name_key TEXT PRIMARY KEY, path TEXT NOT NULL UNIQUE, seen INTEGER NOT NULL DEFAULT 0) WITHOUT ROWID')
        count=0
        for item in metadata:
            count+=1
            if count>max_files:raise InventoryHeld('metadata exceeds inventory bound')
            try:key,path=item
            except (TypeError,ValueError):raise InventoryHeld('invalid inventory metadata') from None
            if (type(key) is not str or not key or len(key.encode('utf-8'))>255
                    or type(path) is not str or not _NAME.fullmatch(path)):
                raise InventoryHeld('invalid inventory metadata')
            try:db.execute('INSERT INTO expected(name_key,path) VALUES(?,?)',(key,path))
            except sqlite3.IntegrityError:raise InventoryHeld('duplicate inventory identity') from None
        found=0
        for index,item in enumerate(entries,1):
            if index>max_entries:raise InventoryHeld('directory exceeds inventory bound')
            try:name,regular=item
            except (TypeError,ValueError):raise InventoryHeld('invalid directory entry') from None
            if type(name) is not str or type(regular) is not bool:
                raise InventoryHeld('invalid directory entry')
            if name.startswith('.incoming-'):raise InventoryHeld('incoming input needs inspection')
            if name.startswith('input-'):
                if not regular or not _NAME.fullmatch(name):raise InventoryHeld('unsafe input entry')
                changed=db.execute('UPDATE expected SET seen=1 WHERE path=? AND seen=0',(name,)).rowcount
                if changed!=1:raise InventoryHeld('unexpected or duplicate input entry')
                found+=1
        if found!=count or db.execute('SELECT 1 FROM expected WHERE seen=0 LIMIT 1').fetchone():
            raise InventoryHeld('input inventory differs')
        return {'files':count,'entries':index if 'index' in locals() else 0}
    finally:
        db.close()
