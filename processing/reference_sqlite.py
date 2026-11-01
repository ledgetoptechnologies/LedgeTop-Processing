"""Conservative classification of SQLite's explicit contention result codes."""
import sqlite3


def is_sqlite_contention(error):
    code = getattr(error, 'sqlite_errorcode', None)
    return (isinstance(error, sqlite3.OperationalError) and type(code) is int
            and (code & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))
