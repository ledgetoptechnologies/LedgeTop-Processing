"""Unwired experimental TaskStore-to-ingress file intent schema."""

QUALIFIED = False
DDL = '''CREATE TABLE viewer_engine_upload_intents (upload_id TEXT PRIMARY KEY, job_id TEXT NOT NULL,
 task_uuid TEXT NOT NULL, generation INTEGER NOT NULL, engine_uuid TEXT NOT NULL, claim_hash TEXT NOT NULL,
 name_key TEXT NOT NULL, name TEXT NOT NULL, path TEXT NOT NULL, bytes INTEGER NOT NULL,
 sha256 TEXT NOT NULL, state TEXT NOT NULL)'''
