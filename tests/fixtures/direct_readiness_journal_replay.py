"""Synthetic subprocess replay; no live worker credentials or service."""
import json
import sys
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from processing import node_gate, direct_worker_readiness_journal_v2 as journal
from processing.scratch_policy import ScratchLedger
from processing.source_spool import SourceSpool

value=json.load(sys.stdin);root=Path(value['root'])
node_gate.canonical_gate_path=lambda _:root/'gate'
ledger=ScratchLedger(root/'private'/'ledger.sqlite',root,capacity_probe=lambda _:1000000,
    clock=lambda:100,safety_floor_bytes=100)
spool=SourceSpool(value['spool'],mount_path='/tmp',mount_id='a'*32,
    marker_name=value['marker'],safety_floor_bytes=0,require_mount=True)
key=serialization.load_pem_private_key(value['key'].encode(),password=None)
import processing.direct_worker_fence_proof_v2 as proof
proof.time.time=lambda:110
result=journal.issue(ledger,spool,value['item'],principal='coordinator-a',expected_worker=value['worker'],
    engine_id='fixture-v2',coordinator_origin='https://127.0.0.1:8443',gate_generation=1,
    challenge_id=value['challenge'],validity_seconds=10,issuer_id='worker-issuer',key_id='worker-key',private_key=key)
print(json.dumps(result,sort_keys=True))
