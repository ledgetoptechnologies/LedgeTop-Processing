"""Synthetic barrier child: die after NodeGate ACK before release confirmation."""
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch
from processing.local_capacity_authority_candidate import LocalCapacityAuthority
from processing.managed_capacity_gate_candidate import ManagedCapacityGate
from processing.node_gate import NodeGate
from processing.scratch_policy import ScratchLedger


def main():
    root=Path(sys.argv[1]);data=json.loads((root/'crash-input.json').read_text())
    ledger=ScratchLedger(root/'ledger.sqlite',root,capacity_probe=lambda _:10000,safety_floor_bytes=100)
    journal=root/'journal.sqlite';binding=root/'authority.json'
    with patch('processing.local_capacity_authority_candidate.LOCAL_CAPACITY_AUTHORITY_QUALIFIED',True), \
         patch('processing.crossfs_admission_candidate.CROSSFS_QUALIFIED',True), \
         patch('processing.crossfs_admission_candidate.CROSSFS_CANONICAL_JOURNAL',journal), \
         patch('processing.managed_capacity_gate_candidate.MANAGED_CAPACITY_QUALIFIED',True), \
         patch('processing.node_gate.canonical_gate_path',return_value=root/'crash-node-gate'), \
         LocalCapacityAuthority(root,binding,journal,{'local':ledger}) as authority:
        gate=ManagedCapacityGate(authority,b'synthetic-private-key-material-32bytes',ledger=ledger)
        fields=gate._permit_fields(data['job'],data['permit'])
        gate._record_release(fields,data['release'],'pending')
        with NodeGate('crash-engine') as node:
            node.acknowledge(data['receipt'],data['ack'])
            os._exit(23)
    raise RuntimeError('expected child exit')

if __name__=='__main__':main()
