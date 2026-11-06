"""Task-owned synthetic worker process for V2 restart qualification."""
import json
import ssl
import sys
import time
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from processing import direct_v2_inventory as inventory, node_gate
from processing.direct_worker_receiver_v2 import DirectWorkerBoundaryV2
from processing.direct_worker_receipt_v2 import WorkerClosureSignerV2
from processing.direct_worker_http import create_direct_worker_server
from processing.scratch_policy import ScratchLedger
from processing.source_spool import SourceSpool

config=json.loads(Path(sys.argv[1]).read_text())
node_gate.canonical_gate_path=lambda _:Path(config['gate'])
options=config['verification'];options['allowed_odm_options']=set(options['allowed_odm_options'])
prepared=inventory.prepare(config['payload'],**options)
index=inventory.prepare_worker_index(prepared,expected_binding=config['expected'],
    reservation_binding=config['reservation'],**options)
root=Path(config['root'])
ledger=ScratchLedger(root/'private'/'ledger.sqlite',root,capacity_probe=lambda _:1000000,
                     clock=lambda:100,safety_floor_bytes=100)
spool=SourceSpool(config['spool'],mount_path='/tmp',mount_id='a'*32,marker_name=config['marker'],
                  safety_floor_bytes=0,require_mount=True)
if config.get('block_first'):
    original_receive=spool.receive_object
    def block_receive(**kwargs):
        source=kwargs['reader']
        class PartialReader:
            def __init__(self):self.first=True
            def read(self,count):
                if self.first:
                    self.first=False
                    return source.read(min(count,1))
                (root/'writer-entered').write_text('synthetic partial writer')
                while True:time.sleep(1)
        kwargs['reader']=PartialReader()
        return original_receive(**kwargs)
    spool.receive_object=block_receive
key=serialization.load_pem_private_key(config['key'].encode(),password=None)
signer=WorkerClosureSignerV2(issuer_id='worker-issuer',key_id='worker-key',identity=config['worker'],private_key=key)
import processing.direct_worker_receiver_v2 as receiver
receiver.time.time=lambda:1000 if config['resume'] else 110
boundary=DirectWorkerBoundaryV2(ledger=ledger,spool=spool,inventory=index,item=config['item'],
    reservation_binding=config['reservation'],expected_worker=config['worker'],engine_id='fixture-v2',
    coordinator_origin='https://127.0.0.1:8443',gate_generation=1,bearer=config['bearer'],
    readiness_proof=config['proof'],challenge_id=config['challenge'],issuer_id='worker-issuer',key_id='worker-key',
    public_key=key.public_key(),closure_expected_binding=config['expected'],closure_verification=options,
    worker_signer=signer,resume_observation_only=config['resume'])
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.minimum_version=ssl.TLSVersion.TLSv1_2
context.load_cert_chain(str(root/'fixture-cert.pem'),str(root/'fixture-key.pem'))
server=create_direct_worker_server(boundary,'127.0.0.1',0,ssl_context=context,idle_timeout=2)
print(server.server_port,flush=True)
try:server.serve_forever(poll_interval=.05)
finally:server.server_close()
