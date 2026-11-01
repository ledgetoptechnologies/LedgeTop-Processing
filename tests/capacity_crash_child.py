"""Owned synthetic child: die after one remote reserve response, before journaling."""
import json
import os
from pathlib import Path
import ssl
import sys
from unittest.mock import patch
from ops.reference_worker_daemon import DaemonStateLock
from processing.multihost_capacity_journal_candidate import MultiHostCapacityJournal, PinnedCapacityConnector


def main():
    root,port,server_cert,client_cert,client_key,pin=sys.argv[1:]
    ctx=ssl.create_default_context(cafile=server_cert);ctx.load_cert_chain(client_cert,client_key)
    connector=PinnedCapacityConnector(host='127.0.0.1',port=int(port),context=ctx,server_pin=pin,authority_id='a'*32)
    real=connector.observe
    def exit_after_reply(**kwargs):
        real(**kwargs)
        os._exit(23)
    connector.observe=exit_after_reply
    plan=dict(attempt_id='attempt',job_id='job',generation=1,manifest_sha256='a'*64,
        environment='staging',ttl_seconds=60,demands=[{'ledger':'local','bytes':300}])
    with patch('processing.multihost_capacity_journal_candidate.MULTIHOST_CAPACITY_QUALIFIED',True), \
         patch('processing.remote_capacity_session_candidate.REMOTE_CAPACITY_QUALIFIED',True), \
         DaemonStateLock(Path(root)) as owner:
        driver=MultiHostCapacityJournal(owner=owner,journal=Path(root)/'intent.sqlite',authorities={'one':connector})
        driver.observe(attempt_id='attempt',plans={'one':plan})
    raise RuntimeError('expected abrupt owned child exit')

if __name__=='__main__':main()
