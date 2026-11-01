"""Synthetic NodeODM in a separately owned process; never processes real data."""
import argparse
import json
from pathlib import Path
import re
import sys
import time

from ops.native_node_fixture import NativeNodeFixture
import ops.native_node_fixture as native_fixture
from tests.joined_ingest_archive import ARCHIVE

native_fixture.ARCHIVE=ARCHIVE


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--root',required=True)
    args=parser.parse_args()
    root=Path(args.root).resolve()
    fixture=NativeNodeFixture(root/'engine.sqlite',synthetic_enabled=True)
    previous=fixture._dispatch
    release=root/'engine-release'
    def gated(method,path,headers,body):
        status,value,binary=previous(method,path,headers,body)
        if (method=='GET' and re.fullmatch(r'/task/[0-9a-f-]{36}/info',path)
                and status==200 and isinstance(value,dict)
                and value.get('status',{}).get('code')==40 and not release.exists()):
            value=dict(value,status={'code':20})
        return status,value,binary
    fixture._dispatch=gated
    try:
        fixture.start()
        ready=root/'engine-ready.json'
        with ready.open('x',encoding='utf-8') as stream:
            json.dump({'state':'ready','origin':fixture.origin},stream)
            stream.flush()
        print('SYNTHETIC_ENGINE_READY',flush=True)
        # EOF or a single stop line is an orderly stop; the outer process owns
        # this pipe and bounds the child lifetime.
        sys.stdin.readline()
    finally:fixture.close()

if __name__=='__main__':main()
