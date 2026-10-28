"""Export only the pinned joint-oracle dependency closure, without Viewer writes."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from tests.test_reference_joint_flow import PINS,VIEWER_COMMIT


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    if args.output.exists():parser.error('output must be a new snapshot directory')
    exports={}
    for relative,expected in PINS.items():
        name='apps/viewer/'+relative
        raw=subprocess.check_output(['git','-C',str(args.repo),'show',VIEWER_COMMIT+':'+name],timeout=10)
        if hashlib.sha256(raw).hexdigest()!=expected:raise ValueError('immutable_viewer_source_pin_changed:'+name)
        exports[name]=raw
    args.output.mkdir(parents=True)
    for name,raw in exports.items():
        target=args.output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
    receipt=dict(viewerCommit=VIEWER_COMMIT,files={'apps/viewer/'+key:value for key,value in PINS.items()},sourceOnly=True)
    (args.output/'snapshot-receipt.json').write_text(json.dumps(receipt,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(viewerCommit=VIEWER_COMMIT,files=len(exports),snapshot=str(args.output))))


if __name__=='__main__':main()
