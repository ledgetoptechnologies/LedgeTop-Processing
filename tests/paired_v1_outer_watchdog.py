"""Own the entire synthetic Python/Node fixture tree under a Windows Job."""
import os
from pathlib import Path
import sys

from tests.paired_owned_windows import run_owned

if os.name!='nt':raise RuntimeError('Windows Job Object required')
os.chdir(Path(__file__).resolve().parents[1])
code,output=run_owned([sys.executable,'-m','unittest','-v',
    'tests.test_paired_v1_viewer_http'],timeout=75)
sys.stdout.buffer.write(output);sys.stdout.flush()
if code:raise SystemExit(code)
