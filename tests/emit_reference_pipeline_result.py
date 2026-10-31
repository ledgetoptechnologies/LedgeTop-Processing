"""Emit an actual stage-journal closure from synthetic engine callbacks only.

Run on Linux with ``python -B -m tests.emit_reference_pipeline_result``.
No real processing, network service, NAS or genuine acquisition is exercised.
"""
import json

from tests.test_reference_pipeline import PipelineTests


def main():
    case=PipelineTests('test_one_parent_original_input_selection_and_one_final_existing_wire_closure')
    try:
        case.setUp()
        case.advance('odm');case.advance('treedar')
        result=case.journal.final_result(case.claim['jobId'])
        binding={key:case.claim[key] for key in ('jobId','generation','environment','pipelineDigest')}
        binding['requestedEngines']=['odm','treedar']
        print(json.dumps(dict(resultManifest=result,binding=binding)))
    finally:case.doCleanups()


if __name__=='__main__':main()
