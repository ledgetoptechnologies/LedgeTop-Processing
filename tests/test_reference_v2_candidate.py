"""Inactive candidate layer conformance; fixture authority remains unimplemented."""
import base64
import copy
from dataclasses import FrozenInstanceError
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing import reference_v2_candidate as m

ROOT=Path(__file__).resolve().parents[1]/'contracts/reference-v2-candidate'
SCHEMA=json.loads((ROOT/'contract.schema.json').read_bytes())
GOLDENS=[json.loads(path.read_bytes()) for path in sorted((ROOT/'valid').glob('*.json'))]
COMBINED=json.loads((ROOT/'valid/combined.json').read_bytes())
SUPPORT=json.loads((ROOT/'valid/support-records.json').read_bytes())
INDEX=json.loads((ROOT/'invalid/index.json').read_bytes())


def wire(value):return json.dumps(value,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def document(kind,engine=None):return copy.deepcopy(next(v['value'] for v in COMBINED['documents'] if v['type']==kind and (engine is None or v['value'].get('stage',{}).get('engine')==engine)))
def reseal(value,key):
    value=copy.deepcopy(value);value.pop(key,None);value[key]=hashlib.sha256(m.canonical_metadata(value)).hexdigest();return value


def samples():
    values={v['type']:copy.deepcopy(v['value']) for fixture in GOLDENS for v in fixture['documents']}
    submission=document('Submission');manifest=submission['inputManifest'];result=document('ResultManifest','odm');original=document('OriginalLidarCustodyProof')['objects'][0]
    request=document('ImmutableMetadataIntent');context=COMBINED['expectedContext']
    values.update(Slug='opaque-1',WorkerId='worker-main',UUID=context['originalBinding']['jobId'],SubmissionUUID=submission['submissionId'],
        Sha256='a'*64,Positive=1,Natural=0,Time='2026-10-28T12:00:00.000Z',Environment='staging',Engine='odm',EngineVersion='3.8.5',
        Basename='original.las',ArchivePath='folder/file.bin',Product='report',Purpose='next_stage_admission',
        JsonValue={'key':[0,1.25,None,'é🌲']},Options={'synthetic-resolution':-1.25,'label':'é🌲'},
        Binding=context['originalBinding'],StageBinding=result['stage'],Pipeline=submission['pipeline'],
        OdmRequest=submission['pipeline']['odm'],TreeDarRequest=submission['pipeline']['treedar'],ProductRequest=submission['pipeline']['odm']['requestedProducts'][0],
        InputManifest=manifest,InputCounts=manifest['inputCounts'],InputObject=manifest['objects'][0],CustodyObject=original,
        OriginalObjectBinding={key:original[key] for key in ('objectId','byteSize','sha256','controlledObjectId','physicalReceiptId')},
        Artifact=result['objects'][0],ProductOutcome=result['products'][0],ProductTarget=result['products'][0]['targets'][0],
        ArchiveMemberReference=result['products'][0]['targets'][0]['memberRefs'][0],
        AcceptedArtifact=document('PartialAcceptance')['acceptedArtifacts'][0],StageResultReference=document('ParentCompletion')['stageResults'][0],
        EptClosureEntry=values['PointcloudZipManifest']['files'][0],
        OdmArchiveClosureManifest=copy.deepcopy(COMBINED['closureDescriptors'][0]['descriptor']),OdmArchiveClosureEntry=copy.deepcopy(COMBINED['closureDescriptors'][0]['descriptor']['files'][0]),
        SettledStage=document('ExecutionSettledReceipt')['settledStages'][0],OriginalReceiptIdentity=request['originalAcquisition'],OriginalObject=request['originalObjects'][0],
        TreeDarBundleLimits=context['treeDarLimits'],ArchiveAdmissionLimits=context['archiveLimits'])
    values['SourceMetadataReauthorization']=dict(schemaVersion=2,receiptType='original_source_reauthorization',receiptId='summary-only',stage=request['stage'],originalCustodyReceiptId='original-custody',originalObjects=[values['OriginalObjectBinding']],checkedAt='2026-10-28T12:00:00.000Z',metadataExpiresAt='2026-10-28T12:05:00.000Z',metadataOperationId='metadata-operation-1',payloadBudgetLedgerId='unchanged-payload-ledger')
    assert set(values)==set(SCHEMA['$defs']),(set(SCHEMA['$defs'])-set(values),set(values)-set(SCHEMA['$defs']))
    return values


class CandidateTests(unittest.TestCase):
    def test_all_58_typed_goldens_and_all_60_definitions_are_supported(self):
        count=0
        for fixture in GOLDENS:
            for entry in fixture['documents']:
                result=m.validate_candidate(wire(entry['value']),record_type=entry['type'])
                self.assertEqual(result.value,entry['value']);self.assertEqual(result.status,m.STATUS);count+=1
        self.assertEqual(count,58)
        for kind,value in samples().items():
            with self.subTest(kind=kind):self.assertEqual(m.validate_candidate(wire(value),record_type=kind).value,value)
        self.assertEqual(len(m.candidate_definitions()),60)

    def test_every_declared_lexical_and_schema_rejection_vector_is_executed(self):
        count=0
        for case in INDEX['cases']:
            if case['phase'] not in ('lexical','schema'):continue
            with self.subTest(case=case['id']),self.assertRaises(m.CandidateError):
                m.validate_candidate((ROOT/case['inputFile']).read_bytes(),record_type=case['documentType'])
            count+=1
        self.assertEqual(count,25)

    def test_semantic_authority_and_archive_vectors_stay_explicit_unimplemented_gates(self):
        report=m.coverage_report();coverage={v['id']:v for v in report['cases']}
        self.assertEqual(len(coverage),57)
        for case in INDEX['cases']:
            expected='APPLICABLE_LEXICAL_STRUCTURAL' if case['phase'] in ('lexical','schema') else 'UNIMPLEMENTED_GATE'
            self.assertEqual(coverage[case['id']]['coverage'],expected)
        self.assertIn('CUSTODY_AUTHORITY',report['unimplemented_gates']);self.assertIn('MEMBER_CLOSURE',report['unimplemented_gates'])
        unchanged=(ROOT/'invalid/self-declared-custody-no-authentication.json').read_bytes()
        result=m.validate_candidate(unchanged,record_type='OriginalLidarCustodyProof')
        self.assertEqual(result.status,m.STATUS);self.assertIn('CUSTODY_AUTHORITY',result.unimplemented_gates)
        with self.assertRaises(TypeError):m.validate_candidate(unchanged,record_type='OriginalLidarCustodyProof',trusted=True)

    def test_raw_result_is_immutable_detached_and_does_not_expose_secret_values_in_repr(self):
        value=COMBINED['expectedContext']['originalBinding']
        result=m.validate_candidate(wire(value),record_type='Binding');detached=result.value;detached['generation']=2
        self.assertEqual(result.value['generation'],1)
        with self.assertRaises(FrozenInstanceError):result.status='authorized'
        with self.assertRaises(TypeError):bool(result)
        self.assertNotIn(value['parentClaimId'],repr(result));self.assertFalse(hasattr(result,'is_authorized'))

    def test_schema_root_resource_and_all_named_digest_metadata_are_pinned(self):
        self.assertEqual(hashlib.sha256(m._read_pinned('contract.schema.json')).hexdigest(),m.PINS['contract.schema.json'])
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'contract.schema.json';path.write_bytes(b'{}')
            with patch.object(m,'ROOT',Path(directory)),self.assertRaisesRegex(m.CandidateError,'PINNED_RESOURCE_DIGEST_MISMATCH'):
                m._read_pinned('contract.schema.json')
        with self.assertRaisesRegex(m.CandidateError,'UNTRUSTED_SCHEMA_RESOURCE'):m._read_pinned('../caller-path')

    def test_unknown_schema_vocabulary_refs_forms_and_formats_fail_closed(self):
        cases=[{'unsupported':False},{'unevaluatedProperties':False},{'format':'uri'},{'$ref':'https://example.invalid/schema'},
            {'$ref':'#/$defs/missing'},{'type':['integer','null']},{'additionalProperties':True},{'x-new-behavior':True},{'items':True},{'if':{'type':'object'}}]
        for case in cases:
            with self.subTest(case=case),self.assertRaises(m.CandidateError):m._compile_schema({'$defs':{},**case})

    def test_raw_bytes_and_fixed_type_are_the_only_validation_inputs(self):
        with self.assertRaisesRegex(m.CandidateError,'RAW_BYTES_REQUIRED'):m.validate_candidate('{}',record_type='Binding')
        with self.assertRaisesRegex(m.CandidateError,'UNKNOWN_CANDIDATE_RECORD_TYPE'):m.validate_candidate(b'{}',record_type='Foreign')
        with self.assertRaises(TypeError):m.validate_candidate(b'{}',record_type='Binding',schema={})
        with self.assertRaises(TypeError):m.validate_candidate(b'{}',record_type='Binding',schema_path='/caller/path')

    def test_oversize_depth_array_and_object_limits_precede_json_materialization(self):
        cases=[(b' '*(m.MAX_BYTES+1),'METADATA_BYTES_EXCEEDED'),(b'['*33+b'0'+b']'*33,'DEPTH_EXCEEDED'),
            (b'['+b','.join([b'0']*10001)+b']','ARRAY_MEMBERS_EXCEEDED'),
            (b'{'+b','.join(('"k%d":0'%i).encode() for i in range(257))+b'}','OBJECT_MEMBERS_EXCEEDED')]
        for data,code in cases:
            with self.subTest(code=code),patch.object(m.json,'loads',side_effect=AssertionError('must not materialize')):
                with self.assertRaisesRegex(m.CandidateError,code):m.validate_candidate(data,record_type='JsonValue')
        self.assertEqual(len(m._decode(b'['+b','.join([b'0']*10000)+b']')),10000)
        m.validate_candidate(b'['*32+b'0'+b']'*32,record_type='JsonValue')

    def test_schema_derived_object_and_jsonvalue_array_boundaries(self):
        m.validate_candidate(wire({'k'+str(i):i for i in range(256)}),record_type='Options')
        m.validate_candidate(wire(list(range(256))),record_type='JsonValue')
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(list(range(257))),record_type='JsonValue')

    def test_brackets_commas_and_escaped_quotes_inside_strings_do_not_spend_container_depth(self):
        value={'label':('[{,}]\\"'*400)}
        self.assertEqual(m.validate_candidate(wire(value),record_type='Options').value,value)

    def test_unicode_scalar_pairs_are_valid_and_lone_surrogates_are_not(self):
        self.assertEqual(m.validate_candidate(b'{"label":"\\ud83c\\udf32"}',record_type='Options').value,{'label':'🌲'})
        for raw in (b'{"label":"\\ud800"}',b'{"label":"\\udc00"}',b'{"\\ud800":0}'):
            with self.assertRaisesRegex(m.CandidateError,'INVALID_UNICODE'):m.validate_candidate(raw,record_type='Options')

    def test_escaped_duplicate_and_all_unsafe_keys_are_rejected_at_every_depth(self):
        with self.assertRaisesRegex(m.CandidateError,'DUPLICATE_KEY'):m.validate_candidate(b'{"label":1,"l\\u0061bel":2}',record_type='Options')
        for key in ('__proto__','constructor','prototype'):
            with self.assertRaisesRegex(m.CandidateError,'UNSAFE_KEY'):m.validate_candidate(wire({'label':{key:0}}),record_type='Options')
        with self.assertRaisesRegex(m.CandidateError,'UNSAFE_KEY'):m.validate_candidate(b'{"\\u005f_proto__":0}',record_type='Options')

    def test_numeric_tokens_remain_distinct_at_integer_fields_and_numeric_consts(self):
        for token in (b'1.0',b'1e0',b'true'):
            with self.assertRaises(m.CandidateError):m.validate_candidate(token,record_type='Positive')
        value=document('Submission');raw=wire(value).replace(b'"schemaVersion":2',b'"schemaVersion":2.0')
        with self.assertRaises(m.CandidateError):m.validate_candidate(raw,record_type='Submission')
        raw=wire(value).replace(b'"schemaVersion":2',b'"schemaVersion":2e0')
        with self.assertRaises(m.CandidateError):m.validate_candidate(raw,record_type='Submission')

    def test_declared_options_numbers_allow_finite_floats_but_never_enter_metadata_digest_domain(self):
        value={'resolution':1.25,'scale':-2.5,'integral-float':1.0,'large':1e100}
        self.assertEqual(m.validate_candidate(wire(value),record_type='Options').value,value)
        with self.assertRaisesRegex(m.CandidateError,'CANONICAL_INTEGER_METADATA_ONLY'):m.canonical_metadata(value)
        for token in (b'NaN',b'Infinity',b'-Infinity',b'1e99999'):
            with self.assertRaises(m.CandidateError):m.validate_candidate(b'{"resolution":'+token+b'}',record_type='Options')

    def test_unsafe_integer_and_all_negative_zero_encodings_fail_even_inside_options(self):
        m.validate_candidate(str(m.MAX_INTEGER).encode(),record_type='Positive')
        m.validate_candidate(str(-m.MAX_INTEGER).encode(),record_type='JsonValue')
        for token in (b'9007199254740992',b'-9007199254740992',b'-0',b'-0.0',b'-0e0',b'-0e100'):
            with self.assertRaises(m.CandidateError):m.validate_candidate(b'{"resolution":'+token+b'}',record_type='Options')
        with self.assertRaisesRegex(m.CandidateError,'UNSAFE_INTEGER'):m.validate_candidate(b'9'*1000000,record_type='JsonValue')

    def test_unique_items_uses_json_equality_with_bool_number_separation(self):
        m.validate_candidate(b'[true,1]',record_type='JsonValue')
        for raw in (b'[1,1.0]',b'[1.0,1e0]',b'[{"a":1,"b":2},{"b":2,"a":1}]'):
            with self.assertRaisesRegex(m.CandidateError,'SCHEMA_'):m.validate_candidate(raw,record_type='JsonValue')

    def test_real_timestamp_roundtrip_and_canonical_spelling(self):
        m.validate_candidate(wire('2024-02-29T00:00:00.000Z'),record_type='Time')
        for value in ('2026-02-29T00:00:00.000Z','2026-10-28T24:00:00.000Z','2026-10-28T12:00:00Z','2026-10-28T12:00:00.000+00:00'):
            with self.assertRaises(m.CandidateError):m.validate_candidate(wire(value),record_type='Time')

    def test_actual_role_format_nullable_and_requested_mode_conditions(self):
        artifact=document('ResultManifest','odm')['objects'][0];artifact['format']='geotiff'
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(artifact),record_type='Artifact')
        artifact['format']='zip';artifact['artifactManifestSha256']=None
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(artifact),record_type='Artifact')
        request=document('Submission')['pipeline']['treedar'];request['requested']=False
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(request),record_type='TreeDarRequest')
        outcome=document('ResultManifest','odm')['products'][0];outcome['state']='not_produced'
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(outcome),record_type='ProductOutcome')
        mode=document('Submission')['pipeline'];mode['mode']='odm_only'
        with self.assertRaises(m.CandidateError):m.validate_candidate(wire(mode),record_type='Pipeline')

    def test_every_named_self_digest_is_checked_and_cannot_be_disabled(self):
        for kind,key in m.SELF_DIGESTS.items():
            value=samples()[kind];value=copy.deepcopy(value);value[key]='0'*64
            with self.subTest(kind=kind),self.assertRaisesRegex(m.CandidateError,'NAMED_SELF_DIGEST_MISMATCH'):
                m.validate_candidate(wire(value),record_type=kind)
        with self.assertRaises(TypeError):m.validate_candidate(wire(document('ResultManifest','odm')),record_type='ResultManifest',verify_digest=False)

    def test_nested_input_manifest_digest_and_nested_physical_hash_fields_are_not_omitted(self):
        submission=document('Submission');submission['inputManifest']['objects'][0]['sha256']='f'*64
        with self.assertRaisesRegex(m.CandidateError,'NAMED_SELF_DIGEST_MISMATCH'):m.validate_candidate(wire(submission),record_type='Submission')
        result=document('ResultManifest','odm');result['objects'][0]['sha256']='f'*64
        with self.assertRaisesRegex(m.CandidateError,'NAMED_SELF_DIGEST_MISMATCH'):m.validate_candidate(wire(result),record_type='ResultManifest')
        updated=reseal(result,'resultManifestSha256');structural=m.validate_candidate(wire(updated),record_type='ResultManifest')
        self.assertIn('MEMBER_CLOSURE',structural.unimplemented_gates)

    def test_canonical_vectors_match_exact_bytes_and_hashes_with_unicode_and_safe_max(self):
        vectors=json.loads((ROOT/'canonical-vectors.json').read_bytes())['vectors']
        for vector in vectors:
            data=m.canonical_metadata(vector['value']);self.assertEqual(data,base64.b64decode(vector['canonicalUtf8Base64']));self.assertEqual(hashlib.sha256(data).hexdigest(),vector['sha256'])
        with self.assertRaises(m.CandidateError):m.canonical_metadata({'non-ascii-key-é':1})

    def test_no_caller_value_is_treated_as_a_file_path_or_network_reference(self):
        m._bundle();seen=[];original=m.os.open
        def opened(path,*args,**kwargs):seen.append(str(path));return original(path,*args,**kwargs)
        with patch.object(m.os,'open',side_effect=opened):
            result=m.validate_candidate(b'{"path":"/private/caller-file","url":"https://not-a-request.invalid"}',record_type='Options')
        self.assertEqual(seen,[]);self.assertEqual(result.status,m.STATUS)

    def test_diagnostics_do_not_echo_payload_values_or_unknown_property_names(self):
        value=copy.deepcopy(COMBINED['expectedContext']['originalBinding']);value['SENSITIVE-UNKNOWN-PROPERTY']='SENSITIVE-VALUE'
        with self.assertRaises(m.CandidateError) as error:m.validate_candidate(wire(value),record_type='Binding')
        self.assertNotIn('SENSITIVE',str(error.exception))

    @unittest.skipUnless(
        importlib.util.find_spec('jsonschema') is not None
        and importlib.util.find_spec('referencing') is not None,
        'optional JSON Schema evaluator dependencies are test-only',
    )
    def test_existing_draft202012_oracle_confirms_all_typed_samples_goldens_and_schema_negatives(self):
        from jsonschema import Draft202012Validator,FormatChecker
        from referencing import Registry
        Draft202012Validator.check_schema(SCHEMA)
        def retrieval(_):raise AssertionError('no remote schema retrieval')
        def validator(kind):return Draft202012Validator({'$ref':'#/$defs/'+kind,'$defs':SCHEMA['$defs']},format_checker=FormatChecker(),registry=Registry(retrieve=retrieval))
        for kind,value in samples().items():validator(kind).validate(value)
        for fixture in GOLDENS:
            for entry in fixture['documents']:validator(entry['type']).validate(entry['value'])
        for case in INDEX['cases']:
            if case['phase']=='schema':self.assertTrue(list(validator(case['documentType']).iter_errors(json.loads((ROOT/case['inputFile']).read_bytes()))),case['id'])
