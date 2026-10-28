"""Synthetic metadata semantics and interrupted-state replay, never issuer authority."""
import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from processing import reference_metadata_journal as m
from processing import reference_v2_candidate as c

ROOT=Path(__file__).resolve().parents[1]/'contracts/reference-v2-candidate/valid'
DOCS=[v for path in ROOT.glob('*.json') for v in json.loads(path.read_bytes())['documents']]
def sample(kind): return copy.deepcopy(next(v['value'] for v in DOCS if v['type']==kind))
def raw(value): return c.canonical_metadata(value)
def seal(value, key):
    value=copy.deepcopy(value);value.pop(key,None)
    value[key]=hashlib.sha256(raw(value)).hexdigest();return value

class MetadataTests(unittest.TestCase):
    def setUp(self):
        self.i=sample('ImmutableMetadataIntent');self.i['round']=1
        self.i=seal(self.i,'intentSha256')
        self.p=sample('ControlBudgetPolicy')
        self.r=sample('AuthorizedResponse')
        for key in self.r:
            if key in self.i and key not in ('receiptType',):self.r[key]=copy.deepcopy(self.i[key])
        self.r['receiptType']='metadata_authorization_receipt'
        self.r=seal(self.r,'receiptSha256')
        self.actor=self.r['authenticatedActorBindingSha256']
        self.now=self.r['checkedAt']
        self.b=dict(originalExpiresAt='2020-01-01T00:00:00.000Z',cumulativeByteBudget=300,
                    totalInputBytes=100,consumedBytes=100,remainingBytes=200)
    def start(self): return m.prepare(raw(self.i),raw(self.p),raw(self.b),self.actor)
    def reload(self,state): return m.restore(raw(self.i),raw(self.p),raw(self.b),self.actor,state.serialized)
    def issue(self): return m.transition(self.start(),'issue',self.now)
    def assertBad(self,fn):
        with self.assertRaises((m.MetadataError,c.CandidateError)):fn()
    def test_interrupted_exact_original_survives_serialized_reload(self):
        prepared=self.reload(self.start());issued=self.reload(m.transition(prepared,'issue',self.now))
        unknown=self.reload(m.transition(issued,'unknown',self.now))
        observed=self.reload(m.transition(unknown,'response',self.now,raw(self.r)))
        self.assertEqual(observed.value['summary']['phase'],'response_observed_unverified')
        self.assertEqual(observed.value['seed']['payload'],self.b)
        self.assertEqual(observed.value['seed']['intent'],self.i)
        self.assertIsNone(observed.value['summary']['currentAuthority'])
        self.assertBad(observed.require_engine_permission)
        with self.assertRaises(TypeError):bool(observed)
    def test_every_response_original_binding_field_is_exact(self):
        excluded={'schemaVersion','protocol','receiptType','requestedAt','stage','controlBudgetLedgerId','intentSha256'}
        for key in set(self.i)&set(self.r)-excluded:
            if key=='intentSha256':continue
            value=copy.deepcopy(self.r)
            if isinstance(value[key],dict): value[key]=copy.deepcopy(value[key]);value[key][next(iter(value[key]))]='wrong'
            elif isinstance(value[key],list):value[key][0]['sha256']='0'*64
            elif type(value[key]) is int:value[key]+=1
            else:value[key]='0'*64 if key.endswith('Sha256') or key.endswith('Digest') else 'wrong'
            value=seal(value,'receiptSha256')
            with self.subTest(key=key):self.assertBad(lambda:m.transition(self.issue(),'response',self.now,raw(value)))
    def test_schema_valid_parent_and_source_mutations_fail_semantic_gate(self):
        changes=[(('parentBinding','environment'),'production'),
                 (('parentBinding','generation'),2),
                 (('parentBinding','parentClaimId'),'different-claim'),
                 (('parentBinding','jobId'),'00000000-0000-4000-8000-000000000099'),
                 (('parentBinding','inputManifestSha256'),'0'*64),
                 (('parentBinding','pipelineDigest'),'0'*64),
                 (('originalAcquisition','receiptId'),'different-acquisition'),
                 (('originalCustody','receiptId'),'different-custody'),
                 (('originalObjects',0,'controlledObjectId'),'different-controlled'),
                 (('originalObjects',0,'custodyPhysicalReceiptId'),'different-physical'),
                 (('originalObjects',0,'byteSize'),999),
                 (('originalObjects',0,'acquisitionReceiptId'),'different-acquisition'),
                 (('purpose',),'custody_observation'),
                 (('payloadBudgetLedgerRevision',),999),
                 (('controlChargeId',),'different-charge')]
        for path,replacement in changes:
            r=copy.deepcopy(self.r);target=r
            for part in path[:-1]:target=target[part]
            target[path[-1]]=replacement;r=seal(r,'receiptSha256')
            c.validate_candidate(raw(r),record_type='AuthorizedResponse')
            with self.subTest(path=path), self.assertRaisesRegex(m.MetadataError,'response request binding'):
                m.transition(self.issue(),'response',self.now,raw(r))
    def test_stage_scope_and_receipt_roles_are_pinned_before_issue(self):
        for mutate in ('stage','predecessor','role'):
            i=copy.deepcopy(self.i)
            if mutate=='stage':i['stage']['binding']['parentClaimId']='different'
            elif mutate=='predecessor':i['predecessorProofSha256']=None
            else:i['originalCustody']['receiptId']=i['originalAcquisition']['receiptId']
            i=seal(i,'intentSha256')
            self.assertBad(lambda:m.prepare(raw(i),raw(self.p),raw(self.b),self.actor))
    def test_serialized_context_and_observation_history_cannot_be_replaced(self):
        state=self.issue().value
        state['seed']['intent']['stage']['workerId']='worker-other'
        self.assertBad(lambda:self.reload(m.MetadataJournal(raw(state))))
        state=self.issue().value;state['events'][0]['observedAt']='2020-01-01T00:00:00.000Z'
        self.assertBad(lambda:self.reload(m.MetadataJournal(raw(state))))

    def test_actor_and_role_alias_are_rejected(self):
        for key,value in [('authenticatedActorBindingSha256','0'*64),('receiptId',self.i['originalAcquisition']['receiptId'])]:
            r=copy.deepcopy(self.r);r[key]=value;r=seal(r,'receiptSha256')
            self.assertBad(lambda:m.transition(self.issue(),'response',self.now,raw(r)))
    def test_original_object_duplicate_and_order_and_acquisition(self):
        for change in ('duplicate','order','acquisition'):
            i=copy.deepcopy(self.i)
            if change=='duplicate':i['originalObjects']*=2
            elif change=='order':
                obj=copy.deepcopy(i['originalObjects'][0]);obj['objectId']='aaa';i['originalObjects'].append(obj)
            else:i['originalObjects'][0]['acquisitionReceiptId']='different'
            i=seal(i,'intentSha256')
            self.assertBad(lambda:m.prepare(raw(i),raw(self.p),raw(self.b),self.actor))
    def test_payload_totals_types_and_round_are_closed(self):
        for key,value in [('remainingBytes',201),('consumedBytes',True),('cumulativeByteBudget',301),('totalInputBytes',0)]:
            b=dict(self.b);b[key]=value
            self.assertBad(lambda:m.prepare(raw(self.i),raw(self.p),raw(b),self.actor))
        self.i['round']=2;self.i=seal(self.i,'intentSha256');self.assertBad(self.start)
    def test_no_concurrent_issue_no_replay_no_unknown_resolution(self):
        issued=self.issue()
        self.assertBad(lambda:m.transition(issued,'issue',self.now))
        unknown=m.transition(issued,'unknown',self.now)
        self.assertBad(lambda:m.transition(unknown,'issue',self.now))
        self.assertBad(lambda:m.transition(unknown,'authorized',self.now))
        self.assertBad(lambda:m.transition(unknown,'next_round',self.now))
    def test_lookup_invalidates_old_observation_before_unknown(self):
        observed=m.transition(self.issue(),'response',self.now,raw(self.r))
        lookup=self.reload(m.transition(observed,'lookup',self.now))
        self.assertIsNone(lookup.value['summary']['latestUnverifiedReceiptSha256'])
        self.assertEqual(lookup.value['events'][1]['response'],self.r)
        unknown=m.transition(lookup,'unknown',self.now)
        self.assertIsNone(unknown.value['summary']['latestUnverifiedReceiptSha256'])
        self.assertEqual(unknown.value['summary']['attemptCount'],2)
    def test_matching_reply_still_unverified_and_different_reply_conflicts(self):
        observed=m.transition(self.issue(),'response',self.now,raw(self.r))
        again=m.transition(observed,'response',self.now,raw(self.r))
        self.assertEqual(again.value['summary'],observed.value['summary'])
        r=copy.deepcopy(self.r);r['receiptId']='another-receipt';r=seal(r,'receiptSha256')
        self.assertBad(lambda:m.transition(observed,'response',self.now,raw(r)))
    def test_denial_is_not_authoritative_resolution(self):
        r=copy.deepcopy(self.r);r['decision']='denied';r['metadataExpiresAt']=None;r['denialCode']='issuer_unavailable'
        r['custodyRevision']=r['revocationRevision']=0;r=seal(r,'receiptSha256')
        state=m.transition(self.issue(),'response',self.now,raw(r))
        self.assertIsNone(state.value['summary']['currentAuthority'])
        self.assertEqual(state.value['summary']['phase'],'response_observed_unverified')
        self.assertBad(lambda:m.transition(state,'next_round',self.now))
    def test_monotonic_observation_and_freshness(self):
        self.assertBad(lambda:m.transition(self.issue(),'unknown','2020-01-01T00:00:00.000Z'))
        self.assertBad(lambda:m.transition(self.issue(),'response','2020-01-01T00:00:00.000Z',raw(self.r)))
        self.assertBad(lambda:m.transition(self.issue(),'response','2030-01-01T00:00:00.000Z',raw(self.r)))
        r=copy.deepcopy(self.r);r['metadataExpiresAt']='2030-01-01T00:00:00.000Z';r=seal(r,'receiptSha256')
        self.assertBad(lambda:m.transition(self.issue(),'response',self.now,raw(r)))
    def test_receipt_checked_before_original_request_is_rejected(self):
        r=copy.deepcopy(self.r);r['checkedAt']='2026-10-28T11:59:59.000Z';r['metadataExpiresAt']='2026-10-28T12:04:59.000Z'
        r=seal(r,'receiptSha256')
        with self.assertRaisesRegex(m.MetadataError,'original request interval'):
            m.transition(self.issue(),'response',self.now,raw(r))

    def test_revision_regression_rejected_without_new_receipt_authority(self):
        observed=m.transition(self.issue(),'response',self.now,raw(self.r))
        for key in ('custodyRevision','revocationRevision'):
            r=copy.deepcopy(self.r);r[key]=max(1,r[key]-1);r=seal(r,'receiptSha256')
            if r[key]!=self.r[key]:self.assertBad(lambda:m.transition(observed,'response',self.now,raw(r)))
    def test_attempt_history_bytes_and_response_limits(self):
        self.p['maxAttemptsPerRound']=1
        state=m.transition(self.issue(),'unknown',self.now)
        self.assertBad(lambda:m.transition(state,'lookup',self.now))
        self.p=sample('ControlBudgetPolicy');self.p['maxHistoryRecords']=3
        self.assertBad(self.issue)
        self.p=sample('ControlBudgetPolicy');self.p['maxHistoryBytes']=100
        self.assertBad(self.start)
        self.p=sample('ControlBudgetPolicy');self.p['maxResponseBytes']=1
        self.assertBad(lambda:m.transition(self.issue(),'response',self.now,raw(self.r)))
    def test_reload_recomputes_all_totals_and_independent_scope(self):
        original=self.issue()
        for field,value in [('attemptCount',0),('currentAuthority','authorized'),('phase','authorized')]:
            state=original.value;state['summary'][field]=value
            self.assertBad(lambda:self.reload(m.MetadataJournal(raw(state))))
        self.i['purpose']='custody_observation';self.i=seal(self.i,'intentSha256')
        self.assertBad(lambda:self.reload(original))
    def test_lexical_unknown_fields_and_bounded_serialization(self):
        self.assertBad(lambda:m.prepare(b'{"x":1,"x":2}',raw(self.p),raw(self.b),self.actor))
        self.assertBad(lambda:self.reload(m.MetadataJournal(b' '* (m.MAX_BYTES+1))))
        state=self.start().value;state['unexpected']=True
        self.assertBad(lambda:self.reload(m.MetadataJournal(raw(state))))
    def test_direct_construction_cannot_bypass_replay_validation(self):
        for value in ({}, {'seed':{}}, dict(self.start().value, schemaVersion=True)):
            self.assertBad(lambda:m.transition(m.MetadataJournal(raw(value)),'issue',self.now))
        state=self.start().value;state['summary']['currentAuthority']='authorized'
        self.assertBad(lambda:m.transition(m.MetadataJournal(raw(state)),'issue',self.now))
    def test_transitions_have_no_filesystem_or_network_effect(self):
        with patch('builtins.open',side_effect=AssertionError('file I/O')), patch('socket.socket',side_effect=AssertionError('network')):
            state=self.issue();state=m.transition(state,'unknown',self.now);self.reload(state)

if __name__=='__main__':unittest.main()
