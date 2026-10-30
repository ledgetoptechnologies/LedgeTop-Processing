import os,sqlite3,subprocess,sys,unittest
from pathlib import Path
from tests.test_reference_v2_precharge_draft import PrechargeTests
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
from processing.reference_v2_stream_slot_draft import StreamSlot,SlotHeld,DDL
from processing.reference_worker_fence_v2 import WorkerFenceHeld

@unittest.skipUnless(os.name=='posix','POSIX only')
class SlotTests(PrechargeTests):
 def setUp(self):
  super().setUp();self.slotdir=Path(self.tmp.name)/'slot';self.slotdir.mkdir(mode=0o700)
  fd=os.open(self.slotdir/'worker.lock',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd)
  self.slotpath=self.slotdir/'slot.sqlite';db=sqlite3.connect(self.slotpath);db.execute(DDL);db.execute("INSERT INTO stream_slot VALUES(1,'epoch-1','idle',NULL)");db.commit();db.close();self.slotpath.chmod(0o600)
  s=self.slotpath.stat();self.slotinode=(s.st_dev,s.st_ino)
 def indexed(self):
  l=IndexedPrecharge(self.path,inode=self.inode,epoch='epoch-1',clock=lambda:self.now);self.addCleanup(l.close);return l
 def slot(self):
  slot=StreamSlot(path=self.slotpath,inode=self.slotinode,epoch='epoch-1',lock_directory=self.slotdir);self.addCleanup(slot.close);return slot
 def test_process_lock_and_clean_closure(self):
  l=self.indexed();s=self.slot();identity=s.begin(l,request_id='r1',object_id='cloud',start=0,end=4)
  with self.assertRaises(WorkerFenceHeld):self.slot()
  with self.assertRaises(SlotHeld):s.finish(identity,stream_closed=False)
  s.finish(identity,stream_closed=True);self.assertEqual(s.state(),'closed')
 def test_replay_never_starts_stream_and_holds_slot(self):
  l=self.indexed();l.precharge(request_id='r1',object_id='cloud',start=0,end=4);s=self.slot()
  with self.assertRaisesRegex(SlotHeld,'replay'):s.begin(l,request_id='r1',object_id='cloud',start=0,end=4)
  self.assertEqual(s.state(),'opening')
 def test_denied_precharge_and_unknown_commit_both_remain_opening(self):
  from unittest.mock import patch
  from processing.reference_v2_precharge_draft import PrechargeHeld
  l=self.indexed();s=self.slot()
  with self.assertRaises(PrechargeHeld):s.begin(l,request_id='r1',object_id='absent',start=0,end=1)
  self.assertEqual(s.state(),'opening')
  with self.assertRaisesRegex(SlotHeld,'quiescence'):s.reconcile_opening(l,writer_quiesced=False)
  self.assertEqual(s.reconcile_opening(l,writer_quiesced=True)['state'],'no_charge_closed_no_authority')
  self.assertEqual(s.state(),'closed');original=l.precharge
  def committed_then_lost(**kw):original(**kw);raise sqlite3.OperationalError('unknown commit')
  with patch.object(l,'precharge',side_effect=committed_then_lost):
   with self.assertRaises(sqlite3.OperationalError):s.begin(l,request_id='r2',object_id='cloud',start=0,end=4)
  self.assertEqual(s.state(),'opening')
  self.assertEqual(s.reconcile_opening(l,writer_quiesced=True)['state'],'charged_unknown_held')
  self.assertEqual(s.state(),'opening')
  self.assertEqual(l.precharge(request_id='r2',object_id='cloud',start=0,end=4)['state'],'replay_no_body')

 def test_ledger_commit_failure_after_slot_transition_stays_held(self):
  from processing.reference_v2_precharge_draft import PrechargeHeld
  class FailCommit(sqlite3.Connection):
   def execute(self,sql,*args):
    if sql=='COMMIT':raise sqlite3.OperationalError('injected commit uncertainty')
    return super().execute(sql,*args)
  l=self.indexed();s=self.slot()
  with self.assertRaises(PrechargeHeld):s.begin(l,request_id='r1',object_id='absent',start=0,end=1)
  l._conn.close();l._conn=sqlite3.connect(self.path,isolation_level=None,factory=FailCommit);l._version=None
  with self.assertRaises(sqlite3.OperationalError):s.reconcile_opening(l,writer_quiesced=True)
  self.assertEqual(s.state(),'closing')
  with self.assertRaisesRegex(SlotHeld,'reconciliation'):s.begin(l,request_id='r2',object_id='cloud',start=0,end=4)
  with self.assertRaisesRegex(SlotHeld,'opening slot required'):s.reconcile_opening(l,writer_quiesced=True)

 def test_oversized_slot_identity_rejected_before_decode(self):
  s=self.slot();s.close();db=sqlite3.connect(self.slotpath)
  try:db.execute("UPDATE stream_slot SET phase='opening',identity=?",('x'*2049,));db.commit()
  finally:db.close()
  with self.assertRaisesRegex(SlotHeld,'bounded slot state'):self.slot()

 def test_process_death_opening_reconciles_only_uncharged(self):
  code="""import os,sys
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
from processing.reference_v2_stream_slot_draft import StreamSlot
l=IndexedPrecharge(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
s=StreamSlot(path=sys.argv[4],inode=(int(sys.argv[5]),int(sys.argv[6])),epoch='epoch-1',lock_directory=sys.argv[7])
original=l.precharge
def die(**kw):
 if sys.argv[8]=='charged':original(**kw)
 os._exit(73)
l.precharge=die
s.begin(l,request_id=sys.argv[9],object_id='cloud',start=0,end=4)
"""
  args=[sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1]),str(self.slotpath),str(self.slotinode[0]),str(self.slotinode[1]),str(self.slotdir)]
  for mode,request in (('nocharge','r1'),('charged','r2')):
   child=subprocess.run(args+[mode,request],timeout=10);self.assertEqual(child.returncode,73)
   l=self.indexed();s=self.slot()
   observed=s.reconcile_opening(l,writer_quiesced=True)['state']
   self.assertEqual(observed,'no_charge_closed_no_authority' if mode=='nocharge' else 'charged_unknown_held')
   self.assertEqual(s.state(),'closed' if mode=='nocharge' else 'opening');s.close();l.close()

 def test_process_exit_after_charge_retains_slot(self):
  code="""import os,sys
from processing.reference_v2_indexed_precharge_draft import IndexedPrecharge
from processing.reference_v2_stream_slot_draft import StreamSlot
l=IndexedPrecharge(sys.argv[1],inode=(int(sys.argv[2]),int(sys.argv[3])),epoch='epoch-1',clock=lambda:1793361600000)
s=StreamSlot(path=sys.argv[4],inode=(int(sys.argv[5]),int(sys.argv[6])),epoch='epoch-1',lock_directory=sys.argv[7])
s.begin(l,request_id='r1',object_id='cloud',start=0,end=4);os._exit(73)
"""
  c=subprocess.run([sys.executable,'-c',code,str(self.path),str(self.inode[0]),str(self.inode[1]),str(self.slotpath),str(self.slotinode[0]),str(self.slotinode[1]),str(self.slotdir)],timeout=10)
  self.assertEqual(c.returncode,73);s=self.slot();self.assertEqual(s.state(),'charged')
  with self.assertRaisesRegex(SlotHeld,'reconciliation'):s.begin(self.indexed(),request_id='r2',object_id='cloud',start=0,end=4)
