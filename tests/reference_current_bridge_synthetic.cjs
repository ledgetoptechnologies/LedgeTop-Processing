'use strict';
// Synthetic-only fixture: reads owner-provided pinned Viewer source, no ACK.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const root=process.env.VIEWER_STAGED_ROOT;
const p=require(path.join(root,'server/referenceProtocol'));
const {ReferenceProvider}=require(path.join(root,'server/referenceProvider'));
const {ReferenceResultJournal,validateResultBinding}=require(path.join(root,'server/referenceResultJournal'));
const {ReferenceAcceptanceLedger}=require(path.join(root,'server/referenceAcceptance'));
const {handleReferenceResult}=require(path.join(root,'server/referenceResultBridge'));
const c=JSON.parse(fs.readFileSync(0,'utf8'));
(async()=>{
 const managed=path.join(c.work,'retained');fs.mkdirSync(managed);
 const fd=fs.openSync(managed,fs.constants.O_RDONLY|fs.constants.O_DIRECTORY);
 const st=fs.fstatSync(fd,{bigint:true});
 const authority={rootFd:fd,device:st.dev,inode:st.ino,assertCurrent:()=>true};
 const journal=new ReferenceResultJournal({authority,maxBytes:1024*1024});
 const ledger=new ReferenceAcceptanceLedger({databasePath:path.join(c.work,'acceptance.sqlite'),managedRoot:managed});
 try{
  let acceptancePosts=0;
  const transport=(url,init)=>{if(init.method==='POST'&&String(url).endsWith('/acceptance'))acceptancePosts++;return fetch(url,init)};
  const provider=new ReferenceProvider({endpoint:c.origin,token:c.token,environment:'staging',enabled:true,maxObjectBytes:1024*1024,fetchImpl:transport});
  const expected=c.expected;
  const manifest=await provider.results(expected.jobId,expected);
  if(manifest.objects.length!==1||manifest.objects[0].role!=='odm_archive')throw Error('synthetic_manifest');
  const obj=manifest.objects[0];if(obj.byteSize<=0)throw Error('synthetic_empty');
  const storageReceiptId=p.sha256(p.canonicalBytes(validateResultBinding(expected,manifest,obj.id)));
  const acceptance={schemaVersion:1,receiptId:'synthetic-receipt-1',storageReceiptId,jobId:expected.jobId,
   generation:expected.generation,environment:'staging',inputManifestSha256:expected.inputManifestSha256,
   pipelineDigest:expected.pipelineDigest,resultManifestSha256:expected.resultManifestSha256,
   acceptedObjectIds:[obj.id],acceptedAt:'2026-10-29T12:00:00.000Z',validationVersion:'ltds-reference-v1'};
  let observed=null;
  const result=await handleReferenceResult({provider,expected,journal,ledger,acceptance,deliverAcceptance:false,
   assertAuthority:()=>true,
   ingestRetained:async({retained})=>{observed=fs.readFileSync(path.join(managed,retained.receipt.relativePath));},
   verifyIngest:async()=>observed!==null&&observed.length===obj.byteSize&&crypto.createHash('sha256').update(observed).digest('hex')===obj.sha256});
  const pending=ledger.pendingOutbox(Date.now()+300000);
  if(result.acknowledgement!=='pending'||pending.length!==1||!observed||acceptancePosts!==0)throw Error('synthetic_pending');
  console.log(JSON.stringify({acknowledgement:result.acknowledgement,pending:pending.length,acceptancePosts,bytes:observed.length,sha256:crypto.createHash('sha256').update(observed).digest('hex')}));
 }finally{journal.close();ledger.close();fs.closeSync(fd)}
})().catch(e=>{console.error('synthetic_bridge_failed',e.code||e.name);process.exitCode=1});
