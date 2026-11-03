'use strict';
// In-memory synthetic Viewer database only; no Viewer checkout or source writes.
const path=require('node:path'), crypto=require('node:crypto');
const root=process.env.PINNED_VIEWER_ROOT;
if(!root)throw new Error('PINNED_VIEWER_ROOT required');
const {openDatabase}=require(path.join(root,'apps/viewer/server/database'));
const {ProcessingRepository}=require(path.join(root,'apps/viewer/server/processingRepository'));
const {GcpRepository}=require(path.join(root,'apps/viewer/server/gcpRepository'));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const db=openDatabase(':memory:');
try {
 const processing=new ProcessingRepository(db),gcp=new GcpRepository(db);
 const project=processing.createProject({displayName:'Synthetic paired project'});
 const draft=processing.createDataset({projectId:project.id,displayName:'Synthetic paired dataset',storageMode:'managed',rootKey:'datasets',relativePath:'synthetic'});
 const csv='point_id,label,latitude,longitude,elevation_m\nGCP-01,Target,44.5,-88.1,243.84\n';
 const second=process.argv[2]==='collision'?'other/photo-1.jpg':'flight/photo-2.jpg';
 const entries=[['csv-1','control.csv',Buffer.from(csv),'text/csv'],['auto-1','excluded/notes.txt',Buffer.from('notes'),'text/plain'],
                ['photo-1','flight/photo-1.jpg',Buffer.from('photo-one'),'image/jpeg'],['photo-2',second,Buffer.from('photo-two'),'image/jpeg']];
 entries.sort((a,b)=>Buffer.compare(Buffer.from(a[1]),Buffer.from(b[1])));
 const files=entries.map(([id,relativePath,bytes,contentType])=>({id,relativePath,byteSize:bytes.length,sha256:sha(bytes),contentType,
     metadata:contentType==='image/jpeg'?{width:6000,height:4000}:{}}));
 const manifestSha256=sha(JSON.stringify(files.map(({relativePath,byteSize,sha256})=>({relativePath,byteSize,sha256}))));
 processing.finalizeDataset(draft.id,files,manifestSha256);
 // Legacy finalized datasets can retain persisted `auto` roles. This mutation
 // occurs only in this in-memory synthetic database before attempt creation.
 db.prepare("UPDATE dataset_files SET processing_role='auto' WHERE id='auto-1'").run();
 const set=gcp.importSet({datasetId:draft.id,displayName:'Control',sourceFormat:'generic-csv-v1',sourceFileId:'csv-1',sourceFilename:'control.csv',sourceSha256:sha(csv),sourceContent:csv,crs:'EPSG:4326',elevationUnits:'m',createdBy:'synthetic',points:[{externalId:'GCP-01',label:'Target',latitude:44.5,longitude:-88.1,elevationM:243.84,description:null}]});
 const task=processing.createTask({projectId:project.id,datasetId:draft.id,displayName:'Synthetic GCP task'});
 const mark=gcp.createCorrespondence({taskId:task.id,pointId:set.points[0].id,imageFileId:'photo-1',pixelX:120.5,pixelY:240.25,createdBy:'synthetic'});
 const provider=processing.upsertProvider({type:'nodeodm',displayName:'Synthetic',endpoint:'http://127.0.0.1:3000',enabled:true});
 const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options:{}});
 const dataset=processing.getDataset(draft.id,true),provenance=processing.getAttemptProvenance(attempt.id).submission;
 const snapshot=processing.getAttemptGcpSnapshot(attempt.id),evidence=processing.getAttemptGcpEvidence(attempt.id);
 if(!snapshot||!evidence)throw new Error('Viewer did not persist synthetic GCP evidence');
 gcp.updateCorrespondence(mark.id,{pixelX:121,pixelY:241});
 const evidenceAfterLiveEdit=processing.getAttemptGcpEvidence(attempt.id);
 process.stdout.write(JSON.stringify({dataset,taskId:task.id,attemptId:attempt.id,provenance,snapshot,evidence,
     evidenceAfterLiveEdit,
     sourceBytes:Object.fromEntries(entries.map(([id,,bytes])=>[id,bytes.toString('base64')]))}));
} finally { db.close(); }
