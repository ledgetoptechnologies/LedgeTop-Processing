'use strict';
// Synthetic in-memory Viewer data only; no real source files or credentials.
const path = require('node:path');
const crypto = require('node:crypto');
const root = process.env.PINNED_VIEWER_ROOT;
const fromViewer = name => require(path.join(root, 'apps/viewer/server', name));
const { openDatabase } = fromViewer('database');
const { ProcessingRepository } = fromViewer('processingRepository');
const { GcpRepository } = fromViewer('gcpRepository');
const { captureHeldSourceExport, gcpEvidenceStream } = fromViewer('viewerPagedSourceExport');
const db = openDatabase(':memory:');
try {
  const mixed=process.argv[2] === 'mixed', mode=process.argv[2] || 'normal';
  const p = new ProcessingRepository(db), g = new GcpRepository(db);
  const project = p.createProject({ displayName: 'Synthetic pair' });
  const dataset = p.createDataset({ projectId: project.id, displayName: 'Survey', storageMode: 'managed', rootKey: 'datasets', relativePath: crypto.randomUUID() });
  const raw = 'point_id,label,latitude,longitude,elevation_m\nA,Target,44.5,-88.1,244\n';
  const rawHash = crypto.createHash('sha256').update(raw).digest('hex');
  const files = Array.from({length:mode === 'normal' ? 16832 : mode === 'mixed' ? 2 : 1}, (_,i) => ({id:`photo-${String(i).padStart(5,'0')}`,relativePath:mode === 'path' ? 'C:photo.jpg' : mode === 'mime' ? 'flight/photo.dng' : `flight/IMG_${String(i).padStart(5,'0')}.jpg`,byteSize:10,sha256:'a'.repeat(64),contentType:mode === 'mime' ? 'image/x-adobe-dng' : 'image/jpeg',metadata:{width:4000,height:3000}}));
  files.push({id:'control',relativePath:'control/raw.csv',byteSize:Buffer.byteLength(raw),sha256:rawHash,contentType:'text/csv'}, {id:'lidar',relativePath:'survey/site.las',byteSize:11,sha256:'b'.repeat(64),contentType:'application/vnd.las',processingRole:'provider_input'});
  if (mixed) files.push({id:'Control',relativePath:'control/other.csv',byteSize:Buffer.byteLength(raw),sha256:rawHash,contentType:'text/csv'});
  p.finalizeDataset(dataset.id,files,'d'.repeat(64));
  const task=p.createTask({projectId:project.id,datasetId:dataset.id,displayName:'ODM'});
  const set=g.importSet({datasetId:dataset.id,displayName:'Control',sourceFormat:'generic-csv-v1',sourceFileId:'control',sourceFilename:'raw.csv',sourceSha256:rawHash,sourceContent:raw,crs:'EPSG:4326',elevationUnits:'m',createdBy:'ops:test',points:[{externalId:'A',label:'Target',latitude:44.5,longitude:-88.1,elevationM:244,description:null}]});
  g.createCorrespondence({taskId:task.id,pointId:set.points[0].id,imageFileId:'photo-00000',pixelX:100,pixelY:200,createdBy:'ops:test'});
  if (mixed) {
    const second=g.importSet({datasetId:dataset.id,displayName:'Second',sourceFormat:'generic-csv-v1',sourceFileId:'Control',sourceFilename:'other.csv',sourceSha256:rawHash,sourceContent:raw,crs:'EPSG:4326',elevationUnits:'m',createdBy:'ops:test',points:[{externalId:'B',label:'Second',latitude:44.6,longitude:-88.2,elevationM:245,description:null}]});
    g.createCorrespondence({taskId:task.id,pointId:second.points[0].id,imageFileId:'photo-00000',pixelX:101,pixelY:201,createdBy:'ops:test'});
  }
  const provider=p.upsertProvider({type:'nodeodm',displayName:'ODM',endpoint:'http://127.0.0.1:3000',enabled:true});
  const attempt=p.createAttempt({taskId:task.id,providerId:provider.id,options:{}});
  const value=captureHeldSourceExport(p,{datasetId:dataset.id,taskId:task.id,attemptId:attempt.id});
  const stored=db.prepare('SELECT evidence_json,content_text FROM processing_attempt_gcp_snapshots WHERE attempt_id=?').get(attempt.id);
  const split=gcpEvidenceStream(Buffer.from('a'.repeat(46079)+'é','utf8'),attempt.id);
  console.log(JSON.stringify({...value,sourceFrames:value.sourceFrames.toString('base64'),selectedFrames:value.selectedFrames.toString('base64'),gcpEvidenceFrames:value.gcpEvidenceFrames.toString('base64'),gcpFileFrames:value.gcpFileFrames.toString('base64'),storedEvidence:Buffer.from(stored.evidence_json,'utf8').toString('base64'),storedFile:Buffer.from(stored.content_text,'utf8').toString('base64'),split:{bytes:split.bytes.toString('base64'),hashes:split.pageHashes,sizes:split.pageBytes,count:split.byteCount,sha256:split.sha256}}));
} finally { db.close(); }
