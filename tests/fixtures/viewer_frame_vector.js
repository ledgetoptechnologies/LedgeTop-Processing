// Synthetic vector using the supplied Viewer checkout; no client data.
const fs=require('node:fs'),path=require('node:path');
const viewer=require(path.join(process.argv[2],'server/viewerPagedSourceExport.js'));
const selected=viewer.frameStream([{type:'dataset_file',ordinal:0,pageObjectId:'img',datasetFileId:'img'}],
  viewer.SELECTED_VERSION,viewer.SELECTED_DOMAIN);
const evidence=viewer.gcpEvidenceStream(Buffer.from('example evidence ü','utf8'),'attempt',2);
const file=viewer.generatedGcpFileStream(Buffer.from('example file ü','utf8'),'attempt',evidence.sha256);
const pack=(v)=>({bytes:v.bytes.toString('base64'),hashes:v.pageHashes,pageBytes:v.pageBytes,
  streamBytes:v.bytes.length,byteCount:v.byteCount,sha256:v.sha256,evidenceSha256:v.evidenceSha256});
fs.writeFileSync(process.argv[3],JSON.stringify({selected:pack(selected),evidence:pack(evidence),file:pack(file)}));
