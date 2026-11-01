'use strict';
// Independent Node canonical-byte and closed-shape check of the shared vectors.
// This does not implement authority, admission or a production client.
const assert=require('node:assert/strict'),crypto=require('node:crypto'),fs=require('node:fs'),path=require('node:path');
const vectors=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/native_input_manifest_vectors.json'),'utf8'));
const rejections=JSON.parse(fs.readFileSync(path.join(__dirname,'fixtures/native_input_manifest_rejections.json'),'utf8'));
const keys=(value,expected)=>assert.deepEqual(Object.keys(value).sort(),expected.slice().sort());
function canonical(value){
 if(typeof value==='string')return JSON.stringify(value);
 if(typeof value==='number'){assert.ok(Number.isSafeInteger(value)&&value>=0&&!Object.is(value,-0));return String(value)}
 if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
 assert.ok(value&&typeof value==='object'&&Object.getPrototypeOf(value)===Object.prototype);
 return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}';
}
assert.equal(vectors.schema,'native-odm-input-v1-candidate');
for(const vector of vectors.vectors){
 const value=JSON.parse(vector.canonical);keys(value,['id','revision','sha256','totalBytes','objectCount','inputCounts','objects']);
 keys(value.inputCounts,['images','lidarObjects','points']);assert.equal(value.id,vector.expectedManifestId);assert.equal(value.revision,1);
 assert.equal(value.objects.length,value.objectCount);assert.equal(value.objects.reduce((sum,obj)=>sum+obj.byteSize,0),value.totalBytes);
 value.objects.forEach((obj,i)=>{keys(obj,['id','name','byteSize','sha256','kind','mediaType']);assert.equal(obj.id,'o'+String(i+1).padStart(6,'0'));assert.ok(obj.byteSize>0)});
 assert.equal(canonical(value),vector.canonical);
 const {sha256,...payload}=value,digest=crypto.createHash('sha256').update(canonical(payload),'utf8').digest('hex');
 assert.equal(digest,sha256);assert.equal(digest,vector.digest);
}
// Independent candidate-profile checker for shared negative fixtures. This is
// test code, not the Viewer owner's implementation or an admission receipt.
function check(body){
 assert.ok(Buffer.byteLength(body)<=4*1024*1024);const m=JSON.parse(body);assert.equal(canonical(m),body);
 keys(m,['id','revision','sha256','totalBytes','objectCount','inputCounts','objects']);
 keys(m.inputCounts,['images','lidarObjects','points']);assert.equal(m.id,'attempt-0001');assert.equal(m.revision,1);
 assert.ok(Array.isArray(m.objects)&&m.objects.length>0&&m.objects.length<=10000);assert.equal(m.objectCount,m.objects.length);
 assert.equal(m.inputCounts.lidarObjects,0);assert.equal(m.inputCounts.points,0);
 const media={'.jpg':['image','image/jpeg'],'.jpeg':['image','image/jpeg'],'.png':['image','image/png'],'.tif':['image','image/tiff'],'.tiff':['image','image/tiff'],'.txt':['auxiliary','text/plain'],'.json':['auxiliary','application/json'],'.csv':['auxiliary','text/csv']};
 const names=new Set();let total=0,images=0,aux=false;
 for(const [i,o] of m.objects.entries()){
  keys(o,['id','name','byteSize','sha256','kind','mediaType']);assert.equal(o.id,'o'+String(i+1).padStart(6,'0'));
  assert.match(o.name,/^[A-Za-z0-9][A-Za-z0-9._ -]{0,179}$/);assert.doesNotMatch(o.name,/[ .]$/);
  assert.doesNotMatch(o.name,/^(con|prn|aux|nul|com[1-9]|lpt[1-9])(?:\.|$)/i);assert.ok(!names.has(o.name.toLowerCase()));names.add(o.name.toLowerCase());
  assert.ok(Number.isSafeInteger(o.byteSize)&&o.byteSize>0);assert.match(o.sha256,/^[a-f0-9]{64}$/);
  assert.deepEqual([o.kind,o.mediaType],media[path.extname(o.name).toLowerCase()]);
  if(o.kind==='image'){assert.equal(aux,false);images++}else aux=true;
  total+=o.byteSize;assert.ok(Number.isSafeInteger(total)&&total<=20*1024**3);
 }
 assert.ok(images>0);assert.equal(m.inputCounts.images,images);assert.equal(m.totalBytes,total);
 const {sha256,...payload}=m;assert.equal(sha256,crypto.createHash('sha256').update(canonical(payload)).digest('hex'));
}
assert.equal(rejections.schema,vectors.schema);
for(const vector of rejections.cases){assert.equal(vector.expected,'reject');assert.throws(()=>{
 const bytes=vector.wireHex?Buffer.from(vector.wireHex,'hex'):Buffer.from(vector.body,'utf8');
 check(new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes));
},vector.name)}
for(const spec of rejections.generated){
 assert.equal(spec.expected,'reject');assert.equal(spec.photos,10000);assert.equal(spec.gcp,true);
 const objects=Array.from({length:spec.photos},(_,i)=>({id:'o'+String(i+1).padStart(6,'0'),name:`photo${String(i).padStart(5,'0')}.jpg`,byteSize:1,sha256:'a'.repeat(64),kind:'image',mediaType:'image/jpeg'}));
 objects.push({id:'o010001',name:'gcp_list.txt',byteSize:1,sha256:'a'.repeat(64),kind:'auxiliary',mediaType:'text/plain'});
 const payload={id:'attempt-0001',revision:1,totalBytes:objects.length,objectCount:objects.length,inputCounts:{images:10000,lidarObjects:0,points:0},objects};
 const sha256=crypto.createHash('sha256').update(canonical(payload)).digest('hex');assert.throws(()=>check(canonical({...payload,sha256})),spec.name);
}
console.log(JSON.stringify({vectors:vectors.vectors.length,rejections:rejections.cases.length,generatedRejections:rejections.generated.length,canonicalAndShape:'ok'}));
