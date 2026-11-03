'use strict';
// Synthetic loopback acceptance service, real Viewer API and storage ledger.
const https=require('node:https'),fs=require('node:fs'),crypto=require('node:crypto'),express=require('express');
const root='/viewer/apps/viewer/server';
const {ReferenceAcceptanceLedger}=require(root+'/referenceAcceptance');
const {createReferenceAcceptanceApi,referenceAcceptanceJsonBodyType}=require(root+'/referenceAcceptanceApi');
const {createViewerReceiptSigner}=require(root+'/referenceReceipt');
// Container writable layer persists across stop/start; only synthetic files.
const dir='/var/synthetic-viewer-state';fs.mkdirSync(dir+'/managed',{recursive:true});
const ledger=new ReferenceAcceptanceLedger({databasePath:dir+'/ledger.sqlite',managedRoot:dir+'/managed'});
const privateKey=crypto.createPrivateKey(fs.readFileSync('/tls/viewer-receipt.pem')),publicKey=crypto.createPublicKey(privateKey);
const signer=createViewerReceiptSigner({issuerId:'synthetic-viewer-issuer',keyId:'synthetic-viewer-key',privateKeyPem:privateKey.export({type:'pkcs8',format:'pem'})});
const nonces=new Set();const repository={consumeServiceNonce(id,nonce){const x=id+'\n'+nonce;if(nonces.has(x))return false;nonces.add(x);return true;},pruneAuthState(){}};
const app=express();app.use(express.json({type:referenceAcceptanceJsonBodyType,verify(req,_res,bytes){req.rawBody=Buffer.from(bytes);}}));
app.use(createReferenceAcceptanceApi({repository,ledger,serviceKeyId:'synthetic-service',serviceAuthKeys:{'synthetic-service':'synthetic-viewer-service-key-material'},receiptSigner:signer,environment:'staging',chunkBytes:1024}));
const server=https.createServer({key:fs.readFileSync('/tls/server.key'),cert:fs.readFileSync('/tls/server.crt')},app);
server.listen(8443,'0.0.0.0',()=>process.stdout.write('READY '+Buffer.from(publicKey.export({type:'spki',format:'pem'})).toString('base64')+'\n'));
for(const signal of ['SIGTERM','SIGINT'])process.on(signal,()=>server.close(()=>{ledger.close();process.exit(0);}));