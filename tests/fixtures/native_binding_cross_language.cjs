'use strict';
// Synthetic loopback-only client; the Viewer modules are supplied by a pinned export.
const fs = require('node:fs');
const path = require('node:path');
const {DatabaseSync} = require('node:sqlite');
const [viewer, root, endpoint, phase] = process.argv.slice(2);
const server = path.join(viewer, 'apps/viewer/server');
const {ProviderCredentials} = require(path.join(server, 'providerCredentials'));
const {installCandidateSchema, createCandidate, createLoopbackTransport} = require(path.join(server, 'nativeManifestBindingCandidate'));
const vectors = JSON.parse(fs.readFileSync(path.join(viewer, 'apps/viewer/test/fixtures/native-input-manifest-v1-candidate.json')));
const db = new DatabaseSync(path.join(root, 'viewer.sqlite'));
const vault = new ProviderCredentials({processing:{},activeKeyId:'fixture',keys:{fixture:'ab'.repeat(32)}});
const transport = createLoopbackTransport({endpoint,environmentToken:'synthetic-staging-token-'+'s'.repeat(32)});
const calls = {reserve:0,bind:0,status:0};
for (const name of Object.keys(calls)) {
  const actual = transport[name];
  transport[name] = async (...args) => { calls[name]++; return actual(...args); };
}
const actualBind = transport.bind;
if (phase === 'lose') transport.bind = async (...args) => { await actualBind(...args); throw new Error('synthetic lost response after receipt'); };
const client = createCandidate({db,vault,transport,enabled:true});
const id = '00000000-0000-4000-8000-000000000001';
(async () => {
  installCandidateSchema(db,true);
  client.prepare({attemptId:'attempt-0001',taskUuid:id,environment:'staging',manifestBytes:Buffer.from(vectors.vectors[0].canonical)});
  if (phase === 'lose') {
    try { await client.bindOrRecover('attempt-0001'); throw new Error('expected hold'); }
    catch (e) { if (e.code !== 'native_binding_hold') throw e; }
    if (db.prepare('SELECT phase FROM native_binding_candidate').get().phase !== 'binding' || JSON.stringify(calls)!==JSON.stringify({reserve:1,bind:1,status:0})) throw new Error('missing binding intent or unexpected requests');
  } else {
    const saved = db.prepare('SELECT * FROM native_binding_candidate').get();
    const context = `native-manifest-capability:${saved.attempt_id}:${saved.task_uuid}:${saved.environment}:${saved.authority}`;
    const capability = vault.open(context,{credentialCiphertext:saved.capability_ciphertext,credentialKeyId:saved.key_id});
    const wrongEnvironment = createLoopbackTransport({endpoint,environmentToken:'synthetic-production-token-'+'p'.repeat(32)});
    try { await wrongEnvironment.status(id,capability); throw new Error('wrong environment accepted'); }
    catch (e) { if (e.code !== 'native_binding_hold') throw e; }
    const receipt = await client.bindOrRecover('attempt-0001');
    if (receipt.state !== 'identity_bound' || receipt.capacityReady !== false) throw new Error('invalid receipt');
    const row = db.prepare('SELECT phase,receipt FROM native_binding_candidate').get();
    if (row.phase !== 'bound' || JSON.stringify(JSON.parse(row.receipt)) !== JSON.stringify(receipt) || JSON.stringify(calls)!==JSON.stringify({reserve:0,bind:0,status:1})) throw new Error('receipt or replay requests differ');
  }
})().then(()=>db.close(),e=>{db.close();console.error(e.message);process.exitCode=1;});
