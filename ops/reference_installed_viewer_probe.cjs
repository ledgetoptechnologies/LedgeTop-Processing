'use strict';
// Read-only SDK probe. The staging bearer stays in SSH stdout captured in
// process memory; it is never printed, written locally, or passed as argv/env.
const fs = require('node:fs');
const path = require('node:path');
const {execFileSync} = require('node:child_process');

async function main() {
  if (!process.argv.includes('--read-only')) throw new Error('EXPLICIT_READ_ONLY_REQUIRED');
  const viewerRoot = process.env.REFERENCE_PROBE_VIEWER_ROOT;
  const caFile = process.env.REFERENCE_PROBE_CA_FILE;
  if (!viewerRoot || !caFile || !path.isAbsolute(viewerRoot) || !path.isAbsolute(caFile)) {
    throw new Error('EXPLICIT_LOCAL_SOURCE_AND_PUBLIC_CA_REQUIRED');
  }
  const {ReferenceProvider} = require(path.join(viewerRoot,'apps/viewer/server/referenceProvider'));
  const privateCaPem = fs.readFileSync(caFile,'utf8');
  const token = execFileSync('ssh',[
    '-o','BatchMode=yes','-o','ConnectTimeout=10','bkoltz@192.168.50.89','python3','-B','-',
  ],{input:"from pathlib import Path\nprint(Path('/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848/config/viewer-staging.token').read_text().strip())\n",
    encoding:'utf8',stdio:['pipe','pipe','pipe'],timeout:15000,maxBuffer:8192}).trim();
  const configuration = {endpoint:'https://192.168.50.89:18443',token,environment:'staging',
    enabled:true,privateCaPem,timeoutMs:10000};
  const capabilities = await new ReferenceProvider(configuration).capabilities();
  if (capabilities.environment !== 'staging' || capabilities.engines.treedar.available !== false
      || capabilities.engines.gaussian_splat.enabled !== false) throw new Error('CAPABILITY_BINDING_FAILED');
  let invalidCredential = false, wrongEnvironment = false;
  try { await new ReferenceProvider({...configuration,token:'invalid-read-only-probe-token-00000000'}).capabilities(); }
  catch (error) { invalidCredential = error.code === 'UNAUTHORIZED'; }
  try { await new ReferenceProvider({...configuration,environment:'production'}).capabilities(); }
  catch (error) { wrongEnvironment = error.code === 'BINDING_MISMATCH'; }
  if (!invalidCredential || !wrongEnvironment) throw new Error('ROLE_OR_ENVIRONMENT_CHECK_FAILED');
  console.log(JSON.stringify({passed:true,actualViewerSdk:true,actualInstalledCoordinator:true,
    privateCaTls:true,authenticatedDiscovery:true,invalidCredentialRejected:invalidCredential,
    credentialEnvironmentBound:wrongEnvironment,treedarAvailable:false,gaussianEnabled:false,
    jobsSubmitted:0,credentialsPrinted:false,providerConfigurationChanged:false}));
}
main().catch(() => { console.error('Read-only installed Viewer probe failed; credentials omitted.'); process.exitCode=1; });
