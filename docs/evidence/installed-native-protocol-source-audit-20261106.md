# Installed native API source audit

Read-only SSH/container-source inspection on the two current processing PCs.
No task init/upload/commit/cancel/remove call, task data/log read, configuration
write, service action or container change was performed. All probes exited;
no helper files or local test containers were created.

Main192.168.50.88 uses `ledgetop-main-processing-nodeodx-1`; cluster192.168.50.89
uses `lt-migration-1120ae14a8988e84513a4a8a46b78741-candidate`. Both application
working directories are `/var/www`.

## Request-shape support

The actual installed routers support Reference NativeEngine's wire:

| Operation | Installed source support |
| --- | --- |
| Initialize | Multipart POST `/task/new/init`, valid unused UUID in `set-uuid`, name and JSON-string options, response `{uuid}` |
| Upload | Multipart POST `/task/new/upload/{uuid}`, file field `images`, response `{success:true}` |
| Commit | Empty POST `/task/new/commit/{uuid}`, saved fields/files loaded, response `{uuid}` before asynchronous initialization |
| Output | GET `/task/{uuid}/output?line=N`, JSON string-array suffix; native does not implement a limit parameter |

Container source references: main/cluster UUID header handling is at
`libs/taskNew.js:53`/`:95`; upload at`:98`/`:140`; initialization at`:148`/`:187`;
commit saved-body handling at`:112`/`:151`. JSON-string options are parsed in
`libs/odmInfo.js:167` on both. Output route is `index.js:465`; `libs/Task.js:667`
returns the entire suffix. The client must retain bounded prefix/lookahead
parsing and close the response, rather than trust a native limit parameter.

## Presentation limits

`Task.js:49` initializes status20 before child work. It sets20 again at`:545`,
before invoking the runner at`:567`. Neither status20 nor commit acknowledgement
alone proves the native command started.

Normal initialization/server logger messages do not append task output. Fresh
runner stdout/stderr enters at`Task.js:609`; whitespace-only chunks can append
blank strings at`:610`. Postprocessing writers at`:308`,`:342`,`:428`,`:525`
and`:527` follow the original runner's exit0. Timeout text in
`TaskManager.js:325` is a server-generated exception after dateStarted.

Nonblank output establishes launcher/command activity, not successful ODM
initialization or continuous processing. Main`odmRunner.js:76` spawns run.sh;
cluster`:87` does so with shell:true. Launcher errors can emit output before the
ODM executable initializes. Public wording must retain that historical limit.

The provisional presentation checkpoint`a5b6053` repeatedly samples line0/8.
Eight initial blank entries can therefore hide later real output forever.
This source-supported liveness defect was sent to its owner for a bounded
advancing-window fix and tests before final contract qualification. It is not
evidence that any real job emitted eight blanks.

## Current test-mode startup inputs

Both current containers evaluate effective`config.test=false`. A bounded probe
used the sole live Node index.js process, its actual argv parsed by installed
minimist, selected regular bounded JSON configuration relative to its cwd, and
the exact loader's precedence. Only the boolean was emitted. Raw argv, config,
environment and credentials were not printed; config.js was not required or
executed. Selected configuration files predate process startup.

Precedence is main`config.js:126`/cluster`:123`:
`argv.test || fromConfigFile('test',false)`. Selection/loading is at`:88`–`:98`;
missing-value fallback is in`libs/utils.js:8`–`:20`. This closes the explicit
simulated-output branch for those startup inputs; it is not a process-memory
attestation or a real-job/timing test.

## Audited source hashes

| File | Main SHA256 | Cluster SHA256 |
| --- | --- | --- |
| libs/taskNew.js | dc1fa27ec7b94189e98edc44838f54bea604539a13588c1d1c37b6196775bee3 | 1b803713301db91dd11b8d2b54816fe3827cf5211edbc488e8f81104736f1f38 |
| libs/Task.js | e4e1e66aba91700742c528922ac81025144833eedc0c37e58540099c82b997ef | a3cba219aa9607d42ea12018c0e08c6742afccc5d3b812a5cb05a11a99cf5d51 |
| libs/odmRunner.js | 8bc89ac56e81608d871b55587fb0c83d31d2241a8b4120afbeeb33775350c244 | 9d45aa82131d572ab037e2f94958b2d8fbf34537048678cfb6c40760a5799857 |
| libs/TaskManager.js | 89ff48a399f87fdcec78ad49fa48fc8f7dd233c5421645bd816e9e0510916bbd | 9f43fff4882e7fba113c69f73328daa927ebb7e5716414eea5d9225b15d50efb |

This audit establishes installed source support and ordering. A small actual
dataset must still qualify requests, runtime timing, usable outputs and Viewer
return; large client data remains unqualified.
