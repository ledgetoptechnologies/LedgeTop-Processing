# Viewer workspace build and rendered fixture review — 2026-11-06

## Frozen source and build

Viewer candidate: `ca5eedf3549a8d3c1463f9c3cede7c3f696d41a3`, still draft Ops #446.
A NEW canonical-LF QA extraction is at
`C:/Users/fstor/AppData/Local/Temp/ltds-viewer-ui-ca5eedf3-20261106`.
Its source archive SHA-256 is
`0a115fba50d71abb46ff28ec10511adfb1b3082d2313cdb57fef76915cb1c26e`.

The exact package-lock bytes were independently compared with both the Git
blob and source archive after installation: unchanged. Canonical LF lock SHA:
`b528bce371d92ed5ce3ddc8eff726ab9ad6ecc58ced27ef8d1070c232d083363`.
The earlier `a0eaead1...` lock identity is its reproducible CRLF export, not a
dependency change.

Host Node24.16.0/npm12.0.2 installed140 packages in5s using
`npm ci --offline --ignore-scripts --no-audit --no-fund`.
Installed versions were Vite8.2.1, Three0.185.1 and 3d-tiles-renderer0.5.1.
No online dependency install or automatic third-party install script ran.

The npm-script wrapper could not resolve node in its child command shell.
The reviewed project postinstall commands were therefore executed directly:
`node scripts/patch-3d-tiles-renderer.mjs`,
`node scripts/install-basis-transcoder.mjs`, then
`node node_modules/vite/bin/vite.js build`.
All operated only on the NEW QA extraction/dependencies/assets. No global PATH
or user/shared package cache was changed. Build Node heap was bounded512MiB.
Vite transformed553 modules and completed in365ms.

Build log SHA-256:
`f4b3f7fab0f08990cabc3dc6e9a980fb8c0774a7aaf665cbdd0c52ac09d21622`.
Existing warnings concern the LAS dependency's CommonJS module variable and
large chunks; build exited0. This review does not qualify real LAS/LAZ rendering.

## Actual browser observations

The existing in-app browser was reused, with one task-owned tab3 pointed only
at a loopback synthetic fixture. No production Viewer, NAS or client dataset
was changed, and no standalone Chrome/Edge process was launched.

Observed through actual UI controls:

- Created synthetic Greenleaf Fire Department, then its New Build project.
- Created Field Sites and placed New Build in it through the customer/group
  dialogs. The resulting overview showed customer → group → project.
- Opened the project and verified customer/project breadcrumb and task area.
- Opened an existing synthetic Johnson task, its metadata and output panel.
- Expanded fullscreen logs and observed real browser polling advance the
  fixture log counter from1 to4, then later16.
- Opened and used the compact navigation drawer to return to Projects.
- Browser console error selection was empty.

Checked document widths:

| UI state | Browser viewport | Document scroll width |
| --- | ---: | ---: |
| Desktop grouped overview | 1280px | 1280px |
| Mobile grouped overview | 390px | 390px |
| Mobile expanded task | 390px | 375px |
| Narrow expanded task/overview | 320px | 305px |

The narrower document widths reflect the vertical scrollbar. None of these
checked states had horizontal page overflow. The status-filter strip scrolls
internally on narrow screens. The dark/orange styling was visually inspected.
A screenshot taken during drawer animation was not treated as its settled
position: subsequent DOM observation showed left0/right250/transformnone.

These are manual synthetic UI observations, not a counted automated suite.
Manual mode deliberately SKIPPED the three automatic browser cases; no pass
claim is made for those cases. The fixture provider/log is native-style;
Reference V2 witness/precedence correctness has separate shared-vector,
source, wire and targeted render-function evidence. This does not establish
production sign-in, client permissions, installed queue/node integration,
Reference V2 browser-state coverage, or the new GCP draft/mark/start flow.

## Retained screenshots

All paths below are under the QA directory above:

| File | SHA-256 |
| --- | --- |
| desktop-hierarchy.png | `502ba0fba1888ad44e60b2aea31f929411ce441654c31927cd1d800d26e85bed` |
| mobile-hierarchy.png | `a7ceddaa4cedc38dac92eef83e2f8593cbb29b13a8bca4f2513131919df34766` |
| mobile-task-logs.png | `fb4529c1220321736d66f97fa8f16000e50c2c3272fce5472b65d12598dbd493` |
| narrow-hierarchy.png | `d0691d06a96ed7cb054b48cbf53f742ba730fb25fc8be8915d6d8be3bb18ef94` |

## Cleanup

Fixture execution handle2192/NodePID8688 listened only on127.0.0.1:49730.
The task-owned browser tab was closed and the temporary viewport override reset.
The user's original Six Labors tab1 remained unchanged. The server was stopped
with its terminal interrupt; the shell reported exit1 from that interruption,
not a test-suite result. Fresh process and socket checks proved PID8688 absent
and zero listening sockets at49730. No task-owned browser or server remains.

Viewer owner is separately implementing the first-GCP-run review draft and
explicit height/marking sequence. Processing receives prepared bound snapshots;
it does not implement that Viewer submission ordering. Keep that follow-up
separate from the no-GCP photo candidate and preserve unsupported-height refusals.
