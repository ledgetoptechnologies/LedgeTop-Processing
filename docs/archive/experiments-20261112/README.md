# Historical Processing experiment index

The November 12 cleanup consolidates 238 historical pull requests into this index. They are archived **unmerged**, not implemented, qualified or deployed by this cleanup. Existing tests remain in Git history. Active source and data are preserved.

Current work stays in the normal PR queue. #66 retains the older photo integration reference, #180 the reference/synthetic experiments, #105 the LiDAR correctness extraction, #384 the read-only dashboard, and #385 retained-main maintenance. Other recent thematic keepers remain open.

## Recovery and limits

The local complete recovery bundle is `C:/Projects/LedgeTop-Processing/_artifacts/processing-cleanup-recovery-20261112/current-219-archive/all-refs-70e8-before.bundle`, SHA256 `70e8c9b2ceb6889dc54e7cfc2caa9b22723d06718067606cbbe2fab38d03bfc0`. Every listed exact head is explicitly present in this verified bundle. Local fresh comments, reviews and inline discussions are preserved alongside it; original GitHub discussions remain accessible on closed PRs. The machine index records exact original branches, heads, reasons and successors.

A successor link means follow-up ownership or source-history containment only. It does not assert that every requirement was implemented. Unique historical code needs fresh, narrow extraction and validation against current main before reuse; do not merge a stale umbrella stack.

Important unresolved historical concerns include native Viewer recovery/fairness and receipt acceptance; stopped checkpointed migration requirements from #55; TreeDar spatial/LAS validation (#105, #195); reference-driver diagnostics (#270); retained custody and cleanup fencing; scale tests using invented bytes; offline response and shutdown recovery. Fixture success never establishes live data or installed acceptance.

No runtime deployment, data cleanup, credentials, release visibility, CI trigger, or source tests are changed by this documentation. Main remains PR-only and Actions main-only.

## Archived review entries

| PR | Historical proposal | Follow-up / keeper | Disposition |
| --- | --- | --- | --- |
| [#18](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/18) | Add a synthetic native-path matrix driving actual Viewer worker modules | #66 | archived-unmerged-historical-experiment |
| [#21](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/21) | Draft: scoped staged-byte acknowledgment receiver | #66 | archived-unmerged-historical-experiment |
| [#24](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/24) | Add an optional Viewer admission socket deadline | #66 | archived-unmerged-historical-experiment |
| [#25](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/25) | Reject unsupported TreeDar execution shapes | #66 | archived-unmerged-historical-experiment |
| [#26](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/26) | Draft: add opt-in bounded broker range downloads | #66 | archived-unmerged-historical-experiment |
| [#27](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/27) | Report Viewer input and upload request limits | #66 | archived-unmerged-historical-experiment |
| [#28](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/28) | Draft reservation-scoped input inventory | #66 | archived-unmerged-historical-experiment |
| [#29](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/29) | Draft bounded input receipt verification | #66 | archived-unmerged-historical-experiment |
| [#30](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/30) | Draft quarantined candidate startup for incremental update | #66 | archived-unmerged-historical-experiment |
| [#35](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/35) | Add provisional input v2 budget planner | #66 | archived-unmerged-historical-experiment |
| [#36](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/36) | Propose full input v2 manifest commitment | #66 | archived-unmerged-historical-experiment |
| [#37](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/37) | Prototype durable v2 reservation binding | #66 | archived-unmerged-historical-experiment |
| [#38](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/38) | Prototype isolated v2 metadata ledger | #66 | archived-unmerged-historical-experiment |
| [#39](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/39) | Prototype durable cross-version enqueue order | #66 | archived-unmerged-historical-experiment |
| [#41](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/41) | Prototype synthetic node capacity service | #66 | archived-unmerged-historical-experiment |
| [#43](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/43) | Bound Viewer upload spool capacity when configured | #66 | archived-unmerged-historical-experiment |
| [#44](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/44) | Add gated authenticated v2 metadata admission | #66 | archived-unmerged-historical-experiment |
| [#45](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/45) | Expose gated v2 metadata capabilities | #66 | archived-unmerged-historical-experiment |
| [#52](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/52) | Keep observed v2 metadata reservations expired | #66 | archived-unmerged-historical-experiment |
| [#54](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/54) | Prototype metadata-bound scratch admission | #66 | archived-unmerged-historical-experiment |
| [#55](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/55) | Verify legacy Viewer rows during schema migration | #66 | archived-unmerged-historical-experiment |
| [#57](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/57) | Require exact container identity when checking absence | #66 | archived-unmerged-historical-experiment |
| [#58](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/58) | Prototype indexed v2 input journal | #66 | archived-unmerged-historical-experiment |
| [#59](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/59) | Prototype durable input hold and cooperative worker lock | #66 | archived-unmerged-historical-experiment |
| [#60](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/60) | Prototype verified local first-input publication | #66 | archived-unmerged-historical-experiment |
| [#61](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/61) | Prototype local multi-object input verification | #66 | archived-unmerged-historical-experiment |
| [#62](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/62) | Prototype retained node gate for verified inputs | #66 | archived-unmerged-historical-experiment |
| [#63](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/63) | Prototype supervised durable input intent | #66 | archived-unmerged-historical-experiment |
| [#64](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/64) | Keep Viewer and queue completion in one transaction | #66 | archived-unmerged-historical-experiment |
| [#65](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/65) | Prototype supervised NodeODX UUID guard | #66 | archived-unmerged-historical-experiment |
| [#67](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/67) | Prototype synthetic stopped-copy Viewer split repair | #66 | archived-unmerged-historical-experiment |
| [#68](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/68) | Propose shared v2 input transfer contract | #66 | archived-unmerged-historical-experiment |
| [#69](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/69) | Prototype v2 grant vector verifier | #66 | archived-unmerged-historical-experiment |
| [#70](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/70) | Prototype durable v2 request precharge | #66 | archived-unmerged-historical-experiment |
| [#71](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/71) | Prototype cached v2 precharge membership | #66 | archived-unmerged-historical-experiment |
| [#72](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/72) | Prototype durable v2 stream slot | #66 | archived-unmerged-historical-experiment |
| [#73](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/73) | Prototype v2 stream opening reconciliation | #66 | archived-unmerged-historical-experiment |
| [#74](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/74) | Add optional Viewer upload spool space guard | #66 | archived-unmerged-historical-experiment |
| [#75](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/75) | Reproduce Viewer upload rename crash gap | #66 | archived-unmerged-historical-experiment |
| [#76](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/76) | Test upload spool denial and partial batch retry | #66 | archived-unmerged-historical-experiment |
| [#77](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/77) | Track interrupted Viewer upload publication | #66 | archived-unmerged-historical-experiment |
| [#78](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/78) | Check upload intents at stopped-state cutover | #66 | archived-unmerged-historical-experiment |
| [#79](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/79) | Draft disabled Viewer input inventory observation | #66 | archived-unmerged-historical-experiment |
| [#80](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/80) | Draft disabled Viewer inventory cutover integration | #66 | archived-unmerged-historical-experiment |
| [#81](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/81) | Draft inventory census rehearsal allowances | #66 | archived-unmerged-historical-experiment |
| [#82](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/82) | Draft legacy coordinator lifetime lock gate | #66 | archived-unmerged-historical-experiment |
| [#83](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/83) | Draft enrolled legacy fence startup enforcement | #66 | archived-unmerged-historical-experiment |
| [#84](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/84) | Draft historical updater hold for enrolled legacy fence | #66 | archived-unmerged-historical-experiment |
| [#85](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/85) | Test in-flight completion during coordinator shutdown | #66 | archived-unmerged-historical-experiment |
| [#86](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/86) | Draft enrolled offline stopped-state guard | #66 | archived-unmerged-historical-experiment |
| [#87](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/87) | Mount coordinator authority lease in Viewer Compose | #66 | archived-unmerged-historical-experiment |
| [#88](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/88) | Align lost commit acknowledgement test with cancellation hold | #66 | archived-unmerged-historical-experiment |
| [#89](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/89) | Recognize exact empty Viewer tables in updater census | #66 | archived-unmerged-historical-experiment |
| [#90](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/90) | Draft: guard Viewer multipart spool capacity | #66 | archived-unmerged-historical-experiment |
| [#91](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/91) | Draft: require persistent node data before replacement | #66 | archived-unmerged-historical-experiment |
| [#92](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/92) | Draft: prepare current reference cutover with retained journals and lease | #66 | archived-unmerged-historical-experiment |
| [#93](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/93) | Bind coordinator cutover to migrated engine | #66 | archived-unmerged-historical-experiment |
| [#94](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/94) | Make LiDAR ground admission independent of point order | #66 | archived-unmerged-historical-experiment |
| [#95](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/95) | Keep Viewer acceptance retries fair across restarts | #66 | archived-unmerged-historical-experiment |
| [#96](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/96) | Fetch large reference objects in bounded broker ranges | #66 | archived-unmerged-historical-experiment |
| [#97](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/97) | Continue bounded reference input fetch progress | #66 | archived-unmerged-historical-experiment |
| [#98](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/98) | Draft source-proof v2 vectors and detached validators | #66 | archived-unmerged-historical-experiment |
| [#99](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/99) | Bound slow broker transfers and hold unsafe retries | #66 | archived-unmerged-historical-experiment |
| [#100](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/100) | Expose queued Viewer option wait reasons | #66 | archived-unmerged-historical-experiment |
| [#101](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/101) | Integrate bounded reference worker fetch progress | #66 | archived-unmerged-historical-experiment |
| [#102](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/102) | Recover receipt-backed archive before path persistence | #66 | archived-unmerged-historical-experiment |
| [#107](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/107) | Draft: preserve NodeODM task data on startup | #66 | archived-unmerged-historical-experiment |
| [#108](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/108) | Fence cooperative maintenance with engine-scoped lease | #66 | archived-unmerged-historical-experiment |
| [#109](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/109) | Draft: preserve NodeODM files on submission conflicts | #66 | archived-unmerged-historical-experiment |
| [#118](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/118) | Hold unverified reference engine cancellations | #66 | archived-unmerged-historical-experiment |
| [#120](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/120) | Prototype bounded proactive Viewer archive capture | #66 | archived-unmerged-historical-experiment |
| [#121](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/121) | Prototype archive schema census compatibility | #66 | archived-unmerged-historical-experiment |
| [#127](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/127) | Prototype enrolled legacy coordinator lock vector | #66 | archived-unmerged-historical-experiment |
| [#128](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/128) | Draft direct Viewer node outbox identity contract | #66 | archived-unmerged-historical-experiment |
| [#129](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/129) | Prototype same-inode reserved output slot | #66 | archived-unmerged-historical-experiment |
| [#130](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/130) | Prototype durable output capture journal | #66 | archived-unmerged-historical-experiment |
| [#131](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/131) | Prototype locked sealed-output reader | #66 | archived-unmerged-historical-experiment |
| [#132](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/132) | Exercise NodeODM download to durable capture | #66 | archived-unmerged-historical-experiment |
| [#133](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/133) | Prototype authenticated Viewer worker claim | #66 | archived-unmerged-historical-experiment |
| [#177](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/177) | Prototype synthetic worker scale preflight | #180 | archive-unmerged-ancestry-contained |
| [#178](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/178) | Prototype bounded reference listener connections | #180 | archive-unmerged-ancestry-contained |
| [#179](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/179) | Prototype synthetic gate-owner RPC | #180 | archive-unmerged-ancestry-contained |
| [#181](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/181) | Test archive schema backup boundary before reopening | #180 | archive-unmerged-unique-historical-work |
| [#182](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/182) | Expose authenticated Viewer dispatcher failure status | #180 | archive-unmerged-unique-historical-work |
| [#183](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/183) | Hold interrupted proactive archive restores for inspection | #180 | archive-unmerged-unique-historical-work |
| [#184](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/184) | Preserve uncertain Viewer archive partials | #180 | archive-unmerged-unique-historical-work |
| [#185](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/185) | Integrate Viewer archive retention and dispatcher status | #180 | archive-unmerged-ancestry-contained |
| [#186](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/186) | Exercise interrupted integrated archive capture | #180 | archive-unmerged-unique-historical-work |
| [#187](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/187) | Route jobs exceeding the small node advertised limit | #180 | archive-unmerged-ancestry-contained |
| [#188](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/188) | Propose Viewer result identity exchange | #180 | archive-unmerged-unique-historical-work |
| [#189](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/189) | Retry archive restores after pre-transfer failures | #180 | archive-unmerged-ancestry-contained |
| [#190](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/190) | Scope archive retry backoff to task generation | #180 | archive-unmerged-ancestry-contained |
| [#192](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/192) | Draft same-inode indexed worker input receiver | #180 | archive-unmerged-ancestry-contained |
| [#193](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/193) | Draft fresh-state allocated receiver bootstrap | #180 | archive-unmerged-ancestry-contained |
| [#194](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/194) | Draft allocated receiver publication checkpoints | #180 | archive-unmerged-unique-historical-work |
| [#195](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/195) | Bound and bind TreeDar result metadata validation | #180 | archive-unmerged-unique-historical-work |
| [#196](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/196) | Hold owner on contradictory NodeODM acknowledgments | #180 | archive-unmerged-unique-historical-work |
| [#197](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/197) | Integrate engine acknowledgment safety into photo candidate | #180 | archive-unmerged-ancestry-contained |
| [#198](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/198) | Exercise staged photo archive interruption and restart | #180 | archive-unmerged-ancestry-contained |
| [#199](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/199) | Exercise coordinator death during archive download | #180 | archive-unmerged-unique-historical-work |
| [#200](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/200) | Allow Viewer-created synthetic fixture tasks | #180 | archive-unmerged-unique-historical-work |
| [#201](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/201) | Rehearse photo candidate against installed predecessor image | #180 | archive-unmerged-ancestry-contained |
| [#202](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/202) | Census nonempty stopped coordinator state for review | #180 | archive-unmerged-ancestry-contained |
| [#203](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/203) | Hold coordinator lock during stopped-state census | #180 | archive-unmerged-ancestry-contained |
| [#204](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/204) | Hold coordinator authority through census and backup | #180 | archive-unmerged-ancestry-contained |
| [#205](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/205) | Prototype disabled coordinator authority descriptor handoff | #180 | archive-unmerged-ancestry-contained |
| [#206](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/206) | Hold coordinator startup after uncertain authority handoff | #180 | archive-unmerged-ancestry-contained |
| [#207](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/207) | Hold supervisor after uncertain coordinator handoff | #180 | archive-unmerged-ancestry-contained |
| [#208](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/208) | Document shared authority descriptor unlock hazard | #180 | archive-unmerged-unique-historical-work |
| [#209](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/209) | Explore independent outer launcher authority | #180 | archive-unmerged-ancestry-contained |
| [#210](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/210) | Prototype disabled XFS archive quota observation | #180 | archive-unmerged-unique-historical-work |
| [#211](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/211) | Hold node maintenance on uncertain active job inventory | #180 | archive-unmerged-ancestry-contained |
| [#212](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/212) | Integrate maintenance inventory holds in photo candidate | #180 | archive-unmerged-ancestry-contained |
| [#213](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/213) | Require valid Docker state in node idle checks | #180 | archive-unmerged-ancestry-contained |
| [#214](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/214) | Stage isolated gateway health probe | #180 | archive-unmerged-unique-historical-work |
| [#215](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/215) | Exercise durable metadata-only source queue | #180 | archive-unmerged-unique-historical-work |
| [#216](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/216) | Recover accepted reference admission after capability changes | #180 | archive-unmerged-ancestry-contained |
| [#217](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/217) | Exercise lost HTTP admission response and replay | #180 | archive-unmerged-ancestry-contained |
| [#218](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/218) | Exercise concurrent reference admission replay | #180 | archive-unmerged-ancestry-contained |
| [#219](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/219) | Recover committed reference admissions during catalog outage | #180 | archive-unmerged-unique-historical-work |
| [#220](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/220) | Bound completion sender socket attempts and replay receipts | #180 | archive-unmerged-unique-historical-work |
| [#221](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/221) | Reconcile unbound photo commit responses | #180 | archive-unmerged-ancestry-contained |
| [#222](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/222) | Reject incomplete photo engine mutation responses | #180 | archive-unmerged-ancestry-contained |
| [#224](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/224) | Reject ambiguous photo engine status responses | #180 | archive-unmerged-unique-historical-work |
| [#225](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/225) | Combine photo failure holds and status validation | #180 | archive-unmerged-ancestry-contained |
| [#226](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/226) | Rehearse PR225 against synthetic predecessor state | #180 | archive-unmerged-unique-historical-work |
| [#227](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/227) | Stage closed-on-start native admission option | #180 | archive-unmerged-ancestry-contained |
| [#228](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/228) | Hold capture when archive child exit is uncertain | #180 | archive-unmerged-ancestry-contained |
| [#229](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/229) | Retain coordinator lease after failed dispatcher drain | #180 | archive-unmerged-ancestry-contained |
| [#230](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/230) | Exercise closed coordinator restarts with queued state | #180 | archive-unmerged-ancestry-contained |
| [#231](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/231) | Avoid per-item delays across deferred archive backlog | #180 | archive-unmerged-ancestry-contained |
| [#232](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/232) | Reject ambiguous automatic archive response framing | #180 | archive-unmerged-ancestry-contained |
| [#233](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/233) | Hold recorded archive when capture receipt is missing | #180 | archive-unmerged-ancestry-contained |
| [#234](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/234) | Stage disabled durable archive-format registry | #180 | archive-unmerged-ancestry-contained |
| [#235](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/235) | Bound new archive registry entries | #180 | archive-unmerged-ancestry-contained |
| [#236](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/236) | Record exact candidate synthetic predecessor rehearsal | #180 | archive-unmerged-unique-historical-work |
| [#237](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/237) | Evaluate coordinator-only photo release boundary | #180 | archive-unmerged-ancestry-contained |
| [#238](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/238) | Evidence: hold rollback after candidate API write | #180 | archive-unmerged-unique-historical-work |
| [#239](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/239) | Stage shared admission barrier for reference jobs | #180 | archive-unmerged-ancestry-contained |
| [#240](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/240) | Exercise shared admission over real loopback TLS | #180 | archive-unmerged-ancestry-contained |
| [#241](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/241) | Allow completed result downloads during another upload | #180 | archive-unmerged-ancestry-contained |
| [#242](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/242) | Capture completed results during later uploads | #180 | archive-unmerged-ancestry-contained |
| [#243](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/243) | Rehearse synthetic Viewer and native coordinator workflow | #180 | archive-unmerged-ancestry-contained |
| [#244](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/244) | Rehearse Viewer upload limits and multipart resume | #180 | archive-unmerged-unique-historical-work |
| [#245](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/245) | Add unwired native input manifest candidate | #180 | archive-unmerged-ancestry-contained |
| [#246](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/246) | Draft disabled native manifest identity binding | #180 | archive-unmerged-ancestry-contained |
| [#247](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/247) | Add synthetic native binding interoperability rehearsal | #180 | archive-unmerged-unique-historical-work |
| [#248](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/248) | Add disabled native plan identity experiment | #180 | archive-unmerged-ancestry-contained |
| [#249](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/249) | Validate strict options in disabled native plans | #180 | archive-unmerged-ancestry-contained |
| [#250](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/250) | Add guarded native plan binding HTTP fixture | #180 | archive-unmerged-ancestry-contained |
| [#251](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/251) | Bound native binding body reads | #180 | archive-unmerged-ancestry-contained |
| [#252](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/252) | Add synthetic Viewer plan binding rehearsal | #180 | archive-unmerged-unique-historical-work |
| [#253](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/253) | Test current native plan cancellation ordering | #180 | archive-unmerged-ancestry-contained |
| [#254](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/254) | Add disabled native plan epoch prototype | #180 | archive-unmerged-ancestry-contained |
| [#255](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/255) | Add disabled native epoch HTTP candidate | #180 | archive-unmerged-ancestry-contained |
| [#256](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/256) | Use header-only authentication for native epoch | #180 | archive-unmerged-ancestry-contained |
| [#257](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/257) | Wake archive capture on durable task completion | #180 | archive-unmerged-unique-historical-work |
| [#258](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/258) | Draft disabled native capacity metadata | #180 | archive-unmerged-unique-historical-work |
| [#259](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/259) | Classify retained archive partials after child exit | #180 | archive-unmerged-unique-historical-work |
| [#260](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/260) | Prototype durable direct Viewer terminal observations | #180 | archive-unmerged-unique-historical-work |
| [#261](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/261) | Stage reference-compatible native archive candidate | #180 | archive-unmerged-ancestry-contained |
| [#262](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/262) | Draft read-only archive state preflight | #180 | archive-unmerged-ancestry-contained |
| [#263](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/263) | Stage guarded backup for nonempty coordinator state | #180 | archive-unmerged-unique-historical-work |
| [#264](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/264) | Draft inactive bounded engine ingress admission guard | #180 | archive-unmerged-ancestry-contained |
| [#265](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/265) | Stage guarded bounded photo upload streaming | #180 | archive-unmerged-ancestry-contained |
| [#266](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/266) | Bind guarded photo stream to stored task file | #180 | archive-unmerged-ancestry-contained |
| [#267](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/267) | Record isolated NodeODM bound upload rehearsal | #180 | archive-unmerged-unique-historical-work |
| [#268](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/268) | Stage disabled guarded dispatcher integration | #180 | archive-unmerged-ancestry-contained |
| [#269](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/269) | Stream guarded Viewer uploads from owned descriptor | #180 | archive-unmerged-ancestry-contained |
| [#270](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/270) | Report background result reconciliation failures | #180 | archive-unmerged-unique-historical-work |
| [#272](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/272) | Classify unavailable reference result reads | #180 | archive-unmerged-unique-historical-work |
| [#274](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/274) | Bound native result acceptance receipts | #180 | archive-unmerged-ancestry-contained |
| [#275](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/275) | Bound and retry native acceptance relays | #180 | archive-unmerged-ancestry-contained |
| [#276](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/276) | Prototype guarded transfer mutation fence | #180 | archive-unmerged-unique-historical-work |
| [#277](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/277) | Rehearse Viewer native receipt with Processing | #180 | archive-unmerged-unique-historical-work |
| [#278](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/278) | Integrate archive, acceptance and guarded transfer candidates | #180 | archive-unmerged-ancestry-contained |
| [#279](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/279) | Hold worker claim observations during uncertain transfers | #180 | archive-unmerged-unique-historical-work |
| [#280](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/280) | Exercise selected-node option discovery over HTTP | #180 | archive-unmerged-ancestry-contained |
| [#281](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/281) | Prototype node-local initialization broker | #180 | archive-unmerged-ancestry-contained |
| [#282](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/282) | Rehearse priority and routing through Viewer-compatible HTTP | #180 | archive-unmerged-unique-historical-work |
| [#283](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/283) | Prototype local coordinator authority witness | #180 | archive-unmerged-ancestry-contained |
| [#284](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/284) | Prototype node broker file uploads | #180 | archive-unmerged-ancestry-contained |
| [#285](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/285) | Prototype live node authority session | #180 | archive-unmerged-ancestry-contained |
| [#286](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/286) | Confirm exact native acceptance receipts | #180 | archive-unmerged-ancestry-contained |
| [#287](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/287) | Prototype durable coordinator intent for broker operations | #180 | archive-unmerged-ancestry-contained |
| [#288](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/288) | Rehearse remote broker initialization with durable intent | #180 | archive-unmerged-ancestry-contained |
| [#289](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/289) | Rehearse Viewer sender against Processing HTTPS | #180 | archive-unmerged-ancestry-contained |
| [#290](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/290) | Rehearse lost receipt response across sender restart | #180 | archive-unmerged-ancestry-contained |
| [#291](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/291) | Prototype authenticated broker data upload | #180 | archive-unmerged-ancestry-contained |
| [#292](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/292) | Rehearse acceptance replay across clean server restart | #180 | archive-unmerged-ancestry-contained |
| [#293](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/293) | Prototype node-side upload receipt | #180 | archive-unmerged-ancestry-contained |
| [#294](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/294) | Rehearse Viewer retry while Processing is offline | #180 | archive-unmerged-ancestry-contained |
| [#295](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/295) | Prototype receipt census for completed upload intents | #180 | archive-unmerged-ancestry-contained |
| [#296](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/296) | Rehearse credential-bound Viewer acceptance retry | #180 | archive-unmerged-ancestry-contained |
| [#297](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/297) | Rehearse empty Viewer schema upgrade and receipt replay | #180 | archive-unmerged-unique-historical-work |
| [#298](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/298) | Prototype indexed upload intent census | #180 | archive-unmerged-ancestry-contained |
| [#299](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/299) | Prototype held runtime for upload census | #180 | archive-unmerged-ancestry-contained |
| [#300](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/300) | Expose queued transfer-journal inspection holds | #180 | archive-unmerged-unique-historical-work |
| [#301](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/301) | Prototype per-input storage reservation | #302 | ancestry-consolidated-unmerged |
| [#303](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/303) | Test worker result retention during coordinator outage | #311 | ancestry-consolidated-unmerged |
| [#304](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/304) | Rehearse worker process recovery during coordinator outage | #311 | ancestry-consolidated-unmerged |
| [#306](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/306) | Rehearse worker result through Viewer receipt | #311 | ancestry-consolidated-unmerged |
| [#308](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/308) | Rehearse native derivative hold after joined delivery | #311 | ancestry-consolidated-unmerged |
| [#309](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/309) | Rehearse worker-to-Viewer ingest after coordinator restart | #311 | ancestry-consolidated-unmerged |
| [#314](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/314) | Prototype atomic Viewer job binding | #316 | ancestry-consolidated-unmerged |
| [#315](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/315) | Prototype credential-bound submission transaction | #316 | ancestry-consolidated-unmerged |
| [#318](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/318) | Preview native TreeDar bundle provenance | #329 | ancestry-consolidated-unmerged |
| [#322](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/322) | Stage disabled remote worker daemon integration | #328 | ancestry-consolidated-unmerged |
| [#323](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/323) | Stage disabled local capacity admission candidate | #324 | ancestry-consolidated-unmerged |
| [#325](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/325) | Test archive recovery checkpoint interruption | #332 | ancestry-consolidated-unmerged |
| [#326](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/326) | Stage disabled fetch pin and conservative batching | #328 | ancestry-consolidated-unmerged |
| [#327](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/327) | Stage disabled v2 delivery intent journal | #328 | ancestry-consolidated-unmerged |
| [#333](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/333) | Handle coordinator startup stop and resource failures | #334 | ancestry-consolidated-unmerged |
| [#339](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/339) | Draft: durable Viewer acceptance transfer and cleanup gates | #368 | ancestry-consolidated-unmerged |
| [#342](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/342) | Draft: candidate resumable NAS source range client | #368 | ancestry-consolidated-unmerged |
| [#352](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/352) | Add inactive receipt protocol foundation | #353 | ancestry-consolidated-unmerged |
| [#356](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/356) | Cache repeated source-range grant validation | #358 | ancestry-consolidated-unmerged |
| [#360](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/360) | Verify retained custody before clearing pending state | #361 | ancestry-consolidated-unmerged |
| [#364](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/364) | Add ordered node routing and live ODM option validation | #368 | ancestry-consolidated-unmerged |

## Additional unique deferred proposals

These keepers retain follow-up ownership only, not ancestor history or implemented functionality. The machine index preserves the original unfinished requirements, including the Gaussian gates from #317 and receipt binding from #335.

| PR | Historical proposal | Follow-up keeper | Disposition |
| --- | --- | --- | --- |
| [#363](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/363) | Prototype node-local scratch reservations | #324 | unique, unmerged, unfinished |
| [#362](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/362) | Gate ordered per-node dispatch on coordinator authority | #365 | unique, unmerged, unfinished |
| [#359](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/359) | Preserve unresolved result custody on removal | #361 | unique, unmerged, unfinished |
| [#357](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/357) | Check backup space before stopping coordinator | #367 | unique, unmerged, unfinished |
| [#340](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/340) | Reject Viewer source seals as spool custody | #368 | unique, unmerged, unfinished |
| [#338](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/338) | Draft: safely cancel queued processing jobs | #368 | unique, unmerged, unfinished |
| [#336](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/336) | Draft: add isolated range budget v2 candidate | #353 | unique, unmerged, unfinished |
| [#335](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/335) | Draft: fixture-only acceptance and spool-v2 release binding | #368 | unique, unmerged, unfinished |
| [#331](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/331) | Hold maintenance when active job ownership is uncertain | #367 | unique, unmerged, unfinished |
| [#330](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/330) | Stage disabled streaming gateway isolation fixture | #368 | unique, unmerged, unfinished |
| [#321](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/321) | Prototype reference init authority witness | #328 | unique, unmerged, unfinished |
| [#320](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/320) | Exercise in-flight coordinator shutdown | #334 | unique, unmerged, unfinished |
| [#319](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/319) | Prototype disabled node gateway authentication | #316 | unique, unmerged, unfinished |
| [#317](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/317) | Test inactive TreeDar and Gaussian capability gates | #329 | unique, unmerged, unfinished |
| [#313](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/313) | Define candidate cross-language receipt envelope | #353 | unique, unmerged, unfinished |
| [#312](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/312) | Hold reference cleanup until downstream custody is qualified | #361 | unique, unmerged, unfinished |
| [#310](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/310) | Experiment with conditional clone census under supervisor | #367 | unique, unmerged, unfinished |
| [#307](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/307) | Prototype held broker evidence inspection | #328 | unique, unmerged, unfinished |
| [#305](https://github.com/ledgetoptechnologies/LedgeTop-Processing/pull/305) | Rehearse coordinator interruption during node init | #328 | unique, unmerged, unfinished |
