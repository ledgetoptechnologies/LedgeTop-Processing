# Viewer presentation integration review — 2026-11-06

## Frozen inputs and scope

Processing source: `e393bc9ce228f4f0f1d1b0b2904857aa62147030`.
Viewer candidate: `01ff415d29734a09517a53182682c3a67a34901d`, parent `76f7c11258e5a0fe0660a17e89a102bd8ed2348d`.
This is a local source review and targeted host cross-language test, not installed or real-photo acceptance. No runtime services were changed. The Viewer candidate is not approved for publication pending the corrections below.

## Review findings sent to the Viewer owner

1. **Receiving reply compatibility:** `sourceIntakeClientV2.js` closes the entire response to a small binding subset. Actual Processing `_public` replies also contain node, transfer, error, result and object-receipt fields, plus queue metadata. Valid 201 admission and GET recovery replies would be rejected. Validate/project the binding subset, preserve compatibility with the full producer DTO, and exclude optional presentation from durable bindings. Require producer-shaped admission and recovery regressions.
2. **Disconnect latch clearing:** `taskOutputStage` returns on `presentationUnavailable` before invoking the helper that clears the attempt/generation witness. A later reachable reply with omitted presentation can resurrect the earlier witness. Require the actual rendered-path confirmed → disconnected → reachable-without-projection regression.
3. **Unrelated ambiguous upload change:** restore the established `deferHeldSubmit(..., 'upload_ambiguous')` behavior; storage admission deferral mislabels uncertainty and loses the previous lease/terminal safeguards and diagnostics.

Root and a separate read-only reviewer inspected the four production paths. Findings are source evidence; corrective commits and regression results remain pending with the Viewer owner. Existing immutable candidate must not be rewritten.

## Accepted-copy cross-language check

Frozen Viewer archive SHA-256: `b33d1c6e19ccc504aa5d99d6737559740ea2ac875923b26be2e3413ff9b6bd89`.
Frozen Processing archive SHA-256: `b6a21c87992d996f420c026fc8d5f0e5eac58e45c2c9d91ddadafd8a83e4b2f4`.
Host: Windows, Node `v24.16.0`, Python `3.13.14`, cryptography `50.0.2`.
Command from frozen Viewer `apps/viewer`: `node --test --test-concurrency=1 test/reference-accepted-copy-v2-candidate.test.js`, with `PROCESSING_SOURCE` set to the frozen Processing archive extraction. Environment setting removed afterward.
Result: **3/3 passed, zero skips/failures**, 662.7859 ms. This closes the previously skipped actual Node receipt → current Python V2 verifier check for these exact inputs. It does not validate Linux deployment, source admission, full pipeline or the presentation corrections.

## Coordination and remaining gates

Exact Processing topic listener remains subscribed, identity verified, PID 108768 alive. Claude reply `bd798cc1-3e38-43c5-9b13-5ba18bda5c2f` arrived automatically in this active chat, was scoped, claimed and acknowledged. This proves current active-turn delivery, not unattended reboot recovery. Claude explicitly reports presentation head `ddd54d42` not final yet; his comparison container is owned and still running. No competing Docker workload was started.

Next: review the Viewer correction, obtain Claude's final qualified immutable head, run the matched integration checks, then prepare refreshed artifacts and installed acceptance. Keep PRs draft; preserve originals and retained results. Later user scheduling direction is **one job per physical node**, concurrent nodes allowed, with ordered eligible-node routing.


## Correction checkpoint and archive integrity finding

Viewer owner delivered child `ee9e750a721b6769762368a2f8f376e72eb59cc1`, preserving original `01ff415d`. Source review confirms receiving-binding subset projection, restored ambiguous-upload holding, and clearing of the UI witness through the rendered disconnect path. Its full producer-shaped POST/GET replay regressions preserve one immutable admission and omit status/display metadata from the durable binding.

Independent Windows frozen-archive selection covered source intake, ambiguous upload/restart, presentation, and actual Node-to-Python accepted-copy verification against isolated combined Processing `68888a30af3a1cbaf3fed9b70e66665cd62731b9`: **25 cases, 24 passed, one failed, no skips**, 1815.5124 ms. The failure is the canonical shared fixture byte hash, not a waived test. The archive converts all 2,149 LF lines to CRLF because the Viewer repository lacks a file-specific LF attribute. Git blob hash remains canonical `1977eab229b0cabb71aef94f82b142c8360ccd932a3bca84634a8f91c0716f71`; the extracted artifact hashes `4709d72844c580d07cf55c3781d0a5eeb5f2d93f1532f20a8b92f99e6711f124`. Replacing CRLF with LF reproduces the canonical hash, diagnosing the packaging difference only; the frozen input was not modified to make the test pass. Owner was asked for a child `.gitattributes` LF pin and a fresh archive proof. Never change the expected hash to accept converted bytes.

Viewer archive SHA-256: `0fbfbfb99bfe4c1f4409510a9d2e26587d5135a0acdb03c49ee937096a234707`.
Processing archive SHA-256: `e4876acb4549f026fa6f6c2e952e7a8b577ca4795ca35b4c572aedf399f53864`.
Full log: `C:/Users/fstor/AppData/Local/Temp/ltds-viewer-ee9e750a-focused-20261106.log`, SHA-256 `e2b69f228f58206ca2e80d69ab978f33cf18fece417c8b2172e0b9d851a8bec3`.

The isolated Processing merge preserves both pre-reservation/claim and start-witness process-death hooks. All six changed/integrated Python source files parse, but this is not functional qualification. It remains outside the release integration branch until Claude's final handoff and combined tests. No Docker workload, runtime installation or NAS operation was performed for this check.
