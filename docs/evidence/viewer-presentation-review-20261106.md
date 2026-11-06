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
