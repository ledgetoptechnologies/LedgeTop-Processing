# Current-root baseline rechecks

Two failures reported on Claude's older recovery checkpoint were independently
checked against frozen current Processing `0ed69685ec2d12622573aa6389e349a4ea077dd9`.
Its full Git archive SHA256 was
`c996fb4503097723a3b2995afa3dbd2069aac9b1f7d04f949e99d8b801b12205`.

The two-case Linux run took2.129seconds: actual normal quarantine/seal/frozen
restart **passed**; the signed-owner test failed exactly on expected400 versus
actual409. This is not a passing two-case selection. It does not identify the
original older-branch quarantine traceback, which remains the peer's separate
ancestry/fixture investigation.

The owner test reused an already admitted submission ID while changing its
signed request. The existing read-only replay path correctly refuses a different
request digest with409/reference_conflict before considering fresh admission.
Test-only commit `103c0bc562079a2751cd236b831e0320f98b73e7` (root selection
`f79bcfe`) separates that replay conflict from fresh principal validation:

- Existing ID with changed owner/signature returns409 and preserves the complete
  persisted private job projection.
- A fresh fully rebound attempt with wrong signed owner returns400 and preserves
  the existing projection. The otherwise identical valid-owner control returns
  201/sourceState receiving and adds exactly one job.

The corrected exact three-case selection passed **3/3 in2.112seconds**, no skips:

```text
tests.test_first_coordinator_quarantine.FirstCoordinatorQuarantineTests.test_normal_quarantine_seal_and_frozen_restart_preserve_legacy_data
tests.test_reference_odm_inputs.ODMInputClosureTests.test_changed_signed_owner_on_existing_submission_is_a_replay_conflict
tests.test_reference_odm_inputs.ODMInputClosureTests.test_fresh_signed_wrong_owner_is_rejected_and_matched_owner_admits
```

The corrected frozen archive SHA256:
`a55f89e96cc2213b835a280199029ab6736adc07b3004d685ae382aa07a9f0a4`.
Both runs used UID10001, read-only source, network none and bounded one-CPU/
512MiB containers. Named and task-label runner inventories were empty afterward.
No API, Store, Driver, credential policy or security validation was changed.
These targeted checks do not qualify all recovery changes, installed admission,
NAS custody, real engine processing or Viewer output delivery.
