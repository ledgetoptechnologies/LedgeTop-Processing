# Repository CI policy

Beau's October 29, 2026 direction: GitHub Actions runs only for pushes to `main`
and explicitly main-guarded manual runs. Do not add pull-request, tag, staging,
dev or feature-branch runs. Run ordinary staging and pre-merge tests locally.
Manual GitHub runs must be guarded to `main`; image publication is main-only
and never runs in a PR. Use Engineering Hub for ordinary branch iteration when
its actual execution path is available; do not claim that path is installed
merely because GitHub branch triggers are absent.

Keep package publishing separate from runtime deployment. Pull-test packages
must be private, linked to this repository and identified by immutable digest.
Node credentials remain read-only; publishing uses the job-scoped GitHub token.

# Branch and review hygiene

Use one branch and one PR per coherent deliverable. Keep implementation, regression
checks and supporting documentation together. Do not create separate permanent
branches or PRs for each test run, review receipt or experimental checkpoint.
Read-only review does not need another worktree.

Keep unfinished experiments out of main. When an experiment is abandoned or
superseded, close its PR with an accurate successor or archive reference and retain
any unique unfinished requirements. Archiving is not evidence of implementation.
After verifying source preservation and current ownership, remove obsolete remote
branches with exact-head checks. Remove local worktrees only when clean, including
ignored files, and inactive; preserve uncommitted work. Never use forced worktree
removal to improve cleanup counts.

Use `docs/archive/experiments-20261112/` for the consolidated historical experiment
index. Add future review evidence to the existing deliverable instead of growing
another queue of receipt-only PRs. Follow the PR-only main and main-only Actions
rules above.
