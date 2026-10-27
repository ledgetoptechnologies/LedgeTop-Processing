# Repository CI policy

Beau's October 27, 2026 direction: GitHub Actions runs for pushes to `main`
and pull requests targeting `main`. Do not add feature-branch push workflows.
Manual GitHub runs must be guarded to `main`; image publication is main-only
and never runs in a PR. Use Engineering Hub for ordinary branch iteration when
its actual execution path is available; do not claim that path is installed
merely because GitHub branch triggers are absent.

Keep package publishing separate from runtime deployment. Pull-test packages
must be private, linked to this repository and identified by immutable digest.
Node credentials remain read-only; publishing uses the job-scoped GitHub token.
