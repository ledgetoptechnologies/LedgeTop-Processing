# Private pull-test package publication

The main-only Processing publisher targets exactly two fresh package namespaces:

- `ghcr.io/ledgetoptechnologies/ledgetop-processing-private-coordinator`
- `ghcr.io/ledgetoptechnologies/ledgetop-processing-private-treedar`

Legacy coordinator, orchestrator and TreeDar packages and their existing tags or
digests are not publication targets. This change neither changes their visibility
nor migrates any runtime. The existing main-only workflow uses its job-scoped
`GITHUB_TOKEN` with `packages: write`; no publisher PAT or additional scope is added.

GitHub documents that a newly published container package defaults to private,
and publication with the workflow's `GITHUB_TOKEN` automatically links it to the
workflow repository. The build also supplies the exact repository source label.
See [GitHub Container registry documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#pushing-container-images).

Preflight checks the exact source repository and both fixed package names. An
authenticated package-metadata HTTP 404 permits first publication at these new
names. A 404 can also hide inaccessible metadata; it is not proof of ownership
or write permission. Other metadata/authentication errors fail closed. If a
package exists, it must already be private and linked to this exact repository;
public, internal, unlinked and wrong-repository metadata are rejected. The same
check runs again immediately before each push to catch intervening changes.
Nothing changes package visibility to make a push succeed.

Both unchanged Docker recipes must build, pass image platform/non-root checks,
and pass bounded offline smoke tests before the first push. Each publication
uses `build-<source-prefix>-<run-id>-<run-attempt>`; there is no `latest` tag.
Run/attempt uniqueness avoids deliberate tag reuse, but it is not a registry
immutability policy. Consumers must use the verified digest, not a mutable tag.
The publisher checks each pushed manifest digest plus private visibility and
exact repository linkage before marking that image verified or pushing the next.
The pair is not transactional: a verified first image can exist if the second
push fails. The retained receipt records that partial outcome.

A lost push response records the exact reference as `push_response_unconfirmed`
and stops, without retrying or pushing the second image. Post-push metadata or
digest failures also stop and preserve the receipt. Operators must reconcile the
original exact reference before considering another attempt; failure is not
proof that no remote image exists. Receipts contain metadata only, never tokens.

Local mocked checks: `python -B .github/scripts/test_publish_images.py`.
These tests do not prove real GitHub grants, organization package policy,
registry visibility, image builds or pullability. Those require a reviewed
main-branch publication and authenticated digest pull checks. No runtime
deployment is part of publication.
