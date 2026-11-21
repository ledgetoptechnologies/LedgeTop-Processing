# Private pull-test package publication

The main-only Processing publisher targets these two fixed package namespaces.

- `ghcr.io/ledgetoptechnologies/ledgetop-processing-private-coordinator`
- `ghcr.io/ledgetoptechnologies/ledgetop-processing-private-treedar`

Legacy coordinator, orchestrator and TreeDar packages and their existing tags or
digests are not publication targets. This change neither changes their visibility
nor migrates any runtime. The existing main-only workflow uses its job-scoped
`GITHUB_TOKEN` with `packages: write`; no publisher PAT or additional scope is added.

Preflight checks the exact source repository and requires both packages to
already exist, be private, and be linked to that repository. Missing or
inaccessible metadata, including an authenticated HTTP 404, stops before any
image build. Public, internal, unlinked and wrong-repository records also stop
publication. The publisher rechecks both packages after smoke tests and before
the first push, then rechecks the individual package immediately before its
push.

An existing public organization package cannot be made private again. Recovery
requires separate approval for a replacement namespace and publisher
configuration. If the registry owner independently approves seeding a fresh
namespace, use an artifact approved for public exposure and verify private
visibility, exact repository linkage and repository Actions permissions before
publishing any application image. [GitHub package visibility and access](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility#configuring-visibility-of-packages-for-an-organization)

Both unchanged Docker recipes must build, pass image platform/non-root checks,
and pass bounded offline smoke tests before the first push. Each publication
uses `build-<source-prefix>-<run-id>-<run-attempt>`; there is no `latest` tag.
Run/attempt uniqueness avoids deliberate tag reuse, but it is not a registry
immutability policy. Consumers must use the verified digest, not a mutable tag.
The publisher checks each pushed manifest digest plus private visibility and
exact repository linkage. It then pulls the immutable `repository@sha256:...`
reference for Linux/amd64 using the existing job registry login, and inspects
that exact reference. Its image ID must equal the locally built and smoke-tested
image ID; Linux/amd64, non-root user `10001:10001`, exact source-repository label
and current source-revision label must also match. Only then does the receipt
record `remoteImageId` and `imageIdentityVerified: true`, mark that image verified,
and permit the next push. A pull/inspection/identity failure stops at
`postpush_image_identity` with `postpush_image_identity_unverified`; the remote
digest remains recorded for reconciliation. This pull is verification, not runtime
deployment.
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
