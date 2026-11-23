# Private package publication and automatic promotion

The main-only Processing publisher targets two fixed package namespaces, named
once in [`processing/release_packages.py`](../processing/release_packages.py):

- `ghcr.io/ledgetoptechnologies/ledgetop-processing-coordinator-v2`
- `ghcr.io/ledgetoptechnologies/ledgetop-processing-treedar-v2`

Earlier coordinator, private-coordinator, orchestrator and TreeDar packages and
their tags or digests are not publication targets. Their visibility is not
changed. `ledgetop-processing-private-coordinator` became public on its first
push (main run 43148199486) and cannot be made private again; publication has
stopped at `preflight_package_not_private` since, which is why the names moved.
The workflow uses its job-scoped `GITHUB_TOKEN` with `packages: write`; no
publisher PAT or extra scope is added.

## One-time package creation (before the first publish)

A package first created by a workflow push from this repository can come out
public. The publisher therefore never creates a package: preflight requires each
package to already exist, be private and be linked to this repository. An org
owner creates each of the two packages once:

1. Organization settings, Packages, Package creation: allow only **private**
   container packages to be created (clear public and internal). Check the
   exact labels on GitHub; this is the org-wide guard against a public default.
2. Seed the name from a person's login, not from Actions, with a harmless
   placeholder that has **no** `org.opencontainers.image.source` label (that
   label links the package at creation). For example, retag any small approved
   base image as `ghcr.io/ledgetoptechnologies/<name>:seed` and push it with
   a personal `write:packages` login. Never use the `verified` tag for a seed.
3. In the new package's settings: confirm visibility is **Private**; under
   "Manage Actions access" add `LedgeTop-Processing` with the **Write** role;
   then "Connect repository" to `ledgetoptechnologies/LedgeTop-Processing`.
4. Check what the publisher will check:
   `gh api /orgs/ledgetoptechnologies/packages/container/<name> --jq '{visibility, repository: .repository.full_name}'`
   must show `private` and `ledgetoptechnologies/LedgeTop-Processing`.

Repeat for both names. The seed image carries none of the identity labels below,
so no host ever deploys it. [GitHub package visibility and access](https://docs.github.com/en/packages/learn-github-packages/configuring-a-packages-access-control-and-visibility#configuring-visibility-of-packages-for-an-organization)

## Publication

Preflight checks the exact source repository and both packages. Missing or
inaccessible metadata, including an authenticated HTTP 404, stops before any
image build. Public, internal, unlinked and wrong-repository records also stop
publication. The publisher rechecks both packages after smoke tests and before
the first push, rechecks each package immediately before its push and after it,
and again before moving its `verified` tag.

Both unchanged Docker recipes must build, pass image platform/non-root checks,
and pass bounded offline smoke tests before the first push. Each image carries
these labels: `org.opencontainers.image.source`, `org.opencontainers.image.revision`
(the commit), `com.ledgetop.processing.package` (`coordinator` or `treedar`),
`com.ledgetop.processing.publish-run-id`, `-publish-ref` and `-publish-event`.
Each publication uses `build-<source-prefix>-<run-id>-<run-attempt>`; there is
no `latest` tag. The publisher checks each pushed manifest digest plus private
visibility and exact repository linkage. It then pulls the immutable
`repository@sha256:...` reference for Linux/amd64 and inspects it: image ID,
platform, non-root user `10001:10001` and every label above must match the
locally built and smoke-tested image. Only then is the image marked verified.
A pull/inspection/identity failure stops at `postpush_image_identity` with
`postpush_image_identity_unverified`; the remote digest remains recorded.

The pair is not transactional: a verified first image can exist if the second
push fails. A lost push response records the exact reference as
`push_response_unconfirmed` and stops, without retrying. Operators reconcile the
original exact reference before another attempt. Receipts contain metadata only.

## Promotion: the `verified` tag

After both images are verified, the publisher promotes them only when:

- the event is a `push` to `refs/heads/main` (a manual run publishes but never
  promotes, `skipped_not_main_push`), and
- that commit is still the head of main (otherwise `skipped_superseded`; the
  newer commit's own run promotes it, so the tag never moves backwards).

For each package it re-verifies the package, tags the verified local image as
`:verified`, pushes it, and requires the registry's digest for `:verified` to
equal the verified digest (`promotion_digest_unverified` otherwise). The run
receipt records `promotion.status` and each image's `verifiedTag`.

`verified` is a moving pointer for discovery only. No host runs it: the
[node-side auto-deploy step](main-maintenance.md#automatic-updates) resolves it
to a digest, pulls that digest, checks the identity labels and the run order, and
updates only through the digest-pinned drain window.

Local mocked checks: `python -B .github/scripts/test_publish_images.py` (from the
repository root). They do not prove real GitHub grants, organization package
policy, registry visibility, image builds or pullability; those need a reviewed
main publication and authenticated digest pulls.
