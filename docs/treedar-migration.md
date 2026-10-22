# TreeDar repository migration

On 2026-10-22 Beau authorized moving TreeDar into the existing LedgeTop Processing
repository. The coordinator remains at the root, and the forestry engine lives
under `engines/treedar` with its own package, tests, Dockerfile and documentation.

## Provenance

- Source: `https://github.com/ledgetoptechnologies/TreeDar`.
- Imported main tip: `fa2423a5e63f4058dd26dc0e57fb27ca64ae026e`.
- Destination base: `c4c2824f0cd81066433a95fd5b76f07d629eff68`.
- Method: non-squashed `git subtree add --prefix=engines/treedar`.
- Import commit: `a71cefe`.

The source main commit and every ancestor remain reachable in the destination;
their original commit IDs are preserved. The initial imported directory's tree
matches the source main tree byte-for-byte. Migration adjustments follow in
separate commits. Original commits use their original root paths; inspect a
historical file with, for example,
`git show fa2423a:src/treedar/cli.py`. The directory-prefixed path applies from
the import onward.

GitHub issues, PR discussions, repository settings and branch metadata are not
Git commits and are not transferred by this import. They remain accessible in
the old repository when Beau archives it. Do not delete that repository. Its
README will point to this destination once the migration is integrated.

## What changes

- Check out LedgeTop Processing for future TreeDar development.
- Run engine setup and tests from `engines/treedar`, not the coordinator root.
- Build the worker from `engines/treedar`; build the coordinator from the root.
- The staging script locates its engine directory from its own path, so it can
  be invoked from the monorepo root without building the coordinator by mistake.

Package name, CLI arguments, worker input/output mounts, runtime dependencies,
NAS datasets, local datasets and generated run folders retain their existing
meaning. No data migration, service deployment or automatic routing integration
is included. Old local environments are not copied; recreate them with the engine
setup script. No archive action is performed by the migration: Beau will archive
the old TreeDar repo after verification.

## Verification before archiving

Check source-commit ancestry and imported-tree equality; run coordinator and
TreeDar tests; build both images; verify engine help and a synthetic pipeline;
and exercise the staging script from a nested monorepo checkout. The TreeDar
suite includes real Docker checks when a Linux Docker engine is available.
