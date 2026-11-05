# Bounded opaque historical pilot-journal retention

This extends only the read-only predecessor census/diagnostic, following the
reviewed bounded retention policy in historical da629ae. It does not infer a
producer ABI or treat documents as instructions, terminal receipts or ownership.

The only newly recognized root is exactly `pilot-journals`. Directories must be
owned and 0700, at most root plus 64 immediate children. Regular files must be
owned, single-link, private, have no special bits, at most 65536 bytes each,
64 files and 1048576 bytes total. Files may be directly below that root or one child
directory; deeper trees, symlinks, hard links, permissive modes and unknown roots
hold. Directory count is independently bounded so empty-directory trees cannot
bypass the file bound. No file is executed, parsed as a producer record, rewritten
or deleted.

Both modes read/hash these bounded **control metadata** files. The diagnostic
still never hashes photos or ZIP bodies and still returns its distinct profile
and `retainedFilesValidated=false`. All paths and content use the private census
key. The returned opaque manifest includes aggregate counts/bytes and a digest of
keyed path/kind/mode/size/content records; no raw filename, token or JSON content
is printed. Full retained-data hashing remains distinct.

Every result explicitly reports `pilotClosureProven=false`. Retaining an opaque
manifest is not proof a producer stopped, cancel succeeded, a supervisor cannot
resume, or a stale process cannot write. First-cutover activation remains held
until separate original-writer/process/outcome/provenance review binds that exact
manifest. Unknown-table guards and all other unknown-path guards remain intact.

Validation: 41/41 Linux tests passed, no skips, in 1.623 seconds (38 census/probe plus
3 optimized safety). One CPU, 768 MiB, UID/GID 10001, no network, read-only source/image,
192 MiB task temporary filesystem. New cases retain three synthetic child journal
directories/24 opaque files without claiming closure, detect content mutation in
the keyed manifest, enforce size/count/total/directory/depth/private-mode bounds,
and keep unrelated roots held. Named container auto-removed; filtered inventory
empty. No installed PC, NAS or client data changed.
