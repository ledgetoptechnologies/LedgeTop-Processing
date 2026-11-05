# Curated Viewer candidate dependency boundary

The narrower Ops PR #446 candidate `84fa092336b7b4e26ff26f4aeda893ae4e5a1bcf`
was frozen as an `apps/viewer` Git archive, SHA256
`97273330d0e205f8e92aeed0420fb2cfe0c996104710b1982a6a16a1b42ac8eb`.
Its Viewer owner reports 76 passing focused Linux tests, one intentional skip
and no failures (77 total). The exact selection/skip explanation is pending.
This is not a passing 175-test Linux run or paired processing acceptance.

Independent real cross-language pairing then attempted whole/ranged GCP result
return and signed output/source cleanup against frozen Processing `18750d4`.
All three stopped before intake because `lod-converter-policy.cjs` correctly
rejected the mounted current licensed converter's schema-2 build information.
No engine or client job was submitted. Exact test containers and volumes were
removed and absence verified.

The curated branch omitted required dependency `6718d1be` from the previously
qualified Viewer pair: ImageSharp 3.2 converter policy, build, patch, locked
dependencies, public license notices and historical receipt compatibility.
The curated policy still expected schema 1 / patch `6d5d99...`; the qualified
current binary has schema 2 / patch `fb35e1...`, binary SHA256
`a68fd91118342b106e521d064a11066611aadf089b79e67a16503fac082bb155`.

The Viewer owner is curating that exact dependency. Do not bypass the build-info
guard, swap to an old binary, relabel old provenance, or transfer the broader
`ddb2e8cb` pair's passing receipts to this candidate. The corrected curated head
requires an exact-source combined rerun. Private license material remains
outside Git and must continue using the existing private build-secret path.

Log: `ltds-combined-qualification-6a7147b312e3.log` in the task's local temporary
artifact directory. This finding changes release readiness, not retained live
data or installed TrueNAS configuration. Cleanup activation remains gated.
