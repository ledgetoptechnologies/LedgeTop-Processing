# Installed GCP parser and vertical datum boundary

Read-only probes used the existing main NodeODX container on `.88`, image
`sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`.
Its package reports NodeODX 2.4.0. The actual parser is
`/code/modules/location.py`, SHA256
`d366ef27f819c0a9b8df4129500db0ff6e0838dbf1634a35466190255c6a9f70`, with
pyproj 3.7.1. No new container, task, source file or configuration was created.

The actual `parse_srs_header` function produced:

| Header | Result |
| --- | --- |
| `EPSG:6349` | Accepted: NAD83(2011) + NAVD88 height, vertical metres |
| `EPSG:6318+5703` | Accepted as the same compound CRS |
| `EPSG:6318+5703(GEOID18)` | Refused |
| `EPSG:6318+5703 GEOID18` | Refused |
| `EPSG:6319` | Accepted: NAD83(2011), three axes including ellipsoidal metres |

The [WebODM Lightning GCP guide](https://docs.webodm.net/how-to/ground-control-points/)
describes a parenthesized geoid suffix. That documentation alone does not prove
compatibility with this engine. The current
[upstream ODM parser](https://github.com/OpenDroneMap/ODM/blob/master/opendm/location.py)
and this installed parser instead feed the EPSG suffix to CRS parsing; the
installed runtime behavior above is the acceptance evidence.

Using only the synthetic coordinate longitude -88, latitude 44, height 100 m
and a generic Wisconsin area of interest, with PROJ network disabled:

| Transformation | Available non-ballpark operations | Reported accuracy | Actual ODM result height |
| --- | --- | --- | --- |
| NAVD88 compound `6349` to WGS84 3D `4979` | None; best operation unavailable | Unqualified | 100 m |
| NAD83 ellipsoidal `6319` to WGS84 3D `4979` | One, best available | 2 m | 100 m |
| NAD83 ellipsoidal `6319` to WGS84 UTM16N `32616` | One, best available | 2 m | 100 m |
| NAD83 ellipsoidal `6319` to native NAD83 UTM16N `6345` | One, best available | 0 m conversion accuracy | 100 m |

The unmodified probe environment had PROJ_NETWORK unset and network access
disabled in pyproj by default. No grid downloads were attempted. The NAVD88
probe's accepted header therefore does not prove its intended vertical
transformation: a successful API return with unchanged Z may be a fallback.
The native-frame 0 m figure describes a mathematical conversion, not field
measurement accuracy; the WGS84 operation's 2 m figure is not survey-grade proof.

Do not silently substitute retained ellipsoidal heights for NAVD88/GEOID18
heights or relabel output datums. Explicit height selection and immutable source
and output provenance are separate Viewer work. NAVD88/geoid readiness and
ellipsoidal readiness remain distinct; actual GCP-file conversion and desired
output semantics still require qualification. No-GCP photo jobs do not depend
on a geoid transformation. All coordinates above are synthetic, not client
measurements.
