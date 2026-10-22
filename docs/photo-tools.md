# Aligned photo processing

These local commands package the experimental photo tools. They consume an
existing OpenSfM alignment from ODM; they do not align the photographs.
Keep source photos, reconstruction files and resulting imagery in private storage.

The reconstruction must use supported Brown camera intrinsics and shot poses.
The run's `dtm.tif` and `chm_raw.tif` must share a projected, metre-based CRS,
shape and north-up transform. Their sum is the surface height. **Surface Z and
the OpenSfM reference altitude must both be ellipsoidal metres.** These commands
do not infer or convert a vertical datum. Confirm that condition before use.

## Original-photo crown views

```powershell
treedar raw-chips C:\TreeDar\Runs\example `
  --opensfm C:\TreeDar\Alignment\opensfm `
  --images C:\TreeDar\Photos --out C:\TreeDar\Runs\example\raw-views `
  --tree-ids C:\TreeDar\label-ids.json --views 3
```

The optional IDs file contains a JSON list, such as `[1, 7, 12]`. Omitting it
processes every positive crown ID. `crowns.tif` must match the surface grid.
Each view uses one original photo, chosen by angle from overhead among cameras
within `--search-radius` metres whose frame contains the crown. The output is
`tree_<id>_v<rank>.jpg` plus `views.json` with photo names and off-nadir angles.
Crowns without a suitable view have no output entry. Use a fresh output folder
for each run so older JPEGs cannot be mistaken for current views.

The default edge margin is 40 pixels. Adjust `--edge-margin` for smaller images.
Chips draw a crown outline; they are useful for human labelling without mosaic
seams. Camera calibration, pose and height errors can still shift that outline.
These JPEG views use a different naming scheme from `treedar chips` and are not
automatically connected to the existing labelpage or Jev commands.

## Surface-projected overview

```powershell
treedar photo-ortho C:\TreeDar\Runs\example `
  --opensfm C:\TreeDar\Alignment\opensfm `
  --images C:\TreeDar\Photos --out C:\TreeDar\Runs\example\photo-ortho.tif `
  --cell 0.1
```

`--bbox minx,miny,maxx,maxy` limits the area in the surface CRS. The renderer
tries up to `--candidates` nearest cameras (default 6), falling back when a
photo does not cover a pixel. `--edge-margin` defaults to 20 pixels. Rendering
uses tiles and a bounded photo cache; very small output cells still increase
runtime and disk space substantially. Try a small area first.

The output is a tiled RGB GeoTIFF with a coverage mask. A valid black photo
pixel remains valid; uncovered pixels are masked. The printed JSON reports
coverage. Source surface gaps remain gaps. The output is an experimental
overview: nearest-camera selection does not solve occlusion, mosaic seams,
brightness differences or crown parallax, and provides no accuracy guarantee.
Use original-photo views for crown labelling.
