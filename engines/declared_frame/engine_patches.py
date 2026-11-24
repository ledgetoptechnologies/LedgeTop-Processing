"""Hook insertions for hash-verified copies of the pinned engine's source.

The engine is AGPL-3.0-or-later; check the OpenSfM fork separately. Neither is stored: each
patch is a few anchored lines, applied only to bytes with a pinned hash and checked
against a pinned result. Legacy headers take the same path as before.
"""
import hashlib

IMAGE = 'sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd'
IMPORT = 'from declared_frame import engine_hooks'

# "installed": the hash was read from the pinned image. "retained": the hash is of a
# saved copy; a different installed file is refused, never patched.
PATCHES = {
    'modules/gcp.py': {
        'basis': 'installed',
        'original': '337e0df0a7da404d2dffcd816cbb1dbe28733c248a05cbf9ed10f8f278992583',
        'patched': '532fc0992bc8d1c5d7a9839e59e4d0f5e76900c59916a1c506ded975944f01f5',
        'edits': [
            ("from modules import location\n",
             "from modules import location\nfrom declared_frame import engine_hooks\n"),
            ("        self.srs = None\n        self.read()\n",
             "        self.srs = None\n        self.survey = None\n        self.read()\n"),
            ("            if lines:\n                self.raw_srs = lines[0] # SRS\n",
             "            if lines:\n"
             "                # LedgeTop 2026-11-23: declared frame, opt-in only.\n"
             "                if engine_hooks.is_marker(lines[0]):\n"
             "                    self.survey = engine_hooks.open_survey(self.gcp_path)\n"
             "                    self.raw_srs = lines[0]\n"
             "                    self.entries = self.survey.entries()\n"
             "                    return\n"
             "                self.raw_srs = lines[0] # SRS\n"),
            ('        output = [self.raw_srs]\n\n        for entry in self.iter_entries():\n'
             '            entry.px *= ratio\n',
             "        if self.survey is not None:\n"
             "            engine_hooks.unported('resized control copy')\n"
             '        output = [self.raw_srs]\n\n        for entry in self.iter_entries():\n'
             '            entry.px *= ratio\n'),
            ("        if self.entries_count() > 0:\n            entry = self.get_entry(0)\n",
             "        if self.survey is not None:\n"
             "            engine_hooks.unported('UTM zone from control')\n"
             "        if self.entries_count() > 0:\n            entry = self.get_entry(0)\n"),
            ("        if os.path.exists(gcp_file_output):\n            os.remove(gcp_file_output)\n"
             "\n        output = [self.wgs84_utm_zone()]\n",
             "        if self.survey is not None:\n"
             "            engine_hooks.unported('UTM control copy')\n"
             "        if os.path.exists(gcp_file_output):\n            os.remove(gcp_file_output)\n"
             "\n        output = [self.wgs84_utm_zone()]\n"),
            ("        if not self.exists() or not os.path.exists(images_dir):\n",
             "        if self.survey is not None:\n"
             "            engine_hooks.unported('filtered control copy')\n"
             "        if not self.exists() or not os.path.exists(images_dir):\n"),
            ("        if os.path.exists(output_json):\n            os.remove(output_json)\n"
             "\n        transformer = ",
             "        if os.path.exists(output_json):\n            os.remove(output_json)\n"
             "\n        if self.survey is not None:\n"
             "            return self.survey.export(output_json, photos)\n"
             "\n        transformer = "),
        ],
    },
    'modules/location.py': {
        'basis': 'installed',
        'original': 'd366ef27f819c0a9b8df4129500db0ff6e0838dbf1634a35466190255c6a9f70',
        'patched': 'a11ea7559f2bef9d5263c63d9b9daf59e6d2590282bf47d9fe76315e7e8f0ba5',
        'edits': [
            ("from osgeo import osr\n",
             "from osgeo import osr\nfrom declared_frame import engine_hooks\n"),
            ("    res = osr.SpatialReference()\n    epsg = srs.to_epsg()\n",
             "    engine_hooks.refuse_declared_crs(srs)\n"
             "    res = osr.SpatialReference()\n    epsg = srs.to_epsg()\n"),
        ],
    },
    'modules/types.py': {
        'basis': 'retained',
        'original': '6d6d8e768e6d7a16ecff6e83ccd0bd9001ce10c7d05f7a72860206ec3faeafcd',
        'patched': '425274319241cc37731275b1364a9ec6d736b1baa3fb2125518f07987a1986db',
        'edits': [
            ("from modules.gcp import GCPFile\n",
             "from modules.gcp import GCPFile\nfrom declared_frame import engine_hooks\n"),
            ("                gcp.check_entries()\n\n",
             "                gcp.check_entries()\n\n"
             "                # LedgeTop 2026-11-23: keep the declared frame; no UTM copy.\n"
             "                if gcp.survey is not None:\n"
             "                    self.gcp = gcp\n"
             "                    self.georef = gcp.survey.georeference(self.photos, output_coords_file)\n"
             "                    return self.georef\n\n"),
            ("            line = f.readline().rstrip()\n            srs = location.parse_srs_header(line)\n",
             "            line = f.readline().rstrip()\n"
             "            if engine_hooks.is_marker(line):\n"
             "                return engine_hooks.georef_from_coords(coords_file)\n"
             "            srs = location.parse_srs_header(line)\n"),
        ],
    },
    'stages/odm_georeferencing.py': {
        'basis': 'retained',
        'original': 'e46b9b8d1691557b18c308e37cabfd2a2c56256065737a1a72698fcbe4069cd9',
        'patched': 'c72b0a0ce1c09b928bdae39fb2981e8025a4222d6138635f9d5ab23b1748b158',
        'edits': [
            ("from modules import location\n",
             "from modules import location\nfrom declared_frame import engine_hooks\n"),
            ("        reconstruction = outputs['reconstruction']\n\n"
             "        # Export GCP information if available\n",
             "        reconstruction = outputs['reconstruction']\n\n"
             "        # LedgeTop 2026-11-23: exact cloud map in the declared frame, nothing else.\n"
             "        if getattr(reconstruction.georef, 'declared_frame', False):\n"
             "            if args.crop > 0 or 'boundary' in outputs or tree.odm_align_file is not None:\n"
             "                engine_hooks.unported('crop, boundary or alignment')\n"
             "            engine_hooks.georeference_cloud(\n"
             "                tree.filtered_point_cloud, tree.odm_georeferencing_model_laz,\n"
             "                reconstruction.georef, tree.opensfm, run=_declared_run)\n"
             "            return\n\n"
             "        # Export GCP information if available\n"),
            ("class ODMGeoreferencingStage(types.ODM_Stage):\n",
             "def _declared_run(command, text):\n"
             "    import subprocess\n"
             "    return subprocess.run(command, input=text, capture_output=True, text=True,\n"
             "                          check=True).stdout\n\n\n"
             "class ODMGeoreferencingStage(types.ODM_Stage):\n"),
        ],
    },
    'opensfm/dataset.py': {
        'basis': 'installed',
        'original': '142d860ed8d059a3b5785060dd48f5b1f4ac103af58e04937e870ce69880ea05',
        'patched': 'a4fd2563e26a486f5494d45b2185a8de16b1c77a70e3315c26d2b6a1d14da1de',
        'edits': [
            ("from opensfm.dataset_base import DataSetBase\n",
             "from opensfm.dataset_base import DataSetBase\n"
             "from declared_frame import engine_hooks\n"),
            ('            "altitude": reference.alt,\n        }\n',
             '            "altitude": reference.alt,\n        }\n'
             '        if engine_hooks.is_declared(self):\n'
             '            reference_lla["declared_frame"] = engine_hooks.reference_frame(self)\n'),
            ("            lla = io.json_load(fin)\n\n        return geo.TopocentricConverter(\n",
             "            lla = io.json_load(fin)\n        engine_hooks.check_reference(self, lla)\n"
             "\n        return geo.TopocentricConverter(\n"),
            ('        """Load ground control points."""\n',
             '        """Load ground control points."""\n'
             '        if engine_hooks.is_declared(self):\n'
             '            return engine_hooks.load_points(self, pymap)\n'),
            ('        """Return the CRS string from ground_control_points.json or gcp_list.txt."""\n',
             '        """Return the CRS string from ground_control_points.json or gcp_list.txt."""\n'
             '        if engine_hooks.is_declared(self):\n'
             "            engine_hooks.unported('control CRS as a string')\n"),
        ],
    },
    'opensfm/actions/export_geocoords.py': {
        'basis': 'retained',
        'original': 'cfc70469ab7e3d4c40ecfcf11e9e0c63fbb780bada2d54d7238568ccac024c1f',
        'patched': '2e63aaf6df260d115c44953f35db21055e66f08a26e86fc9e2a90e277ac785cf',
        'edits': [
            ("from opensfm.dataset import DataSet, UndistortedDataSet\n",
             "from opensfm.dataset import DataSet, UndistortedDataSet\n"
             "from declared_frame import engine_hooks\n"),
            ("    projection = Proj.from_crs(CRS.from_epsg(4979), CRS.from_proj4(proj), always_xy=True).transform\n"
             "    t = geo.get_proj_transform_matrix(reference, projection, offset)\n",
             "    declared = engine_hooks.is_declared(data)\n"
             "    if declared:\n"
             "        projection = engine_hooks.declared_projection(data, proj)\n"
             "    else:\n"
             "        projection = Proj.from_crs(CRS.from_epsg(4979), CRS.from_proj4(proj), always_xy=True).transform\n"
             "    t = geo.get_proj_transform_matrix(reference, projection, offset)\n"
             "    if declared:\n"
             "        engine_hooks.record_affine(data, reference, t, offset)\n"),
        ],
    },
}


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def apply(name, original, *, check_result=True):
    patch = PATCHES[name]
    if sha256(original) != patch['original']:
        raise ValueError(f'engine_source_not_pinned:{name}')
    text = original.decode('utf-8')
    for anchor, replacement in patch['edits']:
        if text.count(anchor) != 1:
            raise ValueError(f'engine_anchor_mismatch:{name}')
        text = text.replace(anchor, replacement)
    patched = text.encode('utf-8')
    if check_result and sha256(patched) != patch['patched']:
        raise ValueError(f'engine_patched_digest_mismatch:{name}')
    return patched
