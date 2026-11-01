"""Local TreeDar publication preview; no transport, authentication or custody.

Only the existing publisher calls this with freshly requalified native evidence
and a rechecked published ZIP. A matching hash is not producer authentication.
"""
import hashlib
import re

from . import reference_protocol as p, reference_treedar_bundle as b

FORMAT = 'ltds-treedar-outer-preview-v1'


def make_preview(source, admission, receipt, config, limits):
    """Derive one closed preview from already-qualified local publication data."""
    if type(source) is not dict or type(admission) is not dict or type(receipt) is not dict:
        raise b.BundleHeld('qualified_local_source_required')
    p._closed(source,('evidenceSha256','proofSha256','proofBytes','controlBytes','controlSha256','rawRoot','manifest'),'native source')
    p._closed(admission,('admissionVersion','jobId','generation','environment','objectId','objectSha256',
        'byteSize','sourceKind','provenanceReceiptId','genuineAcquisition','pointCount','groundPointCount',
        'groundCoverage','crs','crsWktSha256','pointCountAssurance','verticalUnits','verticalDatum',
        'verticalDatumAssurance','rasterGridCells'),'native admission')
    p._closed(receipt,('state','archive','manifestSha256','binding','engineVersion','nativeFiles',
        'sourceStageProofSha256'),'local publication receipt')
    manifest=source['manifest']
    expected={'binding':manifest['binding'],'engineVersion':manifest['engineVersion']}
    manifest=b.validate_manifest(manifest,expected,limits)
    binding=manifest['binding']; lidar=binding['sourceLidar']
    if (receipt.get('state')!='local_native_bundle_published' or receipt.get('binding')!=binding
            or receipt.get('manifestSha256')!=manifest['manifestSha256']
            or receipt.get('nativeFiles')!=manifest['files']
            or receipt.get('engineVersion')!=manifest['engineVersion']):
        raise b.BundleHeld('local_publication_identity_changed')
    expected_admission={'jobId':binding['jobId'],'generation':binding['generation'],
        'environment':binding['environment'],'objectId':lidar['objectId'],
        'objectSha256':lidar['sha256'],'byteSize':lidar['byteSize'],
        'provenanceReceiptId':lidar['provenanceReceiptId'],'sourceKind':'lidar',
        'admissionVersion':p.TREEDAR_ADMISSION_VERSION,'genuineAcquisition':True,'verticalUnits':'metre',
        'verticalDatumAssurance':'authenticated_declaration_no_conversion'}
    if any(type(admission.get(key)) is not type(value) or admission[key]!=value
           for key,value in expected_admission.items()):
        raise b.BundleHeld('source_admission_binding_changed')
    if (type(admission.get('crs')) is not str or not 1<=len(admission['crs'])<=80
            or type(admission.get('verticalDatum')) is not str or not admission['verticalDatum']):
        raise b.BundleHeld('source_spatial_evidence_missing')
    p._hash(admission['crsWktSha256'],'CRS WKT digest')
    p._hash(source['proofSha256'],'native proof digest')
    p._integer(source['proofBytes'],'native proof size')
    p._hash(receipt['sourceStageProofSha256'],'stage proof digest')
    p._hash(config.source_sha256,'bootstrap source digest')
    if (not re.fullmatch(r'sha256:[a-f0-9]{64}',config.image_id)
            or config.engine_version!=manifest['engineVersion']):
        raise b.BundleHeld('engine_identity_changed')
    archive=receipt['archive']
    p._closed(archive,('byteSize','sha256'),'published archive')
    p._integer(archive['byteSize'],'archive size');p._hash(archive['sha256'],'archive digest')
    if archive['byteSize']>limits.maxArchiveBytes:
        raise b.BundleHeld('archive_budget_changed')
    inner=p.canonical_json(manifest)
    return {
        'schemaVersion':1,'format':FORMAT,'state':'preview_not_custody',
        'binding':binding,'archive':archive,
        'inner':{'format':b.FORMAT,'zipEntryByteSize':len(inner),'zipEntrySha256':hashlib.sha256(inner).hexdigest(),
                 'manifestSha256WithoutDigestField':manifest['manifestSha256']},
        'engine':{'name':'treedar','version':config.engine_version,'localDockerImageId':config.image_id,
                  'imageIdentityScope':'observed_local_container_config_not_authenticated_producer',
                  'bootstrapSourceSha256':config.source_sha256},
        'sourceEvidence':{'nativeProofByteSize':source['proofBytes'],'nativeProofSha256':source['proofSha256'],
                          'sourceStageProofSha256':receipt['sourceStageProofSha256'],'admission':admission},
        'nativeFiles':manifest['files'],
        'spatial':{'crsLabel':admission['crs'],'crsWktSha256':admission['crsWktSha256'],
            'wkt':None,'definitionStatus':'digest_only','horizontalUnits':'metre',
            'horizontalAssurance':'producer_reported_projected_las_crs_axes',
            'verticalUnits':'metre','verticalDatum':admission['verticalDatum'],
            'verticalAssurance':{'value':'authenticated_declaration_no_conversion',
                'scope':'local_provenance_verifier_not_remote_transport_authentication'},
            'sharedRasterGeometry':{'files':['dtm.tif','chm.tif','chm_raw.tif','crowns.tif'],
                'scope':'pinned_bootstrap_execution_evidence_only',
                'method':'same_shape_affine_crs_and_finite_block_windows'}},
        'mixedCompletion':'unqualified','transportRole':'unassigned'}


def validate_preview(preview,source,admission,receipt,config,limits):
    """Strict comparison to newly derived local data; no remote trust assertion."""
    expected=make_preview(source,admission,receipt,config,limits)
    if type(preview) is not dict or p.canonical_json(preview)!=p.canonical_json(expected):
        raise b.BundleHeld('outer_preview_changed')
    return expected
