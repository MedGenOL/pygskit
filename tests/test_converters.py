import hail as hl

import pytest

import shutil
from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.converters import convert_vds_to_mt, convert_mt_to_multi_sample_vcf
from pygskit.gskit.file_utils import compress_files, decompress_files
from pathlib import Path

TESTS_DIR = Path(__file__).parent


@pytest.mark.order3
def test_convert_vds_to_mt():
    """
    Test the convert_vds_to_mt function by converting a VDS to a MatrixTable.

    This test initializes Hail in local mode, converts a VDS to a MatrixTable,
    and verifies the successful creation of the MatrixTable by checking for the
    existence of specific success files.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local()

    # Decompress the VDS zip file
    decompress_files(
        zip_path=str(TESTS_DIR / "testdata/vds/cohort.vds.zip"),
        extract_to=str(TESTS_DIR / "testdata/vds/cohort.vds"),
        remove_originals=False,
    )

    # Convert a VDS to a MatrixTable
    vds_path = TESTS_DIR / "testdata/vds/cohort.vds"
    mt_output_path = TESTS_DIR / "testdata/mts/cohort.mt"
    convert_vds_to_mt(
        vds_path=str(vds_path),
        output_path=str(mt_output_path),
        adjust_genotypes=True,
        convert_lgt_to_gt=True,
        skip_split_multi=False,
        skip_keying_by_cols=False,
        overwrite=True,
    )

    # Check if the mt_output_path / '_SUCCESS' file exists
    assert mt_output_path.joinpath("_SUCCESS").exists()

    # Compress the MatrixTable into a zip file and remove the original MatrixTable
    compress_files(
        source_dir=str(mt_output_path),
        output_zip=str(mt_output_path.with_suffix(".mt.zip")),
        remove_originals=True,
    )

    # Cleanup temporary files or directories
    # Both VDS and MatrixTable(s) contain numerous files, so we remove the entire directory after compressing
    if vds_path.exists():
        shutil.rmtree(vds_path)
    if mt_output_path.exists():
        shutil.rmtree(mt_output_path)

    hl.stop()

@pytest.mark.order4
def test_convert_mt_to_cvcf():
    """
    Test the convert_mt_to_multi_sample_vcf function by converting a MatrixTable to a multi-sample (cohort) VCF.

    This test initializes Hail in local mode, converts a MatrixTable to a multi-sample VCF,
    and verifies the successful creation of the VCF by checking for the existence of specific success files.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local()

    # Decompress the MatrixTable zip file
    decompress_files(
        zip_path=str(TESTS_DIR / "testdata/mts/cohort.mt.zip"),
        extract_to=str(TESTS_DIR / "testdata/mts/cohort.mt"),
        remove_originals=False,
    )

    # Convert a MatrixTable to a multi-sample VCF
    mt_path = TESTS_DIR / "testdata/mts/cohort.mt"
    vcf_output_path = TESTS_DIR / "testdata/vcf/cohort.vcf.bgz"
    convert_mt_to_multi_sample_vcf(
        mt_path=str(mt_path),
        vcf_path=str(vcf_output_path),
        filter_adj_genotypes=True,
        min_ac=1,
        split_multi=True
    )

    # Check if the vcf_output_path file exists
    assert vcf_output_path.exists()

    # Cleanup temporary files or directories
    if mt_path.exists():
        shutil.rmtree(mt_path)

    hl.stop()
