import shutil

import hail as hl

from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.file_utils import compress_files, decompress_files
from pygskit.gskit.combiners import combine_gvcfs, combine_matrix_table_rows, combine_matrix_table_cols
from pygskit.gskit.constants import HG38_GENOME_REFERENCE
from pathlib import Path

TESTS_DIR = Path(__file__).parent


def test_combine_gvcfs():
    """
    Test the combine_gvcfs function by combining GVCFs into a VDS.

    This test initializes Hail in local mode, combines GVCFs from the specified directory
    into a Variant Dataset (VDS), and verifies the successful creation of the VDS by
    checking for the existence of specific success files.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local()

    # Combine GVCFs into a VDS
    gvcf_dir = TESTS_DIR / "testdata/gvcfs"
    vds_output_path = TESTS_DIR / "testdata/vds/cohort.vds"
    tmp_path = TESTS_DIR.parent / "local/tmp"
    plan_path = tmp_path / "combiner_plan.json"
    combine_gvcfs(
        gvcf_dir=str(gvcf_dir),
        vds_output_path=str(vds_output_path),
        tmp_path=str(tmp_path),
        save_path=str(plan_path),
        vdses=[],
        kwargs={},
    )

    # Check if the vds_output_path / 'reference_data/_SUCCESS' file exists
    assert vds_output_path.joinpath("reference_data/_SUCCESS").exists()
    # Check if the vds_output_path / 'variant_data/_SUCCESS' file exists
    assert vds_output_path.joinpath("variant_data/_SUCCESS").exists()

    # Compress the VDS into a zip file and remove the original VDS
    compress_files(
        source_dir=str(vds_output_path),
        output_zip=str(vds_output_path.with_suffix(".vds.zip")),
        remove_originals=True,
    )

    hl.stop()



def test_combine_matrix_table_rows():
    """
    Test the combine_matrix_table_rows function by combining rows of two MatrixTables.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local()

    # decompress the MatrixTable zip file
    decompress_files(
        zip_path=str(TESTS_DIR / "testdata/mts/cohort.mt.zip"),
        extract_to=str(TESTS_DIR / "testdata/mts/test.mt"),
        remove_originals=False,
    )

    # Combine rows of two MatrixTables
    mt1_path = TESTS_DIR / "testdata/mts/test.mt"
    mt2_path = mt1_path
    mt_output_path = TESTS_DIR / "testdata/mts/combined.mt"
    combine_matrix_table_rows(mt_paths=[str(mt1_path), str(mt2_path)],
                              output_path=str(mt_output_path),
                              n_partitions=20,
                              overwrite=True)

    # Check if the mt_output_path / '_SUCCESS' file exists
    assert mt_output_path.joinpath("_SUCCESS").exists()

    # Check if the number of rows in the output MatrixTable is
    # equal to the sum of the number of rows in the input MatrixTables
    assert hl.read_matrix_table(str(mt_output_path)).count_rows() == 2*hl.read_matrix_table(str(mt1_path)).count_rows()

    # cleanup temporary files or directories
    shutil.rmtree(mt1_path)
    shutil.rmtree(mt_output_path)

    hl.stop()


def test_combine_matrix_table_cols():
    """
    Test the combine_matrix_table_cols function by combining columns of two MatrixTables.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local()

    # decompress the MatrixTable zip file
    decompress_files(
        zip_path=str(TESTS_DIR / "testdata/mts/cohort.mt.zip"),
        extract_to=str(TESTS_DIR / "testdata/mts/test.mt"),
        remove_originals=False,
    )

    # Combine columns of two MatrixTables
    mt1_path = TESTS_DIR / "testdata/mts/test.mt"
    mt2_path = mt1_path
    mt_output_path = TESTS_DIR / "testdata/mts/combined.mt"
    combine_matrix_table_cols(mt_paths=[str(mt1_path), str(mt2_path)],
                              output_path=str(mt_output_path),
                              n_partitions=20,
                              overwrite=True)

    # Check if the mt_output_path / '_SUCCESS' file exists
    assert mt_output_path.joinpath("_SUCCESS").exists()

    # Check if the number of columns in the output MatrixTable is
    # equal to the sum of the number of columns in the input MatrixTables
    assert hl.read_matrix_table(str(mt_output_path)).count_cols() == 2*hl.read_matrix_table(str(mt1_path)).count_cols()

    # cleanup temporary files or directories
    shutil.rmtree(mt1_path)
    shutil.rmtree(mt_output_path)

    hl.stop()
