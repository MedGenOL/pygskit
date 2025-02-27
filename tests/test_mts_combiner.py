import shutil
from pathlib import Path

import pytest

import hail as hl

from pygskit.gskit.combiners import combine_matrix_table_rows, combine_matrix_table_cols
from pygskit.gskit.file_utils import decompress_files
from pygskit.gskit.utils import init_hail_local

TESTS_DIR = Path(__file__).parent

@pytest.mark.order5
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

@pytest.mark.order6
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
