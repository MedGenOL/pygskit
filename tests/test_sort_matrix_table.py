import shutil
from pathlib import Path
import pytest

import hail as hl

from pygskit.gskit.file_utils import decompress_files
from pygskit.gskit.utils import init_hail_local, sort_mts_cols

TESTS_DIR = Path(__file__).parent

@pytest.mark.order7
def test_sort_mts():
    """
    Test the sort_mts_cols function by sorting the column order of a list of matrix tables.
    :return: None
    """
    # Decompress the VDS zip file
    decompress_files(
        zip_path=str(TESTS_DIR / "testdata/mts/cohort.mt.zip"),
        extract_to=str(TESTS_DIR / "testdata/mts/cohort.mt"),
        remove_originals=False,
    )

    # Initialize Hail in local mode
    init_hail_local()

    # Load the MatrixTables
    mt1_path = TESTS_DIR / "testdata/mts/cohort.mt"
    mt1 = hl.read_matrix_table(str(mt1_path))

    # Randomly shuffle column order of mt1
    import random
    idx = list(range(mt1.count_cols()))
    random.shuffle(idx)
    mt2 = mt1.choose_cols(idx)

    # Sort the column order of the MatrixTables
    mts = [mt1, mt2]
    sorted_mts = sort_mts_cols(mts)

    # Check that matrices can be joined
    mt = hl.MatrixTable.union_rows(*sorted_mts)

    assert mt.count_rows() == mt1.count_rows() + mt2.count_rows()
    assert mt.count_cols() == mt1.count_cols() == mt2.count_cols()

    # Cleanup temporary files or directories
    if mt1_path.exists():
        shutil.rmtree(mt1_path)

    hl.stop()
