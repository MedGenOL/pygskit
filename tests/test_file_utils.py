import logging
from pathlib import Path

from pygskit.gskit.file_utils import validate_vcfs_paths
from pygskit.gskit.constants import GVCF_EXTENSION

TESTS_DIR = Path(__file__).parent


def test_get_vcfs_files():
    """
    Test the get_vcfs_files function by retrieving GVCF files from a directory.
    :return: None
    """
    directory = TESTS_DIR / "testdata/gvcfs"
    vcfs = validate_vcfs_paths(str(directory), pattern=f"*{GVCF_EXTENSION}")
    logging.info(vcfs)
    assert len(vcfs) == 3
