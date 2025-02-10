import glob
import os
from typing import List

from pygskit.gskit.constants import GVCF_EXTENSION, GVCF_EXTENSION_TBI

"""
Utility functions for file handling.
"""


def check_file_exists_and_readable(path: str) -> str:
    """
    Check if a file exists, is a regular file, and is readable.

    Parameters:
        path (str): Path to the file (absolute or relative).

    Returns:
        str: The original file path if it exists and is readable.

    Raises:
        FileNotFoundError: If the file does not exist or the path does not point to a file.
        PermissionError: If the file exists but is not readable.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File '{path}' not found.")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Path '{path}' is not a file.")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"File '{path}' is not readable.")

    return path


def get_vcfs_files(directory: str, pattern: str = None) -> List[str]:
    """
    Retrieve and validate a list of absolute paths to GVCF files in the given directory.

    This function performs the following:
      - Checks that the provided directory exists.
      - Uses glob to find files in the directory matching the given pattern.
      - For each discovered GVCF file:
          * Converts it to an absolute path.
          * Validates that it ends with the expected GVCF extension.
          * Checks that the GVCF file exists and is readable.
          * Checks that the corresponding TBI file (with the expected TBI extension)
            exists and is readable.

    Parameters:
        directory (str): The directory in which to search for GVCF files.
        pattern (str): The glob pattern to match files. If None, defaults to "*{GVCF_EXTENSION}".

    Returns:
        List[str]: A list of validated absolute paths to GVCF files.

    Raises:
        NotADirectoryError: If the provided directory does not exist.
        ValueError: If a file does not have the expected GVCF extension.
        FileNotFoundError or PermissionError: Propagated from the file existence/readability checks.
    """
    if pattern is None:
        pattern = f"*{GVCF_EXTENSION}"

    if not os.path.isdir(directory):
        raise NotADirectoryError(f"'{directory}' is not a valid directory.")

    search_pattern = os.path.join(directory, pattern)
    matching_files = glob.glob(search_pattern)

    valid_paths = []
    for filepath in matching_files:
        abs_path = os.path.abspath(filepath)

        # Validate the file extension using the GVCF_EXTENSION variable.
        if not abs_path.endswith(GVCF_EXTENSION):
            raise ValueError(f"File '{abs_path}' does not end with '{GVCF_EXTENSION}'.")

        # Validate that the GVCF file exists and is readable.
        check_file_exists_and_readable(abs_path)

        # Compute the corresponding TBI file path by replacing the GVCF extension with the TBI extension.
        base = abs_path[:-len(GVCF_EXTENSION)]
        tbi_path = base + GVCF_EXTENSION_TBI

        # Validate that the TBI file exists and is readable.
        check_file_exists_and_readable(tbi_path)

        valid_paths.append(abs_path)

    return valid_paths
