import glob
import os
import shutil
import zipfile
import logging

from typing import List

from pygskit.gskit.constants import GVCF_EXTENSION, GVCF_EXTENSION_TBI

"""
Utility functions for file handling.
"""

# Configure logging. Adjust the level as needed.
logging.basicConfig(level=logging.INFO)

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


def compress_files(source_dir: str, output_zip: str, remove_originals: bool = False) -> None:
    """
    Compresses all files in source_dir (including subdirectories) into a single ZIP archive.

    :param source_dir: The directory containing files to compress.
    :param output_zip: The path to the output ZIP file.
    :param remove_originals: If True, remove the source directory after compression.
    """
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(source_dir):
            for file in files:
                file_path = os.path.join(root, file)
                # Use relative path in the archive to preserve folder structure.
                arcname = os.path.relpath(file_path, source_dir)
                zipf.write(file_path, arcname)
    logging.info(f"Compressed '{source_dir}' into '{output_zip}'")

    if remove_originals:
        shutil.rmtree(source_dir)
        logging.info(f"Removed original directory '{source_dir}' after compression.")


def decompress_files(zip_path: str, extract_to: str, remove_originals: bool = False) -> None:
    """
    Decompresses a ZIP archive into the specified directory.

    :param zip_path: The path to the ZIP archive.
    :param extract_to: The directory to extract the archive contents to.
    :param remove_originals: If True, remove the ZIP archive after extraction.
    """
    with zipfile.ZipFile(zip_path, 'r') as zipf:
        zipf.extractall(extract_to)
    logging.info(f"Extracted '{zip_path}' into '{extract_to}'")

    if remove_originals:
        os.remove(zip_path)
        logging.info(f"Removed archive file '{zip_path}' after decompression.")