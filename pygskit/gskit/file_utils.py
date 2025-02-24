import glob
import os
import shutil
import zipfile
import logging

from typing import Union, List

from pygskit.gskit.constants import GVCF_EXTENSION, GVCF_EXTENSION_TBI, VDS_EXTENSION

"""
Utility functions for file handling.
"""

# Configure logging. Adjust the level as needed.
logging.basicConfig(level=logging.INFO)


def check_path_exists_and_readable(path: str) -> str:
    """
    Check if a file or directory exists and is readable.

    Parameters:
        path (str): Path to the file or directory (absolute or relative).

    Returns:
        str: The original path if it exists and is readable.

    Raises:
        FileNotFoundError: If the file or directory does not exist or the path does not point to a valid file or directory.
        PermissionError: If the file or directory exists but is not readable.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File or directory '{path}' not found.")
    if not (os.path.isfile(path) or os.path.isdir(path)):
        raise FileNotFoundError(f"Path '{path}' is not a valid file or directory.")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"File or directory '{path}' is not readable.")

    return path


def validate_vcfs_paths(directory: str, pattern: str = None) -> List[str]:
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
        check_path_exists_and_readable(abs_path)

        # Compute the corresponding TBI file path by replacing the GVCF extension with the TBI extension.
        base = abs_path[: -len(GVCF_EXTENSION)]
        tbi_path = base + GVCF_EXTENSION_TBI

        # Validate that the TBI file exists and is readable.
        check_path_exists_and_readable(tbi_path)

        valid_paths.append(abs_path)

    return valid_paths


def validate_vds_paths(vdses: Union[str, List[str]]) -> List[str]:
    """
    Validate VDS directories given either as a container directory or a list of VDS paths.

    If a directory is provided, the function checks that it exists and is readable, then iterates
    over all entries in the container. For each entry that is a directory, it verifies the directory
    is accessible and its name ends with the expected VDS_EXTENSION.

    If a list of paths is provided, each path is validated individually in a similar manner.

    Parameters:
        vdses (Union[str, List[str]]): Either a single directory path that contains VDS directories,
                                       or a list of individual VDS directory paths.

    Returns:
        List[str]: List of validated VDS directory paths.

    Raises:
        NotADirectoryError: If a provided path expected to be a directory is not one.
        FileNotFoundError or PermissionError: If a directory does not exist or is not accessible.
        ValueError: If a directory does not end with the expected VDS_EXTENSION.
    """
    validated_paths = []

    if isinstance(vdses, str):
        # vdses is a container directory
        if not os.path.isdir(vdses):
            raise NotADirectoryError(f"'{vdses}' is not a directory.")
        for entry in os.listdir(vdses):
            full_path = os.path.join(vdses, entry)
            if os.path.isdir(full_path):
                check_path_exists_and_readable(full_path)
                if not entry.endswith(VDS_EXTENSION):
                    raise ValueError(f"Directory '{full_path}' does not end with '{VDS_EXTENSION}'.")
                validated_paths.append(full_path)
    elif isinstance(vdses, list):
        # vdses is a list of directory paths
        for path in vdses:
            if not os.path.isdir(path):
                raise NotADirectoryError(f"'{path}' is not a directory.")
            check_path_exists_and_readable(path)
            base_name = os.path.basename(path)
            if not base_name.endswith(VDS_EXTENSION):
                raise ValueError(f"Directory '{path}' does not end with '{VDS_EXTENSION}'.")
            validated_paths.append(path)
    else:
        raise ValueError("Input must be either a directory path (str) or a list of directory paths.")

    return validated_paths




def compress_files(source_dir: str, output_zip: str, remove_originals: bool = False) -> None:
    """
    Compresses all files in source_dir (including subdirectories) into a single ZIP archive.

    :param source_dir: The directory containing files to compress.
    :param output_zip: The path to the output ZIP file.
    :param remove_originals: If True, remove the source directory after compression.
    """
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
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
    with zipfile.ZipFile(zip_path, "r") as zipf:
        zipf.extractall(extract_to)
    logging.info(f"Extracted '{zip_path}' into '{extract_to}'")

    if remove_originals:
        os.remove(zip_path)
        logging.info(f"Removed archive file '{zip_path}' after decompression.")
