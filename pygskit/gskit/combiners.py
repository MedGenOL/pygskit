import logging
from typing import List

import hail as hl
from pygskit.gskit.file_utils import get_vcfs_files


def combine_gvcfs(gvcf_dir: str,
                  vds_output_path: str,
                  tmp_path: str) -> None:
    """
    Combines GVCF files into a VDS using Hail's GVCF combiner.

    This function initializes Hail with the specified configuration, validates the input GVCF
    file paths using `get_vcfs_files`, and runs Hail's GVCF combiner to produce a variant
    dataset (VDS). It ensures that Hail is stopped properly even if an error occurs.

    Parameters:
        gvcf_dir (str): Directory containing GVCF files.
        vds_output_path (str): Path where the output VDS will be written.
        tmp_path (str): Temporary directory path used during processing.

    Returns:
        None

    Raises:
        FileNotFoundError, ValueError, PermissionError: If file validations fail.
    """
    try:
        # Validate and retrieve GVCF file paths
        gvcfs: List[str] = get_vcfs_files(gvcf_dir)
        logging.info(f"Validated {len(gvcfs)} GVCF file paths.")

        # Set up and run the Hail GVCF combiner
        combiner = hl.vds.new_combiner(
            output_path=vds_output_path,
            temp_path=tmp_path,
            gvcf_paths=gvcfs,
            use_genome_default_intervals=True,
            gvcf_reference_entry_fields_to_keep=[],
        )
        combiner.run()
        logging.info("GVCF combination completed successfully.")

    finally:
        # Ensure Hail is stopped to release resources, regardless of success or error.
        hl.stop()
        logging.info("Hail stopped.")