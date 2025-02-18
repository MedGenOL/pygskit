import logging
from typing import List

import hail as hl
from pygskit.gskit.file_utils import validate_vcfs_paths, validate_vds_paths


def combine_gvcfs(gvcf_dir: str,
                  vds_output_path: str,
                  tmp_path: str,
                  save_path: str,
                  vdses: List[str],
                  kwargs: {}) -> None:
    """
    Combines GVCF files into a VDS using Hail's GVCF combiner.

    This function initializes Hail with the specified configuration, validates the input GVCF
    file paths using `get_vcfs_files`, and runs Hail's GVCF combiner to produce a variant
    dataset (VDS). It ensures that Hail is stopped properly even if an error occurs.

    Parameters:
        gvcf_dir (str): Directory containing GVCF files.
        vds_output_path (str): Path where the output VDS will be written.
        tmp_path (str): Temporary directory path used during processing.
        save_path (str): Path to save the combiner plan.
        vdses (List[str]): List of VDS paths to be combined.
        kwargs (dict): Additional keyword arguments to pass to the combiner.

    Returns:
        None

    Raises:
        FileNotFoundError, ValueError, PermissionError: If file validations fail.
    """
    try:
        if not (gvcf_dir or vdses):
            raise ValueError("Either GVCF files or VDS files must be provided.")

        validated_gvcfs: List[str] = []
        validated_vdses: List[str] = []

        if gvcf_dir:
            # Validate and retrieve GVCF file paths.
            validated_gvcfs = validate_vcfs_paths(gvcf_dir)
            logging.info(f"Validated {len(validated_gvcfs)} GVCF file paths.")

        if vdses:
            # Validate and retrieve VDS file paths.
            validated_vdses = validate_vds_paths(vdses)
            logging.info(f"Validated {len(validated_vdses)} VDS file paths.")

        # Set up and run the Hail GVCF combiner
        combiner = hl.vds.new_combiner(
            output_path=vds_output_path,
            temp_path=tmp_path,
            gvcf_paths=validated_gvcfs,
            save_path=save_path,
            vds_paths=validated_vdses,
            use_genome_default_intervals=True, # TODO: Use default intervals for the genome, but must be customized
            **kwargs,
        )
        combiner.run()
        logging.info("GVCF combination completed successfully.")

    finally:
        # Ensure Hail is stopped to release resources, regardless of success or error.
        hl.stop()
        logging.info("Hail stopped.")
