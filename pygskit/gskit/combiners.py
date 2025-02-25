import os
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
    dataset (VDS). The resulting VDS is written to the specified output path.

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

    except Exception as e:
        logging.exception("An error occurred during GVCF combination.")
        raise

    finally:
        logging.info("GVCF combination completed successfully.")


def combine_vdses(vdses_dir: str,
                  output_path: str,
                  validate: bool = True,
                  overwrite: bool = False) -> None:
    """
    Combine multiple VDS directories into a single VDS.

    This function takes a container directory that holds VDS directories (each VDS is
    represented as a directory with a specific extension), check every path is valid, combines their
    rows using Hail's union_rows, optionally validates the resulting VDS, and writes the result
    to the specified output path.

    Parameters:
        vdses_dir (str): Container directory containing VDS directories.
        output_path (str): Path where the combined VDS will be written.
        validate (bool): Whether to validate the combined VDS using Hail's validator.
        overwrite (bool): Whether to overwrite the output if it already exists.

    Raises:
        ValueError: If no valid VDS directories are found.
        Exception: If an error occurs during VDS combination or writing.
    """
    try:
        # Ensure a container directory is provided and valid.
        if not vdses_dir:
            raise ValueError("No VDS directory provided.")
        if not os.path.isdir(vdses_dir):
            raise NotADirectoryError(f"Provided path '{vdses_dir}' is not a directory.")

        # Validate VDS directories within the container.
        vdses_paths: List[str] = validate_vds_paths(vdses_dir)
        if not vdses_paths:
            raise ValueError(f"No valid VDS directories found in '{vdses_dir}'.")
        logging.info(f"Validated {len(vdses_paths)} VDS directories.")

        # Combine the VDS directories using Hail's union_rows function.
        vdses = [(hl.vds.read_vds(path)) for path in vdses_paths]
        combined_vds = hl.vds.VariantDataset.union_rows(*vdses)
        logging.info("Successfully combined VDS directories.")

        # Optionally validate the combined VDS.
        if validate:
            hl.vds.VariantDataset.validate(combined_vds)
            logging.info("Combined VDS validated successfully.")

        # Write the combined VDS to the output path.
        combined_vds.write(output_path, overwrite=overwrite)

    except Exception as e:
        logging.exception("An error occurred during VDS combination.")
        raise

    finally:
        logging.info(f"Combined VDS written to {output_path}")

