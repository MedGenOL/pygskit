#!/usr/bin/env python
"""
This script combines GVCFs into a VDS using Hail's gvcf combiner.

Usage:
    python gvcf_combiner.py --path-to-gvcfs <path> \
                            --vds-output-path <path> \
                            --tmp-path <path> \
                            --driver-memory <str> \
                            --local <str> \
                            --reference-genome <str>
"""

import logging
import sys
from typing import NoReturn

import click
import hail as hl
from pygskit.gskit.combiners import combine_gvcfs  # The actual combining function.
from pygskit.gskit.utils import init_hail_local  # Initializes Hail with the desired configuration.

# Set up logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_gvcf_combiner(
    path_to_gvcfs: str,
    vds_output_path: str,
    tmp_path: str,
    driver_memory: str,
    n_cpus: int,
    reference_genome: str,
) -> NoReturn:
    """
    Combines GVCFs into a VDS using Hail's gvcf combiner.

    Parameters:
        path_to_gvcfs (str): Path to the file listing GVCF paths.
        vds_output_path (str): Output path for the VDS file.
        tmp_path (str): Temporary directory path with sufficient space.
        n_cpus: Number of CPUs to use for local computation.
        driver_memory (str): Memory allocation for the Spark driver.
        reference_genome (str): Reference genome to use (e.g., GRCh37, GRCh38).
    """
    try:
        init_hail_local(n_cpus, driver_memory, reference_genome)
        logger.info("Combining GVCFs from '%s' into VDS '%s'", path_to_gvcfs, vds_output_path)
        combine_gvcfs(path_to_gvcfs, vds_output_path, tmp_path)
        logger.info("Successfully combined GVCFs into VDS")
    except Exception as e:
        logger.error("An error occurred during GVCF combination: %s", str(e), exc_info=True)
        sys.exit(1)
    finally:
        logger.info("Stopping Hail")
        hl.stop()


@click.command("gvcf_combiner", help="Combine GVCFs into a VDS using Hail's gvcf combiner.")
@click.option(
    "-d",
    "--path-to-gvcfs",
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True),
    help="Path to the file containing GVCF paths and its corresponding tbi files.",
)
@click.option(
    "-o",
    "--vds-output-path",
    required=True,
    type=click.Path(),
    help="Output path for the VDS file.",
)
@click.option(
    "-t",
    "--tmp-path",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    help="Temporary directory path with enough space.",
)
@click.option(
    "-dm",
    "--driver-memory",
    default="256g",
    show_default=True,
    help="Memory allocation for Spark driver.",
)
@click.option(
    "-nc",
    "--n-cpus",
    default=4,
    show_default=True,
    help="Number of CPUs to use for local computation.",
)
@click.option(
    "-rg",
    "--reference-genome",
    default="GRCh38",
    show_default=True,
    help="Reference genome to use (e.g., GRCh37, GRCh38).",
)
def gvcf_combiner(
    path_to_gvcfs: str,
    vds_output_path: str,
    tmp_path: str,
    driver_memory: str,
    n_cpus: int,
    reference_genome: str,
) -> None:
    """
    Combine GVCFs into a VDS using Hail's gvcf combiner.

    Parameters:
        path_to_gvcfs (str): Path to the file listing GVCF paths.
        vds_output_path (str): Output path for the VDS file.
        tmp_path (str): Temporary directory path with sufficient space.
        driver_memory (str): Memory allocation for the Spark driver.
        n_cpus: Number of CPUs to use for local computation.
        reference_genome (str): Reference genome to use (e.g., GRCh37, GRCh38).
    """
    logger.info("Starting GVCF combiner")
    run_gvcf_combiner(path_to_gvcfs, vds_output_path, tmp_path, driver_memory, n_cpus, reference_genome)

