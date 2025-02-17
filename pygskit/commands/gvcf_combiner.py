"""
Combine GVCFs into a VDS using Hail's gvcf combiner.

Usage:
    pygskit gvcf_combiner.py --path-to-gvcfs <path> \
                             --vds-output-path <path> \
                             --tmp-path <path> \
                             --driver-memory <str> \
                             --n-cpus <int> \
                             --reference-genome <str>
"""

import logging
import sys
from typing import NoReturn

import click
import hail as hl
from pygskit.gskit.combiners import combine_gvcfs
from pygskit.gskit.utils import init_hail_local

# Configure logging.
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
        path_to_gvcfs (str): Path to a directory containing GVCF files and their corresponding .tbi files.
        vds_output_path (str): Output path where the VDS file will be written.
        tmp_path (str): Path to a temporary directory with sufficient disk space.
        driver_memory (str): Memory allocation for the Spark driver (e.g., '256g').
        n_cpus (int): Number of CPUs to allocate for local computation.
        reference_genome (str): The reference genome to use (e.g., 'GRCh37', 'GRCh38').

    Raises:
        SystemExit: Exits with status 1 if an error occurs during combination.
    """
    try:
        # Initialize Hail with the given configuration.
        init_hail_local(n_cpus=n_cpus, driver_memory=driver_memory, reference_genome=reference_genome)
        logger.info("Starting combination of GVCFs from '%s' into VDS '%s'", path_to_gvcfs, vds_output_path)

        # Combine the GVCFs.
        combine_gvcfs(path_to_gvcfs, vds_output_path, tmp_path)
        logger.info("Successfully combined GVCFs into VDS at '%s'", vds_output_path)
    except Exception as e:
        logger.exception("An error occurred during the GVCF combination")
        sys.exit(1)
    finally:
        logger.info("Stopping Hail session")
        hl.stop()


@click.command("gvcf_combiner", help="Combine GVCFs into a VDS using Hail's gvcf combiner.")
@click.option(
    "-d",
    "--path-to-gvcfs",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="Path to a directory with the gvcf files along with their corresponding .tbi files.",
)
@click.option(
    "-o",
    "--vds-output-path",
    required=True,
    type=click.Path(),
    help="Output path for the resulting VDS file.",
)
@click.option(
    "-t",
    "--tmp-path",
    required=True,
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    help="Temporary directory path with enough space to store intermediate files.",
)
@click.option(
    "-dm",
    "--driver-memory",
    default="256g",
    show_default=True,
    help="Memory allocation for the Spark driver.",
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
    CLI entry point for combining GVCFs into a VDS using Hail's gvcf combiner.

    This function parses command-line arguments and initiates the combination process.

    Parameters:
        path_to_gvcfs (str): Path to a directory containing GVCF files and their corresponding .tbi files.
        vds_output_path (str): Path where the output VDS file will be stored.
        tmp_path (str): Temporary directory path for intermediate files.
        driver_memory (str): Memory allocation for the Spark driver.
        n_cpus (int): Number of CPUs to allocate for computation.
        reference_genome (str): The reference genome version (e.g., GRCh37, GRCh38).
    """
    logger.info("Initializing GVCF combiner")
    run_gvcf_combiner(
        path_to_gvcfs=path_to_gvcfs,
        vds_output_path=vds_output_path,
        tmp_path=tmp_path,
        driver_memory=driver_memory,
        n_cpus=n_cpus,
        reference_genome=reference_genome,
    )


if __name__ == "__main__":
    gvcf_combiner()

