import logging
import sys
from typing import NoReturn

import click
import hail as hl

from pygskit.gskit.combiners import combine_vdses
from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.constants import HG38_GENOME_REFERENCE

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_vds_combiner(
        vdses_dir: str,
        output_path: str,
        driver_memory: str,
        n_cpus: int,
        reference_genome: str,
        validate: bool,
        overwrite: bool,
) -> NoReturn:
    """
    Combine multiple VDS directories into a single VDS.

    Parameters:
        vdses_dir (str): Container directory containing VDS directories.
        output_path (str): Path where the combined VDS will be written.
        driver_memory (str): Memory allocation for the Spark driver.
        n_cpus (int): Number of CPUs to allocate for local computation.
        reference_genome (str): Reference genome to init Hail with.
        validate (bool): Whether to validate the combined VDS.
        overwrite (bool): Whether to overwrite the output if it already exists.

    Raises:
        SystemExit: Exits with status 1 if an error occurs during combination.
    """
    try:
        # Initialize Hail with the given configuration.
        init_hail_local(n_cores=n_cpus, driver_memory=driver_memory, reference_genome=reference_genome)
        logger.info("Starting VDS combination from '%s' into VDS '%s'", vdses_dir, output_path)

        # Combine the VDS directories.
        combine_vdses(vdses_dir, output_path, validate=validate, overwrite=overwrite)
        logger.info("Successfully combined VDS directories into VDS at '%s'", output_path)
    except Exception as e:
        logger.exception("An error occurred during the VDS combination")
        sys.exit(1)
    finally:
        logger.info("Stopping Hail session")
        hl.stop()


@click.command("vds_combiner", help="Combine VDS directories into a single VDS.")
@click.option(
    "-d",
    "--vdses-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="Path to the container directory with VDS directories."
)
@click.option(
    "-o",
    "--output-path",
    required=True,
    type=click.Path(),
    help="Output path for the combined VDS."
)
@click.option(
    "-dm",
    "--driver-memory",
    default="8g",
    show_default=True,
    help="Memory allocation for the Spark driver."
)
@click.option(
    "-nc",
    "--n-cpus",
    default=4,
    show_default=True,
    help="Number of CPUs to use for local computation."
)
@click.option("-rg",
              "--reference-genome",
              default=HG38_GENOME_REFERENCE,
              show_default=True,
              help="Reference genome to init Hail with.")
@click.option(
    "--validate",
    is_flag=True,
    help="Whether to validate the combined VDS."
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite the output if it exists."
)
@click.pass_context
def vds_combiner(
        ctx,
        vdses_dir: str,
        output_path: str,
        driver_memory: str,
        n_cpus: int,
        validate: bool,
        overwrite: bool,
        reference_genome: str
) -> None:
    """
    Combine VDS directories into a single VDS.
    """
    logger.info("Initializing VDS combiner")
    run_vds_combiner(
        vdses_dir=vdses_dir,
        output_path=output_path,
        driver_memory=driver_memory,
        n_cpus=n_cpus,
        validate=validate,
        overwrite=overwrite,
        reference_genome=reference_genome
    )

