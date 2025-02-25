import os
import logging
import sys
from typing import NoReturn

import click
import hail as hl

from pygskit.gskit.combiners import combine_matrix_table_rows, combine_matrix_table_cols
from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.constants import HG38_GENOME_REFERENCE

# Configure logging.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def run_mts_combiner(
        mts_dir: str,
        output_path: str,
        overwrite: bool,
        combine_by: str,
        n_partitions: int,
        n_cpus: int,
        driver_memory: str,
        reference_genome: str
) -> NoReturn:
    """
    Combine multiple MatrixTable directories into a single MatrixTable.

    Parameters:
        mts_dir (str): Container directory containing MatrixTable directories.
        output_path (str): Path where the combined MatrixTable will be written.
        overwrite (bool): Whether to overwrite the output if it already exists.
        combine_by (str): The method to use for combining the MatrixTables. Options are 'rows' and 'cols'.
        n_partitions (int): Number of partitions to use when writing the combined MatrixTable.
        driver_memory (str): Memory allocation for the Spark driver.
        n_cpus (int): Number of CPUs to allocate for local computation.
        reference_genome (str): Reference genome to init Hail with.

    Raises:
        SystemExit: Exits with status 1 if an error occurs during combination.
    """
    try:
        # Initialize Hail with the given configuration.
        init_hail_local(n_cores=n_cpus, driver_memory=driver_memory, reference_genome=reference_genome)
        logger.info("Starting MatrixTable combination from '%s' into MatrixTable '%s'", mts_dir, output_path)

        # Get the paths of the MatrixTable directories.
        mt_paths = [os.path.join(mts_dir, mt) for mt in os.listdir(mts_dir)]

        # Combine the MatrixTable directories.
        if combine_by == 'rows':
            combine_matrix_table_rows(mt_paths, output_path, n_partitions=n_partitions, overwrite=overwrite)
        elif combine_by == 'cols':
            combine_matrix_table_cols(mt_paths, output_path,  n_partitions=n_partitions, overwrite=overwrite)
        else:
            raise ValueError(f"Invalid combine_by option: {combine_by}. Must be 'rows' or 'cols'.")
        logger.info("Successfully combined MatrixTable directories into MatrixTable at '%s'", output_path)
    except Exception as e:
        logger.exception("An error occurred during the MatrixTable combination")
        sys.exit(1)
    finally:
        logger.info("Stopping Hail session")
        hl.stop()


@click.command("mts_combiner", help="Combine MatrixTable directories into a single MatrixTable.")
@click.option("-i",
    "--mts-dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Container directory containing MatrixTable directories.",
)
@click.option(
    "-o",
    "--output-path",
    required=True,
    type=click.Path(),
    help="Output path for the combined MatrixTable.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Whether to overwrite the output if it already exists.",
)
@click.option(
    '-cb',
    "--combine-by",
    required=True,
    type=click.Choice(['rows', 'cols']),
    help="The axis to combine the MatrixTables on. Options are 'rows' and 'cols'.",
)
@click.option(
    "-np",
    "--n-partitions",
    type=int,
    help="Number of partitions to use when writing the combined MatrixTable.",
)
@click.option(
    "-dm",
    "--driver-memory",
    default="8g",
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
    default=HG38_GENOME_REFERENCE,
    show_default=True,
    help="Reference genome to init Hail with.",
)
def mts_combiner(
    mts_dir: str,
    output_path: str,
    overwrite: bool,
    combine_by: str,
    n_partitions: int,
    n_cpus: int,
    driver_memory: str,
    reference_genome: str
) -> NoReturn:
    run_mts_combiner(
        mts_dir=mts_dir,
        output_path=output_path,
        overwrite=overwrite,
        combine_by=combine_by,
        n_partitions=n_partitions,
        n_cpus=n_cpus,
        driver_memory=driver_memory,
        reference_genome=reference_genome
    )
