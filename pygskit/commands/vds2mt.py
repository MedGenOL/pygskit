import sys
import click
import hail as hl
from pygskit.gskit.converters import convert_vds_to_mt
from pygskit.gskit.constants import HG38_GENOME_REFERENCE


def run_vds2mt(
    vds_path: str,
    output_path: str,
    annotate_adjusted_gt: bool,
    skip_split_multi: bool,
    local_cores: int,
    driver_memory: str,
    reference_genome: str,
    overwrite: bool,
) -> None:
    """
    Convert a Variant Dataset (VDS) to a dense MatrixTable (MT).

    This function initializes Hail, reads a VDS file, optionally splits multi-allelic variants,
    converts the dataset to a dense MatrixTable, annotates it with `adj`, and writes the result
    to the specified output path.

    Parameters:
        vds_path (str): Path to the input VDS file.
        output_path (str): Path to save the output MatrixTable.
        annotate_adjusted_gt (bool): If True, annotate the MatrixTable with adjusted genotypes.
                                     Recommended for downstream analyses.
        skip_split_multi (bool): If True, skip splitting multi-allelic variants.
        local_cores (int): Number of local cores for Hail initialization.
        driver_memory (str): Memory allocated to the Spark driver.
        reference_genome (str): Reference genome to use (e.g., 'GRCh37', 'GRCh38').
        overwrite (bool): If True, overwrite the output MatrixTable if it exists.
    """
    # Initialize Hail with the provided configuration.
    hl.init(
        local=f"local[{local_cores}]",
        spark_conf={"spark.driver.memory": driver_memory},
        default_reference=reference_genome,
    )

    try:
        convert_vds_to_mt(
            vds_path=vds_path,
            output_path=output_path,
            adjust_genotypes=annotate_adjusted_gt,
            skip_split_multi=skip_split_multi,
            overwrite=overwrite,
        )

    except Exception as e:
        click.secho(f"Error: {str(e)}", fg="red", err=True)
        sys.exit(1)

    finally:
        hl.stop()
        click.echo("Hail stopped.")


@click.command(
    "vds2mt", help="Convert a Variant Dataset (VDS) to a dense MatrixTable (MT)."
)
@click.option("--vds-path", required=True, type=str, help="Path to the input VDS file.")
@click.option(
    "--output-path",
    required=True,
    type=str,
    help="Path to save the output MatrixTable.",
)
@click.option(
    "--annotate-adjusted-gt",
    is_flag=True,
    default=False,
    help="Annotate the MatrixTable with adjusted genotypes.",
)
@click.option(
    "--skip-split-multi",
    is_flag=True,
    default=False,
    help="Skip splitting multi-allelic variants.",
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
    help="Reference genome to use (e.g., GRCh37, GRCh38).",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite the output MatrixTable if it exists.",
)
@click.pass_context
def vds2mt(
    ctx,
    vds_path: str,
    output_path: str,
    annotate_adjusted_gt: bool,
    skip_split_multi: bool,
    n_cpus: int,
    driver_memory: str,
    reference_genome: str,
    overwrite: bool,
) -> None:
    """
    CLI command to convert a Variant Dataset (VDS) to a dense MatrixTable (MT).
    If specified, the multi-allelic variants will be split, and the MatrixTable will be annotated with `adj`.
    """
    run_vds2mt(
        vds_path=vds_path,
        output_path=output_path,
        annotate_adjusted_gt=annotate_adjusted_gt,
        skip_split_multi=skip_split_multi,
        local_cores=n_cpus,
        driver_memory=driver_memory,
        reference_genome=reference_genome,
        overwrite=overwrite,
    )

