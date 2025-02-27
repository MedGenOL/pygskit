import sys
import click
import hail as hl
from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.converters import convert_mt_to_multi_sample_vcf
from pygskit.gskit.constants import HG38_GENOME_REFERENCE


def run_mt2vcf(
    mt_path: str,
    vcf_path: str,
    filter_adj_genotypes: bool,
    min_ac: int,
    split_multi: bool,
    local_cores: int,
    driver_memory: str,
    reference_genome: str,
) -> None:
    """
    Convert a dense MatrixTable (MT) to a multi-sample VCF file.

    This function initializes Hail, reads a MatrixTable file, optionally filters genotypes based on the 'adj' field,
    filters variants based on the alternate allele count, splits multi-allelic variants, and writes the result
    to a VCF file. AC and AF fields are computed for each variant using the hl.variant_qc function.

    Parameters:
        mt_path (str): Path to the input MatrixTable file.
        vcf_path (str): Path to save the output VCF file. Ensure the path ends with '.vcf.bgz'.
        filter_adj_genotypes (bool): If True, filter genotypes based on the 'adj' field.
        min_ac (int): Minimum alternate allele count for filtering variants.
                      Only variants with AC >= min_ac will be retained.
        split_multi (bool): If True, split multi-allelic variants.
        local_cores (int): Number of local cores for Hail initialization.
        driver_memory (str): Memory allocated to the Spark driver.
        reference_genome (str): Reference genome to use (e.g., 'GRCh37', 'GRCh38').
    """
    # Initialize Hail with the provided configuration.
    init_hail_local(
        n_cores=local_cores,
        driver_memory=driver_memory,
        reference_genome=reference_genome,
    )

    try:
        convert_mt_to_multi_sample_vcf(
            mt_path=mt_path,
            vcf_path=vcf_path,
            filter_adj_genotypes=filter_adj_genotypes,
            min_ac=min_ac,
            split_multi=split_multi,
        )

    except Exception as e:
        click.secho(f"Error: {str(e)}", fg="red", err=True)
        sys.exit(1)

    finally:
        hl.stop()
        click.echo("Hail stopped.")


@click.command("mt2vcf", help="Convert a dense MatrixTable to a multi-sample VCF file.")
@click.option(
    "-mt",
    "--mt-path",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True),
    help="Path to the input MatrixTable directory.",
)
@click.option(
    "-vcf",
    "--vcf-path",
    required=True,
    type=str,
    help="Path to save the output VCF file. Ensure the path ends with '.vcf.bgz'.",
)
@click.option(
    "-adj",
    "--filter-adj-genotypes",
    is_flag=True,
    help="Filter genotypes based on the 'adj' field.",
)
@click.option(
    "-ac",
    "--min-ac",
    type=int,
    default=1,
    help="Minimum alternate allele count for filtering variants. Disables filtering if set to 0.",
)
@click.option(
    "-sm",
    "--split-multi",
    is_flag=True,
    help="Split multi-allelic variants."
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
@click.pass_context
def mt2vcf(
    ctx,
    mt_path: str,
    vcf_path: str,
    filter_adj_genotypes: bool,
    min_ac: int,
    split_multi: bool,
    driver_memory: str,
    n_cpus: int,
    reference_genome: str,
) -> None:
    """
    Convert a dense MatrixTable to a multi-sample VCF file.
    """
    run_mt2vcf(
        mt_path=mt_path,
        vcf_path=vcf_path,
        filter_adj_genotypes=filter_adj_genotypes,
        min_ac=min_ac,
        split_multi=split_multi,
        local_cores=n_cpus,
        driver_memory=driver_memory,
        reference_genome=reference_genome
    )
