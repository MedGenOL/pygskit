import click
import hail as hl
from gnomad.utils.annotations import annotate_adj

"""
This script converts a Variant Dataset (VDS) to a dense MatrixTable (MT) using Hail.
It reads a VDS file, splits multi-allelic variants, converts the VDS to a dense MT,
annotates the MT with adj, and writes the resulting MT to an output path.

Usage:
    python vds2mt.py --vds-path <input_vds_path> --output-path <output_mt_path> [options]

Options:
    --vds-path            Path to the input VDS file (required).
    --output-path         Path to save the output MatrixTable (required).
    --local-cores         Number of local cores for Hail initialization (default: 100).
    --driver-memory       Memory allocated to the Spark driver (default: 256g).
    --skip-split-multi    Skip splitting multi-allelic variants (default: False).
    --overwrite           Overwrite the output MatrixTable if it exists (default: False).
"""


@click.command()
@click.option("--vds-path", required=True, help="Path to the input VDS file.")
@click.option("--output-path", required=True, help="Path to save the output MatrixTable.")
@click.option("--local-cores", default=100, help="Number of local cores for Hail initialization.")
@click.option("--driver-memory", default="256g", help="Memory allocated to the Spark driver.")
@click.option(
    "--skip-split-multi",
    is_flag=True,
    default=False,
    help="Skip splitting multi-allelic variants.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite the output MatrixTable if it exists.",
)
def convert_vds_to_mt(
    vds_path, output_path, local_cores, driver_memory, skip_split_multi, overwrite
):
    """
    CLI tool to convert a Variant Dataset (VDS) to a dense MatrixTable (MT).
    """
    # Initialize Hail
    hl.init(
        local=f"local[{local_cores}]",
        spark_conf={"spark.driver.memory": driver_memory},
        default_reference="GRCh38",
    )

    try:
        # Read the VDS
        click.echo(f"Reading VDS from {vds_path}...")
        vds = hl.vds.read_vds(vds_path)

        # Split multi-allelic variants
        if not skip_split_multi:
            click.echo("Splitting multi-allelic variants...")
            vds = hl.vds.split_multi(vds)

        # Convert to dense MatrixTable
        click.echo("Converting VDS to dense MatrixTable...")
        mt = hl.vds.to_dense_mt(vds)

        # Annotate with adj
        click.echo("Annotating MatrixTable with adj...")
        mt = annotate_adj(mt)

        # Describe the MatrixTable schema
        click.echo("MatrixTable schema:")
        mt.describe()

        # Write the MatrixTable to output
        click.echo(f"Writing MatrixTable to {output_path}...")
        mt.write(output_path, overwrite=overwrite)
        click.echo("MatrixTable successfully written.")

    except Exception as e:
        click.echo(f"Error: {str(e)}", err=True)

    finally:
        # Stop Hail
        hl.stop()
        click.echo("Hail stopped.")


if __name__ == "__main__":
    convert_vds_to_mt()
