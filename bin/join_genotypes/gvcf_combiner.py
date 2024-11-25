import click
import hail as hl
import os

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

# Function to read paths to GVCFs
# Function to check if a file exists and is readable
def check_file_exists_and_readable(path):
    """
    Check if a file exists and is readable.
    :param path: Absolute path to the file.
    :return: Path to the file if it exists and is readable.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"File {path} not found.")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"File {path} is not readable.")
    return path


# Function to read and validate paths to GVCFs in a single iteration
def parse_gvcf_paths(path) -> list:
    """
    Read and validate paths to GVCFs from a file.
    Expected format: One path per line.
    :param path:
    :return:
    """
    # Check if the input file exists and is readable
    check_file_exists_and_readable(path)

    # Read and validate paths from the file in a single iteration
    with open(path, 'r') as file:
        paths = []
        for line in file:
            gvcf_path = line.strip()
            # check file end with the ext .g.vcf.gz
            if not gvcf_path.endswith('.g.vcf.gz'):
                raise ValueError(f"File {gvcf_path} does not end with '.g.vcf.gz'.")
            check_file_exists_and_readable(gvcf_path)  # Validate each path
            paths.append(gvcf_path)

    return paths


@click.command()
@click.option(
    '--path-to-gvcfs',
    required=True,
    type=click.Path(exists=True),
    help='Path to the file containing GVCF paths.'
)
@click.option(
    '--vds-output-path',
    required=True,
    type=click.Path(),
    help='Output path for the VDS file.'
)
@click.option(
    '--tmp-path',
    required=True,
    type=click.Path(),
    help='Temporary directory path with enough space.'
)
@click.option(
    '--driver-memory',
    default='256g',
    show_default=True,
    help='Memory allocation for Spark driver.'
)
@click.option(
    '--local',
    default='local[100]',
    show_default=True,
    help='Local computation configuration for Hail.'
)
@click.option(
    '--reference-genome',
    default='GRCh38',
    show_default=True,
    help='Reference genome to use (e.g., GRCh37, GRCh38).'
)
def combine_gvcfs(path_to_gvcfs, vds_output_path, tmp_path, driver_memory, local, reference_genome):
    """
    CLI to combine GVCFs into a VDS using Hail's gvcf combiner.
    """
    # Initialize Hail in local mode
    hl.init(
        local=local,
        spark_conf={'spark.driver.memory': driver_memory},
        default_reference=reference_genome
    )

    # Read GVCF paths
    gvcfs = parse_gvcf_paths(path_to_gvcfs)

    # Run Hail GVCF combiner
    combiner = hl.vds.new_combiner(
        output_path=vds_output_path,
        temp_path=tmp_path,
        gvcf_paths=gvcfs,
        use_genome_default_intervals=True,
        gvcf_reference_entry_fields_to_keep=[],
    )
    combiner.run()

    # Stop Hail
    hl.stop()

if __name__ == '__main__':
    combine_gvcfs()
