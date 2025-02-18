import shutil

from sympy.physics.units import planck

from pygskit.gskit.utils import init_hail_local
from pygskit.gskit.file_utils import compress_files, decompress_files
from pygskit.gskit.combiners import combine_gvcfs
from pygskit.gskit.constants import HG38_GENOME_REFERENCE
from pathlib import Path

TESTS_DIR = Path(__file__).parent


def test_combine_gvcfs():
    """
    Test the combine_gvcfs function by combining GVCFs into a VDS.

    This test initializes Hail in local mode, combines GVCFs from the specified directory
    into a Variant Dataset (VDS), and verifies the successful creation of the VDS by
    checking for the existence of specific success files.

    :return: None
    """
    # Initialize Hail in local mode
    init_hail_local(n_cores=4, driver_memory="8g", reference_genome=HG38_GENOME_REFERENCE)

    # Combine GVCFs into a VDS
    gvcf_dir = TESTS_DIR / "testdata/gvcfs"
    vds_output_path = TESTS_DIR / "testdata/vds/cohort.vds"
    tmp_path = TESTS_DIR.parent / "local/tmp"
    plan_path = tmp_path / "combiner_plan.json"
    combine_gvcfs(
        gvcf_dir=str(gvcf_dir),
        vds_output_path=str(vds_output_path),
        tmp_path=str(tmp_path),
        save_path=str(plan_path),
        vdses=[],
        kwargs={},
    )

    # Check if the vds_output_path / 'reference_data/_SUCCESS' file exists
    assert vds_output_path.joinpath("reference_data/_SUCCESS").exists()
    # Check if the vds_output_path / 'variant_data/_SUCCESS' file exists
    assert vds_output_path.joinpath("variant_data/_SUCCESS").exists()

    # Compress the VDS into a zip file and remove the original VDS
    compress_files(
        source_dir=str(vds_output_path),
        output_zip=str(vds_output_path.with_suffix(".vds.zip")),
        remove_originals=True,
    )
