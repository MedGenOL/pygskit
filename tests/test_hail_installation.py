"""
Test if Hail is installed correctly and can be imported in this conda environment.

Run this script with:
     python test_hail_installation.py
"""
import logging
from pygskit.gskit.utils import init_hail_local


def test_hail_installed():
    import hail as hl

    # Initialize Hail in local mode
    init_hail_local(n_cores=4,
                    driver_memory='8g',
                    reference_genome='GRCh38')

    # Create a MatrixTable using Hail's balding_nichols_model
    mt = hl.balding_nichols_model(n_populations=3,
                                  n_samples=10,
                                  n_variants=100)
    logging.info(mt.show())
    assert mt.count() == (100, 10)
    hl.stop()


if __name__ == "__main__":
    test_hail_installed()