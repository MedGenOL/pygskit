import hail as hl
import logging
from pygskit.gskit.constants import HG38_GENOME_REFERENCE


def init_hail_local(
    n_cores: int = 4, driver_memory: str = "4g", reference_genome: str = HG38_GENOME_REFERENCE
) -> None:
    """
    Initialize Hail in local mode with the specified configuration.

    Parameters:
        n_cores (int): Number of cores to use in local mode (e.g., 4 will initialize Hail with "local[4]").
        driver_memory (str): Memory allocation for the Spark driver (e.g., '4g', '8g').
        reference_genome (str): The reference genome to use for Hail initialization (default is HG38).

    Returns:
        None

    Raises:
        ValueError: If n_cores is less than 1.
        Exception: Any exceptions raised by hl.init() if Hail initialization fails.
    """
    if n_cores < 1:
        raise ValueError("n_cores must be at least 1.")

    # Configure logging if it hasn't been configured elsewhere
    logging.basicConfig(level=logging.INFO)
    logging.info(
        f"Initializing Hail in local mode with {n_cores} cores, driver memory: {driver_memory}, "
        f"and reference genome: {reference_genome}."
    )

    hl.init(
        local=f"local[{n_cores}]",
        spark_conf={"spark.driver.memory": driver_memory},
        default_reference=reference_genome,
    )

    logging.info("Hail initialized successfully.")
