import hail as hl
import logging
from typing import List
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
        master=f"local[{n_cores}]",
        spark_conf={"spark.driver.memory": driver_memory},
        default_reference=reference_genome,
    )

    logging.info("Hail initialized successfully.")



def sort_mts_cols(mts: List[hl.MatrixTable], ref_index: int = 0) -> List[hl.MatrixTable]:
    """
    Sort the column order of a list of matrix tables to match a reference matrix table.

    The column order of each matrix table is updated to match the order defined by the reference matrix table.
    All matrix tables are assumed to have equal number of columns and the same column keys.

    Parameters:
        mts (List[hl.MatrixTable]: List of matrix table objects.
        ref_index (int): The index of the reference matrix table in the list. The reference table's columns
                         remain unchanged. Defaults to 0.

    Returns:
        List[hl.MatrixTable]: A new list of matrix table objects with columns reordered to match the reference.

    Raises:
        IndexError: If ref_index is out of range for the list of matrix tables.

    Example:
        sorted_mts = sort_mts_cols([mt1, mt2, mt3], ref_index=0)
    """
    if not 0 <= ref_index < len(mts):
        raise IndexError("ref_index is out of range for the provided list of matrix tables.")

    # Compute the column order based on the reference matrix table.
    ref_mt = mts[ref_index].add_col_index()

    sorted_mts = []
    for i, mt in enumerate(mts):
        if i == ref_index:
            # Leave the reference matrix table unchanged.
            sorted_mts.append(mt)
        else:
            mt_indexed = mt.add_col_index()
            new_order = mt_indexed.index_cols(ref_mt.col_key).col_idx.collect()
            sorted_mts.append(mt_indexed.choose_cols(new_order))

    return sorted_mts

