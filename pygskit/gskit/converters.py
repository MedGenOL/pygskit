import logging
import hail as hl
from gnomad.utils.annotations import annotate_adj
from pygskit.gskit.constants import ADJ_GT_FIELD


def convert_vds_to_mt(
    vds_path: str,
    output_path: str,
    adjust_genotypes: bool = True,
    skip_split_multi: bool = True,
    overwrite: bool = False,
) -> None:
    """
    Convert a Variant Dataset (VDS) to MatrixTable (MT).
    Split multi-allelic variants, convert the VDS to a dense MT, and optionally annotate the MT with adj.

    Parameters:
        vds_path (str): Path to the input VDS.
        output_path (str): Path where the output MatrixTable will be written.
        adjust_genotypes (bool): If True, annotate the MatrixTable with adjusted genotypes.
        skip_split_multi (bool): If True, skip splitting multi-allelic variants.
        overwrite (bool): Whether to overwrite the output if it already exists.
    """
    try:
        logging.info(f"Reading VDS from {vds_path}...")
        vds = hl.vds.read_vds(vds_path)

        if not skip_split_multi:
            logging.info("Splitting multi-allelic variants...")
            vds = hl.vds.split_multi(vds)

        logging.info("Converting VDS to dense MatrixTable...")
        mt = hl.vds.to_dense_mt(vds)

        if adjust_genotypes:
            logging.info("Annotating MatrixTable with adjusted genotypes...")
            mt = annotate_adj(mt)  # Ensure annotate_adj is defined or imported

        # convert LGT to GT
        mt = mt.annotate_entries(GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA))

        logging.info("MatrixTable schema:")
        logging.info(mt.describe())

        logging.info(f"Writing MatrixTable to {output_path}...")
        mt.write(output_path, overwrite=overwrite)
        logging.info("MatrixTable successfully written.")

    except Exception as e:
        logging.exception("An error occurred during VDS to MT conversion.")
        raise  # Optionally re-raise the exception if needed for further handling

    finally:
        hl.stop()
        logging.info("Hail stopped.")


def convert_mt_to_multi_sample_vcf(
    mt_path: str, vcf_path: str, filter_adj_genotypes: bool = True, min_ac: int = 1
) -> None:
    """
    Convert a Hail MatrixTable to a multi-sample VCF file.

    This function reads a MatrixTable, optionally filters entries to only include
    adjusted genotypes, computes variant quality control metrics, annotates the rows
    with VCF-compatible info fields, filters variants based on the minimum alternate allele
    count (AC), drops non-VCF compatible fields, and exports the result to a VCF file.

    Parameters:
        mt_path (str): Path to the input MatrixTable.
        vcf_path (str): Path where the output VCF will be written.
        filter_adj_genotypes (bool): If True, filter entries to adjusted genotypes. Recommended.
        min_ac (int): Minimum alternate allele count (AC) for a variant to be retained.
    """
    try:
        logging.info(f"Reading MatrixTable from {mt_path}...")
        mt = hl.read_matrix_table(mt_path)

        if filter_adj_genotypes:
            # check if adj field is present
            if ADJ_GT_FIELD not in mt.entry:
                raise ValueError("MatrixTable does not contain adjusted genotypes.")
            logging.info("Filtering entries to adjusted genotypes...")
            mt = mt.filter_entries(mt.adj, keep=True)
        else:
            logging.info("Skipping filtering for adjusted genotypes.")

        logging.info("Computing variant QC metrics...")
        mt = hl.variant_qc(mt)

        logging.info("Annotating rows with VCF-compatible info fields (AC, AN, call_rate)...")
        mt = mt.annotate_rows(
            info=hl.struct(
                AC=mt.variant_qc.AC[1], AN=mt.variant_qc.AN, call_rate=mt.variant_qc.call_rate
            )
        )

        logging.info(f"Filtering rows with AC >= {min_ac}...")
        mt = mt.filter_rows(mt.info.AC >= min_ac, keep=True)

        logging.info("Dropping fields that are not compatible with VCF format...")
        mt = mt.drop(*[mt.gvcf_info, mt.adj, mt.variant_qc])

        logging.info(f"Exporting VCF to {vcf_path}...")
        hl.export_vcf(mt, vcf_path)
        logging.info("VCF successfully written.")

    except Exception as e:
        logging.exception("An error occurred during conversion to VCF.")
        raise

    finally:
        hl.stop()
        logging.info("Hail context stopped.")
