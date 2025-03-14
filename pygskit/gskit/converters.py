import logging
import hail as hl
from gnomad.utils.annotations import annotate_adj
from pygskit.gskit.constants import ADJ_GT_FIELD


def convert_vds_to_mt(
    vds_path: str,
    output_path: str,
    adjust_genotypes: bool = True,
    skip_split_multi: bool = False,
    convert_lgt_to_gt: bool = True,
    skip_keying_by_cols: bool = False,
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
        convert_lgt_to_gt (bool): If True, convert LGT to GT. Recommended after splitting multi-allelic variants.
        skip_keying_by_cols (bool): If True, skip keying the MatrixTable by columns.
        overwrite (bool): Whether to overwrite the output if it already exists.
    """
    try:
        logging.info(f"Reading VDS from {vds_path}...")
        vds = hl.vds.read_vds(vds_path)

        logging.info("Converting VDS to dense MatrixTable...")
        mt = hl.vds.to_dense_mt(vds)

        if adjust_genotypes:
            logging.info("Annotating MatrixTable with adjusted genotypes...")
            mt = annotate_adj(mt)

        # convert LGT to GT
        if convert_lgt_to_gt:
            logging.warning("This step is recommended before running any downstream analyses (e.g., split_multi_hts).")
            logging.info("Converting LGT to GT...")
            mt = mt.annotate_entries(GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA))

        if not skip_split_multi:
            logging.info("Splitting multi-allelic variants...")
            mt = hl.split_multi_hts(mt)

        if not skip_keying_by_cols:
            logging.info("Keying MatrixTable by columns...")
            mt = mt.key_cols_by(mt['s'])

        logging.info("MatrixTable schema:")
        logging.info(mt.describe())

        logging.info(f"Writing MatrixTable to {output_path}...")
        mt.write(output_path, overwrite=overwrite)
        logging.info("MatrixTable successfully written.")

    except Exception as e:
        logging.exception("An error occurred during VDS to MT conversion.")
        raise

    finally:
        hl.stop()
        logging.info("Hail stopped.")


def convert_mt_to_multi_sample_vcf(
    mt_path: str,
    vcf_path: str,
    filter_adj_genotypes: bool = True,
    min_ac: int = 1,
    split_multi: bool = True
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
        split_multi (bool): Whether to split multi-allelic variants.
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

        if not split_multi:
            logging.info("Skipping splitting multi-allelic variants: option disabled.")
        elif "was_split" in mt.row:
            logging.info("Skipping splitting multi-allelic variants: already split.")
        else:
            logging.info("Splitting multi-allelic variants...")
            mt = hl.split_multi_hts(mt)

        logging.info("Computing variant QC metrics...")
        # compute variant QC metrics and annotate/update (e.g., AC/AF) into info field
        # this recommended after filtering (e.g., adj genotypes)
        mt = hl.variant_qc(mt)

        logging.info("Annotating rows with VCF-compatible info fields (AF, AC, AN, call_rate)...")
        mt = mt.annotate_rows(
            info=hl.struct(
                AF=mt.variant_qc.AF[1],
                AC=mt.variant_qc.AC[1],
                AN=mt.variant_qc.AN,
                call_rate=mt.variant_qc.call_rate # TODO: Maybe change to CR?
            )
        )

        if min_ac >= 1:
            logging.info(f"Filtering rows with AC >= {min_ac}...")
            mt = mt.filter_rows(mt.info.AC >= min_ac, keep=True)
        else:
            logging.info("Skipping filtering rows based on AC: option disabled.")

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
