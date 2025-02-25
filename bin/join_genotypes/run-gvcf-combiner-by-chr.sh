#!/bin/bash

set -euo pipefail

# Usage:
#   ./run_gvcf_combiner.sh
#
# This script combines GVCFs into a VDS using Hail's gvcf combiner for each chromosome.
#
# Options (common for all chromosomes):
#   BASE_PATH_TO_GVCFS: Directory containing per-chromosome subdirectories of GVCF files. [required]
#   BASE_VDS_OUTPUT_PATH: Base output directory for the resulting VDS files. [required]
#   TMP_PATH: Temporary directory for intermediate files. [required]
#   DRIVER_MEMORY: Memory allocation for the Spark driver. [default: 256g]
#   N_CPUS: Number of CPUs for local computation. [default: 100]
#   REFERENCE_GENOME: Reference genome to use (e.g., GRCh37, GRCh38). [default: GRCh38]
#
# Logs for each chromosome are saved to a file named "gvcf_combiner.<CHR>.log".

# Define common parameters
BASE_PATH_TO_GVCFS="/mnt/nfs/KOL_UOL/projects/CHD_1000WGS/variant_calling/split_vcfs"
BASE_VDS_OUTPUT_PATH="/mnt/nfs/work/1k_joint"
BASE_TMP_PATH="/mnt/nfs/tmp_jc"
DRIVER_MEMORY="512g"
N_CPUS=120
REFERENCE_GENOME="GRCh38"

# Define the list of chromosomes to process.
# Modify the list below to include only the chromosomes you wish to process.
chromosomes=("chr8" "chr9" "chr10" "chr11" "chr12" "chr13" "chr14" "chr15" "chr16" \
             "chr17" "chr18" "chr19" "chr20" "chr21" "chr22" "chrX" "chrY")

# Iterate over each chromosome
for CHR in "${chromosomes[@]}"; do
    echo "---------------------------------------"
    echo "Processing $CHR ..."

    # Define chromosome-specific paths
    PATH_TO_GVCFS="${BASE_PATH_TO_GVCFS}/${CHR}"
    VDS_OUTPUT_PATH="${BASE_VDS_OUTPUT_PATH}/1k_cohort.${CHR}.vds"
    PATH_TO_SAVE_PLAN="${BASE_VDS_OUTPUT_PATH}/1k_cohort.${CHR}.plan.json"
    PATH_TMP="${BASE_TMP_PATH}/${CHR}"
    LOG_FILE="gvcf_combiner.${CHR}.log"

    # Inform the user about the parameters
    echo "Starting pygskit gvcf_combiner for ${CHR} with the following parameters:"
    echo "  Path to GVCFs:      ${PATH_TO_GVCFS}"
    echo "  VDS Output Path:    ${VDS_OUTPUT_PATH}"
    echo "  Temporary Directory:${PATH_TMP}"
    echo "  Plan Save Path:     ${PATH_TO_SAVE_PLAN}"
    echo "  Driver Memory:      ${DRIVER_MEMORY}"
    echo "  Number of CPUs:     ${N_CPUS}"
    echo "  Reference Genome:   ${REFERENCE_GENOME}"
    echo "  Log File:           ${LOG_FILE}"

    # Execute the gvcf combiner (adjust the command as necessary)
    pygskit gvcf_combiner \
        --path-to-gvcfs "${PATH_TO_GVCFS}" \
        --vds-output-path "${VDS_OUTPUT_PATH}" \
        --tmp-path "${PATH_TMP}" \
        --save-path-plan "${PATH_TO_SAVE_PLAN}" \
        --driver-memory "${DRIVER_MEMORY}" \
        --n-cpus "${N_CPUS}" \
        --reference-genome "${REFERENCE_GENOME}" \
        &> "${LOG_FILE}"

    echo "$CHR processed. See ${LOG_FILE} for details."
done

echo "---------------------------------------"
echo "All chromosomes processed."