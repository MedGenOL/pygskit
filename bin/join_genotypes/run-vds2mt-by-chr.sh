#!/bin/bash

set -euo pipefail

# Usage:
#   ./run_vds2mt_converter.sh
#
# This script converts VDS files to dense MatrixTables (MT) using pygskit vds2mt for each chromosome.
#
# Options (common for all chromosomes):
#   BASE_VDS_INPUT_PATH: Directory containing per-chromosome VDS files. [required]
#   BASE_MT_OUTPUT_PATH: Base output directory for the resulting MatrixTables. [required]
#   DRIVER_MEMORY: Memory allocation for the Spark driver. [default: 8g]
#   N_CPUS: Number of CPUs for local computation. [default: 4]
#   REFERENCE_GENOME: Reference genome to use (e.g., GRCh37, GRCh38). [default: GRCh38]
#
# Logs for each chromosome are saved to a file named "vds2mt_converter.<CHR>.log".

# Define common parameters
BASE_VDS_INPUT_PATH="/mnt/nfs/work/1k_joint"   # VDS(s) input directory
BASE_MT_OUTPUT_PATH="/mnt/nfs/work/1k_joint_mt"  # Output directory for MatrixTables
DRIVER_MEMORY="512g"
N_CPUS=120
REFERENCE_GENOME="GRCh38"

mkdir -p "${BASE_MT_OUTPUT_PATH}"

# Define the list of chromosomes to process.
chromosomes=("chr1" "chr2" "chr3" "chr4" "chr5" "chr6" "chr7" "chr8" "chr9" "chr10" "chr11" "chr12" "chr13" \
             "chr14" "chr15" "chr16" "chr17" "chr18" "chr19" "chr20" "chr21" "chr22" "chrX" "chrY")

# Iterate over each chromosome
for CHR in "${chromosomes[@]}"; do
    echo "---------------------------------------"
    echo "Processing $CHR ..."

    # Define chromosome-specific paths
    VDS_INPUT_PATH="${BASE_VDS_INPUT_PATH}/1k_cohort.${CHR}.vds"
    MT_OUTPUT_PATH="${BASE_MT_OUTPUT_PATH}/1k_cohort.${CHR}.mt"
    LOG_FILE="vds2mt_converter.${CHR}.log"

    # Inform the user about the parameters
    echo "Starting pygskit vds2mt for ${CHR} with the following parameters:"
    echo "  VDS Input Path:     ${VDS_INPUT_PATH}"
    echo "  MatrixTable Output: ${MT_OUTPUT_PATH}"
    echo "  Driver Memory:      ${DRIVER_MEMORY}"
    echo "  Number of CPUs:     ${N_CPUS}"
    echo "  Reference Genome:   ${REFERENCE_GENOME}"
    echo "  Log File:           ${LOG_FILE}"

    # Execute the vds2mt converter
    pygskit vds2mt \
        --vds-path "${VDS_INPUT_PATH}" \
        --output-path "${MT_OUTPUT_PATH}" \
        --driver-memory "${DRIVER_MEMORY}" \
        --n-cpus "${N_CPUS}" \
        --annotate-adjusted-gt \
        --reference-genome "${REFERENCE_GENOME}" \
        --overwrite \
        &> "${LOG_FILE}"

    echo "$CHR processed. See ${LOG_FILE} for details."
done

echo "---------------------------------------"
echo "All chromosomes processed."
