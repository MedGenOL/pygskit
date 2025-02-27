#!/bin/bash
set -euo pipefail

# Hail's parameters in local mode
DRIVER_MEMORY="512g"
N_CPUS=120
REFERENCE_GENOME="GRCh38"

# chromosomes to process
CHROMS=("chr1" "chr2" "chr3" "chr4" "chr5" "chr6" "chr7" "chr8" "chr9" "chr10" "chr11" "chr12" \
        "chr13" "chr14" "chr15" "chr16" "chr17" "chr18" "chr19" "chr20" "chr21" "chr22" "chrX" "chrY")

for CHR in "${CHROMS[@]}"; do
    # Define input and log file paths based on the current chromosome
    MT_DIR="/mnt/nfs/work/1k_joint_mts/1k_cohort.${CHR}.mt"
    VCF_PATH="/mnt/nfs/work/1k_joint_vcfs/1k_cohort.${CHR}.vcf.bgz"
    LOG_FILE="mt2vcf.${CHR}.log"

    echo "---------------------------------------"
    echo "Running mt2vcf for ${CHR} with the following parameters:"
    echo "  MT_DIR:           ${MT_DIR}"
    echo "  VCF_PATH:         ${VCF_PATH}"
    echo "  DRIVER_MEMORY:    ${DRIVER_MEMORY}"
    echo "  N_CPUS:           ${N_CPUS}"
    echo "  REFERENCE_GENOME: ${REFERENCE_GENOME}"
    echo "  Log file:         ${LOG_FILE}"
    echo "---------------------------------------"

    # Run the mt2vcf command
    pygskit mt2vcf \
        -mt "${MT_DIR}" \
        -vcf "${VCF_PATH}" \
        --filter-adj-genotypes \
        --split-multi \
        --min-ac 1 \
        -dm "${DRIVER_MEMORY}" \
        -nc "${N_CPUS}" \
        -rg "${REFERENCE_GENOME}" \
        &> "${LOG_FILE}"

    echo "Job completed for ${CHR}. See ${LOG_FILE} for details."
done
