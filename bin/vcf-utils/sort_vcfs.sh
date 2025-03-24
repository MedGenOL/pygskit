#!/bin/bash

#SBATCH --job-name=sort_vcfs
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=3-0
#SBATCH --output=slurm.%A_%a.out
#SBATCH --error=slurm.%A_%a.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=email@uol.de
#SBATCH --partition=rosa.p
#SBATCH --array=1-24


# This script sorts the samples and positions of multiple chromosome VCFs.
# The samples are sorted according to a reference sample file created beforehand.
# All chromosome VCFs are expected to have the same set of samples.

set -euo pipefail

# Load required modules
module load hpc-env/13.1
module load BCFtools/1.18-GCC-13.1.0

# Define chromosomes array
CHROMOSOMES=( {1..22} X Y )
CHR=${CHROMOSOMES[$((SLURM_ARRAY_TASK_ID-1))]}

# Define base directory
BASE_DIR="${GROUPDSS}/KOL_UOL/projects/1000_CHD_WGS/joint_genotyping"

# Input and output directories
INPUT_DIR="${BASE_DIR}/raw/1k_joint_vcfs"
SORTED_DIR="${BASE_DIR}/processed/sorted_vcfs"

mkdir -p "${SORTED_DIR}"

# Input VCF
INPUT_VCF="${INPUT_DIR}/1k_cohort.chr${CHR}.vcf.bgz"
SORTED_VCF="${SORTED_DIR}/1k_cohort.chr${CHR}.sorted.vcf.gz"

# Reference samples order (create this beforehand from chr1)
SAMPLES_ORDER_FILE="${BASE_DIR}/processed/samples_reference.txt"

if [[ -f "${INPUT_VCF}" && -f "${SAMPLES_ORDER_FILE}" ]]; then

    echo "Sorting samples for chromosome ${CHR}..."
    # Sort samples
    bcftools view "${INPUT_VCF}" --samples-file "${SAMPLES_ORDER_FILE}" --force-samples -Oz -o "${SORTED_DIR}/chr${CHR}.samples_sorted.vcf.gz" --threads 4
    bcftools index "${SORTED_DIR}/chr${CHR}.samples_sorted.vcf.gz"

    echo "Sorting positions for chromosome ${CHR}..."
    # Sort positions
    bcftools sort "${SORTED_DIR}/chr${CHR}.samples_sorted.vcf.gz" -Oz -o "${SORTED_VCF}"
    bcftools index -tf "${SORTED_VCF}"

    # Remove intermediate file
    rm "${SORTED_DIR}/chr${CHR}.samples_sorted.vcf.gz"*

    echo "Sorting completed for chromosome ${CHR}."

else
    echo "Error: Input VCF or samples reference file missing for chromosome ${CHR}, exiting."
    exit 1
fi
