#!/bin/bash

#SBATCH --job-name=filter_vcf
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

# This script filters variants applying different criteria to each chromosome VCF.
# The criteria used here as example is: INFO/AC >= 10.

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
INPUT_DIR="${BASE_DIR}/processed/sorted_vcfs"
FILTERED_DIR="${BASE_DIR}/processed/filtered_vcfs"

mkdir -p "${FILTERED_DIR}"

# Filter variants with INFO/AC >= 10 for each chromosome
INPUT_VCF="${INPUT_DIR}/1k_cohort.chr${CHR}.sorted.vcf.gz"
FILTERED_VCF="${FILTERED_DIR}/1k_cohort.chr${CHR}.sorted.filtered.vcf.gz"

if [[ -f "${INPUT_VCF}" ]]; then
    echo "Filtering chromosome ${CHR}..."
    bcftools view -i 'INFO/AC>=10' --threads 4 -Oz -o "${FILTERED_VCF}" "${INPUT_VCF}"
    bcftools index -tf "${FILTERED_VCF}"
else
    echo "Error: File ${INPUT_VCF} not found, exiting."
    exit 1
fi

echo "Filtering completed for chromosome ${CHR}."
