#!/bin/bash

#SBATCH --job-name=concat_vcfs
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=24G
#SBATCH --time=3-0
#SBATCH --output=slurm.%A_%a.out
#SBATCH --error=slurm.%A_%a.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=email@uol.de
#SBATCH --partition=rosa.p

# This script concatenates and sorts the filtered chromosome VCFs.
# The chromosome VCFs are expected to have the same set of samples in the same order.
# The chromosome VCFs are expected to be sorted by position.

set -euo pipefail

module load hpc-env/13.1
module load BCFtools/1.18-GCC-13.1.0

# Base directories
BASE_DIR="${GROUPDSS}/KOL_UOL/projects/1000_CHD_WGS/joint_genotyping"

# Input and output directories
FILTERED_DIR="${BASE_DIR}/processed/filtered_vcfs"
CONCAT_DIR="${BASE_DIR}/processed/concat_vcf"
CONCAT_VCF="${CONCAT_DIR}/1k_cohort.concatenated.filtered.vcf.gz"

mkdir -p "${CONCAT_DIR}"

chromosomes=({1..22} X Y)

# List of input files ordered by chromosome
input_files=()
for chr in "${chromosomes[@]}"; do
    vcf="${FILTERED_DIR}/1k_cohort.chr${chr}.sorted.filtered.vcf.gz"
    if [[ ! -f "${vcf}" ]]; then
        echo "ERROR: Missing file ${vcf}" >&2
        exit 1
    fi
    input_files+=("${vcf}")
done

# Concatenate already sorted VCFs in correct chromosome order
echo "$(date): Concatenating filtered chromosome VCFs in correct order..."
bcftools concat -a "${input_files[@]}" --threads 8 -Oz -o "${CONCAT_VCF}"

# Index the concatenated VCF
if [[ -f "${CONCAT_VCF}" ]]; then
    echo "$(date): Indexing concatenated VCF..."
    bcftools index -tf "${CONCAT_VCF}"
    echo "$(date): Concatenation completed successfully."
else
    echo "$(date): ERROR - File ${CONCAT_VCF} not found. Exiting." >&2
    exit 1
fi