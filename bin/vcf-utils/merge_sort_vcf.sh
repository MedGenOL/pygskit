#!/bin/bash

#SBATCH --job-name=merge_sort_vcfs
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=3-0
#SBATCH --output=slurm.%A_%a.out
#SBATCH --error=slurm.%A_%a.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=email@uol.de
#SBATCH --partition=rosa.p

module load hpc-env/13.1
module load BCFtools/1.18-GCC-13.1.0

# Define base directory
BASE_DIR="${GROUPDSS}/KOL_UOL/projects/1000_CHD_WGS/joint_genotyping"

FILTERED_DIR="${BASE_DIR}/processed/filtered_vcfs"
MERGED_DIR="${BASE_DIR}/processed/merged_vcf"
MERGED_VCF="${MERGED_DIR}/1k_cohort.merged.filtered.sorted.vcf.gz"

mkdir -p "${MERGED_DIR}"

echo "Merging filtered chromosome VCFs..."
bcftools merge "${FILTERED_DIR}"/1k_cohort.chr*.filtered.vcf.gz \
         --threads 4 -Oz -o "${MERGED_DIR}/1k_cohort.merged.filtered.vcf.gz"

echo "Sorting merged VCF..."
bcftools sort --threads 4 "${MERGED_DIR}/1k_cohort.merged.filtered.vcf.gz" -Oz -o "${MERGED_VCF}"

echo "Indexing sorted merged VCF..."
bcftools index "${MERGED_VCF}"

echo "Merged, sorted, and indexed VCF ready: ${MERGED_VCF}"