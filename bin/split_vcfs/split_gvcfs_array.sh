#!/bin/bash

#SBATCH --job-name=split_vcfs_array
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=3-0
#SBATCH --output=slurm.%A_%a.out
#SBATCH --error=slurm.%A_%a.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=email@uol.de
#SBATCH --partition=rosa.p
#SBATCH --array=1-1005%50            # Array range for N VCFs in batches of 50

set -euo pipefail

module load hpc-env/13.1
module load BCFtools/1.18-GCC-13.1.0
module load BEDTools/2.31.1-GCC-13.1.0

unset TMPDIR
export TMPDIR=$WORK/tmp

# Input and output files/directories
INPUT_GVCF_FILES=(/path/to/*.g.vcf.gz) # Array of GVCF files
OUTPUT_DIR="/path/to/output"

# Create a temporary file list of all GVCF files using the array
FILE_LIST=$(mktemp)
printf '%s\n' "${INPUT_GVCF_FILES[@]}" > "$FILE_LIST"

# Get the file corresponding to this array task
GVCF_FILE=$(sed -n "${SLURM_ARRAY_TASK_ID}p" "$FILE_LIST")

if [[ ! -f "$GVCF_FILE" ]]; then
    echo "Error: File not found for task ${SLURM_ARRAY_TASK_ID}"
    rm "$FILE_LIST"
    exit 1
fi

base=$(basename "$GVCF_FILE")

# Define the list of chromosomes
chromosomes=("chr1" "chr2" "chr3" "chr4" "chr5" "chr6" "chr7" "chr8" "chr9" "chr10" \
             "chr11" "chr12" "chr13" "chr14" "chr15" "chr16" "chr17" "chr18" "chr19" "chr20" \
             "chr21" "chr22" "chrX" "chrY")

# split the GVCF file by chromosome
for chrom in "${chromosomes[@]}"; do
    # Create output directory for this chromosome
    mkdir -p "${OUTPUT_DIR}/${chrom}"

    # Construct output file name:
    # e.g., sample.g.vcf.gz becomes sample.chr1.g.vcf.gz in the chr1 subdirectory.
    OUTPUT_FILE="${OUTPUT_DIR}/${chrom}/${base%.g.vcf.gz}.${chrom}.g.vcf.gz"

    echo "Processing file: ${base} for ${chrom}..."

    # Filter for the chromosome and compress the output
    bcftools view -r "${chrom}" -O z -o "${OUTPUT_FILE}" "${GVCF_FILE}"

    # Indexing
    tabix -p vcf "${OUTPUT_FILE}"
done

rm "$FILE_LIST"
