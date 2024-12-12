#!/bin/bash

#SBATCH --job-name=vcf_genotype_count
#SBATCH --nodes=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --time=3-0
#SBATCH --output=slurm.%A_%a.out
#SBATCH --error=slurm.%A_%a.err
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=<uol.mail>
#SBATCH --partition=rosa.p
#SBATCH --array=1-1005%50            # Array range for N VCFs in batches of 50

module load hpc-env/13.1
module load BCFtools/1.18-GCC-13.1.0
module load BEDTools/2.31.1-GCC-13.1.0

unset TMPDIR
export TMPDIR=$WORK/tmp

# Variables
VCF_LIST=$1      # File with a list of VCF paths (one per line)
BED_FILE=$2      # BED file with region coordinates
SCRIPT_PATH=$3   # Path to the count_genotypes_per_regions.sh script
OUTPUT_DIR=$4    # Output directory

mkdir -p "$OUTPUT_DIR"

# Get the VCF file for this array task
VCF_FILE=$(sed -n "$((SLURM_ARRAY_TASK_ID))p" "$VCF_LIST")

# Check if the VCF file exists
if [[ ! -f "$VCF_FILE" ]]; then
    echo "Error: VCF file $VCF_FILE does not exist or is inaccessible."
    exit 1
fi

# Define output file for this VCF
BASENAME=$(basename "$VCF_FILE" ".deepvariant.vcf.gz")
OUTPUT_PREFIX="${OUTPUT_DIR}/${BASENAME}_${SLURM_ARRAY_TASK_ID}"

# Run the genotype counting script for this VCF
bash "$SCRIPT_PATH" -v "$VCF_FILE" -r "$BED_FILE" -o "${OUTPUT_PREFIX}.tsv"

# Check if the output was successfully created
if [[ ! -f "${OUTPUT_PREFIX}.tsv" ]]; then
    echo "Error: Output file ${OUTPUT_PREFIX}.tsv not created."
    exit 1
fi

echo "Processed VCF: $VCF_FILE"
