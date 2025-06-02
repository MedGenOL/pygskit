#!/bin/bash
# Script to convert BAM files to CRAM format
# Usage: ./bam2cram.sh <bam_list_file> <output_directory> [num_threads] [reference_genome]

# Enable strict error handling
set -euo pipefail

# Check for required tools
for cmd in samtools parallel; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: $cmd is required but not found on PATH"
        exit 1
    fi
done

# Default reference genome path
DEFAULT_REFERENCE="$GROUPDSS/KOL_UOL/reference_GRCh38/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta"

# Validate command-line arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <bam_list_file> <output_directory> [num_threads] [reference_genome]"
    echo "  bam_list_file:    A text file with one BAM file path per line"
    echo "  output_directory: Directory where CRAM files will be saved"
    echo "  num_threads:      Number of parallel jobs (default: 8)"
    echo "  reference_genome: Path to reference genome (default: $DEFAULT_REFERENCE)"
    exit 1
fi

bam_list="$1"
output_dir="$2"
num_threads="${3:-8}"  # Use 8 threads by default if not specified
ref_genome="${4:-$DEFAULT_REFERENCE}"  # Use default reference genome if not specified

# Validate input files
if [ ! -f "$bam_list" ]; then
    echo "BAM file list not found: $bam_list"
    exit 1
fi

# Create output directory if it doesn't exist
if [ ! -d "$output_dir" ]; then
    mkdir -p "$output_dir" || { echo "Failed to create output directory: $output_dir"; exit 1; }
fi

# Check if reference genome exists
if [ ! -f "$ref_genome" ]; then
    echo "Reference genome not found: $ref_genome"
    echo "Please provide a valid reference genome file."
    exit 1
fi

# Validate BAM files exist before starting conversion
echo "Validating BAM files..."
invalid_files=0
while IFS= read -r bam_file; do
    if [ ! -f "$bam_file" ]; then
        echo "Warning: BAM file not found: $bam_file"
        invalid_files=$((invalid_files + 1))
    fi
done < "$bam_list"

if [ $invalid_files -gt 0 ]; then
    echo "Error: $invalid_files BAM files not found. Please check your input list."
    exit 1
fi

# Count total number of files for progress tracking
total_files=$(wc -l < "$bam_list")
echo "Starting conversion of $total_files BAM files to CRAM format..."
echo "Using reference genome: $ref_genome"
echo "Using $num_threads parallel jobs"

convert_to_cram() {
    bam_file="$1"
    output_dir="$2"
    ref_genome="$3"
    out_file="$output_dir"/"$(basename "${bam_file%.bam}.cram")"

    # Skip if output file already exists
    if [ -f "$out_file" ]; then
        echo "Skipping $bam_file - output file already exists"
        return 0
    fi

    # Do the conversion
    if samtools view -C -T "$ref_genome" -o "$out_file" "$bam_file"; then
        echo "Converted $bam_file to $out_file"
        return 0
    else
        echo "Error converting $bam_file"
        return 1
    fi
}

export -f convert_to_cram
export ref_genome

# Start time for tracking
start_time=$(date +%s)

# Run conversion with progress bar
parallel --bar -j "$num_threads" convert_to_cram {} "$output_dir" "$ref_genome" :::: "$bam_list"

# Calculate and display execution time
end_time=$(date +%s)
duration=$((end_time - start_time))
echo "Conversion completed in $duration seconds"

# Verify all files were converted
echo "Verifying all files were converted..."
converted_count=$(find "$output_dir" -name "*.cram" | wc -l)
echo "$converted_count out of $total_files files converted to CRAM format"

if [ "$converted_count" -ne "$total_files" ]; then
    echo "Warning: Not all files were converted. Please check the log for errors."
    exit 1
else
    echo "All files successfully converted!"
    exit 0
fi