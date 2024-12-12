#!/bin/bash

# Enable strict error handling
set -euo pipefail

# Function to display usage
display_usage() {
    echo "Usage: $0 -v input.vcf -r regions.bed -o output.tsv [-t temp_intersect.bed]"
    echo "Description: Processes a VCF file to a BED format and counts genotypes in specified regions."
    exit 1
}

# Function to parse and filter VCF
parse_and_filter_vcf() {
    local vcf_input_file=$1
    local bed_output_file=$2

    # Filter variants that have passed all filters and output as BED
    # Specific filter example: bcftools filter --threads 4 -i 'FORMAT/GQ >= 20 && FORMAT/DP >= 10' 
    bcftools view --threads 4 -f PASS "$vcf_input_file" | \
    bcftools norm --threads 4 -m -any | \
    bcftools query -f '%CHROM\t%POS\t%POS\t[%GT]\n' > "$bed_output_file"
}

# Function to count genotypes per gene
count_genotypes() {
    local query_bed=$1
    local region_bed=$2
    local output_file=$3
    local temp_intersect_file=${4:-}

    # Create a unique temporary sorted BED file for this job
    sorted_bed=$(mktemp)
    sort -k1,1 -k2,2n "$region_bed" > "$sorted_bed"

    # Intersect query BED with regions BED and count genotypes
    if [[ -n "$temp_intersect_file" ]]; then
        bedtools intersect -a "$query_bed" -b "$sorted_bed" -wa -wb > "$temp_intersect_file"
        intersect_file="$temp_intersect_file"
    else
        intersect_file=$(mktemp)
        bedtools intersect -a "$query_bed" -b "$sorted_bed" -wa -wb > "$intersect_file"
    fi

    awk '
    BEGIN {
       FS = "\t";  # Set the field separator to tab
       OFS = "\t";
    }
    {
       gene = $(NF-8);  # Gene name expected in 8th column from the end
       genotype = $4;   # Genotype expected in 4th column from the start
       gsub(/ /, "", genotype);  # Remove spaces from genotype

       # Count heterozygous genotypes
       if (genotype == "0/1") {
           het[gene]++;
       }

       # Count homozygous genotypes
       else if (genotype == "1/1") {
           hom[gene]++;
       }
       # Count total genotypes
       total[gene]++;
    }
    END {
        # Print header
        print "gene", "n_hets", "n_homs", "n_total";

        # Print counts for each gene
        for (gene in total) {
            print gene, (het[gene] ? het[gene] : 0), (hom[gene] ? hom[gene] : 0), total[gene];
        }
    }' "$intersect_file" > "$output_file"

    # Clean up sorted BED file
    # rm "${region_bed}.sorted"

    # Clean up temporary intersect file if not specified by user
    if [[ -z "$temp_intersect_file" ]]; then
        rm "$intersect_file"
    fi
}

# Parse command-line arguments
while getopts ":v:r:o:t:" opt; do
    case ${opt} in
        v) vcf_input="$OPTARG" ;;
        r) regions_bed="$OPTARG" ;;
        o) output_file="$OPTARG" ;;
        t) temp_intersect_file="$OPTARG" ;;
        *) display_usage ;;
    esac
done

# Check if required arguments are provided
if [[ -z "${vcf_input:-}" || -z "${regions_bed:-}" || -z "${output_file:-}" ]]; then
    display_usage
fi

# Generate intermediate BED file
bed_output="${output_file%.tsv}.bed"

echo "Parsing and filtering VCF to BED..."
parse_and_filter_vcf "$vcf_input" "$bed_output"
echo "BED file saved to $bed_output."

echo "Counting genotypes..."
count_genotypes "$bed_output" "$regions_bed" "$output_file" "${temp_intersect_file:-}"
echo "Genotype counts saved to $output_file."
