#!/bin/bash

bam_list="$1"
output_dir="$2"

if [ ! -f "$bam_list" ]; then
    echo "BAM file list not found: $bam_list"
    exit 1
fi

if [ ! -d "$output_dir" ]; then
    mkdir -p "$output_dir" || { echo "Failed to create output directory: $output_dir"; exit 1; }
fi

convert_to_cram() {
    bam_file="$1"
    output_dir="$2"
    out_file="$output_dir"/"$(basename "${bam_file%.bam}.cram")"
    samtools view -C -T /fs/s6k/groups/agmedgen/KOL_UOL/reference_GRCh38/Homo_sapiens/GATK/GRCh38/Sequence/WholeGenomeFasta/Homo_sapiens_assembly38.fasta -o "$out_file" "$bam_file"
    echo "Converted $bam_file to $out_file"
}

export -f convert_to_cram

parallel -a "$bam_list" -j8 convert_to_cram {} "$output_dir"
~                                                            
