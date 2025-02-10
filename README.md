## Analysis of 1000 CHD Genomes (1K_WGS_CHD)

### Project Overview

This repository contains scripts, data, and notebooks for the analysis of 1000 genomes from patients with Congenital
Heart Disease (CHD), sequenced using whole-genome sequencing (WGS). The goal of this project is to identify genetic
variants associated with CHD and to perform a comprehensive structural variant characterization and association analysis.

The raw sequencing data used in this analysis is available at the European Genome-Phenome Archive (EGA) under the 
accession number EGAXXXXXXXXXXX. Please note that access to the data may be restricted and require appropriate 
permissions.

### Project Structure

This project is divided into several key steps, each of which is documented in the corresponding script or Jupyter notebook:

    Alignment and Variant Calling
        Performed using the nf-core/sarek pipeline, a community-developed pipeline for analysis of whole-genome 
            sequencing data. This step includes:
            Alignment of the raw sequencing reads to the reference genome.
            Variant calling for single nucleotide variants (SNVs) and small insertions/deletions (INDELs).

    Joint Genotyping and Quality Control
        Genotyping across all samples to create a unified call set.
        Application of quality control (QC) filters to ensure the reliability of the variant calls, including filtering
        based on depth, genotype quality, and variant quality score recalibration.

    Structural Variant Characterization
        Characterization of structural variants (SVs), including large deletions, duplications, inversions, and 
        translocations.
        The pipeline includes both read-pair and split-read based approaches for identifying structural variation.

    Association Analysis
        Conducting association studies between the identified variants and congenital heart disease phenotypes.
        Statistical models such as logistic regression or mixed-effects models will be used to assess the significance
        of each variant in relation to CHD.

### Repository Contents

The repository is organized into the following folders to maintain a clean structure and reproducible workflow:

    data/:
        raw/: Raw sequencing data and other unprocessed files.
        processed/: Processed data including genotypes, QC-filtered variants, and structural variant call files.
        samplesheets/: Metadata, phenotypic information, and other sample-related information (CHD diagnosis).

    notebooks/: Jupyter notebooks that contain detailed steps for data exploration, cleaning, statistical analysis, 
                    and visualization. The notebooks are structured in the order of the analysis workflow.

    scripts/: Python scripts that handle data processing, variant calling, and statistical analysis. These are 
                modular and reusable across different stages of the project.

    results/:
        figures/: Generated plots, graphs, and other visual representations of the data.
        tables/: Summary tables of the results, including variant counts, QC statistics, and association results.
        reports/: Final reports, summaries, or PDFs generated for presentation or publication purposes.

    logs/: Log files capturing the execution of pipelines and scripts, useful for debugging and tracking the workflow.

    tests/: Scripts and unit tests that ensure the correct functionality of the data processing pipeline 
            and analysis code.


### Installing the Required Dependencies via Conda

```bash
git clone https://github.com/MedGenOL/pygskit
cd pygskit
conda env create -f environment.yml
conda activate pygskit
```


