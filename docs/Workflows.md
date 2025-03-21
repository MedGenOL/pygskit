### Use case 1: From individual gvcfs to a cohort-level VCF.

[Diagram of the workflow here]

This is a simple workflow that takes a list of individual gvcfs and combines them into a single cohort-level VCF.

The workflow consists of the following steps:
1. combine sample-level gvcf into a VDS (Variant Dataset) using the gvcf_combiner tool.
2. convert the VDS to a dense MatrixTable using the vds2mt tool.
3. convert the MatrixTable to a cohort VCF using the mt2vcf tool.


### Use case 2: Ancestry inference.

[Diagram of the workflow here]

The pipeline takes a cohort-level VCF and infers the ancestry of each sample using the 1000 Genomes Project data.

The workflow consists of the following steps:
1. convert the cohort-level VCF to a MatrixTable using the vcf2mt tool.
2. convert the 1000 Genomes Project data to a MatrixTable using the vcf2mt tool.
3. overlap and keep high quality variants between the cohort and 1000 Genomes Project data.
4. run PCA on the combined MatrixTable.
5. run HDBSCAN on the PCA results to cluster samples into populations.

### Use case 3: Gene burden testing in a case-control setting.

### Use case 4: Gene-set enrichment analysis in a case-control setting.

