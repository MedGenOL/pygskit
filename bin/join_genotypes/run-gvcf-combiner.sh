#!/bin/bash
set -euo pipefail

# Usage: ./run_gvcf_combiner.sh
#
# Combine GVCFs into a VDS using Hail's gvcf combiner.
#
# Options:
#   PATH_TO_GVCFS:      Directory with gvcf files and their corresponding .tbi files. [required]
#   VDS_OUTPUT_PATH:    Output path for the resulting VDS file. [required]
#   TMP_PATH:           Temporary directory with enough space for intermediate files. [required]
#   DRIVER_MEMORY:      Memory allocation for the Spark driver. [default: 256g]
#   N_CPUS:             Number of CPUs for local computation. [default: 90]
#   REFERENCE_GENOME:   Reference genome to use (e.g., GRCh37, GRCh38). [default: GRCh38]
#   LOG_FILE:           Log file to capture the output.

# Define parameters
PATH_TO_GVCFS="/mnt/nfs/KOL_UOL/projects/CHD_1000WGS/variant_calling/gvcfs"
VDS_OUTPUT_PATH="/mnt/nfs/tmp/1k_joint/1k_cohort.vds"
TMP_PATH="/mnt/nfs/tmp"
DRIVER_MEMORY="256g"
N_CPUS=90
REFERENCE_GENOME="GRCh38"
LOG_FILE="gvcf_combiner.log"

# Inform the user about the parameters
echo "Starting pygskit gvcf_combiner with the following parameters:"
echo "  Path to GVCFs:      ${PATH_TO_GVCFS}"
echo "  VDS Output Path:    ${VDS_OUTPUT_PATH}"
echo "  Temporary Path:     ${TMP_PATH}"
echo "  Driver Memory:      ${DRIVER_MEMORY}"
echo "  Number of CPUs:     ${N_CPUS}"
echo "  Reference Genome:   ${REFERENCE_GENOME}"
echo "  Log file:           ${LOG_FILE}"

# Run the command in the background using nohup
nohup pygskit gvcf_combiner \
    --path-to-gvcfs "${PATH_TO_GVCFS}" \
    --vds-output-path "${VDS_OUTPUT_PATH}" \
    --tmp-path "${TMP_PATH}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --n-cpus "${N_CPUS}" \
    --reference-genome "${REFERENCE_GENOME}" \
    > "${LOG_FILE}" 2>&1 &

echo "pygskit gvcf_combiner started in background."
echo "Log file: ${LOG_FILE}"
