#!/bin/bash

set -euo pipefail

# Usage:
#   ./run_mts_combiner.sh
#
# This script combines multiple MatrixTable directories into a single MatrixTable
# using the 'pygskit mts_combiner' command.
#
# Required environment variables:
#   MTS_DIR:         Directory containing the MatrixTable directories.
#   OUTPUT_PATH:     Path to save the combined MatrixTable.
#   COMBINE_BY:      Method to combine the MatrixTables ('rows' or 'cols').
#
# Optional parameters:
#   N_PARTITIONS:    Number of partitions to use when writing the combined MatrixTable. [default: 200]
#   DRIVER_MEMORY:   Memory allocation for the Spark driver. [default: 8g]
#   N_CPUS:          Number of CPUs for local computation. [default: 4]
#   REFERENCE_GENOME:Reference genome to use (e.g., GRCh37, GRCh38). [default: GRCh38]
#
# Logs are saved to a file defined by LOG_FILE.

# Define common parameters (modify these as necessary)
MTS_DIR="/mnt/nfs/work/1k_joint_mt"
OUTPUT_PATH="/mnt/nfs/work/1k_cohort_all.mt"
COMBINE_BY="rows"
N_PARTITIONS=4000
DRIVER_MEMORY="512g"
N_CPUS=120
REFERENCE_GENOME="GRCh38"
LOG_FILE="mts_combiner.log"

echo "---------------------------------------"
echo "Running MatrixTable combiner with the following parameters:"
echo "  MTS_DIR:          ${MTS_DIR}"
echo "  OUTPUT_PATH:      ${OUTPUT_PATH}"
echo "  COMBINE_BY:       ${COMBINE_BY}"
echo "  N_PARTITIONS:     ${N_PARTITIONS}"
echo "  DRIVER_MEMORY:    ${DRIVER_MEMORY}"
echo "  N_CPUS:           ${N_CPUS}"
echo "  REFERENCE_GENOME: ${REFERENCE_GENOME}"
echo "  Log file:         ${LOG_FILE}"
echo "---------------------------------------"

# Run the mts_combiner command and redirect output to the log file
pygskit mts_combiner \
    -i "${MTS_DIR}" \
    -o "${OUTPUT_PATH}" \
    --overwrite \
    --combine-by "${COMBINE_BY}" \
    -np "${N_PARTITIONS}" \
    -dm "${DRIVER_MEMORY}" \
    -nc "${N_CPUS}" \
    -rg "${REFERENCE_GENOME}" \
    &> "${LOG_FILE}"

echo "MatrixTable combination complete. See ${LOG_FILE} for details."
