#!/bin/bash
#SBATCH --mem=5G
#SBATCH -J search

# =============================================================================
# Script:      search_datasets.sh
# Description: Searches the raw data directory for sequencing run directories
#              that meet the archival criteria, merges them with any previously
#              failed transfers, applies exclusion filters, and produces a
#              ranked dataset list file ready for submission as a SLURM array
#              job. The SLURM array upper bound and dataset list filename are
#              written to previous_batch_job.txt for consumption by automation.sh.
#
#              Dataset inclusion criteria:
#                1. Datasets older than AGE days (configured in config_file.txt)
#                2. Name does not start with "__caa"
#                3. Name does not contain "RERUN" (reruns are archived with
#                   their parent run)
#                4. Name does not contain "copy" or "Copy"
#                5. Name does not contain "test"
#                6. Not a symlink
#                7. Has at least 4 underscore-delimited fields (e.g.
#                   231101_VL00115_408_AAF2CKHM5)
#
#              Dataset exclusion filters (applied after discovery):
#                - Already successfully transferred
#                - Currently locked (being processed by another instance)
#                - Exceeded the maximum retry limit (3 attempts)
#
# Usage:       Submitted via sbatch by automation.sh — not run directly.
#              search_datasets.sh <APP_DIR> <IRODS_TRACK_DIR> <RAW_DATA_DIRECTORY>
#
# Arguments:
#   $1  APP_DIR              - Application root directory (contains config_file.txt)
#   $2  IRODS_TRACK_DIR      - Directory holding all iRODS tracking files
#   $3  RAW_DATA_DIRECTORY   - Root directory to search for sequencing runs
#
# SLURM directives:
#   --mem=5G   Memory for this job
#   -J search  Job name
#
# Config file variables expected (sourced from config_file.txt):
#   AGE  - Minimum age in days a directory must be to qualify for archiving
# =============================================================================


# -----------------------------------------------------------------------------
# Section 1: Argument Intake and Configuration
# Assign positional parameters to named variables and source the application
# config to obtain AGE and any other pipeline-wide settings.
# -----------------------------------------------------------------------------
APP_DIR="${1}"

IRODS_TRACK_DIR="${2}"

RAW_DATA_DIRECTORY="${3}"

SBATCH_LOGS="${IRODS_TRACK_DIR}/sbatch_logs"

# Timestamp used to make all output filenames unique per invocation
INSTANCE_SUFFIX=$(date +"%d-%m-%Y_%H%M%S")

source ${APP_DIR}/config_file.txt


# -----------------------------------------------------------------------------
# Section 2: Dataset Discovery
# Search RAW_DATA_DIRECTORY three levels deep for directories meeting all
# inclusion criteria. Results are written to a hidden timestamped file.
# The basename field-count check (>= 4 fields when split on "_") ensures only
# properly named run directories are included.
# -----------------------------------------------------------------------------

# Hidden temporary file — the dot prefix avoids it being picked up by later
# glob patterns that match datasets2process* files
DATASET_IDENTIFIED=".dataset_identified_list_${INSTANCE_SUFFIX}.txt"

find ${RAW_DATA_DIRECTORY}/*/*/ \
    -mindepth 1 -maxdepth 1 \
    -mtime +${AGE} \
    -type d \
    ! -name "__caa*" \
    ! -name "*RERUN" \
    ! -name "*copy*" \
    ! -name "*Copy*" \
    ! -name "*test*" \
    ! -type l | while read -r DIR; do
        BASE=$(basename "$DIR")
        # Validate run ID format: must have at least 4 underscore-delimited fields
        # e.g. 231101_VL00115_408_AAF2CKHM5 is valid; a plain directory name is not
        FIELD_COUNT=$(echo "$BASE" | awk -F'_' '{print NF}')
        if [ "$FIELD_COUNT" -ge 4 ]; then
            echo "$DIR"
        fi
    done > ${IRODS_TRACK_DIR}/${DATASET_IDENTIFIED}


# -----------------------------------------------------------------------------
# Section 3: Instance Number Determination
# Scan existing datasets2process list files to find the highest instance
# number used so far, then set the next instance number to one higher.
# This ensures each invocation produces a uniquely numbered output file.
# -----------------------------------------------------------------------------
INSTANCE_NUMBER=1

for FILE in $(ls ${IRODS_TRACK_DIR}/datasets2process*.list_* 2>/dev/null); do
    [[ -e "$FILE" ]] || continue                        # Skip if glob produced no match
    NUM="${FILE##*list_}"                               # Strip everything up to the last "list_"
    [[ "$NUM" =~ ^[0-9]+$ ]] || continue             # Only process pure integer suffixes
    (( NUM >= INSTANCE_NUMBER )) && INSTANCE_NUMBER=$(( NUM + 1 ))
done


# -----------------------------------------------------------------------------
# Section 4: Dataset List Generation
# Produce the final list of datasets to process by:
#   1. Merging newly discovered datasets with any previously failed transfers
#   2. Removing datasets already successfully transferred
#   3. Removing datasets currently locked by another running instance
#   4. Removing datasets that have exceeded the maximum retry count
#
# The resulting list is written to a uniquely named file. Each invocation
# produces its own list so that prior runs are not overwritten.
# -----------------------------------------------------------------------------
DATASETS_TO_PROCESS="datasets2process_${INSTANCE_SUFFIX}.list_${INSTANCE_NUMBER}"

cat ${IRODS_TRACK_DIR}/${DATASET_IDENTIFIED} \
    ${IRODS_TRACK_DIR}/failedtransfer_datasets.txt \
    | sort -u \
    | grep -vFf ${IRODS_TRACK_DIR}/successfultransfer_datasets.txt \
    | grep -vFf ${IRODS_TRACK_DIR}/locked_datasets.txt \
    | grep -vFf ${IRODS_TRACK_DIR}/max_retries_exceeded_failed_datasets.txt \
    > ${IRODS_TRACK_DIR}/${DATASETS_TO_PROCESS}


# -----------------------------------------------------------------------------
# Section 5: SLURM Array Bounds Calculation
# Count the non-blank lines in the dataset list to determine how many array
# tasks are needed. SUB_ARRAY_MAX is the zero-based upper index (count - 1),
# matching the --array=0-<N> syntax used by automation.sh. A value of -1
# means no datasets were found and automation.sh will exit early.
# -----------------------------------------------------------------------------
NO_OF_DATASETS=$(grep -cve '^\s*$' ${IRODS_TRACK_DIR}/${DATASETS_TO_PROCESS})
SUB_ARRAY_MAX=$((NO_OF_DATASETS - 1))


# -----------------------------------------------------------------------------
# Section 6: Batch Job State File
# Write the array bounds and dataset list filename to previous_batch_job.txt.
# automation.sh sources this file to configure the downstream array job
# submission and dependency chaining.
# -----------------------------------------------------------------------------
echo "SUB_ARRAY_MAX=${SUB_ARRAY_MAX}"               >  ${IRODS_TRACK_DIR}/previous_batch_job.txt
echo "DATASETS_TO_PROCESS=${DATASETS_TO_PROCESS}"   >> ${IRODS_TRACK_DIR}/previous_batch_job.txt


# -----------------------------------------------------------------------------
# Section 7: SLURM Log Rename
# Rename the generic SLURM output log to include the instance number, making
# it easier to correlate logs with their corresponding dataset list file.
#
# Before: search-<JOB_ID>.out
# After:  search-<JOB_ID>.out_<INSTANCE_NUMBER>
# -----------------------------------------------------------------------------
mv ${SBATCH_LOGS}/search-${SLURM_JOB_ID}.out \
   ${SBATCH_LOGS}/search-${SLURM_JOB_ID}.out_${INSTANCE_NUMBER}
