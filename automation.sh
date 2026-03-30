#!/bin/bash
# =============================================================================
# Script:      automation.sh
# Description: Automates the iRODS archive transfer pipeline by:
#                1. Loading required environment modules
#                2. Validating input and sourcing configuration
#                3. Initialising tracking files for transfer state management
#                4. Submitting a SLURM dataset search job and waiting for completion
#                5. Submitting a SLURM array job for iRODS archive transfers,
#                   optionally chaining it as a dependency on a previously running job
#
# Usage:       ./automation.sh <path/to/application/dir>
#
# Arguments:
#   $1  - Path to the application directory (required).
#         Must contain a config_file.txt and the scripts/ subdirectory.
#
# Config file variables expected (sourced from <APP_DIR>/config_file.txt):
#   ARCHIVE_LOGS_DIR              - Base directory for archive logs
#   PHENGS_RAW_DATA               - Root directory to search for raw datasets
#   SLURM_PARTITION               - SLURM partition to submit jobs to
#   MAX_IRODS_ARCHIVE_INSTANCES   - Max parallel tasks in the SLURM array job
#   DATASETS_TO_PROCESS           - Filename (not full path) of the dataset list
#                                   produced by the search job
#   SUB_ARRAY_MAX                 - Upper index bound for the SLURM array job
#                                   (set by the search job via previous_batch_job.txt)
# =============================================================================

# -----------------------------------------------------------------------------
# Section 1: Environment Setup
# Load required HPC modules before any processing begins.
# -----------------------------------------------------------------------------
module use /hpscol02/tenant1/ngsservice/modulefiles/
module load phe/phengs-environment


# -----------------------------------------------------------------------------
# Section 2: Argument Validation
# Ensure the application directory path has been supplied.
# -----------------------------------------------------------------------------
#set -e

APP_DIR="$1"

if [ -z "$APP_DIR" ]; then
    echo "Error: Path to application directory is not included as first and only argument"
    echo "USAGE: ./automation.sh <path/to/application/dir>"
    exit 1
fi


# -----------------------------------------------------------------------------
# Section 3: Configuration
# Source variables from the application config file, then derive paths used
# throughout the rest of the script.
# -----------------------------------------------------------------------------
source ${APP_DIR}/config_file.txt

# Top-level directory for all iRODS archive tracking logs (sourced from config)
IRODS_TRACK_DIR="${ARCHIVE_LOGS_DIR}/irods_archive_logs"

# Directory for sbatch stdout/stderr log files
SBATCH_LOGS="${ARCHIVE_LOGS_DIR}/irods_archive_logs/sbatch_logs"

# Directory to search for datasets to transfer
RAW_DATA_DIRECTORY="${PHENGS_RAW_DATA}"

# Ensure the sbatch log directory exists
mkdir -p ${SBATCH_LOGS}


# -----------------------------------------------------------------------------
# Section 4: Tracking File Initialisation
# Create master tracking files if they do not already exist.
# These files persist state across script invocations.
# -----------------------------------------------------------------------------

# Records all datasets where a transfer attempt has failed
[ ! -f ${IRODS_TRACK_DIR}/failedtransfer_datasets.txt ] && \
    touch ${IRODS_TRACK_DIR}/failedtransfer_datasets.txt

# Records all datasets that have been transferred successfully
[ ! -f ${IRODS_TRACK_DIR}/successfultransfer_datasets.txt ] && \
    touch ${IRODS_TRACK_DIR}/successfultransfer_datasets.txt

# Datasets currently being processed are added here to prevent duplicate submission
[ ! -f ${IRODS_TRACK_DIR}/locked_datasets.txt ] && \
    touch ${IRODS_TRACK_DIR}/locked_datasets.txt

# Datasets that have exhausted the maximum number of retry attempts
[ ! -f ${IRODS_TRACK_DIR}/max_retries_exceeded_failed_datasets.txt ] && \
    touch ${IRODS_TRACK_DIR}/max_retries_exceeded_failed_datasets.txt

# Find jobid of previous archive instance and assign it to DEPENDENCY variable
if [[ -f "${IRODS_TRACK_DIR}/previous_batch_job.txt" ]]; then
    DEPENDENCY=$(grep -oP '(?<=DEPENDENCY=)\S+' "${IRODS_TRACK_DIR}/previous_batch_job.txt" 2>/dev/null || echo "NOJOB")
fi

# -----------------------------------------------------------------------------
# Section 5: Dataset Search Job
# Submit a SLURM job to search for datasets to process, then poll until it
# reaches a terminal state before continuing.
# -----------------------------------------------------------------------------

# Submit the search job and capture its job ID
SEARCH_JOB_ID=$(sbatch --parsable \
    -p ${SLURM_PARTITION} \
    -o ${SBATCH_LOGS}/search-%A.out \
    ${APP_DIR}/scripts/search_datasets.sh \
    ${APP_DIR} \
    ${IRODS_TRACK_DIR} \
    ${RAW_DATA_DIRECTORY})

echo "Search job id : ${SEARCH_JOB_ID}"

# Give SLURM a moment to register the job before polling begins
sleep 30

# Poll the search job until it reaches a terminal state
while true; do
    JOB_STATE=$(sacct -n -X -o state -j "$SEARCH_JOB_ID" | grep -Ev '^\s*$' | head -1 | xargs)

    case "$JOB_STATE" in
        COMPLETED)
            echo "Job $SEARCH_JOB_ID completed successfully."
            break
            ;;
        PENDING|RUNNING|CONFIGURING|COMPLETING)
            # Job is still active — wait before checking again
            # echo "Job is still running... checking again in 2 minutes."
            sleep 30
            ;;
        FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED)
            echo "Job $SEARCH_JOB_ID failed or ended with error state: $JOB_STATE"
            exit 1
            ;;
        *)
            echo "Unexpected or unknown job state: $JOB_STATE"
            exit 1
            ;;
    esac
done


# -----------------------------------------------------------------------------
# Section 6: Dependency Resolution
# Check whether a previous transfer array job is still running.
# If so, the new array job will be chained to run only after it completes.
# The previous_batch_job.txt file is written by this script at the end of each run
# to persist the job ID across invocations.
# -----------------------------------------------------------------------------

# Source variables written by the search job (e.g. SUB_ARRAY_MAX, DATASETS_TO_PROCESS)
source ${IRODS_TRACK_DIR}/previous_batch_job.txt

# Check for a previous job dependency recorded in the tracking file

# Determine whether the previously recorded job is still active in the queue
if squeue -j ${DEPENDENCY} &>/dev/null 2>&1; then
    DEPENDENCY_RUNNING=true
else
    DEPENDENCY_RUNNING=false
fi

# Debug output (temporary — prefixed with #test)
#echo "SUB_ARRAY_MAX= ${SUB_ARRAY_MAX}"                          #test
#echo "DEPENDENCY=${DEPENDENCY} ; Status: ${DEPENDENCY_RUNNING}" #test
#echo "File name for list of datasets = ${DATASETS_TO_PROCESS}"  #test


# -----------------------------------------------------------------------------
# Section 7: Pre-submission Guard
# Exit early if the search job found no datasets to process.
# -----------------------------------------------------------------------------
if (( SUB_ARRAY_MAX < 0 )); then
    echo "No datasets to process...EXITING"
    exit 1
fi

cat ${IRODS_TRACK_DIR}/${DATASETS_TO_PROCESS} >> ${IRODS_TRACK_DIR}/locked_datasets.txt

#exit 1 #test exit


# -----------------------------------------------------------------------------
# Section 8: iRODS Archive Transfer Job Submission
# Submit a SLURM array job to perform the iRODS transfers.
# If a previous transfer job is still running, submit with a dependency so
# this job only starts after the prior one completes successfully.
# The resulting job ID is saved to previous_batch_job.txt for the next invocation.
# -----------------------------------------------------------------------------
if [[ -f "${IRODS_TRACK_DIR}/previous_batch_job.txt" && $DEPENDENCY_RUNNING == true ]]; then
    # Chain onto the previous job — only start after it completes successfully
    JOBID_ARCHIVE_ARRAY=$(sbatch --parsable \
        --dependency=afterany:${DEPENDENCY} \
        -p ${SLURM_PARTITION} \
        --array=0-${SUB_ARRAY_MAX}%${MAX_IRODS_ARCHIVE_INSTANCES} \
        -o ${SBATCH_LOGS}/irods_transfer-%A-%a.out \
        ${APP_DIR}/submit_irods_archive.sbatch \
		${APP_DIR} \
        ${IRODS_TRACK_DIR}/${DATASETS_TO_PROCESS} \
        "${SBATCH_LOGS}")
else
    # No prior job running — submit independently
    JOBID_ARCHIVE_ARRAY=$(sbatch --parsable \
        --array=0-${SUB_ARRAY_MAX}%${MAX_IRODS_ARCHIVE_INSTANCES} \
        -p ${SLURM_PARTITION} \
        -o ${SBATCH_LOGS}/irods_transfer-%A-%a.out \
        ${APP_DIR}/submit_irods_archive.sbatch \
		${APP_DIR} \
        ${IRODS_TRACK_DIR}/${DATASETS_TO_PROCESS} \
        "${SBATCH_LOGS}")
fi

# Persist the new job ID so the next invocation can chain from it if needed
echo "DEPENDENCY=${JOBID_ARCHIVE_ARRAY}" > ${IRODS_TRACK_DIR}/previous_batch_job.txt

# -----------------------------------------------------------------------------
# Section 9: Release datasets that were part of this instance of archiving from hold list.
# This is to ensure that if array job fails due to some technical reasons, datasets donot remain in hold list.
# Under regular operations the datasets are removed from hold list once the dataset specific archive job is over (irrespective of success or failure)
#------------------------------------------------------------------------------


UNIQ_SUFFIX=$(echo ${DATASETS_TO_PROCESS} | cut -d. -f2)
# Make sure all locked datasets of above array job  are released after array job is finished, successfully or not.
sbatch -p ${SLURM_PARTITION} \
	-o ${SBATCH_LOGS}/LOCK_REMOVED.out_${UNIQ_SUFFIX} \
	-J UNLOCK_${UNIQ_SUFFIX} \
	--dependency=afterany:${JOBID_ARCHIVE_ARRAY} \
	--mem=2G \
	${APP_DIR}/scripts/submit_releasehold.sbatch \
	${DATASETS_TO_PROCESS} \
	${IRODS_TRACK_DIR}/locked_datasets.txt \
	${IRODS_TRACK_DIR}

# -----------------------------------------------------------------------------

