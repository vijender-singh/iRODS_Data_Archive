#!/bin/bash
# =============================================================================
# Script:      submit_irods_archive.sbatch
# Description: SLURM batch script executed as part of a job array. Each array
#              task picks one dataset from a provided list (by array task index),
#              runs the iRODS archive transfer for that dataset, then renames
#              the SLURM output log to include the dataset run ID for easier
#              identification.
#
# Usage:       Submitted via sbatch (typically by automation.sh), not run directly.
#              sbatch --array=0-<N>%<MAX> submit_irods_archive.sbatch \
#                  <path/to/datasets_list.txt> <path/to/sbatch_logs>
#
# Arguments:
#   $1  - Full path to the file containing one dataset path per line.
#         The array task index ($SLURM_ARRAY_TASK_ID) selects the line to process.
#   $2  - Directory where sbatch log files are written. The default log
#         (irods_transfer-<ARRAY_JOB_ID>-<TASK_ID>.out) is renamed to
#         <RUNID>-<ARRAY_JOB_ID>-<TASK_ID>.out after the transfer completes.
#
# Config file variables expected (sourced from config_file.txt in cwd):
#   APP_DIR  - Path to the application directory containing iRODS_archive.sh
# =============================================================================

# -----------------------------------------------------------------------------
# SLURM Directives
# -----------------------------------------------------------------------------
#SBATCH --mem=5G          # Memory allocated per task
#SBATCH -J start-%a       # Job name — %a is replaced with the array task ID


# -----------------------------------------------------------------------------
# Section 1: SLURM Job Diagnostics
# Print key SLURM environment variables to the job log for traceability.
# -----------------------------------------------------------------------------
echo "=========================================="
echo "SLURM JOB DETAILS"
echo "=========================================="
echo "Job ID:           $SLURM_JOB_ID"
echo "Array Job ID:     $SLURM_ARRAY_JOB_ID"
echo "Array Task ID:    $SLURM_ARRAY_TASK_ID"
echo "Job Name:         $SLURM_JOB_NAME"
echo "User:             $USER"
echo "Partition:        $SLURM_JOB_PARTITION"
echo "=========================================="
echo "NODE INFORMATION"
echo "=========================================="
echo "Node ID:          $SLURM_NODEID"
echo "Hostname:         $(hostname -f)"
echo "CPUs Allocated:   $SLURM_CPUS_ON_NODE"
echo "CPUs per Task:    $SLURM_CPUS_PER_TASK"
echo "Memory per Node:  $SLURM_MEM_PER_NODE MB"
echo "Working Directory:$SLURM_SUBMIT_DIR"
echo "=========================================="
echo "TIMING"
echo "=========================================="
echo "Submit Time: $(squeue -j $SLURM_JOB_ID -h -o %V 2>/dev/null || echo 'N/A')"
echo "Start Time:  $(date '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="
echo ""


# -----------------------------------------------------------------------------
# Section 2: Configuration
# Source variables from config_file.txt in the current working directory.
# Expects APP_DIR to be defined within it.
# -----------------------------------------------------------------------------

APP_DIR="${1}"

source $(APP_DIR)/config_file.txt


# -----------------------------------------------------------------------------
# Section 3: Input Resolution
# Receive arguments passed by the sbatch submission, then select the dataset
# assigned to this particular array task using the task index as a line offset.
# -----------------------------------------------------------------------------

# Full path to the file listing datasets to process (one path per line)
RUNIDs_WITH_FULL_PATH_FILE=$2

# Directory where sbatch output logs are stored
SBATCH_LOGS=$3

# Load all dataset paths from the list file into an array
readarray -t DATASETS < ${RUNIDs_WITH_FULL_PATH_FILE}

# Select the dataset assigned to this array task (0-indexed)
INPUT_DATASET=${DATASETS[$SLURM_ARRAY_TASK_ID]}

# Extract the run ID (basename of the dataset path) for use in logging
RUNID=$(basename ${INPUT_DATASET})


# -----------------------------------------------------------------------------
# Section 4: iRODS Archive Transfer
# Execute the archive script for the selected dataset, transferring all data.
# -----------------------------------------------------------------------------
sh ${APP_DIR}/iRODS_archive.sh ${INPUT_DATASET} --all


# -----------------------------------------------------------------------------
# Section 5: Log File Renaming
# Rename the generic SLURM output log to include the run ID, making it easier
# to identify which log belongs to which dataset after the job completes.
#
# Before: irods_transfer-<ARRAY_JOB_ID>-<TASK_ID>.out
# After:  <RUNID>-<ARRAY_JOB_ID>-<TASK_ID>.out
# -----------------------------------------------------------------------------
mv ${SBATCH_LOGS}/irods_transfer-${SLURM_ARRAY_JOB_ID}-${SLURM_ARRAY_TASK_ID}.out \
   ${SBATCH_LOGS}/${RUNID}-${SLURM_ARRAY_JOB_ID}-${SLURM_ARRAY_TASK_ID}.out

