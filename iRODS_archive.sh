#!/bin/bash
# =============================================================================
# Script:      iRODS_archive.sh
# Description: Orchestrates the full iRODS archive pipeline for a single
#              sequencing run dataset. Depending on the flags provided, it
#              submits one or more SLURM jobs covering:
#                - Tarballing and metadata collection
#                - Checksum generation
#                - iRODS transfer
#              Each stage is submitted as a separate SLURM job and polled to
#              completion before the next stage begins. Transfer outcomes are
#              recorded in universal tracking and statistics files.
#
# Usage:
#   ./iRODS_archive.sh <DATA_DIR_PATH> [--all | --tarball | --chksum | --archive | --archive_only] [--dryrun] [--debug]
#
# Arguments:
#   <DATA_DIR_PATH>   Absolute path to the RUN_DIR under the raw_data directory.
#                     Ignored when --dryrun is set (a preset path is used instead).
#
# Options:
#   --all           Run tarballing, metadata collection, checksum, and iRODS transfer
#   --tarball       Run tarballing, metadata collection, and checksum
#   --chksum        Run checksum generation only
#   --archive       Run checksum and iRODS transfer
#   --archive_only  Run iRODS transfer only (use only if checksums are already up to date)
#   --dryrun        Use a preset run directory and simulate the pipeline without real transfers
#   --debug         Enable detailed process output for debugging
#
# Examples:
#   ./iRODS_archive.sh /hpscol02/tenant1/ngsservice/raw_data/miniseq/MN01572/240730_MN01572_0250_A000H7CC3K \
#       --all
#
#   ./iRODS_archive.sh --dryrun --all
#
#   ./iRODS_archive.sh /hpscol02/tenant1/ngsservice/raw_data/miniseq/MN01572/240730_MN01572_0250_A000H7CC3K \
#       --tarball --chksum
#
# Config file variables expected (sourced from config_file.txt):
#   PROCESS_DIR                   - Base directory for per-run processing directories
#   ARCHIVE_LOGS_DIR              - Base directory for archive tracking logs
#   SLURM_PARTITION               - SLURM partition to submit jobs to
#   PSWD                          - Credential passed to the iRODS transfer script
# =============================================================================

#set -x


# -----------------------------------------------------------------------------
# Section 1: Default Flag Initialisation
# All pipeline stage flags default to false and are enabled by argument parsing.
# -----------------------------------------------------------------------------
RUN_ALL=false
RUN_TARBALL=false
RUN_CHKSUM=false
RUN_ARCHIVE=false
DRYRUN=false
RUN_ARCHIVE_ONLY=false
DEBUG=false


# -----------------------------------------------------------------------------
# Section 2: Usage Message
# Printed when arguments are missing or invalid.
# -----------------------------------------------------------------------------
usage() {
    echo " "
    echo "Usage: $0 <DATA_DIR_PATH> < --all | --tarball |--chksum | --archive > [--dryrun] [--debug]"
    echo " "
    echo -e "Options:"
    echo -e "  <DATA_DIR_PATH>  Absolute path to RUN_DIR under raw_data directory (ignored if --dryrun is set)"
    echo -e "  --all            Run the tarballing, checksum and archive stages in order of mention"
    echo -e "  --tarball        Run the tarballing and checksum stages together in order of mention"
    echo -e "  --chksum         Run the checksum stage alone"
    echo -e "  --archive        Run the checksum and archive stages together in order of mention"
    echo -e "  --archive_only   Run the archive stage only (Run only if sure of checksums in file are uptodate)"
    echo -e "  --dryrun         Use preset RUN_DIR and simulate dry-run mode given in examples below"
    echo -e "  --debug          A detailed output of the processes to debug the code/process."
    echo
    echo -e "Examples:"
    echo -e "  $0 /hpscol02/tenant1/ngsservice/raw_data/miniseq/MN01572/240730_MN01572_0250_A000H7CC3K \ "
    echo -e "       --all"
    echo -e " "
    echo -e "  $0 --dryrun --all"
    echo -e " "
	echo -e "  $0 /hpscol02/tenant1/ngsservice/raw_data/miniseq/MN01572/240730_MN01572_0250_A000H7CC3K \ "
    echo -e "      --tarball --chksum"
    exit 1
}


# -----------------------------------------------------------------------------
# Section 3: Environment and Configuration
# Source credentials and application configuration before any processing.
# TRANSFER_STATS_COLLECTION enables collection of per-run timing statistics.
# -----------------------------------------------------------------------------
APP_DIR=/home/phe.gov.uk/vijender.singh/irods_archive_and_retrieval
source /home/phe.gov.uk/vijender.singh/.irods_credential
source ${APP_DIR}/config_file.txt

TRANSFER_STATS_COLLECTION=true

# Record the overall process start time (epoch seconds) for elapsed time calculations
PROCESS_START_TIME=$(date +%s)


# -----------------------------------------------------------------------------
# Section 4: Argument Parsing
# Separate flag arguments from positional arguments. Flags set the pipeline
# stage booleans; the first positional argument is the dataset path.
# -----------------------------------------------------------------------------
if [ $# -lt 1 ]; then
    usage
fi

POSITIONAL=()
for arg in "$@"; do
    case "$arg" in
        --all)          RUN_ALL=true ;;
        --tarball)      RUN_TARBALL=true ;;
        --chksum)       RUN_CHKSUM=true ;;
        --archive)      RUN_ARCHIVE=true ;;
        --dryrun)       DRYRUN=true ;;
        --archive_only) RUN_ARCHIVE_ONLY=true ;;
        --debug)        DEBUG=true ;;
        -*)
            echo "Unknown option: $arg"
            usage
            ;;
        *) POSITIONAL+=("$arg") ;;
    esac
done

# Restore positional parameters after flag extraction
set -- "${POSITIONAL[@]}"

# Re-source config after positional restore (ensures any env-dependent vars are set)
source ${APP_DIR}/config_file.txt


# -----------------------------------------------------------------------------
# Section 5: Run Directory Resolution
# Set the dataset path, run ID, processing directory, and run status depending
# on whether this is a dry run or a real run.
# -----------------------------------------------------------------------------
if $DRYRUN; then
    # Use a fixed preset dataset path for simulation purposes
    RAW_DATA_DIR_PATH="/hpscol02/tenant1/ngsservice/raw_data/miniseq/MN01572/240730_MN01572_0250_A000H7CC3K"
    RUN_ID=$(basename ${RAW_DATA_DIR_PATH})
    RUN_PROCESS_DIR="${PROCESS_DIR}/MN01572/240730_MN01572_0250_A000H7CC3K_dryRun"
    RUN_STATUS="dryrun"
else
    if [ -z "$1" ]; then
        echo "Error: RUN_DIR not provided."
        usage
	elif [ -z "$2" ]; then
		echo "Error: RUN type is not specified. Provide one of --all, --tarball, --chksum, --archive"
		usage
	elif [ ! -d "${1}" ]; then
		echo "ERROR: Directory not found: ${1}" >&2
    	exit 1
    fi
    RAW_DATA_DIR_PATH="$1"
    RUN_ID=$(basename ${RAW_DATA_DIR_PATH})
    RUN_PROCESS_DIR="${PROCESS_DIR}"/${RUN_ID}
    RUN_STATUS="True-Run"
fi

# Create the per-run processing directory if it does not exist
mkdir -p ${RUN_PROCESS_DIR}

# -----------------------------------------------------------------------------
# Section 6: Log File Setup
# Define paths for the metadata log, master log, and per-attempt log file.
# Each invocation for the same run ID creates a new numbered log file
# (e.g. <RUN_ID>.log_1, <RUN_ID>.log_2, <RUN_ID>.log_3).
# A maximum of 3 attempts is enforced before the run is flagged as exceeded.
# -----------------------------------------------------------------------------
META_LOG=${RUN_PROCESS_DIR}/${RUN_ID}.metadata

MASTER_LOG_FILE=${RUN_PROCESS_DIR}/${RUN_ID}.masterlog

LOG_FILE_PREFIX="${RUN_ID}.log"

# Determine the next attempt number by inspecting existing numbered log files
MOST_RECENT_LOG_N=$(ls ${RUN_PROCESS_DIR}/${LOG_FILE_PREFIX}_* 2>/dev/null \
    | sed -E 's/.*_([0-9]+)$/\1/' | sort -n | tail -1)

if [ -z "$MOST_RECENT_LOG_N" ]; then
    ARCHIVE_ATTEMPT=1
else
    ARCHIVE_ATTEMPT=$((MOST_RECENT_LOG_N + 1))
fi

# Enforce the maximum retry limit before creating the new log file
if (( ARCHIVE_ATTEMPT > 3 )); then
    echo "ERROR: Maximum archive attempts (3) exceeded. Exiting." >> ${LOG_FILE}
    echo "${RAW_DATA_DIR_PATH}" >> ${ARCHIVE_LOGS_DIR}/irods_archive_logs/max_retries_exceeded_failed_datasets.txt
    exit 1
fi

LOG_FILE="${RUN_PROCESS_DIR}/${LOG_FILE_PREFIX}_${ARCHIVE_ATTEMPT}"


# -----------------------------------------------------------------------------
# Section 7: Shared File Path Definitions
# Paths to files shared across stages and used for tracking and statistics.
# -----------------------------------------------------------------------------

# Checksum file produced by the checksum stage and consumed by the archive stage
CHKSUM_FILE="${RUN_PROCESS_DIR}/source_chksum_${RUN_ID}.txt"

# Universal log recording transfer SUCCESS/FAILED status for all run IDs
UNIVERSAL_TRANSFER_LOG_FILE="${PROCESS_DIR}"/Universal_transfer_log.txt

# Universal statistics file recording timing and size metrics per run
UNIVERSAL_TRANSFER_STAT_FILE="${PROCESS_DIR}"/Universal_transfer_stat.txt


# -----------------------------------------------------------------------------
# Section 8: Tracking File Initialisation
# Ensure required tracking and log files exist before any stage is run.
# If the universal log file is new, write its header row.
# Any previous FAILED record for this run ID is removed so the run can be
# retried cleanly.
# -----------------------------------------------------------------------------

#test -f ${UNIVERSAL_TRANSFER_LOG_FILE} || touch ${UNIVERSAL_TRANSFER_LOG_FILE}
test -f ${META_LOG}  || touch ${META_LOG}
test -f ${LOG_FILE}  || touch ${LOG_FILE}

# Create universal transfer log with header if it does not already exist
if ! test -f "${UNIVERSAL_TRANSFER_LOG_FILE}"; then
    touch "${UNIVERSAL_TRANSFER_LOG_FILE}"
    echo -e "RUNID \t TRANSFER_STATUS" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
fi

# Remove any prior record for this run ID to allow a fresh attempt
sed -i "/^${RUN_ID}/d" ${UNIVERSAL_TRANSFER_LOG_FILE}


# -----------------------------------------------------------------------------
# Section 9: Pre-run Summary
# Print key configuration values to stdout for operator visibility.
# -----------------------------------------------------------------------------
echo -e "APP DIRECTORY :        ${APP_DIR}
    RAW_DATA directory :   ${RAW_DATA_DIR_PATH}
    PROCESSING directory : ${RUN_PROCESS_DIR}
    METADATA file :        ${META_LOG}
    PROCESS_LOG file :     ${LOG_FILE}
    RUN_TYPE :             ${RUN_STATUS}
    SLURM PARTITION:       ${SLURM_PARTITION}
    DEBUG :                ${DEBUG}"


# -----------------------------------------------------------------------------
# Section 10: Duplicate Run Guard
# If this run ID already has a SUCCESS entry in the universal transfer log,
# skip all processing and exit cleanly.
# -----------------------------------------------------------------------------
STATUS_RUN_ID=$(awk -v id="${RUN_ID}" '$1 == id {print $2; exit}' "${UNIVERSAL_TRANSFER_LOG_FILE}")

if [ "${STATUS_RUN_ID}" = "SUCCESS" ]; then
    echo "[ ALERT ] THIS RUNID IS PROCESSED SUCCESSFULLY IN PREVIOUS ATTEMPTS" >> ${LOG_FILE}
    exit 0
fi


# -----------------------------------------------------------------------------
# Section 11: Stage Label Resolution
# Build a human-readable string describing which stages will execute.
# Used in the log file header written below.
# -----------------------------------------------------------------------------
if $RUN_ALL; then
    STAGES_TO_RUN="tarballing, metadata_collection, checksum, irods_transfer"
elif $RUN_TARBALL; then
    STAGES_TO_RUN="tarballing, metadata_collection, checksum"
elif $RUN_ARCHIVE; then
    STAGES_TO_RUN="checksum, irods_transfer"
elif $RUN_ARCHIVE_ONLY; then
    STAGES_TO_RUN="irods_transfer"
elif $RUN_CHKSUM; then
    STAGES_TO_RUN="checksum"
fi

# Write the run header block to the attempt log file
{
echo -e "\n\n ¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬¬ "
echo -e "\n ============= START iRODS archive  process  | $(date '+%Y-%m-%d %H:%M:%S') ==========\n"
echo -e "  PROCESSES STEPS TO EXECUTE : ${STAGES_TO_RUN} "
echo -e "  Attempt number : ${ARCHIVE_ATTEMPT}"
echo -e "\n ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ \n"
} >> ${LOG_FILE}


# -----------------------------------------------------------------------------
# Section 12: Pipeline Stage Functions
# Each stage submits a SLURM job and polls until it reaches a terminal state.
# Polling uses sacct with a 30-second sleep interval between checks.
# -----------------------------------------------------------------------------

# --- Tarballing Stage ---
# Submits the tarball and metadata collection job, then waits for completion.
tarball_stage() {
    JID1=$(sbatch --parsable \
        -p ${SLURM_PARTITION} \
        --mem=5G \
        -J TARBALL-main \
        -o ${RUN_PROCESS_DIR}/${RUN_ID}-%j.out \
        ${APP_DIR}/scripts/tarball_metadata_collection.sh \
            ${RAW_DATA_DIR_PATH} ${RUN_PROCESS_DIR} ${META_LOG} ${LOG_FILE} \
            ${RUN_STATUS} ${SLURM_PARTITION} ${APP_DIR} ${DEBUG} \
            ${UNIVERSAL_TRANSFER_LOG_FILE} ${TRANSFER_STATS_COLLECTION})
    echo $JID1
    sleep 15
    while true; do
        JOB_STATE=$(sacct -n -X -o state -j "$JID1" | grep -Ev '^\s*$' | head -1 | xargs)
        #echo "stage 1 $JID1 job status : $JOB_STATE"
        case "$JOB_STATE" in
            COMPLETED)
                echo "Job $JID1 completed successfully."
                break
                ;;
            PENDING|RUNNING|CONFIGURING|COMPLETING)
                # echo "Job is still running... checking again in 2 minutes."
                sleep 30
                ;;
            FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED)
                echo "Job $JID1 failed or ended with error state: $JOB_STATE"
                exit 1
                ;;
            *)
                echo "Unexpected or unknown job state in main script $JOB_STATE"
                exit 1
                ;;
        esac
    done
}

# --- Checksum Stage ---
# Truncates the checksum file, submits the checksum job, then waits for completion.
checksum_stage() {
    #CHKSUM_FILE="${RUN_PROCESS_DIR}/source_chksum_${RUN_ID}.txt"
    >${CHKSUM_FILE}
    JID2=$(sbatch --parsable \
        -p ${SLURM_PARTITION} \
        -J CHKSUM \
        --mem=5G \
        -o ${RUN_PROCESS_DIR}/${RUN_ID}_chksum.log \
        ${APP_DIR}/scripts/source_chksum.sh \
            ${RUN_ID} ${RUN_PROCESS_DIR} ${CHKSUM_FILE})
    sleep 15
    while true; do
        JOB_STATE=$(sacct -n -X -o state -j "$JID2" | grep -Ev '^\s*$' | head -1 | xargs)
        case "$JOB_STATE" in
            COMPLETED)
                echo "Job $JID2 completed successfully."
                break
                ;;
            PENDING|RUNNING|CONFIGURING|COMPLETING)
                # echo "Job is still running... checking again in 2 minutes."
                sleep 30
                ;;
            FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED)
                echo "Job $JID2 failed or ended with error state: $JOB_STATE"
                exit 1
                ;;
            *)
                echo "Unexpected or unknown job state checksum: $JOB_STATE"
                exit 1
                ;;
        esac
    done
}

# --- Archive Stage ---
# Submits the iRODS transfer job. In dryrun mode, appends a 'debug' argument
# to the job script to prevent real data being written to iRODS.
archive_stage() {
    if [ "${RUN_STATUS}" = "dryrun" ]; then
        JID3=$(sbatch --parsable \
            -p ${SLURM_PARTITION} \
            --mem=5G \
            -J iRODS_transfer \
            -o ${RUN_PROCESS_DIR}/${RUN_ID}_iRODS.log \
            ${APP_DIR}/scripts/copy_2_iRODS.sh \
                ${META_LOG} ${RUN_PROCESS_DIR} ${LOG_FILE} ${CHKSUM_FILE} \
                ${UNIVERSAL_TRANSFER_LOG_FILE} ${PROCESS_START_TIME} ${PSWD} \
                ${APP_DIR} ${RAW_DATA_DIR_PATH} debug)
    else
        JID3=$(sbatch --parsable \
            -p ${SLURM_PARTITION} \
            --mem=5G \
            -J iRODS_transfer \
            -o ${RUN_PROCESS_DIR}/${RUN_ID}_iRODS.log \
            ${APP_DIR}/scripts/copy_2_iRODS.sh \
                ${META_LOG} ${RUN_PROCESS_DIR} ${LOG_FILE} ${CHKSUM_FILE} \
                ${UNIVERSAL_TRANSFER_LOG_FILE} ${PROCESS_START_TIME} ${PSWD} \
                ${APP_DIR} ${RAW_DATA_DIR_PATH})
    fi
    sleep 15
    while true; do
        JOB_STATE=$(sacct -n -X -o state -j "$JID3" | grep -Ev '^\s*$' | head -1 | xargs)
        case "$JOB_STATE" in
            COMPLETED)
                echo "Job $JID3 completed successfully."
                break
                ;;
            PENDING|RUNNING|CONFIGURING|COMPLETING)
                # echo "Job is still running... checking again in 2 minutes."
                sleep 30
                ;;
            FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED)
                echo "Job $JID3 failed or ended with error state: $JOB_STATE"
                exit 1
                ;;
            *)
                echo "Unexpected or unknown job state checksum: $JOB_STATE"
                exit 1
                ;;
        esac
    done
}


# -----------------------------------------------------------------------------
# Section 13: Pipeline Execution
# Call the appropriate stage functions based on the flags provided.
# For --archive_only, validate that a non-empty checksum file exists first.
# -----------------------------------------------------------------------------
if $RUN_ALL; then
    tarball_stage
    checksum_stage
    archive_stage
elif $RUN_TARBALL; then
    tarball_stage
    checksum_stage
elif $RUN_ARCHIVE; then
    checksum_stage
    archive_stage
elif $RUN_ARCHIVE_ONLY; then
    # Guard: require a populated checksum file before attempting archive-only transfer
    if [ ! -s "${CHKSUM_FILE}" ] || [ -z "$(tr -d ' \t\n\r' < "${CHKSUM_FILE}")" ]; then
        echo "ERROR :   Required Checksum file is either missing or is empty."
        echo "          The file searched and found missing or empty is ${CHKSUM_FILE}"
        echo "FIX   :   To create checksum file and run archive please use --archive flag instead."
        exit 1
    else
        archive_stage
    fi
elif $RUN_CHKSUM; then
    checksum_stage
fi


# -----------------------------------------------------------------------------
# Section 14: Master Log Consolidation
# Append the per-attempt log to the cumulative master log for this run ID.
# -----------------------------------------------------------------------------
cat ${LOG_FILE} >> ${MASTER_LOG_FILE}


# -----------------------------------------------------------------------------
# Section 15: Final Status Update (iRODS Main Tracking)
# Poll the universal transfer log until a SUCCESS or FAILED status line appears
# for this run ID (written by the archive stage job). On success, record the
# dataset path in the appropriate tracking file and remove the locked entry.
# Retries up to MAX_RETRIES_STATUS_UPDATE times with a 30-second sleep.
# -----------------------------------------------------------------------------
FINAL_LOGS_UPDATED=false
MAX_RETRIES_STATUS_UPDATE=50
ATTEMPT_STATUS_UPDATE=0

while [[ $FINAL_LOGS_UPDATED == false ]]; do
    (( ATTEMPT_STATUS_UPDATE++ ))

    # Abort if the status line has not appeared within the retry limit
    if (( ATTEMPT_STATUS_UPDATE > MAX_RETRIES_STATUS_UPDATE )); then
        echo "ERROR: Max retries reached to update status in iRODS tracking. Check 'IRODS MAIN TRACKING' code section in iRODS_archive.sh Exiting." >&2
        # Release the locked state so the dataset can be retried in a future run
        sed -i "\|${RAW_DATA_DIR_PATH}|d" ${ARCHIVE_LOGS_DIR}/irods_archive_logs/locked_datasets.txt
        exit 1
    fi

    UNIVERSAL_LOG_STATUS_LINE=$(grep "^${RUN_ID}" ${UNIVERSAL_TRANSFER_LOG_FILE})

    if [[ -z "$UNIVERSAL_LOG_STATUS_LINE" ]]; then
        # Status not yet written by the archive job — wait and retry
        sleep 30
    else
        # Status is available — record outcome and release lock
        FINAL_RUN_STATUS=$(echo "$UNIVERSAL_LOG_STATUS_LINE" | awk '{print $2}' | tr -d ' ')

        if [[ "$FINAL_RUN_STATUS" == "FAILED" ]]; then
            echo "${RAW_DATA_DIR_PATH}" >> ${ARCHIVE_LOGS_DIR}/irods_archive_logs/failedtransfer_datasets.txt
        else
            echo "${RAW_DATA_DIR_PATH}" >> ${ARCHIVE_LOGS_DIR}/irods_archive_logs/successfultransfer_datasets.txt
        fi

        sed -i "\|${RAW_DATA_DIR_PATH}|d" ${ARCHIVE_LOGS_DIR}/irods_archive_logs/locked_datasets.txt
        FINAL_LOGS_UPDATED=true
    fi
done


# -----------------------------------------------------------------------------
# Section 16: Transfer Statistics Collection
# If TRANSFER_STATS_COLLECTION is enabled, append per-run timing and size
# metrics to the universal statistics file. The stats are sourced from a
# .transfer.stats file produced alongside the attempt log.
# Columns: RUN_ID, UNCOMPRESSED_DATA_BYTES, UNCOMPRESSED_DATA_h, TARBALL_TIME,
#          TAR_DIR_SIZE_BYTES, TAR_DIR_SIZE_HUMAN, IROD_TRANSFER_TIME,
#          IRODS_TRANSFER_SPEED, TIME_TAKEN_ENTIRE_PROCESS
# -----------------------------------------------------------------------------
if ${TRANSFER_STATS_COLLECTION}; then
    # Create the statistics file with a header row if it does not yet exist
    if [ ! -f "${UNIVERSAL_TRANSFER_STAT_FILE}" ]; then
        touch ${UNIVERSAL_TRANSFER_STAT_FILE}
        echo -e "RUN_ID \t UNCOMPRESSED_DATA_BYTES \t UNCOMPRESSED_DATA_h \t TARBALL_TIME \t TAR_DIR_SIZE_BYTES \t TAR_DIR_SIZE_HUMAN \t IROD_TRANSFER_TIME \t IRODS_TRANSFER_SPEED \t TIME_TAKEN_ENTIRE_PROCESS" >> ${UNIVERSAL_TRANSFER_STAT_FILE}
    fi

    # Derive the stats file path from the log file path
    DATA_TRANSFER_STAT_FILE=$(echo ${LOG_FILE} | sed 's/.log/.transfer.stats/')

    # Source the stats variables, then append them as a new row
    source ${DATA_TRANSFER_STAT_FILE}
    echo -e "${RUN_ID} \t ${UNCOMPRESSED_DATA_BYTES} \t ${UNCOMPRESSED_DATA_h} \t ${TARBALL_TIME} \t ${TAR_DIR_SIZE_BYTES} \t ${TAR_DIR_SIZE_HUMAN} \t ${IROD_TRANSFER_TIME} \t ${IRODS_TRANSFER_SPEED} \t ${TIME_TAKEN_ENTIRE_PROCESS}" >> ${UNIVERSAL_TRANSFER_STAT_FILE}
fi
