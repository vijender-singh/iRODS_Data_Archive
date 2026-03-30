#!/bin/bash
# =============================================================================
# Script:      tarball_metadata_collection.sh
# Description: Discovers all directories associated with a sequencing run ID
#              across the raw_data, machine_fastqs, run_data, and results data
#              stores. For each discovered directory it submits a SLURM tarball
#              job via SUBMIT_TARBALL_PROCESS. Optionally submits a directory
#              size collection job. Once all tarball jobs complete, a collation
#              job merges the per-tarball step logs into the master log file.
#
#              Directories prefixed with "__caa_" are excluded throughout.
#              Tarballed objects are prefixed with their data-store tag so that
#              iRODS objects can be distinguished:
#                machine_fastqs-*   run_data-*   results-*   raw_data-*
#
# Usage:       Called by iRODS_archive.sh via sbatch — not run directly.
#              tarball_metadata_collection.sh \
#                  <RAW_DATA_DIR_PATH> \    ($1)
#                  <RUN_PROCESS_DIR>   \    ($2)
#                  <META_LOG>          \    ($3)
#                  <LOG_FILE>          \    ($4)
#                  <RUN_STATUS>        \    ($5)  "dryrun" triggers debug/dry mode
#                  <SLURM_PARTITION>   \    ($6)
#                  <APP_DIR>           \    ($7)
#                  <DEBUG>             \    ($8)
#                  <UNIVERSAL_TRANSFER_LOG_FILE> \ ($9)
#                  <TRANSFER_STATS_COLLECTION>     ($10) true/false
#
# Dependencies:
#   ${SCRIPT_DIR}/logging.sh   - Provides log_message, timer, timer2,
#                                human_readable_size helper functions
#   SUBMIT_TARBALL_PROCESS     - Function (sourced from logging.sh or similar)
#                                that submits a single tarball SLURM job
#   SUBMIT_SIZE_COLLECTION     - Function that submits a directory-size job
#   GET_RUN_INDEXES            - Function that returns indexset IDs for a run
#   ${SCRIPT_DIR}/meta_collector.sh - Collects run metadata into META_LOG
# =============================================================================

set -x


# -----------------------------------------------------------------------------
# Section 1: Argument Intake
# Positional parameters are assigned to named variables for clarity.
# Parameters $1-$4 are dataset/processing paths; $5 controls dry-run mode;
# $6-$10 are runtime configuration values.
# -----------------------------------------------------------------------------
RAW_DATA_DIR_PATH=$1        # Absolute path to the sequencing run directory
RUN_PROCESS_DIR=$2          # Temp working directory for tarballs and logs
META_LOG=$3                 # Metadata log file path (written by this script)
LOG_FILE=$4                 # Master log file for this archive attempt
                            # $5 (RUN_STATUS) evaluated in Section 3 below
SLURM_PARTITION=$6          # SLURM partition for all job submissions
APP_DIR=$7                  # Application root directory
DEBUG=$8                    # Debug flag (overridden to "debug" in dry-run mode)
UNIVERSAL_TRANSFER_LOG_FILE=$9   # Shared transfer status tracking file
TRANSFER_STATS_COLLECTION=${10}  # "true" to collect directory size statistics


# -----------------------------------------------------------------------------
# Section 2: Environment Setup
# Source helper functions and initialise shared state before any processing.
# -----------------------------------------------------------------------------
SCRIPT_DIR=${APP_DIR}/scripts
source ${SCRIPT_DIR}/logging.sh

# Accumulates all directories tarballed for this run (used for size reporting)
DIR_LIST_ASSOCIATED_WITH_RUNID=()

# Record the stage start time for elapsed-time reporting at the end
START_TIME_1=$(date +%s)


# -----------------------------------------------------------------------------
# Section 3: Dry-run Mode Detection
# If RUN_STATUS ($5) is "dryrun", override DEBUG and set DRY_RUN flag so that
# SUBMIT_TARBALL_PROCESS and the archive stage simulate rather than transfer.
# -----------------------------------------------------------------------------
if [ "${5}" = "dryrun" ]; then
    DEBUG="debug"
    DRY_RUN="true"
    set -x
else
    DEBUG="No debug: Actual instance"
    DRY_RUN="FALSE"
fi


# -----------------------------------------------------------------------------
# Section 4: Tarball Configuration
# Following pararmeters are sourced from config file
# CPU cores allocated to each tar job.
# Memory allocated to each tar SLURM job
# Split tarballs that exceed SPLIT_TARBALL_SIZE
# Maximum size per tarball split part
# -----------------------------------------------------------------------------
source ${APP_DIR}/config_file.txt

# -----------------------------------------------------------------------------
# Section 5: Directory Path Derivation
# From the raw_data run path, derive sibling paths for machine_fastqs,
# run_data, and results using simple string substitution.
# -----------------------------------------------------------------------------

# Extract the run ID (leaf directory name) and its parent path
RUN_ID=$(basename ${RAW_DATA_DIR_PATH})
DIR_PATH=$(dirname ${RAW_DATA_DIR_PATH})

# Sibling data-store root directories for this instrument/run
MACHINE_FASTQ_PATH=$(sed 's/raw_data/machine_fastqs/' <<< $DIR_PATH)
RUN_DATA_PATH=$(sed 's/raw_data/run_data/' <<< $DIR_PATH)
RESULTS_PATH=$(echo ${RAW_DATA_DIR_PATH} | sed 's|raw_data|_|' | cut -d"_" -f1)"results"


# -----------------------------------------------------------------------------
# Section 6: Processing Directory and Log Setup
# Ensure the working directory exists and create subdirectories for tar logs
# and generated sbatch scripts.
# -----------------------------------------------------------------------------
mkdir -p ${RUN_PROCESS_DIR}

TAR_LOGS=${RUN_PROCESS_DIR}/tarlogs
TAR_SCRIPTS_DIR=${RUN_PROCESS_DIR}/tarlogs/tar_scripts
mkdir -p ${TAR_LOGS} ${TAR_SCRIPTS_DIR}


# -----------------------------------------------------------------------------
# Section 7: Metadata Collection
# Write the run ID to the metadata log, log all resolved paths, then call
# meta_collector.sh to populate the metadata log with indexset and workflow info.
# -----------------------------------------------------------------------------

# Seed the metadata log with the run ID
echo "RUN_ID=${RUN_ID}" > ${META_LOG}

log_message "STARTING" "RUNID : ${RUN_ID}"
#log_message "INFO" "CPUs used in pigz : ${SLURM_CPUS_PER_TASK}"
log_message "INFO" "Process directory path        : ${RUN_PROCESS_DIR}"
log_message "INFO" "raw_data directory path       : ${RAW_DATA_DIR_PATH}"
log_message "INFO" "machine_fastqs directory path : ${MACHINE_FASTQ_PATH}"
log_message "INFO" "run_data directory path       : ${RUN_DATA_PATH}"
log_message "INFO" "results directory path        : ${RESULTS_PATH}"
log_message "INFO" "Instance type [debug?]        : ${DEBUG}"
log_message "INFO" "Is this a dryrun?             : ${DRY_RUN}"
log_message "INFO" "Is this RUN in Debug mode?    : ${DEBUG}"
log_message "STAGE" "START OF ARCHIVAL PROCESS"

# Collect indexset IDs and append to metadata log
INDEXSETS=$(GET_RUN_INDEXES "${MACHINE_FASTQ_PATH}" "${RUN_DATA_PATH}" "${RUN_ID}")
echo "INDEXSETS=\"${INDEXSETS}\"" >> ${META_LOG}
echo -e " \n"

# Run the metadata collector script to enrich the metadata log
${SCRIPT_DIR}/meta_collector.sh \
    ${RUN_ID} ${RUN_PROCESS_DIR} ${META_LOG} \
    ${DIR_PATH} ${MACHINE_FASTQ_PATH} ${DEBUG} ${SCRIPT_DIR}


# -----------------------------------------------------------------------------
# Section 8: Tarball Job Submission
# For each data store (raw_data, machine_fastqs run dirs, machine_fastqs
# indexset dirs, run_data indexset dirs, run_data unclassified dirs, results
# indexset dirs, results unclassified dirs), find matching subdirectories and
# submit a tarball job for each via SUBMIT_TARBALL_PROCESS.
#
# Directories named "*__caa_*" are excluded from all searches.
# All submitted job IDs are appended to JID_LIST for dependency tracking.
# Each processed directory is added to DIR_LIST_ASSOCIATED_WITH_RUNID for
# records.
# -----------------------------------------------------------------------------
JID_LIST=""

# --- 8a: raw_data directories ---
# Tarballs the primary sequencing run directory and any siblings sharing the run ID.
RAW_DATA_DIRS=$(find "${DIR_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" ! -name "*__caa_*")

if [ -n "${RAW_DATA_DIRS}" ]; then
    for RAW_DATA_DIR in ${RAW_DATA_DIRS}; do
        DIR_NAME_RAW_DATA_DIR=$(basename "${RAW_DATA_DIR}")
        DATA_tag_raw="raw_data"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${RAW_DATA_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"    \
            "${DIR_NAME_RAW_DATA_DIR}" \
            "${DATA_tag_raw}"       \
            "${DIR_PATH}"           \
            "${DRY_RUN}"            \
            "${TAR_LOGS}"           \
            "${SPLIT_TARBALL}"      \
            "${LOG_FILE}"           \
            "${SLURM_PARTITION}"    \
            "${TAR_CORES}"          \
            "${TAR_MEM}"            \
            "${TAR_SCRIPTS_DIR}"    \
            "${SCRIPT_DIR}"         \
            "${SPLIT_TARBALL_SIZE}" \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No raw_data directories to tarball for RUN : ${RUN_ID}"
fi

# --- 8b: machine_fastqs run directories (no indexset suffix) ---
# These are the upstream run-level directories that seed individual indexset
# directories. Excluded: *indexset*, *_all*, *__caa_*
MFQ_RUNDIRS=$(find "${MACHINE_FASTQ_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" ! -name "*indexset*" ! -name "*_all*" ! -name "*__caa_*")

if [ -n "${MFQ_RUNDIRS}" ]; then
    for MFQ_RUNDIR in ${MFQ_RUNDIRS}; do
        DIR_NAME_MFQ_RUNDIR=$(basename "${MFQ_RUNDIR}")
        DATA_tag_rmf="machine_fastqs_runDir"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${MFQ_RUNDIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"    \
            "${DIR_NAME_MFQ_RUNDIR}" \
            "${DATA_tag_rmf}"       \
            "${MACHINE_FASTQ_PATH}" \
            "${DRY_RUN}"            \
            "${TAR_LOGS}"           \
            "${SPLIT_TARBALL}"      \
            "${LOG_FILE}"           \
            "${SLURM_PARTITION}"    \
            "${TAR_CORES}"          \
            "${TAR_MEM}"            \
            "${TAR_SCRIPTS_DIR}"    \
            "${SCRIPT_DIR}"         \
            "${SPLIT_TARBALL_SIZE}" \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No Run directories to tarball in machine_fastqs folder for RUN : ${RUN_ID}"
fi

# --- 8c: machine_fastqs indexset directories ---
# Per-indexset (or *_all) output directories produced by demultiplexing.
MFQ_INDEX_DIRS=$(find "${MACHINE_FASTQ_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" \( -name "*indexset*" -o -name "*_all" \) ! -name "__caa_*")

if [ -n "${MFQ_INDEX_DIRS}" ]; then
    for MFQ_INDEX_DIR in ${MFQ_INDEX_DIRS}; do
        DIR_NAME_MFQ_INDEX_DIR=$(basename "${MFQ_INDEX_DIR}")
        DATA_tag_mf="machine_fastqs"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${MFQ_INDEX_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"      \
            "${DIR_NAME_MFQ_INDEX_DIR}" \
            "${DATA_tag_mf}"          \
            "${MACHINE_FASTQ_PATH}"   \
            "${DRY_RUN}"              \
            "${TAR_LOGS}"             \
            "${SPLIT_TARBALL}"        \
            "${LOG_FILE}"             \
            "${SLURM_PARTITION}"      \
            "${TAR_CORES}"            \
            "${TAR_MEM}"              \
            "${TAR_SCRIPTS_DIR}"      \
            "${SCRIPT_DIR}"           \
            "${SPLIT_TARBALL_SIZE}"   \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No indexset directories to tarball in machine_fastqs folder for RUN : ${RUN_ID}"
fi

# --- 8d: run_data indexset directories ---
# Per-indexset (or *_all) pipeline output directories under run_data.
RUNDIR_INDEX_DIRS=$(find "${RUN_DATA_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" \( -name "*indexset*" -o -name "*_all" \) ! -name "__caa_*")

if [ -n "${RUNDIR_INDEX_DIRS}" ]; then
    for RUNDIR_INDEX_DIR in ${RUNDIR_INDEX_DIRS}; do
        DIR_NAME_RUNDIR_INDEX_DIR=$(basename "${RUNDIR_INDEX_DIR}")
        DATA_tag_rd="run_data"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${RUNDIR_INDEX_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"        \
            "${DIR_NAME_RUNDIR_INDEX_DIR}" \
            "${DATA_tag_rd}"            \
            "${RUN_DATA_PATH}"          \
            "${DRY_RUN}"                \
            "${TAR_LOGS}"               \
            "${SPLIT_TARBALL}"          \
            "${LOG_FILE}"               \
            "${SLURM_PARTITION}"        \
            "${TAR_CORES}"              \
            "${TAR_MEM}"                \
            "${TAR_SCRIPTS_DIR}"        \
            "${SCRIPT_DIR}"             \
            "${SPLIT_TARBALL_SIZE}"     \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No run_data directories to tarball for RUN : ${RUN_ID}"
fi

# --- 8e: run_data unclassified directories ---
# run_data directories that have no indexset or _all suffix — archived
# separately with the "run_data_unclassified" tag for traceability.
OTHER_RUNDIR_DIRS=$(find "${RUN_DATA_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" ! -name "*indexset*" ! -name "*_all" ! -name "__caa_*")

if [ -n "${OTHER_RUNDIR_DIRS}" ]; then
    for OTHER_RUNDIR_DIR in ${OTHER_RUNDIR_DIRS}; do
        DIR_NAME_OTHER_RUNDIR_DIR=$(basename "${OTHER_RUNDIR_DIR}")
        DATA_tag_rdu="run_data_unclassified"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${OTHER_RUNDIR_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"       \
            "${DIR_NAME_OTHER_RUNDIR_DIR}" \
            "${DATA_tag_rdu}"          \
            "${RUN_DATA_PATH}"         \
            "${DRY_RUN}"               \
            "${TAR_LOGS}"              \
            "${SPLIT_TARBALL}"         \
            "${LOG_FILE}"              \
            "${SLURM_PARTITION}"       \
            "${TAR_CORES}"             \
            "${TAR_MEM}"               \
            "${TAR_SCRIPTS_DIR}"       \
            "${SCRIPT_DIR}"            \
            "${SPLIT_TARBALL_SIZE}"    \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No unclassified (without suffix indexsetN or -all) run_data directories to tarball for RUN : ${RUN_ID}"
fi

# --- 8f: results indexset directories ---
# Per-indexset (or *_all) analysis result directories.
RESULTS_INDEX_DIRS=$(find "${RESULTS_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" \( -name "*indexset*" -o -name "*_all" \) ! -name "__caa_*")

if [ -n "${RESULTS_INDEX_DIRS}" ]; then
    for RESULTS_INDEX_DIR in ${RESULTS_INDEX_DIRS}; do
        DIR_NAME_RESULTS_INDEX_DIR=$(basename "${RESULTS_INDEX_DIR}")
        DATA_tag_results="results"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${RESULTS_INDEX_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"        \
            "${DIR_NAME_RESULTS_INDEX_DIR}" \
            "${DATA_tag_results}"       \
            "${RESULTS_PATH}"           \
            "${DRY_RUN}"                \
            "${TAR_LOGS}"               \
            "${SPLIT_TARBALL}"          \
            "${LOG_FILE}"               \
            "${SLURM_PARTITION}"        \
            "${TAR_CORES}"              \
            "${TAR_MEM}"                \
            "${TAR_SCRIPTS_DIR}"        \
            "${SCRIPT_DIR}"             \
            "${SPLIT_TARBALL_SIZE}"     \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No results directories to tarball for RUN : ${RUN_ID}"
fi

# --- 8g: results unclassified directories ---
# Results directories without an indexset or _all suffix.
OTHER_RESULTS_DIRS=$(find "${RESULTS_PATH}" -maxdepth 1 -mindepth 1 -type d \
    -name "${RUN_ID}*" ! -name "*indexset*" ! -name "*_all" ! -name "__caa_*")

if [ -n "${OTHER_RESULTS_DIRS}" ]; then
    for OTHER_RESULTS_DIR in ${OTHER_RESULTS_DIRS}; do
        DIR_NAME_RESULTS_OTHER_DIR=$(basename "${OTHER_RESULTS_DIR}")
        DATA_tag_results_U="results_unclassified"
        DIR_LIST_ASSOCIATED_WITH_RUNID+=("${OTHER_RESULTS_DIR}")
        JID=$(SUBMIT_TARBALL_PROCESS \
            "${RUN_PROCESS_DIR}"       \
            "${DIR_NAME_RESULTS_OTHER_DIR}" \
            "${DATA_tag_results_U}"    \
            "${RESULTS_PATH}"          \
            "${DRY_RUN}"               \
            "${TAR_LOGS}"              \
            "${SPLIT_TARBALL}"         \
            "${LOG_FILE}"              \
            "${SLURM_PARTITION}"       \
            "${TAR_CORES}"             \
            "${TAR_MEM}"               \
            "${TAR_SCRIPTS_DIR}"       \
            "${SCRIPT_DIR}"            \
            "${SPLIT_TARBALL_SIZE}"    \
            "${DEBUG}"
        )
        JID_LIST+="${JID}:"
    done
else
    log_message "INFO" "No unclassified (without suffix indexsetN or -all) results directories to tarball for RUN : ${RUN_ID}"
fi


# -----------------------------------------------------------------------------
# Section 9: Directory List and Statistics Job
# Write all tarballed directory paths to a file for reference. If statistics
# collection is enabled, submit a size collection job and add it to JID_LIST.
# -----------------------------------------------------------------------------

# Write the list of all tarballed directories for this run
PROCESSED_DIRS_LIST_FILE=${RUN_PROCESS_DIR}/${RUN_ID}.dirlist
> ${PROCESSED_DIRS_LIST_FILE}
for DIR_TARRRED in ${DIR_LIST_ASSOCIATED_WITH_RUNID[@]}; do
    echo ${DIR_TARRRED} >> ${PROCESSED_DIRS_LIST_FILE}
done

# Initialise the transfer stats file with the run ID
DATA_TRANSFER_STAT_FILE=$(echo ${LOG_FILE} | sed 's/.log/.transfer.stats/')
echo "RUN_ID=\"${RUN_ID}\"" >> ${DATA_TRANSFER_STAT_FILE}

# Optionally submit a job to collect uncompressed source directory sizes
if ${TRANSFER_STATS_COLLECTION}; then
    JID_DIR_SIZE=$(SUBMIT_SIZE_COLLECTION \
        "${LOG_FILE}"               \
        "${SLURM_PARTITION}"        \
        "${TAR_SCRIPTS_DIR}"        \
        "${PROCESSED_DIRS_LIST_FILE}" \
        "${TAR_LOGS}"               \
        "${RUN_ID}"                 \
        "${DATA_TRANSFER_STAT_FILE}" \
        "${SCRIPT_DIR}"
    )
    JID_LIST+="${JID_DIR_SIZE}:"
fi

# Strip the trailing colon from the job ID list
JID_LIST=${JID_LIST%:}


# -----------------------------------------------------------------------------
# Section 10: Wait for All Tarball Jobs to Complete
# Use squeue to wait until none of the submitted jobs remain in the queue,
# then use sacct to verify each job's final state.
# Jobs still in RUNNING state after 7 hours (840 * 30s polls) are treated
# as failures. Any non-COMPLETED final state causes an immediate exit.
# -----------------------------------------------------------------------------
sleep 20
echo "Job dependency  $JID_LIST"

# Coarse wait: block until all jobs have left the queue
while squeue -j "$JID_LIST" -h | grep -q .; do
    sleep 30
done
sleep 30
sleep 30

# Fine-grained status check: verify each job individually
for DEP_JOBID in ${JID_LIST//:/ }; do
    IND_JOB_STATUS=$(sacct -j "$DEP_JOBID" --format=State --noheader | head -n 1 | awk '{print $1}')

    # If sacct still reports RUNNING, poll until it transitions (max 7 hours)
    if [[ "$IND_JOB_STATUS" == "RUNNING" ]]; then
        MAX_WAIT=840
        WAIT_COUNT=0
        while [[ "$IND_JOB_STATUS" == "RUNNING" ]]; do
            if [ $WAIT_COUNT -gt $MAX_WAIT ]; then
                echo -e "${RUN_ID} \t FAILED \t   \t REASON : One of the Tar process $DEP_JOBID job status is in RUNNING state even after 7hr, check: ${RUN_PROCESS_DIR}-${SLURM_ARRAY_JOB_ID}.out" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
                exit 1
            fi
            sleep 30
            WAIT_COUNT=$((WAIT_COUNT + 1))
            IND_JOB_STATUS=$(sacct -j "$DEP_JOBID" --format=State --noheader | head -n 1 | awk '{print $1}')
        done
    fi

    # Evaluate terminal state
    if [[ "$IND_JOB_STATUS" == "COMPLETED" ]]; then
        echo "Job $DEP_JOBID COMPLETED"
    else
        echo "Job $DEP_JOBID FAILED (status=$IND_JOB_STATUS)"
        log_message "INFO" "One or multiple tar jobs have failed"
        echo -e "${RUN_ID} \t FAILED \t   \t REASON : One or more Tar processes failed, check: ${TAR_LOGS} \t $(date +'%Y/%m/%d %H:%M:%S')" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
        exit 1
    fi
done


# -----------------------------------------------------------------------------
# Section 11: Tarball Size and Timing Statistics
# Now that all tarball jobs are confirmed complete, measure the total size of
# the produced .tar.gz files and record the elapsed time for this stage.
# Both values are appended to the transfer stats file.
# -----------------------------------------------------------------------------
TARREED_FILE_SIZE_BYTES=$(du -cb "${RUN_PROCESS_DIR}"/*.tar.gz* 2>/dev/null | tail -n1 | awk '{print $1}')
TARREED_FILE_SIZE_HUMAN=$(human_readable_size $TARREED_FILE_SIZE_BYTES)

echo "TAR_DIR_SIZE_BYTES=\"${TARREED_FILE_SIZE_BYTES}\"" >> ${DATA_TRANSFER_STAT_FILE}
echo "TAR_DIR_SIZE_HUMAN=\"${TARREED_FILE_SIZE_HUMAN}\"" >> ${DATA_TRANSFER_STAT_FILE}

TARBALL_TIME=$(timer2 ${START_TIME_1})
echo "TARBALL_TIME=\"${TARBALL_TIME}\"" >> ${DATA_TRANSFER_STAT_FILE}


# -----------------------------------------------------------------------------
# Section 12: Log Collation Job
# Generate and submit a short sbatch script that merges all per-tarball step
# logs (*.steptarlog) into the master log file. Variable substitution in the
# heredoc is handled via sed after the file is written, since the heredoc uses
# single-quoted 'EOF' to prevent premature expansion.
# -----------------------------------------------------------------------------
COLLATE_SCRIPT="${TAR_SCRIPTS_DIR}/collate_script.sbatch"

# Write the collation sbatch script using a literal heredoc (no expansion)
cat > ${COLLATE_SCRIPT} <<'EOF'
#!/bin/bash
#SBATCH -J COLLATE
#SBATCH -p ${SLURM_PARTITION}
#SBATCH -c 1
#SBATCH --mem=2G
#SBATCH -o ${TAR_LOGS}/collate_logs-%A.logtar
chmod 777 ${TAR_LOGS}/*.steptarlog
cat ${TAR_LOGS}/*.steptarlog >> ${LOG_FILE}
EOF

# Substitute runtime variable values into the generated script
sed -i "s|\${SLURM_PARTITION}|${SLURM_PARTITION}|g" "${COLLATE_SCRIPT}"
sed -i "s|\${TAR_LOGS}|${TAR_LOGS}|g"               "${COLLATE_SCRIPT}"
sed -i "s|\${LOG_FILE_TAR}|${LOG_FILE_TAR}|g"       "${COLLATE_SCRIPT}"
sed -i "s|\${LOG_FILE}|${LOG_FILE}|g"               "${COLLATE_SCRIPT}"
chmod +x "${COLLATE_SCRIPT}"

# Submit the collation job
JID4=$(sbatch --parsable "${COLLATE_SCRIPT}") || {
    echo "ERROR: sbatch failed" >&2
    return 1
}
echo " Last dependency jobid  ${JID4}"

# Poll until the collation job reaches a terminal state
sleep 10
while true; do
    JOB_STATE2=$(sacct -n -X -o state -j "$JID4" | grep -Ev '^\s*$' | head -1 | xargs)
    echo "collate job status: $JOB_STATE2"
    case "$JOB_STATE2" in
        COMPLETED)
            #echo "Job $JIDCOL completed successfully."
            break
            ;;
        PENDING|RUNNING|CONFIGURING|COMPLETING)
            #echo "Job is still running... checking again in 2 minutes."
            sleep 30
            ;;
        FAILED|CANCELLED|TIMEOUT|NODE_FAIL|PREEMPTED)
            #echo "Job $JIDCOL failed or ended with error state: $JOB_STATE2"
            echo -e "${RUN_ID} \t FAILED \t   \t REASON : Collating tar outputs processes failed, check tar process output: ${RUN_PROCESS_DIR} \t $(date +'%Y/%m/%d %H:%M:%S')" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
            exit 1
            ;;
        *)
            echo "Unexpected or unknown job state in tarball script: $JOB_STATE2"
            echo -e "${RUN_ID} \t FAILED \t   \t REASON : Collating tar outputs processes failed, check tar process output: ${RUN_PROCESS_DIR} \t $(date +'%Y/%m/%d %H:%M:%S')" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
            exit 1
            ;;
    esac
done

# Log total elapsed time for the metadata collection and tarballing stage
timer $START_TIME_1 "metadata collection and Tarballing Run related data including Rawdata"
echo "Tarball process complete !!!!"
