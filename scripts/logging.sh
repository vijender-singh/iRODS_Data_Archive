#!/bin/bash
# =============================================================================
# Script:      logging.sh
# Description: Shared library of helper functions sourced by the iRODS archive
#              pipeline scripts. Provides:
#                - Human-readable size formatting
#                - Command execution with timing and log output
#                - iRODS transfer execution with speed calculation
#                - Structured log message writing
#                - Elapsed time reporting (two variants)
#                - Run indexset discovery
#                - iRODS transfer checksum verification
#                - SLURM tarball job generation and submission
#                - SLURM directory size estimation job submission
#
# Usage:       source ${SCRIPT_DIR}/logging.sh
#
# External variables expected by certain functions (must be set by caller):
#   LOG_FILE          - Master log file path (used by log_message, timer, log_and_run_irods)
#   LOG_FILE_TAR      - Per-tarball step log file path (used by log_and_run)
#   LOG_TAR_PROCESS   - Per-tarball stdout/stderr capture log (used by log_and_run)
#   IRODS_LOG_FILE    - iRODS transfer detail log (used by log_and_run_irods)
# =============================================================================


# =============================================================================
# Function:    human_readable_size
# Description: Converts a byte count into a human-readable string with the
#              most appropriate unit (B, KB, MB, or GB), rounded to 2 d.p.
#
# Arguments:
#   $1  BYTES  - Integer byte count to convert
#
# Output:      Prints "<value> <unit>" to stdout (e.g. "1.23 GB")
# =============================================================================
human_readable_size() {
    local BYTES=$1
    local UNIT="B"
    local SIZE=$BYTES

    if [ "$BYTES" -ge 1073741824 ]; then
        SIZE=$(echo "scale=2; $BYTES / 1073741824" | bc)
        UNIT="GB"
    elif [ "$BYTES" -ge 1048576 ]; then
        SIZE=$(echo "scale=2; $BYTES / 1048576" | bc)
        UNIT="MB"
    elif [ "$BYTES" -ge 1024 ]; then
        SIZE=$(echo "scale=2; $BYTES / 1024" | bc)
        UNIT="KB"
    fi

    echo "${SIZE} ${UNIT}"
}


# =============================================================================
# Function:    log_and_run
# Description: Executes an arbitrary shell command, captures its stdout and
#              stderr, and writes a structured entry to LOG_FILE_TAR showing
#              the start timestamp, outcome (SUCCESS/ERROR), elapsed time, and
#              exit code. Full output is also appended to LOG_TAR_PROCESS if
#              that file exists. Exits with the command's exit code on failure.
#
# Arguments:
#   $@  CMD  - The full command string to execute (passed to bash -c)
#
# Output:     Prints the exit status code to stdout on success.
#             Writes structured log entries to LOG_FILE_TAR (and LOG_TAR_PROCESS).
#
# Globals:
#   LOG_FILE_TAR     - Per-tarball step log (must be set by caller)
#   LOG_TAR_PROCESS  - Verbose process log (written only if the file exists)
# =============================================================================
log_and_run() {
    local CMD="$*"

    # Capture start timestamp and epoch for elapsed time calculation
    local TIMESTAMP_START
    TIMESTAMP_START=$(date +"%Y-%m-%d %H:%M:%S")
    local START_TIME
    START_TIME=$(date +%s)

    echo "   (\_)    [$TIMESTAMP_START] Running: $CMD" >> "$LOG_FILE_TAR"

    # Execute the command and capture combined stdout/stderr
    OUTPUT=$(bash -c "$CMD" 2>&1)
    STATUS=$?

    # Append full command output to the verbose process log if it exists
    if [ -f "${LOG_TAR_PROCESS}" ]; then
        {
            echo "======================================================================================="
            echo "PROCESS: $CMD"
            echo "======================================================================================="
            echo " "
            printf "Below is stdout n stderr of tar process step:\n%s\n" "$OUTPUT"
            echo " "
            echo -e "---------------------------------------------------------------------------------------\n\n"
        } >> "${LOG_TAR_PROCESS}"
    fi

    # Calculate elapsed wall-clock time and format as HH:MM:SS
    local END_TIME
    END_TIME=$(date +%s)
    local TIMESTAMP_END
    TIMESTAMP_END=$(date +"%Y-%m-%d %H:%M:%S")
    local ELAPSED=$((END_TIME - START_TIME))

    local HOURS=$((ELAPSED / 3600))
    local MINUTES=$(( (ELAPSED % 3600) / 60 ))
    local SECONDS=$((ELAPSED % 60))
    local FORMATTED_ELAPSED
    FORMATTED_ELAPSED=$(printf "%02d:%02d:%02d" $HOURS $MINUTES $SECONDS)

    # Write outcome to the step log
    if [ $STATUS -eq 0 ]; then
        echo "[$FORMATTED_ELAPSED] [$TIMESTAMP_END] SUCCESS: $CMD" >> "$LOG_FILE_TAR"
        #echo "Output: $OUTPUT" >> "$LOG_FILE"
        #echo "Time Elapsed: $FORMATTED_ELAPSED" >> "$LOG_FILE"
    else
		{
        echo "[$TIMESTAMP_END] ERROR: $CMD"
        echo "Exit Code: $STATUS"
        echo "Error Output: $OUTPUT"
        echo "Time Elapsed: $FORMATTED_ELAPSED"
		} >> "$LOG_FILE_TAR"

        echo "Command failed: $CMD" >&2
        exit $STATUS
    fi

    echo $STATUS
}


# =============================================================================
# Function:    log_and_run_irods
# Description: Executes an iRODS iput command, logs the result to IRODS_LOG_FILE
#              and LOG_FILE, and calculates the achieved transfer speed based on
#              the supplied file size and elapsed time.
#
# Arguments:
#   $1   FILESIZE  - Size of the file being transferred, in bytes (used for
#                    speed calculation)
#   $@   CMD       - The iput command to execute (all remaining arguments)
#
# Output:     Prints the human-readable transfer speed to stdout.
#             Writes structured log entries to LOG_FILE and IRODS_LOG_FILE.
#
# Globals:
#   LOG_FILE        - Master log file (must be set by caller)
#   IRODS_LOG_FILE  - iRODS-specific detail log (must be set by caller)
# =============================================================================
log_and_run_irods() {
    local FILESIZE="$1"   # Transfer file size in bytes for speed calculation
    shift
    local CMD="$*"

    # Capture start timestamp and epoch
    local TIMESTAMP_START
    TIMESTAMP_START=$(date +"%Y-%m-%d %H:%M:%S")
    local START_TIME
    START_TIME=$(date +%s)

    # Pad the running prefix to a fixed column width for aligned log output
    STATUS_COLUMN=60
    PREFIX_RUNNING="   (\_)    [$TIMESTAMP_START]"
    PAD_RUNNING=$(printf "%*s" $((STATUS_COLUMN - ${#PREFIX_RUNNING})) "")
    #echo "   (\_)    [$TIMESTAMP_START]               Running: $CMD" >> "$LOG_FILE"
    echo "${PREFIX_RUNNING}${PAD_RUNNING}Running: $CMD" >> "$LOG_FILE"

    # Execute the iput command and capture combined stdout/stderr
    OUTPUT=$(bash -c "$CMD" 2>&1)
    STATUS=$?

    # Append full output to the iRODS detail log
    {
        echo "          ======================================================================================="
        echo "          |> iput PROCESS: $CMD"
        echo " "
        printf "        [X]  stdout n stderr of IRODS transfer (iput) step: \n\t\t\t=>%s\n" "$OUTPUT"
        echo " "
        echo -e "       =======================================================================================\n\n"
    } >> "${IRODS_LOG_FILE}"

    # Calculate elapsed time and format as HH:MM:SS
    local END_TIME
    END_TIME=$(date +%s)
    local TIMESTAMP_END
    TIMESTAMP_END=$(date +"%Y-%m-%d %H:%M:%S")
    local ELAPSED=$((END_TIME - START_TIME))

    local HOURS=$((ELAPSED / 3600))
    local MINUTES=$(( (ELAPSED % 3600) / 60 ))
    local SECONDS=$((ELAPSED % 60))
    local FORMATTED_ELAPSED
    FORMATTED_ELAPSED=$(printf "%02d:%02d:%02d" $HOURS $MINUTES $SECONDS)

    # Calculate transfer speed in bytes/sec (guard against zero elapsed time)
    local SPEED=0
    if [ "$ELAPSED" -gt 0 ]; then
        SPEED=$((FILESIZE / ELAPSED))
    fi

    # Convert speed to the most appropriate human-readable unit
    local HUMAN_SPEED
    if [ "$SPEED" -gt $((1024*1024*1024)) ]; then
        HUMAN_SPEED=$(printf "%.2f GB/s" "$(echo "$SPEED / 1073741824" | bc -l)")
    elif [ "$SPEED" -gt $((1024*1024)) ]; then
        HUMAN_SPEED=$(printf "%.2f MB/s" "$(echo "$SPEED / 1048576" | bc -l)")
    elif [ "$SPEED" -gt 1024 ]; then
        HUMAN_SPEED=$(printf "%.2f KB/s" "$(echo "$SPEED / 1024" | bc -l)")
    else
        HUMAN_SPEED="${SPEED} B/s"
    fi

    HUMAN_SIZE=$(human_readable_size $FILESIZE)
	PREFIX_SUCCESS="[$FORMATTED_ELAPSED] [$TIMESTAMP_END] [$HUMAN_SIZE] [$HUMAN_SPEED]"
	PAD_SUCCESS=$(printf "%*s" $((STATUS_COLUMN - ${#PREFIX_SUCCESS})) "")

    if [ $STATUS -eq 0 ]; then
        echo "${PREFIX_SUCCESS}${PAD_SUCCESS}SUCCESS: $CMD" >> "$LOG_FILE"
    else
        echo "[$TIMESTAMP_END] ERROR: $CMD" >> "$LOG_FILE"
        echo "Exit Code: $STATUS" >> "$LOG_FILE"
        echo "Error Output: $OUTPUT" >> "$LOG_FILE"
        echo "Time Elapsed: $FORMATTED_ELAPSED" >> "$LOG_FILE"
        echo "Command failed: $CMD" >&2
        exit $STATUS
    fi
}



# =============================================================================
# Function:    SPEED_CALC
# Description: Calculates the transfer speed in human readable format.
#
# Arguments:
#   $1   FILESIZE     - Log level / label string (e.g. INFO, STAGE, STARTING)
#   $2   START_TIME  - Transfer/Process start time)
#
# Output:     Speed ".
#
# =============================================================================

SPEED_CALC() {
    local FILESIZE=$1
    local START_TIME=$2

    local END_TIME
    END_TIME=$(date +%s)

    local ELAPSED=$((END_TIME - START_TIME))

    local SPEED=0
    if [ "$ELAPSED" -gt 0 ]; then
        SPEED=$((FILESIZE / ELAPSED))
    fi

    local HUMAN_SPEED
    if [ "$SPEED" -gt $((1024*1024*1024)) ]; then
        HUMAN_SPEED=$(printf "%.2f GB/s" "$(echo "$SPEED / 1073741824" | bc -l)")
    elif [ "$SPEED" -gt $((1024*1024)) ]; then
        HUMAN_SPEED=$(printf "%.2f MB/s" "$(echo "$SPEED / 1048576" | bc -l)")
    elif [ "$SPEED" -gt 1024 ]; then
        HUMAN_SPEED=$(printf "%.2f KB/s" "$(echo "$SPEED / 1024" | bc -l)")
    else
        HUMAN_SPEED="${SPEED} B/s"
    fi

    echo ${HUMAN_SPEED}
}


# =============================================================================
# Function:    log_message
# Description: Appends a single labelled message line to LOG_FILE.
#
# Arguments:
#   $1   FLAG     - Log level / label string (e.g. INFO, STAGE, STARTING)
#   $@   MESSAGE  - Message text (all remaining arguments joined)
#
# Output:     Appends "           [FLAG] message" to LOG_FILE.
#
# Globals:
#   LOG_FILE  - Must be set by the calling script
# =============================================================================
log_message() {
    local FLAG="$1"
    shift
    local MESSAGE=$*
    echo -e "           [$FLAG] $MESSAGE" >> "$LOG_FILE"
}


# =============================================================================
# Function:    timer
# Description: Calculates and logs the elapsed time since a given start epoch,
#              labelled with a stage description. Output is appended to LOG_FILE.
#
# Arguments:
#   $1   START_TIME  - Epoch seconds recorded at stage start (date +%s)
#   $@   STAGE       - Human-readable description of the timed stage
#
# Output:     Appends "==> [HH:MM:SS] time taken for <STAGE>" to LOG_FILE.
#
# Globals:
#   LOG_FILE  - Must be set by the calling script
# =============================================================================
timer() {
    local START_TIME=$1
    shift
    local STAGE=$*

    local END_TIME
    END_TIME=$(date +%s)
    local ELAPSED=$((END_TIME - START_TIME))

    # Format elapsed seconds as HH:MM:SS
    local HRS=$((ELAPSED / 3600))
    local MINS=$(( (ELAPSED % 3600) / 60 ))
    local SECS=$((ELAPSED % 60))
    local F_ELAPSED
    F_ELAPSED=$(printf "%02d:%02d:%02d" $HRS $MINS $SECS)

    echo -e "==> [$F_ELAPSED] time taken for ${STAGE}" >> "$LOG_FILE"
}


# =============================================================================
# Function:    timer2
# Description: Calculates the elapsed time since a given start epoch and
#              prints it to stdout as HH:MM:SS. Unlike timer(), this variant
#              does not write to LOG_FILE and is intended for capturing the
#              value into a variable.
#
# Arguments:
#   $1   START_TIME  - Epoch seconds recorded at stage start (date +%s)
#
# Output:     Prints elapsed time as HH:MM:SS to stdout.
# =============================================================================
timer2() {
    local START_TIME=$1

    local END_TIME
    END_TIME=$(date +%s)
    local ELAPSED=$((END_TIME - START_TIME))

    # Format elapsed seconds as HH:MM:SS
    local HRS=$((ELAPSED / 3600))
    local MINS=$(( (ELAPSED % 3600) / 60 ))
    local SECS=$((ELAPSED % 60))
    local F_ELAPSED
    F_ELAPSED=$(printf "%02d:%02d:%02d" $HRS $MINS $SECS)

    echo -e "${F_ELAPSED}"
}


# =============================================================================
# Function:    GET_RUN_INDEXES
# Description: Discovers all indexset identifiers associated with a run ID by
#              scanning matching subdirectories in machine_fastqs and run_data.
#              Extracts tokens containing "index" from directory basenames,
#              deduplicates them, and returns them as a newline-separated list.
#
# Arguments:
#   $1   MACHINE_FASTQ_DIR  - machine_fastqs root directory for this instrument
#   $2   RUN_DATA_DIR       - run_data root directory for this instrument
#   $3   PREFIX             - Run ID prefix used to filter matching directories
#
# Output:     Prints sorted, unique indexset tokens to stdout.
# =============================================================================
GET_RUN_INDEXES() {
    local MACHINE_FASTQ_DIR="$1"
    local RUN_DATA_DIR="$2"
    local PREFIX="$3"
    local QUERY="index"
    local MATCHES=""

    # Collect all directories matching the run prefix from both data stores
    local LSTDIRS
    LSTDIRS=$(ls -d ${MACHINE_FASTQ_DIR}/${PREFIX}* 2>/dev/null)
    LSTDIRS+=" "
    LSTDIRS+=$(ls -d ${RUN_DATA_DIR}/${PREFIX}* 2>/dev/null)

    # Extract tokens containing "index" from each directory's basename
    for DDIR in ${LSTDIRS}; do
        local BASE
        BASE=$(basename "$DDIR")
        OLDIFS=$IFS
        # Split on "-" and "_" to isolate individual tokens
        local IFS="-_${IFS}"
        PART=$(echo $BASE | grep -oP "\b\w*index\w*\b")
        MATCHES+="$PART"$'\n'
        IFS=$OLDIFS
    done

    # Deduplicate and sort the collected indexset tokens
    INDEX_OUT=$(printf "%s" "$MATCHES" | sort | uniq)
    echo $INDEX_OUT
}


# =============================================================================
# Function:    TRANSFER_CHECK
# Description: Verifies that an iRODS object was transferred successfully by
#              comparing its iRODS checksum (from iCAT) against the expected
#              source checksum. Retries up to MAX_ATTEMPT times with a 60-second
#              sleep between attempts if the iRODS checksum is not yet available.
#              Logs a warning via log_message if the checksums do not match or
#              if the iRODS checksum cannot be retrieved.
#
# Arguments:
#   $1   IRODS_OBJECT_PATH  - Full iRODS path to the transferred object
#   $2   SOURCE_CHECKSUM    - Expected checksum of the source file
#   $3   IRODS_CHECKSUM     - Optionally pre-fetched iRODS checksum (may be empty)
#
# Globals:
#   log_message  - Logging function (sourced from this file)
# =============================================================================
TRANSFER_CHECK() {
    local IRODS_OBJECT_PATH="$1"
    local SOURCE_CHECKSUM="$2"
    local IRODS_CHECKSUM="$3"
    local FILENAME_UNDER_TRANSFER
    FILENAME_UNDER_TRANSFER=$(basename ${IRODS_OBJECT_PATH})
    local ATTEMPT=1
    local MAX_ATTEMPT=6

    # Retry fetching the iRODS checksum until it is available or attempts are exhausted
    while [ $ATTEMPT -le $MAX_ATTEMPT ]; do
        if [ -n "${IRODS_CHECKSUM}" ]; then
            break
        fi
        IRODS_CHECKSUM=$(ichksum ${IRODS_OBJECT_PATH} 2>/dev/null | cut -d":" -f2)
        if [ -n "${IRODS_CHECKSUM}" ]; then
            break
        fi
        if [ $ATTEMPT -lt $MAX_ATTEMPT ]; then
            sleep 60
        fi
        ATTEMPT=$((ATTEMPT + 1))
    done

    # Evaluate the checksum comparison result
    if [ -n "${SOURCE_CHECKSUM:-}" ] && [ "${SOURCE_CHECKSUM}" = "${IRODS_CHECKSUM}" ]; then
        # Checksums match — brief pause before returning
        sleep 1
    elif [ -z "${IRODS_CHECKSUM}" ]; then
        # iRODS checksum could not be retrieved from iCAT after all retries
        log_message "INFO" "Proces : TRANSFER_CHECK"
        log_message "INFO" "${FILENAME_UNDER_TRANSFER} : IRODS checksum cannot be determined from iCAT."
        log_message "INFO" "IRODS object path : ${IRODS_OBJECT_PATH}"
        log_message "INFO" "${FILENAME_UNDER_TRANSFER}  TRANSFER_UNSUCCESSFUL CHECKSUM FAILED"
    else
        # Checksums retrieved but do not match
        log_message "INFO" "Proces : TRANSFER_CHECK"
        log_message "INFO" "${FILENAME_UNDER_TRANSFER} : ${SOURCE_CHECKSUM} : ${IRODS_CHECKSUM}"
        log_message "INFO" "${FILENAME_UNDER_TRANSFER}  TRANSFER_UNSUCCESSFUL CHECKSUM FAILED"
    fi
}


# =============================================================================
# Function:    SUBMIT_TARBALL_PROCESS
# Description: Generates and submits a SLURM sbatch script that tarballs a
#              single source directory into the run processing directory.
#              Supports split tarballs (piped through split) and applies
#              different exclude patterns for run_data vs other data stores.
#              In dry-run mode, creates a zero-byte placeholder instead.
#
#              The sbatch script is written using a quoted heredoc (no
#              premature expansion) and then sed-substituted with runtime
#              values before submission.
#
# Arguments:
#   $1   RUN_PROCESS_DIR    - Output directory for tarballs and logs
#   $2   TARGET_DIR         - Basename of the directory to tarball
#   $3   DATA_TAG           - Data store label (e.g. raw_data, machine_fastqs)
#   $4   SOURCE_DIR_PATH    - Parent directory containing TARGET_DIR
#   $5   DRY_RUN            - "true" to skip real submission and touch a placeholder
#   $6   TAR_LOGS           - Directory for tarball log files
#   $7   SPLIT_TARBALL      - "TRUE" to pipe output through split
#   $8   LOG_FILE           - Master log file path
#   $9   SLURM_PARTITION    - SLURM partition for the submitted job
#   $10  TAR_CORES          - CPU cores for the SBATCH job (#SBATCH -c)
#   $11  TAR_MEM            - Memory for the SBATCH job (#SBATCH --mem)
#   $12  TAR_SCRIPTS_DIR    - Directory to write the generated sbatch script
#   $13  SCRIPT_DIR         - Application scripts directory (for logging.sh)
#   $14  SPLIT_TARBALL_SIZE - Maximum size per split part (e.g. "20G")
#   $15  DEBUG              - "true" to use verbose tar flag (cvzf vs czf)
#
# Output:     Prints the SLURM job ID to stdout on successful submission.
#             Returns 0 in dry-run mode (no job submitted).
# =============================================================================
SUBMIT_TARBALL_PROCESS() {
    local RUN_PROCESS_DIR=${1}
    local TARGET_DIR=${2}
    local DATA_TAG=${3}
    local SOURCE_DIR_PATH=${4}
    local DRY_RUN=${5}
    local TAR_LOGS=${6}
    local SPLIT_TARBALL=${7}
    local LOG_FILE=${8}
    local SLURM_PARTITION=${9}
    local TAR_CORES=${10}
    local TAR_MEM=${11}
    local TAR_SCRIPTS_DIR=${12}
    local SCRIPT_DIR=${13}
    local SPLIT_TARBALL_SIZE=${14}
    local DEBUG=${15}

    local STATUS=""
    local JOBID=""
    local TAR_FLAG=""

    # Select verbose or quiet tar flag based on debug mode
    if [ "$DEBUG" = true ]; then
        TAR_FLAG="cvzf"
    else
        TAR_FLAG="czf"
    fi

    mkdir -p ${TAR_LOGS}

    # Paths for the generated sbatch script and its associated log files
    SBATCH_SCRIPT="${TAR_SCRIPTS_DIR}/${DATA_TAG}-${TARGET_DIR}.sbatch"
    LOG_FILE_TAR="${TAR_LOGS}/${DATA_TAG}-${TARGET_DIR}.tar.gz.steptarlog"
    RUN_TAR_LOG="${TAR_LOGS}/tarlog.status"

    # Initialise the step log file
    > "${LOG_FILE_TAR}"
    if [ ! -f "${RUN_TAR_LOG}" ]; then
        touch "${RUN_TAR_LOG}"
    fi

    # In dry-run mode, create a placeholder file and return without submitting
    if [ "$DRY_RUN" = true ]; then
        touch "${RUN_PROCESS_DIR}/${DATA_TAG}-${TARGET_DIR}.tar.gz"
        # Fix: DRY_RUN should probably return here or set a dummy JOBID
        echo "DRY_RUN mode - no job submitted"
        return 0
    fi

    # Write the sbatch script using a quoted heredoc to prevent premature expansion.
    # Runtime values are injected via sed substitution after the file is written.
    # Fix: Use quoted heredoc to prevent premature variable expansion
    cat > "${SBATCH_SCRIPT}" <<'EOF'
#!/bin/bash
#SBATCH -p ${SLURM_PARTITION}
#SBATCH -J ${DATA_TAG}-TAR
#SBATCH -c ${TAR_CORES}
#SBATCH --mem=${TAR_MEM}
#SBATCH -o ${TAR_LOGS}/${DATA_TAG}-${TARGET_DIR}.logtar
set -exo pipefail
#set -x
export LOG_TAR_PROCESS=${TAR_LOGS}/${DATA_TAG}-${TARGET_DIR}.logtar
export LOG_FILE_TAR="${LOG_FILE_TAR}"
export SCRIPT_DIR="${SCRIPT_DIR}"
export LOG_FILE="${LOG_FILE}"
source "${SCRIPT_DIR}/logging.sh"
echo "DEBUG: LOG_FILE_TAR=$LOG_FILE_TAR" >&2
echo "DEBUG: SCRIPT_DIR=$SCRIPT_DIR" >&2
echo "DEBUG: LOG_FILE=${LOG_FILE}" >&2
TAR_STATUS=$(grep -E "${DATA_TAG}-${TARGET_DIR}.tar.gz" "${RUN_TAR_LOG}" 2>/dev/null | awk '{print $2}' || true)
if [ -z "${TAR_STATUS}" ] || [ "${TAR_STATUS}" != "0" ]; then
    if [ "${SPLIT_TARBALL}" = "TRUE" ]; then
        if [ "${DATA_TAG}" = "run_data" ] || [ "${DATA_TAG}" = "run_data_unclassified" ]; then
            STATUS=$(log_and_run "tar -${TAR_FLAG} - --exclude=\"Undetermined_*.fastq.gz\" --exclude=\"*.processed.*.fastq.gz\" -C \"${SOURCE_DIR_PATH}\" \"${TARGET_DIR}\" | split -b \"${SPLIT_TARBALL_SIZE}\" - \"${RUN_PROCESS_DIR}/${DATA_TAG}-${TARGET_DIR}.tar.gz.\"")
        else
            STATUS=$(log_and_run "tar -${TAR_FLAG} - --exclude=\"Undetermined_*.fastq.gz\" -C \"${SOURCE_DIR_PATH}\" \"${TARGET_DIR}\" | split -b \"${SPLIT_TARBALL_SIZE}\" - \"${RUN_PROCESS_DIR}/${DATA_TAG}-${TARGET_DIR}.tar.gz.\"")
        fi
    else
        if [ "${DATA_TAG}" = "run_data" ] || [ "${DATA_TAG}" = "run_data_unclassified" ]; then
            STATUS=$(log_and_run "tar -${TAR_FLAG} \"${RUN_PROCESS_DIR}/${DATA_TAG}-${TARGET_DIR}.tar.gz\" --exclude=\"Undetermined_*.fastq.gz\" --exclude=\"*.processed.*.fastq.gz\" -C \"${SOURCE_DIR_PATH}\" \"${TARGET_DIR}\"")
        else
            STATUS=$(log_and_run "tar -${TAR_FLAG} \"${RUN_PROCESS_DIR}/${DATA_TAG}-${TARGET_DIR}.tar.gz\" --exclude=\"Undetermined_*.fastq.gz\" -C \"${SOURCE_DIR_PATH}\" \"${TARGET_DIR}\"")
        fi
    fi
   ENTRY="${DATA_TAG}-${TARGET_DIR}.tar.gz"
    if grep -q "^${ENTRY}\(\s\+.*\)\?$" "${RUN_TAR_LOG}"; then
        sed -i "s|^${ENTRY}\(\s\+.*\)\?$|${ENTRY} ${STATUS}|" "${RUN_TAR_LOG}"
    else
        echo "${ENTRY} ${STATUS}" >> "${RUN_TAR_LOG}"
    fi
else
    log_message "INFO" "${DATA_TAG}-${TARGET_DIR} : SKIPPED : tarball successfully completed in previous attempt/run."
fi
EOF

    # Substitute all runtime variable values into the generated sbatch script
    sed -i "s|\${SLURM_PARTITION}|${SLURM_PARTITION}|g" "${SBATCH_SCRIPT}"
    sed -i "s|\${TAR_CORES}|${TAR_CORES}|g"             "${SBATCH_SCRIPT}"
    sed -i "s|\${TAR_MEM}|${TAR_MEM}|g"                 "${SBATCH_SCRIPT}"
    sed -i "s|\${TAR_LOGS}|${TAR_LOGS}|g"               "${SBATCH_SCRIPT}"
    sed -i "s|\${DATA_TAG}|${DATA_TAG}|g"               "${SBATCH_SCRIPT}"
    sed -i "s|\${TARGET_DIR}|${TARGET_DIR}|g"           "${SBATCH_SCRIPT}"
    sed -i "s|\${SCRIPT_DIR}|${SCRIPT_DIR}|g"           "${SBATCH_SCRIPT}"
    sed -i "s|\${RUN_TAR_LOG}|${RUN_TAR_LOG}|g"         "${SBATCH_SCRIPT}"
    sed -i "s|\${SPLIT_TARBALL}|${SPLIT_TARBALL}|g"     "${SBATCH_SCRIPT}"
    sed -i "s|\${TAR_FLAG}|${TAR_FLAG}|g"               "${SBATCH_SCRIPT}"
    sed -i "s|\${SOURCE_DIR_PATH}|${SOURCE_DIR_PATH}|g" "${SBATCH_SCRIPT}"
    sed -i "s|\${SPLIT_TARBALL_SIZE}|${SPLIT_TARBALL_SIZE}|g" "${SBATCH_SCRIPT}"
    sed -i "s|\${RUN_PROCESS_DIR}|${RUN_PROCESS_DIR}|g" "${SBATCH_SCRIPT}"
    sed -i "s|\${TAR_LOGS}|${TAR_LOGS}|g"               "${SBATCH_SCRIPT}"
    sed -i "s|\${LOG_FILE_TAR}|${LOG_FILE_TAR}|g"       "${SBATCH_SCRIPT}"
    sed -i "s|\${LOG_FILE}|${LOG_FILE}|g"               "${SBATCH_SCRIPT}"

    chmod +x "${SBATCH_SCRIPT}"

    # Submit the generated script and return the job ID
    JOBID=$(sbatch --parsable "${SBATCH_SCRIPT}") || {
        echo "ERROR: sbatch failed" >&2
        return 1
    }
    echo "${JOBID}"
}


# =============================================================================
# Function:    SUBMIT_SIZE_COLLECTION
# Description: Generates and submits a SLURM sbatch script that calculates the
#              total uncompressed size of all directories tarballed for a run.
#              Reads directory paths from PROCESSED_DIRS_LIST_FILE, sums their
#              sizes using du, and appends UNCOMPRESSED_DATA_BYTES and
#              UNCOMPRESSED_DATA_h to DATA_TRANSFER_STAT_FILE.
#
# Arguments:
#   $1   LOG_FILE                  - Master log file path (exported into job)
#   $2   SLURM_PARTITION           - SLURM partition for the submitted job
#   $3   TAR_SCRIPTS_DIR           - Directory to write the generated sbatch script
#   $4   PROCESSED_DIRS_LIST_FILE  - File listing all directories to measure
#   $5   TAR_LOGS                  - Directory for the job's stdout log
#   $6   RUN_ID                    - Run identifier (used in job name and log name)
#   $7   DATA_TRANSFER_STAT_FILE   - Stats file to append size values to
#   $8   SCRIPT_DIR                - Application scripts directory (for logging.sh)
#
# Output:     Prints the SLURM job ID to stdout on successful submission.
# =============================================================================
SUBMIT_SIZE_COLLECTION() {
    local LOG_FILE=${1}
    local SLURM_PARTITION=${2}
    local TAR_SCRIPTS_DIR=${3}
    local PROCESSED_DIRS_LIST_FILE=${4}
    local TAR_LOGS=${5}
    local RUN_ID=${6}
    local DATA_TRANSFER_STAT_FILE=${7}
    local SCRIPT_DIR=${8}

    local TOTAL_BYTES=0
    local DIR_BYTES=""
    local DATA_SIZE=""

    SBATCH_SCRIPT_SIZE="${TAR_SCRIPTS_DIR}/${RUN_ID}_sizestimation.sbatch"
    #echo ${SBATCH_SCRIPT_SIZE}

    # Write the size estimation sbatch script using a quoted heredoc.
    # Runtime values are injected via sed substitution after the file is written.
    cat > "${SBATCH_SCRIPT_SIZE}" <<'EOF'
#!/bin/bash
#SBATCH -p ${SLURM_PARTITION}
#SBATCH -J SIZE_ESTMN
#SBATCH -c 1
#SBATCH --mem=5G
#SBATCH -o ${TAR_LOGS}/${RUN_ID}.dataSizeEstimation
set -x
source "${SCRIPT_DIR}/logging.sh"
DIR_LIST_FOR_SIZE_ESTIMATION=$(cat ${PROCESSED_DIRS_LIST_FILE})
for EACH_DIR_IN_LIST in ${DIR_LIST_FOR_SIZE_ESTIMATION}; do
    DIR_BYTES=$(du -sb "$EACH_DIR_IN_LIST" | awk '{print $1}')
    TOTAL_BYTES=$((TOTAL_BYTES + DIR_BYTES))
    done
DATA_SIZE=$(human_readable_size $TOTAL_BYTES)
# DATA_TRANSFER_STAT_FILE=$(echo ${LOG_FILE} | sed 's/.log/.transfer.stats/')
echo "UNCOMPRESSED_DATA_BYTES=\"${TOTAL_BYTES}\"" >> ${DATA_TRANSFER_STAT_FILE}
echo "UNCOMPRESSED_DATA_h=\"${DATA_SIZE}\"" >> ${DATA_TRANSFER_STAT_FILE}
EOF

    # Substitute all runtime variable values into the generated sbatch script
    sed -i "s|\${SLURM_PARTITION}|${SLURM_PARTITION}|g"                       "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${LOG_FILE}|${LOG_FILE}|g"                                      "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${PROCESSED_DIRS_LIST_FILE}|${PROCESSED_DIRS_LIST_FILE}|g"     "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${RUN_ID}|${RUN_ID}|g"                                         "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${TAR_LOGS}|${TAR_LOGS}|g"                                     "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${DATA_TRANSFER_STAT_FILE}|${DATA_TRANSFER_STAT_FILE}|g"       "${SBATCH_SCRIPT_SIZE}"
    sed -i "s|\${SCRIPT_DIR}|${SCRIPT_DIR}|g"                                 "${SBATCH_SCRIPT_SIZE}"

    chmod +x "${SBATCH_SCRIPT_SIZE}"

    # Submit the generated script and return the job ID
    JOBID_SIZE=$(sbatch --parsable "${SBATCH_SCRIPT_SIZE}") || {
        echo "ERROR: sbatch failed" >&2
        return 1
    }
    echo "${JOBID_SIZE}"
}
