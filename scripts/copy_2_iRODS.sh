#!/bin/bash
# =============================================================================
# Script:      copy_2_iRODS.sh
# Description: Transfers all tarballed files for a sequencing run to iRODS,
#              adds metadata to each iRODS object, verifies transfer integrity
#              via checksum comparison, and records the final SUCCESS or FAILED
#              status in the universal transfer log.
#
#              Transfer targets are organised into five categories, each placed
#              in a structured iRODS directory hierarchy under:
#                bix_seqdata_archive/year_<YYYY>/<RUN_ID>/
#
#              Categories:
#                1. Collated samplesheet CSV
#                2. raw_data tarballs
#                3. machine_fastqs run directory tarballs
#                4. Per-indexset tarballs (machine_fastqs, run_data, results)
#                5. Unclassified tarballs (run_data_unclassified, results_unclassified)
#
#              For each file, if a matching checksum is already recorded in
#              iRODS (iCAT), the transfer is skipped. Otherwise the file is
#              uploaded with iput, the iRODS checksum retrieved, and integrity
#              verified by TRANSFER_CHECK. Metadata is then added with imeta.
#
# Usage:       Called by iRODS_archive.sh via sbatch — not run directly.
#              copy_2_iRODS.sh \
#                  <PATH2METADATA_LOG>          \   ($1)
#                  <TAR_FOLDER>                 \   ($2)
#                  <LOG_FILE>                   \   ($3)
#                  <CHECKSUM_FILE>              \   ($4)
#                  <UNIVERSAL_TRANSFER_LOG_FILE>\   ($5)
#                  <PROCESS_START_TIME>         \   ($6)
#                  <PSWD>                       \   ($7)
#                  <APP_DIR>                    \   ($8)
#                  <RAW_DATA_DIR_PATH>          \   ($9)
#                  [debug]                          ($10) optional, enables set -x
#
# Config file variables expected (sourced from config_file.txt):
#   IPUT_FLAGS   - Additional flags passed to iput (e.g. transfer threads)
#
# Metadata log variables expected (sourced from PATH2METADATA_LOG):
#   RUN_ID                           - Sequencing run identifier
#   INDEXSETS                        - Space-separated list of indexset labels
#   Sample_Name_<RUN_ID>_<INDEXSET>  - Sample names for each indexset
#   WORKFLOW_<RUN_ID>_<INDEXSET>     - Workflows run for each indexset
# =============================================================================

#set -x


# -----------------------------------------------------------------------------
# Section 1: Argument Intake
# Positional parameters assigned to named variables.
# -----------------------------------------------------------------------------
PATH2METADATA_LOG=$1    # Full path to the metadata log file for this run
TAR_FOLDER=${2}         # Directory containing the tarballed files to transfer
LOG_FILE=${3}           # Instance log file for this archive attempt
CHECKSUM_FILE=${4}      # File containing source checksums for all tarballs
UNIVERSAL_TRANSFER_LOG_FILE=${5}  # Shared transfer status tracking file
PROCESS_START_TIME=${6} # Epoch seconds from the start of iRODS_archive.sh
PSWD=${7}               # iRODS password used to authenticate via iinit
APP_DIR=${8}            # Application root directory
RAW_DATA_DIR_PATH=${9}  # Absolute path to the original raw_data run directory
DEBUG_OPT=${10}         # Set to "debug" to enable set -x tracing


# -----------------------------------------------------------------------------
# Section 2: Environment Setup
# Source the logging helper library and the application config. Record the
# start time for this script's elapsed time calculation.
# -----------------------------------------------------------------------------
source ${APP_DIR}/scripts/logging.sh
source ${APP_DIR}/config_file.txt

# Record start time for the iRODS transfer stage timing
START_TIME_2=$(date +%s)

# Derive the transfer stats file path from the log file path
DATA_TRANSFER_STAT_FILE=$(echo ${LOG_FILE} | sed 's/.log/.transfer.stats/')

# Extract the basename of the metadata log for reference
METADATA_LOGFILE=$(basename ${PATH2METADATA_LOG})


# -----------------------------------------------------------------------------
# Section 3: Metadata Log Sourcing
# Source the metadata log to load run-specific variables:
#   RUN_ID, INDEXSETS, Sample_Name_*, WORKFLOW_* etc.
# These variables are used throughout for iRODS paths and metadata tagging.
# -----------------------------------------------------------------------------
source ${PATH2METADATA_LOG}


# -----------------------------------------------------------------------------
# Section 4: Debug Mode
# Enable shell tracing if the debug option was passed.
# -----------------------------------------------------------------------------
if [ "${DEBUG_OPT}" = "debug" ]; then
    set -x
fi


# -----------------------------------------------------------------------------
# Section 5: iRODS Connection
# Authenticate to iRODS using the supplied password, verify connectivity with
# ils and icd, and log the outcome. Exit immediately if login fails.
# -----------------------------------------------------------------------------

# Inline login success banner — written to LOG_FILE on successful icd
logmsg() {
    echo " "
    echo "========================================================="
    echo "===>        iRODS Archive process Begins             <==="
    echo "===>      iRODS server login was successfull         <==="
    echo "========================================================="
    echo " "
}

echo $PSWD | iinit 
ils
icd

if [ $? -eq 0 ]; then
    logmsg >> ${LOG_FILE}
else
    echo "iRODS is not accessible, please check the connection or login credentials for iRODS" >> ${LOG_FILE}
    echo "ERROR: to test run iinit and check if contents can be listed by ils command"          >> ${LOG_FILE}
    echo -e "${RUN_ID} \t FAILED \t REASON : iRODS login failed. Check iRODS log file for details at : ${TAR_FOLDER} \t $(date +'%Y/%m/%d %H:%M:%S')" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
    echo -e "==================================$(date +'%Y/%m/%d %H:%M:%S')==================================" >> ${LOG_FILE}
    ils >> ${LOG_FILE}
    exit 1
fi


# -----------------------------------------------------------------------------
# Section 6: Runtime Variable Setup
# Derive iRODS transfer configuration and path components used throughout.
# Variables sourced from metadata log (RUN_ID, INDEXSETS, Sample_Name_*, etc.)
# are available from Section 3 onward.
#
# Metadata log variables available after sourcing PATH2METADATA_LOG:
#   RUN_ID
#   INDEXSETS
#   Sample_Name_<RUNID>_<INDEXSET>   - sample names per indexset
#   WORKFLOW_<RUNID>_<INDEXSET>      - workflows per indexset
#   Sample_Name_<RUNID>              - all sample names for the run
#   WORKFLOW_<RUNID>                 - all workflows for the run
# -----------------------------------------------------------------------------

# iRODS transfer flags: restart log path + any additional flags from config
# iRODS_transfer_flags="-N 1"
iRODS_transfer_flags="-X ${TAR_FOLDER}/${RUN_ID}.restartlog ${IPUT_FLAGS}"

# Parse date, sequencer, lane/var and flowcell components from the run ID
# Run ID format: YYMMDD_SEQUENCERID_VAR_FLOWCELLID  (or YYYYMMDD variant)
IFS=_ read -r RUN_DATE SEQUENCER_ID VAR FLOWCELL_ID <<< ${RUN_ID}

# iRODS home path resolved at runtime
IHOME=$(icd; ipwd)

# iRODS-specific detail log for iput stdout/stderr (consumed by log_and_run_irods)
export IRODS_LOG_FILE=${TAR_FOLDER}/${RUN_ID}_iRODS.log


# -----------------------------------------------------------------------------
# Section 7: iRODS Year/Run Directory Creation
# Determine the year directory name from RUN_DATE (handles both YYMMDD and
# YYYYMMDD formats) and create the top-level run directory in iRODS.
# -----------------------------------------------------------------------------

# Distinguish 6-digit (YYMMDD) from 8-digit (YYYYMMDD) run date formats.
# Assumption: the leading digits always represent the year.
YEAR_FORMAT=`echo ${#RUN_DATE}`

if [ ${YEAR_FORMAT} -eq 6 ]; then
    YEAR_DIR="bix_seqdata_archive/year_20$(echo ${RUN_DATE:0:2})"
elif [ ${YEAR_FORMAT} -eq 8 ]; then
    YEAR_DIR="bix_seqdata_archive/year_$(echo ${RUN_DATE:0:4})"
else
    echo "Year format is not correct" #&& exit 1
fi

# Create the year-level and run-level iRODS collections
imkdir -p ${YEAR_DIR}
imkdir -p ${YEAR_DIR}/${RUN_ID}

echo -e "\n*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*_*\n"
echo -e "|> RUN_ID : ${RUN_ID}  \n|"
echo -e "Samplesheet Upload"


# -----------------------------------------------------------------------------
# Section 8: add_imeta_iRODS_object Function
# Adds standard metadata AVUs (Attribute-Value-Unit pairs) to an iRODS data
# object or collection using imeta. Applies run-level fields (RUN_DATE,
# SEQUENCER_ID, FLOWCELL_ID) and optionally an Indexset AVU. Then iterates
# over Sample_Name and Workflow_Name values sourced from the metadata log.
#
# Arguments:
#   $1  DATA_OBJECT_ID      - Run ID with optional indexset suffix
#                             (e.g. 250207_VL00114_727_AAG2V2GM5_indexsetA)
#   $2  iRODS_OBJECT_NAME   - Full iRODS path to the data object or collection
#                             (e.g. raw_data-250207_VL00114_727_AAG2V2GM5.tar.gz)
#   $3  OBJECT_FLAG         - iRODS object type flag: "d" (data object) or
#                             "C" (collection)
#   $4  DATA_STAGE_IMETA    - Data store label: raw_data, machine_fastqs,
#                             run_data, or results
#   $5  METAVARIABLE_SUFFIX - Indexset label (e.g. indexsetA) or "ALL" to
#                             skip adding the Indexset AVU
# -----------------------------------------------------------------------------
add_imeta_iRODS_object() {
    DATA_OBJECT_ID=${1}
    IRODS_OBJECT_NAME=${2}
    OBJECT_FLAG=${3}
    DATA_STAGE_IMETA=${4}
    METAVARIABLE_SUFFIX=${5}
    # echo ${RUN_ID} ${RUN_DATE} ${SEQUENCER_ID} ${FLOWCELL_ID} ${DATA_STAGE_IMETA} # for test

    # Add core run-level metadata AVUs to the iRODS object
    #imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} RUN_ID ${RUN_ID}
    imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} RUN_DATE     ${RUN_DATE}
    imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} SEQUENCER_ID ${SEQUENCER_ID}
    imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} FLOWCELL_ID  ${FLOWCELL_ID}

    # Add indexset AVU only when a specific indexset is applicable
    if [ "${METAVARIABLE_SUFFIX}" != "ALL" ]; then
        imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} Indexset ${METAVARIABLE_SUFFIX}
    fi
    #imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} DATA_STAGE ${DATA_STAGE_IMETA}

    # Add per-sample metadata: Sample_Name_<METAVARIABLE_SUFFIX> is an
    # indirect variable reference resolved from the sourced metadata log
    SAMPLE_NAME_IMETA=Sample_Name_${METAVARIABLE_SUFFIX}
    for EACH_SAMPLE_NAME in ${!SAMPLE_NAME_IMETA}; do
        #echo $EACH_SAMPLE_NAME
        imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} Sample_Name ${EACH_SAMPLE_NAME}
    done

    # Add per-workflow metadata: WORKFLOW_<METAVARIABLE_SUFFIX> is an
    # indirect variable reference resolved from the sourced metadata log
    WORKFLOW_IMETA=WORKFLOW_${METAVARIABLE_SUFFIX}
    for EACH_WORKFLOW in ${!WORKFLOW_IMETA}; do
        #echo $EACH_WORKFLOW
        imeta add -${OBJECT_FLAG} ${IRODS_OBJECT_NAME} Workflow_Name ${EACH_WORKFLOW}
    done
}


# -----------------------------------------------------------------------------
# Section 9: Samplesheet Archiving
# Upload the collated samplesheet CSV to iRODS if not already present with a
# verified checksum. Adds DATA_STAGE and OBJECT_ID metadata on upload.
# -----------------------------------------------------------------------------
echo -e "\n*_* SAMPLESHEET ARCHIVING *_*\n"

if [ -e ${TAR_FOLDER}/${RUN_ID}_samplesheet.csv ]; then
    RUN_CSV_FILE="${RUN_ID}_samplesheet.csv"
    SOURCE_CHKSUM=$(grep "src_chksum_${RUN_CSV_FILE}" ${CHECKSUM_FILE} | cut -d" " -f2)
    IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUN_CSV_FILE} 2>/dev/null | cut -d":" -f2)

    if [ -n "${SOURCE_CHKSUM:-}" ] && [ "${SOURCE_CHKSUM}" = "${IRODS_CHKSUM}" ]; then
        # Already archived and verified — skip
        echo "      ${RUN_CSV_FILE} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
        log_message "INFO" "${RUN_CSV_FILE} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
        log_message "INFO" "Skipping transfer : ${RUN_CSV_FILE} is already archived and integrity is verified with checksum"
    else
        IRODS_OBJECT_PATH_CSV="${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUN_CSV_FILE}"

        # Remove any stale partial object before re-uploading
        if ils ${IRODS_OBJECT_PATH_CSV} >/dev/null 2>&1; then
            irm -rf ${IRODS_OBJECT_PATH_CSV}
            sleep 20
        fi

        FSIZE=$(stat -c%s "${TAR_FOLDER}/${RUN_ID}_samplesheet.csv")
        log_and_run_irods $FSIZE \
            "iput -f ${iRODS_transfer_flags} -k ${TAR_FOLDER}/${RUN_ID}_samplesheet.csv ${YEAR_DIR}/${RUN_ID}/"
        sleep 20   # Allow iCAT database to be updated with the checksum value
        IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUN_CSV_FILE} 2>/dev/null | cut -d":" -f2)
        TRANSFER_CHECK "${IRODS_OBJECT_PATH_CSV}" "${SOURCE_CHKSUM}" "${IRODS_CHKSUM}"
        echo "      ${IRODS_OBJECT_PATH_CSV} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"

        # Tag with data stage and object identity metadata
        imeta add -d ${YEAR_DIR}/${RUN_ID}/${RUN_ID}_samplesheet.csv OBJECT_ID ${RUN_ID}_samplesheet.csv
        imeta add -d ${YEAR_DIR}/${RUN_ID}/${RUN_ID}_samplesheet.csv DATA_STAGE samplesheet
    fi
else
    log_message "WARNING :ATTENTION" "No Samplesheet found for the run to archive, RUNID : ${RUN_ID}"
fi

#unset SOURCE_CHKSUM IRODS_CHKSUM


# -----------------------------------------------------------------------------
# Section 10: Raw Data Archiving
# Upload all raw_data-<RUN_ID>*.tar.gz* files to the run-level iRODS
# collection. Metadata (DATA_STAGE, RUN_ID, OBJECT_ID) and run-level AVUs
# are added via add_imeta_iRODS_object for the first split part (suffix .aa).
# -----------------------------------------------------------------------------
echo -e "\n*_* RAW DATA ARCHIVING *_*\n"

RAW_DATA_FILES=$(find ${TAR_FOLDER} -maxdepth 1 -type f -name "raw_data-${RUN_ID}*.tar.gz*")

if [ -n "RAW_DATA_FILES" ]; then
    echo "Raw data "
    for EACH_RAW_DATA_FILE in ${RAW_DATA_FILES}; do
        echo "  File Under Process: ${EACH_RAW_DATA_FILE}"
        RAW_DATA_FILE_NAME=$(basename ${EACH_RAW_DATA_FILE})
        SOURCE_CHKSUM=$(grep "src_chksum_${RAW_DATA_FILE_NAME}" ${CHECKSUM_FILE} | cut -d" " -f2)
        IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME} 2>/dev/null | cut -d":" -f2)

        if [ -n "${SOURCE_CHKSUM:-}" ] && [ "${SOURCE_CHKSUM}" = "${IRODS_CHKSUM}" ]; then
            # Already archived and verified — skip
            echo "      ${RAW_DATA_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "${RAW_DATA_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "Skipping transfer : ${EACH_RAW_DATA_FILE} is already archived and integrity is verified with checksum"
        else
            IRODS_OBJECT_PATH_RAW="${IHOME}/${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME}"

            # Remove any stale partial object before re-uploading
            if ils ${IRODS_OBJECT_PATH_RAW} >/dev/null 2>&1; then
                irm -rf ${IRODS_OBJECT_PATH_RAW}
                sleep 20
            fi

            FSIZE=$(stat -c%s "${EACH_RAW_DATA_FILE}")
            log_and_run_irods $FSIZE \
                "iput -f ${iRODS_transfer_flags} -k ${EACH_RAW_DATA_FILE} ${YEAR_DIR}/${RUN_ID}/"
            sleep 20   # Allow iCAT database to be updated with the checksum value
            IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME} 2>/dev/null | cut -d":" -f2)
            #IRODS_OBJECT_PATH_RAW="${IHOME}/${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME}"
            TRANSFER_CHECK "${IRODS_OBJECT_PATH_RAW}" "${SOURCE_CHKSUM}" "${IRODS_CHKSUM}"
            echo "      ${RAW_DATA_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"

            # Add metadata only to the first split part (.aa suffix) to avoid
            # duplicate AVUs across split parts of the same tarball
            if [[ "${RAW_DATA_FILE_NAME##*.}" == "aa" ]]; then
                #echo "     Run location: "${YEAR_DIR}/${RUN_ID}/""
                #echo "     Working Dir: $(ipwd)"
                #printf "       Data Object list at this location : %s" "$(ils ${YEAR_DIR}/${RUN_ID}/ | tr '\n' ' ')"
                #echo "     Raw data obj with metadata: ${RAW_DATA_FILE_NAME}"
                imeta add -d "${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME}" DATA_STAGE raw_data
                imeta add -d "${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME}" RUN_ID     ${RUN_ID}
                imeta add -d "${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME}" OBJECT_ID  ${RAW_DATA_FILE_NAME}
                add_imeta_iRODS_object ${RUN_ID} ${YEAR_DIR}/${RUN_ID}/${RAW_DATA_FILE_NAME} d raw_data ALL
            fi
        fi
#       unset SOURCE_CHKSUM IRODS_CHKSUM
    done
fi


# -----------------------------------------------------------------------------
# Section 11: machine_fastqs Run Directory Archiving
# Upload machine_fastqs_runDir-<RUN_ID>*.tar.gz* files to a dedicated
# sub-collection and add OBJECT_ID metadata per file.
# -----------------------------------------------------------------------------
echo -e "\n*_* MACHINE_FASTQS RUNDIR ARCHIVING *_*\n"

imkdir -p ${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}

MFQ_RUNDIRS_IRODS=$(find ${TAR_FOLDER} -maxdepth 1 -type f -name "machine_fastqs_runDir-${RUN_ID}*.tar.gz*")

if [ -n "$MFQ_RUNDIRS_IRODS" ]; then
    echo "Machine_fastqs Rundir(s):"
    for MFQ_RUNDIR_IRODS in ${MFQ_RUNDIRS_IRODS}; do
        echo "  File Under Process:  ${MFQ_RUNDIR_IRODS}"
        MFQRD_NAME=$(basename ${MFQ_RUNDIR_IRODS})
        SOURCE_CHKSUM=$(grep "src_chksum_${MFQRD_NAME}" ${CHECKSUM_FILE} | cut -d" " -f2)
        IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/${MFQRD_NAME} 2>/dev/null | cut -d":" -f2)

        if [ -n "${SOURCE_CHKSUM:-}" ] && [ "${SOURCE_CHKSUM}" = "${IRODS_CHKSUM}" ]; then
            # Already archived and verified — skip
            echo "      ${MFQRD_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "${MFQRD_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "Skipping transfer : ${MFQ_RUNDIR_IRODS} is already archived and integrity is verified with checksum"
        else
            IRODS_OBJECT_PATH_MFQRD="${IHOME}/${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/${MFQRD_NAME}"

            # Remove any stale partial object before re-uploading
            if ils ${IRODS_OBJECT_PATH_MFQRD} >/dev/null 2>&1; then
                irm -rf ${IRODS_OBJECT_PATH_MFQRD}
                sleep 20
            fi

            FSIZE=$(stat -c%s "${MFQ_RUNDIR_IRODS}")
            log_and_run_irods $FSIZE \
                "iput -f ${iRODS_transfer_flags} -k ${MFQ_RUNDIR_IRODS} ${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/"
            sleep 20   # Allow iCAT database to be updated with the checksum value
            IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/${MFQRD_NAME} 2>/dev/null | cut -d":" -f2)
            #IRODS_OBJECT_PATH_MFQRD="${IHOME}/${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/${MFQRD_NAME}"
            TRANSFER_CHECK "${IRODS_OBJECT_PATH_MFQRD}" "${SOURCE_CHKSUM}" "${IRODS_CHKSUM}"
            echo "      ${MFQRD_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
        fi
#       unset SOURCE_CHKSUM IRODS_CHKSUM
        imeta add -d ${YEAR_DIR}/${RUN_ID}/machine_fastqs_runDir-${RUN_ID}/${MFQRD_NAME} \
            OBJECT_ID machine_fastqs_runDir-${MFQRD_NAME}
    done
fi


# -----------------------------------------------------------------------------
# Section 12: Per-Indexset Archiving
# Iterate over all known indexsets (plus "all") and upload the corresponding
# machine_fastqs, run_data, and results tarballs into a per-indexset iRODS
# sub-collection. Metadata is added at both collection and object level.
# Objects are matched by DATA_STAGE prefix using a case statement.
# -----------------------------------------------------------------------------
echo -e "\n*_* PER-INDEXSET ARCHIVING *_*\n"

log_message "INFO" " These indexsets were used in RUN  \t : ${INDEXSETS}"

INDEXSETS+=" all"
for INDEXSET in ${INDEXSETS}; do
    mapfile -t FILES_TO_ARCHIVE < <(find "${TAR_FOLDER}" -maxdepth 1 -type f \
        \( -name "machine_fastqs-*${INDEXSET}*" \
           -o -name "run_data-*${INDEXSET}*" \
           -o -name "results-*${INDEXSET}*" \))

    if [ ${#FILES_TO_ARCHIVE[@]} -eq 0 ]; then
        log_message "INFO" "No ${INDEXSET}-labelled files to archive for indexset: ${INDEXSET}"
    else
        echo "Indexset Under process : ${INDEXSET} of Run : ${RUN_ID}"
        RUNID_INDEXSET="${RUN_ID}_${INDEXSET}"

        # Create and tag the per-indexset iRODS collection
        imkdir -p "${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}"
        add_imeta_iRODS_object ${RUNID_INDEXSET} ${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET} C mf_rd_res ${INDEXSET}
        imeta add -C ${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET} RUN_ID ${RUN_ID}
        #imeta add -C ${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET} Indexset ${INDEXSET}

        for FILE_TO_ARCHIVE in "${FILES_TO_ARCHIVE[@]}"; do
            echo "  File Under Process:  ${FILE_TO_ARCHIVE}"
            FILE_TO_ARCHIVE_NAME=$(basename "${FILE_TO_ARCHIVE}")
            FILE_PREFIX="${FILE_TO_ARCHIVE_NAME%%-*}"   # Extract data-store tag (e.g. machine_fastqs)
            FSIZE=$(stat -c%s "${FILE_TO_ARCHIVE}")
            SOURCE_CHKSUM=$(grep "src_chksum_${FILE_TO_ARCHIVE_NAME}" ${CHECKSUM_FILE} | cut -d" " -f2)
            IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME} 2>/dev/null | cut -d":" -f2)

            if [ -n "${SOURCE_CHKSUM:-}" ] && [ "${SOURCE_CHKSUM}" = "${IRODS_CHKSUM}" ]; then
                # Already archived and verified — skip
                echo "      ${FILE_TO_ARCHIVE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
                log_message "INFO" "${FILE_TO_ARCHIVE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
                log_message "INFO" "Skipping transfer : ${FILE_TO_ARCHIVE} is already archived and integrity is verified with checksum"
            else
                IRODS_OBJECT_PATH_ARCHIVE_NAME="${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME}"

                # Remove any stale partial object before re-uploading
                if ils ${IRODS_OBJECT_PATH_ARCHIVE_NAME} >/dev/null 2>&1; then
                    irm -rf ${IRODS_OBJECT_PATH_ARCHIVE_NAME}
                    sleep 20
                fi

                log_and_run_irods $FSIZE \
                    "iput -f ${iRODS_transfer_flags} -k ${FILE_TO_ARCHIVE} ${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/"
                sleep 20   # Allow iCAT database to be updated with the checksum value
                IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME} 2>/dev/null | cut -d":" -f2)
                #IRODS_OBJECT_PATH_ARCHIVE_NAME="${IHOME}/${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME}"
                TRANSFER_CHECK "${IRODS_OBJECT_PATH_ARCHIVE_NAME}" "${SOURCE_CHKSUM}" "${IRODS_CHKSUM}"
                echo "      ${FILE_TO_ARCHIVE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"

                # Add DATA_STAGE and OBJECT_ID metadata based on the file's data-store prefix
                case "$FILE_PREFIX" in
                    machine_fastqs|run_data|results)
                        imeta add -d "${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME}" \
                            DATA_STAGE "${FILE_PREFIX}"
                        imeta add -d "${YEAR_DIR}/${RUN_ID}/${RUNID_INDEXSET}/${FILE_TO_ARCHIVE_NAME}" \
                            OBJECT_ID  "${FILE_TO_ARCHIVE_NAME}"
                        ;;
                    *)
                        log_message "WARNING :ATTENTION" "File not archived: ${FILE_TO_ARCHIVE_NAME}"
                        ;;
                esac
            fi
#           unset SOURCE_CHKSUM IRODS_CHKSUM
        done
    fi
done


# -----------------------------------------------------------------------------
# Section 13: Unclassified File Archiving
# Upload any run_data_unclassified or results_unclassified tarballs that were
# not covered by the indexset loop into a dedicated UNCLASSIFIED sub-collection.
# Adds OBJECT_ID metadata per file.
# -----------------------------------------------------------------------------
echo -e "\n*_* UNCLASSIFIED ARCHIVING *_*\n"

mapfile -t UNCLASSIFIED < <(find "${TAR_FOLDER}" -maxdepth 1 -type f \
    \( -name "run_data_unclassified*" -o -name "results_unclassified*" \))

if [ ${#UNCLASSIFIED[@]} -eq 0 ]; then
    log_message "INFO" "No Loose ends : Everything Archived properly"
else
    UNCLASSIFIED_DIR="${RUN_ID}_UNCLASSIFIED"
    imkdir -p ${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}
    echo "Unclassified Data :"

    for UNCLASSIFIED_FILE in "${UNCLASSIFIED[@]}"; do
        echo "  File Under Process:  ${UNCLASSIFIED_FILE}"
        UNCLASSIFIED_FILE_NAME=$(basename "${UNCLASSIFIED_FILE}")
        SOURCE_CHKSUM=$(grep "src_chksum_${UNCLASSIFIED_FILE_NAME}" ${CHECKSUM_FILE} | cut -d" " -f2)
        IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}/${UNCLASSIFIED_FILE_NAME} 2>/dev/null | cut -d":" -f2)
        FSIZE=$(stat -c%s "${UNCLASSIFIED_FILE}")

        if [ -n "${SOURCE_CHKSUM:-}" ] && [ "${SOURCE_CHKSUM}" = "${IRODS_CHKSUM}" ]; then
            # Already archived and verified — skip
            echo "      ${UNCLASSIFIED_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "${UNCLASSIFIED_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            log_message "INFO" "Skipping transfer : ${UNCLASSIFIED_FILE} is already archived and integrity is verified with checksum"
        else
            IRODS_OBJECT_PATH_UNCLASSIFIED="${IHOME}/${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}/${UNCLASSIFIED_FILE_NAME}"

            # Remove any stale partial object before re-uploading
            if ils ${IRODS_OBJECT_PATH_UNCLASSIFIED} >/dev/null 2>&1; then
                irm -rf ${IRODS_OBJECT_PATH_UNCLASSIFIED}
                sleep 20
            fi

            log_and_run_irods $FSIZE \
                "iput -f ${iRODS_transfer_flags} -k ${UNCLASSIFIED_FILE} ${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}/"
            sleep 20   # Allow iCAT database to be updated with the checksum value
            IRODS_CHKSUM=$(ichksum ${IHOME}/${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}/${UNCLASSIFIED_FILE_NAME} 2>/dev/null | cut -d":" -f2)
            TRANSFER_CHECK "${IRODS_OBJECT_PATH_UNCLASSIFIED}" "${SOURCE_CHKSUM}" "${IRODS_CHKSUM}"
            echo "      ${UNCLASSIFIED_FILE_NAME} : ${SOURCE_CHKSUM} : ${IRODS_CHKSUM}"
            imeta add -d ${YEAR_DIR}/${RUN_ID}/${UNCLASSIFIED_DIR}/${UNCLASSIFIED_FILE_NAME} \
                OBJECT_ID ${UNCLASSIFIED_FILE_NAME}
        fi
    done
fi


# -----------------------------------------------------------------------------
# Section 14: Transfer Outcome and Statistics
# Count checksum failures in the log to determine overall run status.
# Record elapsed times and transfer speed into the stats file.
# Write the final SUCCESS or FAILED line to the universal transfer log.
# On success, delete the local tarballs to reclaim disk space.
# On failure, the tarballs are retained for investigation.
# -----------------------------------------------------------------------------

# Count the number of failed checksum verifications recorded in the log
FAILED_TRANSFER_COUNT=$(grep -c "ERROR: iput" ${LOG_FILE})
FAILED_CHECKSUM_COUNT=$(grep -c "TRANSFER_UNSUCCESSFUL CHECKSUM FAILED" ${LOG_FILE})

# Record elapsed time for the iRODS transfer stage
timer $START_TIME_2 "Archiving to iRODS and addition of metadata."
IROD_TRANSFER_TIME=$(timer2 ${START_TIME_2})
TIME_TAKEN_ENTIRE_PROCESS=$(timer2 ${PROCESS_START_TIME})

# Source the stats file to get TAR_DIR_SIZE_BYTES for speed calculation
source ${DATA_TRANSFER_STAT_FILE}
IRODS_TRANSFER_SPEED=$(SPEED_CALC ${TAR_DIR_SIZE_BYTES} ${START_TIME_2})

# Append timing and speed metrics to the stats file
{
echo "IROD_TRANSFER_TIME=\"${IROD_TRANSFER_TIME}\""
echo "IRODS_TRANSFER_SPEED=\"${IRODS_TRANSFER_SPEED}\""
echo "TIME_TAKEN_ENTIRE_PROCESS=\"${TIME_TAKEN_ENTIRE_PROCESS}\""
} >> ${DATA_TRANSFER_STAT_FILE}

if [ "${FAILED_CHECKSUM_COUNT}" -eq 0 ] && [ "${FAILED_TRANSFER_COUNT}" -eq 0 ]; then
    # All transfers verified — record success and clean up local tarballs
    echo -e "${RUN_ID} \t SUCCESS \t ${TIME_TAKEN_ENTIRE_PROCESS} \t Log : ${LOG_FILE} \t RAW_DATA_DIR_PATH : ${RAW_DATA_DIR_PATH}" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
    rm ${TAR_FOLDER}/*.tar.gz.*
    log_message "INFO" "tar files are deleted on source : ${TAR_FOLDER} to release space"
else
    # One or more checksum failures — record failure with count
    echo -e "${RUN_ID} \t FAILED..${FAILED_COUNT} \t ${TIME_TAKEN_ENTIRE_PROCESS} \t Log : ${LOG_FILE} \t RAW_DATA_DIR_PATH : ${RAW_DATA_DIR_PATH}" >> ${UNIVERSAL_TRANSFER_LOG_FILE}
fi


# -----------------------------------------------------------------------------
# Section 15: Debug Mode Teardown
# Disable shell tracing if it was enabled in Section 4.
# -----------------------------------------------------------------------------
if [ "${DEBUG_OPT}" = "debug" ]; then
    set +x
fi
