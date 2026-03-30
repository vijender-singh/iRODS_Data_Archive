#!/bin/bash
# =============================================================================
# Script:      extract.sh
# Description: Retrieves archived sequencing run data from iRODS by querying
#              the iCAT metadata catalogue for objects matching a supplied
#              attribute-value pair and data stage. Supports retrieval at
#              multiple levels of granularity:
#                - A single data stage (raw_data, machine_fastqs, run_data,
#                  results) for a sample or run
#                - All data for a specific indexset (IndexsetRUN)
#                - All data for an entire run (RUN)
#
#              After retrieval, files can optionally be uncompressed and
#              untarred via untar_uncompress.sh.
#
# Usage:
#   ./extract.sh -o <output_dir> -a <attribute> -v <value> \
#       -s <raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN> \
#       [-i <indexset>] [-r TRUE|FALSE] [-z TRUE|FALSE] [-l TRUE|FALSE]
#
# Options:
#   -o  <output_dir>     Local directory for retrieved data (REQUIRED)
#   -a  <attribute>      iRODS metadata attribute to search on (REQUIRED)
#                        e.g. Sample_Name, RUN_ID
#   -v  <value>          Value for the metadata attribute (REQUIRED)
#   -s  <data_stage>     Data stage to retrieve (REQUIRED). One of:
#                          raw_data       - Raw sequencing run data
#                          machine_fastqs - Per-indexset FASTQ files
#                          run_data       - Per-indexset pipeline outputs
#                          results        - Per-indexset analysis results
#                          IndexsetRUN    - All data for a specific indexset
#                          RUN            - All data for the entire run
#   -i  <indexset>       Indexset label (e.g. A, B, C). Used with -a RUN_ID
#                        to scope retrieval to a single indexset. If omitted,
#                        all indexsets for the run are retrieved.
#   -r  TRUE|FALSE       Restore objects on SMED storage before download.
#                        Requires elevated privileges. [Default: FALSE]
#   -z  TRUE|FALSE       Uncompress and untar retrieved files. [Default: TRUE]
#   -l  TRUE|FALSE       Enable detailed process logging. [Default: TRUE]
#
# iRODS search strategy:
#   When searching by Sample_Name, raw_data objects are queried at the data
#   object level (-d) since they carry all MOLISIDs across all indexsets.
#   Collections (machine_fastqs, run_data, results) are queried at the
#   collection level (-C) as they are scoped to a specific indexset.
#
# Example:
#   ./extract.sh \
#       -o /data/retrieved \
#       -a Sample_Name \
#       -v SAMPLE123 \
#       -s machine_fastqs
# =============================================================================

set -x

echo "======================================extract.sh============================="


# -----------------------------------------------------------------------------
# Section 1: Environment Setup
# Load application configuration and iRODS credentials.
# -----------------------------------------------------------------------------
APP_DIR=/home/phe.gov.uk/vijender.singh/irods_archive_and_retrieval
source /home/phe.gov.uk/vijender.singh/.irods_credential


# -----------------------------------------------------------------------------
# Section 2: Default Variable Initialisation
# All arguments default to empty or defined safe values before parsing.
# -----------------------------------------------------------------------------
OUTPUT_DIR=""
SMED_RESTORE=FALSE
IMETA_ATTRIBUTE=""
IMETA_VALUE=""
DATA_STAGE=""       # Acceptable values: raw_data | machine_fastqs | run_data | results | IndexsetRUN | RUN
INDEXSET=""
UNCOMPRESS="TRUE"
VERBOSE="TRUE"
FILEPATH=""


# -----------------------------------------------------------------------------
# Section 3: Usage Message
# Printed when required arguments are missing or an invalid option is supplied.
# -----------------------------------------------------------------------------
usage_extract() {
    echo -e "\t extract \t : Extract a data object based on attribute:value pair and data stage"
    echo -e " $0 extract -o <outdir> -a <attribute> -v <value> -s <raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN> [optional arguments]"
    echo -e " \t Arguments"
    echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here [REQUIRED]."
    echo -e "\t -a \t <imeta attribute> \t\t Provide the attribute of imetadata. Full list is given below [REQUIRED]."
    echo -e "\t -v \t <imeta value> \t\t\t "'Provide the imeta-attribute "Value". [REQUIRED].'
    echo -e "\t -s \t raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN   specify one of the data stages [REQUIRED]"
    echo -e "\t    \t\t\t raw_data \t: raw_data of the run where given sample was included"
    echo -e "\t    \t\t\t machine_fastqs \t: machine_fastqs of indexset (if applicable) where given sample was included"
    echo -e "\t    \t\t\t run_data \t: run_data of indexset (if applicable) where given sample was included"
    echo -e "\t    \t\t\t results \t: results of indexset (if applicable) where given sample was included"
    echo -e "\t    \t\t\t IndexsetRUN \t: All data of an indexset including raw_data, machine_fastqs, run_data, results where given sample was included"
    echo -e "\t    \t\t\t RUN \t: All data of the RUN including all indexsets and their raw_data, machine_fastqs, run_data, results where given sample was included"
    echo -e "\t -i \t  A|B|C|D|... \t: Provide Indexset value of RUN, used with attribute RUN_ID. If not provided, data for all indexsets used in RUN will be downloaded"
    echo -e "\t -r \t TRUE|FALSE \t\t\t Optional: allow files to be restored on SMED from where the object was originally archived [Not recommended; requires sufficient privileges] [Default FALSE]"
    echo -e "\t -z \t TRUE|FALSE \t\t\t Optional: Uncompress and untar the data [Default TRUE]"
    echo -e "\t -l \t TRUE|FALSE \t\t\t Optional: Detailed log of the archive process [Default TRUE]"
    exit 1
}


# -----------------------------------------------------------------------------
# Section 4: Argument Parsing
# Parse all supported flags. Unknown flags or missing values trigger usage.
# -----------------------------------------------------------------------------
while getopts ":o:a:v:s:r:i:e:z:l:p:w" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG" ;;
        a) IMETA_ATTRIBUTE="$OPTARG" ;;
        v) IMETA_VALUE="$OPTARG" ;;
        s) DATA_STAGE="$OPTARG" ;;
        r) SMED_RESTORE="$OPTARG" ;;
        i) INDEXSET="$OPTARG" ;;
        z) UNCOMPRESS="$OPTARG" ;;
        l) VERBOSE="$OPTARG" ;;
        w) APP_DIR="$OPTARG" ;;
        h) usage ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage_extract ;;
        :)  echo "Option -$OPTARG requires an argument." >&2; usage_extract ;;
    esac
done


# -----------------------------------------------------------------------------
# Section 5: Log File Initialisation
# Create a timestamped log file name for this retrieval session.
# -----------------------------------------------------------------------------
SUFFIX=$(date +"%Y-%m-%d-%H%M%S")

LOG_FILE=data-retrieval_log-${SUFFIX}.log


# -----------------------------------------------------------------------------
# Section 6: Data Stage Validation
# Ensure the supplied -s value is one of the six accepted stage labels.
# -----------------------------------------------------------------------------
if [[ "$DATA_STAGE" != "raw_data"       && \
      "$DATA_STAGE" != "machine_fastqs" && \
      "$DATA_STAGE" != "run_data"        && \
      "$DATA_STAGE" != "results"         && \
      "$DATA_STAGE" != "IndexsetRUN"     && \
      "$DATA_STAGE" != "RUN" ]]; then
    echo "Error: Invalid function '$DATA_STAGE'. Must be 'raw_data', 'machine_fastqs', 'run_data', 'results', 'IndexsetRUN' or 'RUN'" >&2
    usage_extract
fi


# -----------------------------------------------------------------------------
# Section 7: Configuration Summary
# Print all resolved argument values for operator confirmation before login.
#
# iRODS search strategy note:
#   The raw_data object carries all MOLISIDs across every indexset, so it is
#   searched at the data object level (-d). Collections (machine_fastqs,
#   run_data, results) are scoped to a specific indexset and are searched at
#   the collection level (-C).
#
#   1) Searching data objects (-d) by MOLISID returns the raw_data object and
#      its parent collection path.
#   2) Searching collections (-C) by MOLISID returns the specific indexset
#      collection containing that MOLISID.
# -----------------------------------------------------------------------------
echo "OUTPUT_DIR=${OUTPUT_DIR} \n
        IMETA_ATTRIBUTE=$IMETA_ATTRIBUTE \n
        IMETA_VALUE=$IMETA_VALUE \n
        DATA_STAGE=$DATA_STAGE \n
        SMED_RESTORE=$SMED_RESTORE \n
        INDEXSET=$INDEXSET \n
        UNCOMPRESS=$UNCOMPRESS \n
        VERBOSE=$VERBOSE \n
        APP_DIR=$APP_DIR"

#exit 1


# -----------------------------------------------------------------------------
# Section 8: iRODS Authentication
# Authenticate and verify connectivity. Exit with a logged error if login fails.
# -----------------------------------------------------------------------------
logmsg() {
    echo " "
    echo "========================================================="
    echo "===>        iRODS retrieval process Begins            <==="
    echo "===>      iRODS server login was successfull          <==="
    echo "========================================================="
    echo " "
}

echo $PSWD | iinit
ils

if [ $? -eq 0 ]; then
    logmsg >> ${LOG_FILE}
else
	{
    echo "iRODS is not accessible, please check the connection or login credentials for iRODS"
    echo "ERROR: to test run iinit and check if contents can be listed by ils command"
    echo -e "RETRIEVAL FAILED \t REASON : iRODS login failed. Check iRODS log file for details"
    echo -e "==================================$(date +'%Y/%m/%d %H:%M:%S')=================================="
    ils 
	}	>> ${LOG_FILE}
    exit 1
fi


# -----------------------------------------------------------------------------
# Section 9: iRODS Item Discovery
# Build EXTRACT_ITEM — a list of iRODS paths to retrieve — by querying iCAT
# using iquest. The query strategy differs by attribute and data stage:
#
#   IMETA_ATTRIBUTE = Sample_Name
#     Searches metadata attached to data objects (raw_data) or collections
#     (machine_fastqs, run_data, results, IndexsetRUN, RUN).
#
#   IMETA_ATTRIBUTE = RUN_ID, no INDEXSET specified
#     Searches across all indexset collections for the run.
#
#   IMETA_ATTRIBUTE = RUN_ID, INDEXSET specified
#     Scopes the search to the specific indexset collection.
#
# For split tarballs (e.g. .tar.gz.aa, .tar.gz.ab), the first split part name
# is generalised (last two characters stripped, replaced with %) so that all
# parts are captured in a single LIKE query.
# -----------------------------------------------------------------------------

if [ "${IMETA_ATTRIBUTE}" = "Sample_Name" ]; then

    case "$DATA_STAGE" in
        RUN)
            # Retrieve the top-level run collection for this sample
            EXTRACT_ITEM=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            ;;
        IndexsetRUN)
            # Retrieve the indexset collection containing this sample
            EXTRACT_ITEM=$(iquest "%s" "SELECT COLL_NAME WHERE META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE'")
            ;;
        raw_data)
            # Locate the collection holding the raw_data object
            OBJ_PATH=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            # Get the first raw_data object name (e.g. raw_data-221221_VL00163_96_AAATTMVM5.tar.gz.aa)
            OBJ_NAME_F=($(iquest "%s" "SELECT DATA_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'"))
            # Generalise the name for LIKE search — strips last 2 chars (.aa) and appends %
            # to match all split parts (.aa, .ab, .ac ...)
            OBJ_NAME_GENERALISED="${OBJ_NAME_F[0]%??}%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH' AND DATA_NAME LIKE '$OBJ_NAME_GENERALISED'")
            ;;
        machine_fastqs)
            # Retrieve the indexset collection path for this sample
            OBJ_PATH_MFQ=$(iquest "%s" "SELECT COLL_NAME WHERE META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE'")
            # Extract the indexset directory name (e.g. 230616_VL00114_323_AACN25LM5_indexsetA)
            RUN_ID_INDEXSET=$(basename ${OBJ_PATH_MFQ})
            # Build a LIKE pattern to match all machine_fastqs split parts for this indexset
            MFQ_NAME_GENERALISED="machine_fastqs-${RUN_ID_INDEXSET}.tar.gz.%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_MFQ' AND DATA_NAME LIKE '$MFQ_NAME_GENERALISED'")
            ;;
        run_data)
            # Similar approach to machine_fastqs
            OBJ_PATH_RD=$(iquest "%s" "SELECT COLL_NAME WHERE META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE'")
            RUN_ID_INDEXSET=$(basename ${OBJ_PATH_RD})
            RUNDATA_NAME_GENERALISED="run_data-${RUN_ID_INDEXSET}.tar.gz.%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_RD' AND DATA_NAME LIKE '$RUNDATA_NAME_GENERALISED'")
            ;;
        results)
            # Similar approach to machine_fastqs and run_data
            OBJ_PATH_RES=$(iquest "%s" "SELECT COLL_NAME WHERE META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE'")
            RES_ID_INDEXSET=$(basename ${OBJ_PATH_RES})
            RES_NAME_GENERALISED="results-${RES_ID_INDEXSET}.tar.gz.%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_RES' AND DATA_NAME LIKE '$RES_NAME_GENERALISED'")
            ;;
    esac

elif [[ "${IMETA_ATTRIBUTE}" = "RUN_ID" && -z "${INDEXSET}" ]]; then

    # Collect all indexset collection paths for this RUN_ID — used by several stages below
    INDEXSET_DIRS=$(iquest "%s" "SELECT COLL_NAME WHERE META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE'")

    case "$DATA_STAGE" in
        RUN)
            # Retrieve the top-level run collection
            EXTRACT_ITEM=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            ;;
        IndexsetRUN)
            # All indexset collections for this RUN_ID
            EXTRACT_ITEM=${INDEXSET_DIRS}
            ;;
        raw_data)
            # Locate the raw_data object collection and generalise the name for split-part matching
            OBJ_PATH_RAW=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            OBJ_NAME_F=($(iquest "%s" "SELECT DATA_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'"))
            OBJ_NAME_GENERALISED="${OBJ_NAME_F[0]%??}%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_RAW' AND DATA_NAME LIKE '$OBJ_NAME_GENERALISED'")
            ;;
        machine_fastqs|run_data|results)
            # Map the data stage to its file prefix for LIKE matching
            if [ "$DATA_STAGE" = "machine_fastqs" ]; then
                DATA_PREFIX="machine_fastqs"
            elif [ "$DATA_STAGE" = "run_data" ]; then
                DATA_PREFIX="run_data"
            elif [ "$DATA_STAGE" = "results" ]; then
                DATA_PREFIX="results"
            fi
            EXTRACT_ITEM=""
            OBJ_NAME_ID_GENERALISED="${DATA_PREFIX}-${IMETA_VALUE}%"
            # Iterate across all indexset directories to collect matching objects from each
            for INDEXSET_DIR in $INDEXSET_DIRS; do
                OBJ_NAMES_PULLED=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$INDEXSET_DIR' AND DATA_NAME LIKE '$OBJ_NAME_ID_GENERALISED'")
                EXTRACT_ITEM=" ${EXTRACT_ITEM} ${OBJ_NAMES_PULLED} "
            done
            ;;
    esac

elif [[ "${IMETA_ATTRIBUTE}" = "RUN_ID" && ! -z "${INDEXSET}" ]]; then

    # Scope all queries to a specific indexset when -i is supplied
    INDEXSET_LABEL="indexset${INDEXSET}"

    case "$DATA_STAGE" in
        RUN)
            # Retrieve the top-level run collection (indexset does not narrow RUN scope)
            EXTRACT_ITEM=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            ;;
        IndexsetRUN)
            # Retrieve the specific indexset collection matching both RUN_ID and Indexset AVUs
            EXTRACT_ITEM=$(iquest "%s" "SELECT COLL_NAME WHERE \
                    META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE' AND \
                    META_COLL_ATTR_NAME = 'Indexset' AND META_COLL_ATTR_VALUE = '$INDEXSET_LABEL'")
            ;;
        raw_data)
            # raw_data is not indexset-scoped but included here for completeness
            OBJ_PATH_RAW=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
            OBJ_NAME_F=($(iquest "%s" "SELECT DATA_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'"))
            OBJ_NAME_GENERALISED="${OBJ_NAME_F[0]%??}%"
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_RAW' AND DATA_NAME LIKE '$OBJ_NAME_GENERALISED'")
            ;;
        machine_fastqs|run_data|results)
            # Map stage to file prefix, then scope query to the specific indexset collection
            if [ "$DATA_STAGE" = "machine_fastqs" ]; then
                DATA_PREFIX="machine_fastqs"
            elif [ "$DATA_STAGE" = "run_data" ]; then
                DATA_PREFIX="run_data"
            elif [ "$DATA_STAGE" = "results" ]; then
                DATA_PREFIX="results"
            fi
            OBJ_NAME_ID_GENERALISED="${DATA_PREFIX}-${IMETA_VALUE}%"
            OBJ_PATH_INDXDIR=$(iquest "%s" "SELECT COLL_NAME WHERE \
                    META_COLL_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_COLL_ATTR_VALUE = '$IMETA_VALUE' AND \
                    META_COLL_ATTR_NAME = 'Indexset' AND META_COLL_ATTR_VALUE = '$INDEXSET_LABEL'")
            EXTRACT_ITEM=$(iquest "%s/%s" "SELECT COLL_NAME, DATA_NAME WHERE COLL_NAME='$OBJ_PATH_INDXDIR' AND DATA_NAME LIKE '$OBJ_NAME_ID_GENERALISED'")
            ;;
    esac

fi


# -----------------------------------------------------------------------------
# Section 10: Retrieval List Validation
# If no items were found (empty result or iRODS CAT_NO_ROWS_FOUND), print an
# informative error and exit. Otherwise, print the list of items to retrieve.
# -----------------------------------------------------------------------------
if [[ -z "${EXTRACT_ITEM}" || "${EXTRACT_ITEM}" == *"CAT_NO_ROWS_FOUND"* ]]; then
    echo "EXTRACT_ITEM list : ${EXTRACT_ITEM}"
    echo "NO items to extract were identified. Something went wrong. Please check the logs."
    echo "Please check the log file for details. Log File: ${LOG_FILE}"
    exit 1
else
    echo -e "\n=====================FOLLOWING ITEMS WILL BE RETRIEVED==================\n"
    for EACH_ITEM in $EXTRACT_ITEM; do
        echo "  - ${EACH_ITEM}"
    done
    echo -e "\n========================================================================\n"
fi


# -----------------------------------------------------------------------------
# Section 11: Output Directory and Run Structure Report
# Create the output directory, then generate a human-readable iRODS data
# structure report for the run of interest. The report is written to a .txt
# file and includes search criteria, retrieval guidance, and an itree listing.
# -----------------------------------------------------------------------------
mkdir -p ${OUTPUT_DIR}

# Resolve the top-level run collection for the structure report
RUNID_IRODS_DATA_PATH=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
RUN_ID_UNDER_SEARCH=$(basename ${RUNID_IRODS_DATA_PATH})

{
    echo "=======================================SEARCH CRITERIA======================================================"
    echo "IMETA_ATTRIBUTE=$IMETA_ATTRIBUTE \n
    IMETA_VALUE=$IMETA_VALUE \n
    DATA_STAGE=$DATA_STAGE \n
    INDEXSET=$INDEXSET \n"
    echo -e "\n =========================================================================================================\n\n"

    echo "Below are the details of the data generated from the analysis of the RUN ID of interest: ${RUN_ID_UNDER_SEARCH}"
    echo
    echo "• Directories (called COLLECTIONS in iRODS terminology) are shown with paths ending in '/'."
    echo
    echo "• Data objects are listed with their full paths so they can be retrieved using the 'iget' command."
    echo
    echo "  Example:"
    echo "    iget -K -X retrieval.restart -R s3_resc --retries 3 <OBJECT_WITH_PATH> <DESTINATION_DIR>"
    echo
    echo "  Options:"
    echo "    -K           Perform checksum verification to ensure data integrity."
    echo "    -R           Specify the resource from which the object is retrieved. Keep it as s3_resc."
    echo "    --retries    Number of retry attempts if the transfer fails (useful for large files)."
    echo "    -X           File used to store restart information for interrupted transfers."
    echo -e "\n =========================================================================================================\n\n"
    itree -F --indent=6 -f ${RUNID_IRODS_DATA_PATH}
    echo -e "\n\n ========================================================================================================="
} >> ${OUTPUT_DIR}/${RUN_ID_UNDER_SEARCH}.irods_data_structure.txt


# -----------------------------------------------------------------------------
# Section 12: iRODS File Retrieval
# Iterate over each resolved iRODS item and download it to a subdirectory of
# OUTPUT_DIR named after the item's base name (without extension). A per-item
# restart file is used to support resuming interrupted transfers.
#
# iget flags:
#   -f             Force overwrite of existing local files
#   -K             Verify checksum after transfer
#   -r             Recursive retrieval (handles collections)
#   -X <file>      Restart file for resuming interrupted transfers
#   --lfrestart    Large-file restart file
#   -R s3_resc     Specify the iRODS source resource
#   --retries 3    Retry up to 3 times on failure
# -----------------------------------------------------------------------------
EXEC_DIR=$(pwd)

for EACH_ITEM in ${EXTRACT_ITEM}; do
    # Derive a per-item subdirectory name from the base name (strip extension)
    DEST_DIR=$(basename ${EACH_ITEM} | cut -d"." -f1)
    OBJECT=$(basename ${EACH_ITEM})

    mkdir -p ${OUTPUT_DIR}/${DEST_DIR}
    cd ${OUTPUT_DIR}/${DEST_DIR}

    echo "${OBJECT} retrieval from iRODS begin and copied to DEST DIR: ${OUTPUT_DIR}/${DEST_DIR}"

    iget -f -K -r \
        -X ${DEST_DIR}.Xrestart \
        --lfrestart ${DEST_DIR}.lrestart \
        -R s3_resc \
        --retries 3 \
        ${EACH_ITEM} ${OUTPUT_DIR}/${DEST_DIR}/

    IGET_STATUS=$?
    #CHKSUM=$(ichksum ${EACH_ITEM})
    #echo "Checksum value on iRODS : ${CHKSUM}"

    if [ $IGET_STATUS -eq 0 ]; then
        echo "${OBJECT} transfer Successful"
    else
        echo "${OBJECT} Transfer has encountered an issue, please check."
    fi

    # Return to the original working directory before processing the next item
    cd ${EXEC_DIR}
done


# -----------------------------------------------------------------------------
# Section 13: Decompression
# If UNCOMPRESS is TRUE, call untar_uncompress.sh to extract all retrieved
# tarballs in OUTPUT_DIR. Otherwise, leave the files compressed.
# -----------------------------------------------------------------------------
if [[ $UNCOMPRESS = "TRUE" ]]; then
    ${APP_DIR}/scripts/untar_uncompress.sh ${OUTPUT_DIR}
else
    echo "All requested files are restored and are tarred and compressed in .gz format"
fi
