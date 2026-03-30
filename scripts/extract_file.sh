#!/bin/bash
# =============================================================================
# Script:      extract_file.sh
# Description: Retrieves a single file or directory from iRODS to a local
#              output directory using iget. Supports restart files to resume
#              interrupted transfers and verifies checksum integrity (-K flag).
#
# Usage:
#   ./extract_file.sh extract -o <output_dir> -p <irods_file_path>
#
# Options:
#   -o  <output_dir>       Path to the local output directory (REQUIRED).
#                          Created automatically if it does not exist.
#   -p  <irods_file_path>  Full iRODS path to the file or collection to
#                          retrieve (REQUIRED).
#
# Example:
#   ./extract_file.sh extract \
#       -o /data/retrieved \
#       -p /bix_seqdata_archive/year_2024/240730_MN01572_0250_A000H7CC3K/raw_data-240730_MN01572_0250_A000H7CC3K.tar.gz.aa
# =============================================================================

set -x

echo "====================================== extract_file.sh ============================="


# -----------------------------------------------------------------------------
# Section 1: Environment Setup
# Load application configuration and iRODS credentials before any processing.
# -----------------------------------------------------------------------------
APP_DIR=/home/phe.gov.uk/vijender.singh/irods_archive_and_retrieval
source /home/phe.gov.uk/vijender.singh/.irods_credential


# -----------------------------------------------------------------------------
# Section 2: Default Variable Initialisation
# Both required arguments default to empty; validated after parsing.
# -----------------------------------------------------------------------------
OUTPUT_DIR=""
FILEPATH=""


# -----------------------------------------------------------------------------
# Section 3: Usage Message
# Printed when required arguments are missing or an invalid option is supplied.
# -----------------------------------------------------------------------------
usage_extract_file() {
    echo -e "\t extract_file \t : Extract a file when path is specified."
    echo -e " $0 extract -o <outdir> -p <file_path>"
    echo -e " \t Arguments"
    echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here [REQUIRED]."
    echo -e "\t -p \t <path_to_the_file_on_iRODS> [REQUIRED]."
    exit 1
}


# -----------------------------------------------------------------------------
# Section 4: Argument Parsing
# Parse -o (output directory) and -p (iRODS file path) flags.
# Any unknown flag or missing argument value triggers the usage message.
# -----------------------------------------------------------------------------
while getopts ":o:p" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG" ;;
        p) FILEPATH="$OPTARG" ;;
        h) usage ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage_extract_file ;;
        :)  echo "Option -$OPTARG requires an argument." >&2; usage_extract_file ;;
    esac
done

# Validate that both required arguments were provided
if [ -z "${OUTPUT_DIR}" ] || [ -z "${FILEPATH}" ]; then
    usage_extract_file
fi

# Print resolved configuration for operator confirmation
echo "OUTPUT_DIR=${OUTPUT_DIR} \n
        FILEPATH=${FILEPATH}
        APP_DIR=$APP_DIR"


# -----------------------------------------------------------------------------
# Section 5: iRODS Authentication
# Authenticate to iRODS using the credential sourced in Section 1 and verify
# connectivity with ils. Exit with a clear error message if login fails.
# -----------------------------------------------------------------------------
echo $PSWD | iinit
ils

if [ $? -eq 0 ]; then
    echo "Successfully logged on iRODS."
else
    echo "iRODS is not accessible, please check the connection or login credentials for iRODS"
    echo "ERROR: to test run iinit and check if contents can be listed by ils command"
    echo -e "RETRIEVAL FAILED \t REASON : iRODS login failed. Check iRODS log file for details"
    echo -e "==================================$(date +'%Y/%m/%d %H:%M:%S')=================================="
    ils
    exit 1
fi


# -----------------------------------------------------------------------------
# Section 6: Output Directory Preparation
# Create the local output directory (and any missing parents) if it does not
# already exist.
# -----------------------------------------------------------------------------
mkdir -p ${OUTPUT_DIR}


# -----------------------------------------------------------------------------
# Section 7: iRODS File Retrieval
# Retrieve the specified iRODS object to the local output directory using iget.
#
# Flags used:
#   -f             Force overwrite of existing local files
#   -K             Verify checksum after transfer
#   -r             Recursive retrieval (handles collections)
#   -X <file>      Restart file — allows resuming an interrupted transfer
#   --lfrestart    Large-file restart file for resuming partial large transfers
#   -R s3_resc     Specify the source iRODS resource (S3 backend)
#   --retries 3    Retry the transfer up to 3 times on failure
# -----------------------------------------------------------------------------
iget -f -K -r \
    -X fileTransfer.Xrestart \
    --lfrestart fileTransfer.lrestart \
    -R s3_resc \
    --retries 3 \
    ${FILEPATH} ${OUTPUT_DIR}/

IGET_STATUS=$?


# -----------------------------------------------------------------------------
# Section 8: Transfer Status Reporting
# Report success or failure based on the iget exit code.
# -----------------------------------------------------------------------------
if [ $IGET_STATUS -eq 0 ]; then
    echo "${FILEPATH} transfer Successful"
else
    echo "${FILEPATH} Transfer has encountered an issue, please check."
fi
