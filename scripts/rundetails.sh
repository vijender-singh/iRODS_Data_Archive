#!/bin/bash
# =============================================================================
# Script:      rundetails.sh
# Description: Queries the iRODS metadata catalogue (iCAT) for a data object
#              matching a supplied attribute-value pair and generates a
#              human-readable run data structure report. The report is written
#              to a .rundetails.txt file in the specified output directory and
#              includes:
#                - The search criteria used
#                - Guidance on how to retrieve individual objects with iget
#                - A full iRODS collection tree (itree) for the matched run
#
# Usage:
#   ./rundetails.sh -o <output_dir> -a <attribute> -v <value>
#
# Options:
#   -o  <output_dir>   Local directory where the report file will be written
#                      (REQUIRED). Created automatically if it does not exist.
#   -a  <attribute>    iRODS metadata attribute to search on (REQUIRED).
#                      e.g. Sample_Name, RUN_ID
#   -v  <value>        Value for the metadata attribute (REQUIRED).
#
# Output:
#   ${OUTPUT_DIR}/${RUN_ID}.rundetails.txt  - iRODS data structure report
#
# Example:
#   ./rundetails.sh \
#       -o /data/reports \
#       -a RUN_ID \
#       -v 240730_MN01572_0250_A000H7CC3K
# =============================================================================

set -x

echo "====================================== rundetails.sh ============================="


# -----------------------------------------------------------------------------
# Section 1: Environment Setup
# Load application configuration and iRODS credentials before any processing.
# -----------------------------------------------------------------------------
APP_DIR=/home/phe.gov.uk/vijender.singh/irods_archive_and_retrieval
source /home/phe.gov.uk/vijender.singh/.irods_credential


# -----------------------------------------------------------------------------
# Section 2: Default Variable Initialisation
# All required arguments default to empty and are validated after parsing.
# -----------------------------------------------------------------------------
OUTPUT_DIR=""
IMETA_ATTRIBUTE=""
IMETA_VALUE=""


# -----------------------------------------------------------------------------
# Section 3: Usage Message
# Printed when required arguments are missing or an invalid option is supplied.
# -----------------------------------------------------------------------------
usage_rundetails() {
    echo -e "\t rundetails \t : Extract details of run with attribute : value pair"
    echo -e " $0 extract -o <outdir> -a <attribute> -v <value>"
    echo -e " \t Arguments"
    echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here [REQUIRED]."
    echo -e "\t -a \t <imeta attribute> \t\t Provide the attribute of imetadata. Full list is given below [REQUIRED]."
    echo -e "\t -v \t <imeta value> \t\t\t "'Provide the imeta-attribute "Value". [REQUIRED].'
    exit 1
}


# -----------------------------------------------------------------------------
# Section 4: Argument Parsing
# Parse -o (output directory), -a (attribute), and -v (value) flags.
# Unknown flags or missing argument values trigger the usage message.
# -----------------------------------------------------------------------------
while getopts ":o:a:v" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG" ;;
        a) IMETA_ATTRIBUTE="$OPTARG" ;;
        v) IMETA_VALUE="$OPTARG" ;;
        h) usage ;;
        \?) echo "Invalid option: -$OPTARG" >&2; usage_rundetails ;;
        :)  echo "Option -$OPTARG requires an argument." >&2; usage_rundetails ;;
    esac
done

# Validate that all three required arguments were provided
if [ -z "${OUTPUT_DIR}" ] || [ -z "${IMETA_ATTRIBUTE}" ] || [ -z "${IMETA_VALUE}" ]; then
    usage_rundetails
fi

# Print resolved configuration for operator confirmation
echo "OUTPUT_DIR=${OUTPUT_DIR}
        IMETA_ATTRIBUTE=${IMETA_ATTRIBUTE}
        IMETA_VALUE=${IMETA_VALUE}
        APP_DIR=${APP_DIR}"
echo "==================================================="


# -----------------------------------------------------------------------------
# Section 5: iRODS Authentication
# Authenticate to iRODS using the credential sourced in Section 1 and verify
# connectivity with ils. Exit with a clear error message if login fails.
# -----------------------------------------------------------------------------
logmsg() {
    echo " "
    echo "========================================================="
    echo "===>        iRODS run details process Begins          <==="
    echo "===>      iRODS server login was successfull          <==="
    echo "========================================================="
    echo " "
}

echo $PSWD | iinit
ils

if [ $? -eq 0 ]; then
    logmsg
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
# Section 7: Run Data Structure Report Generation
# Query iCAT for the collection path matching the supplied attribute-value pair,
# derive the run ID from the collection basename, then write a structured report
# to a .rundetails.txt file. The report includes search criteria, iget usage
# guidance, and a full itree listing of the run's iRODS collection.
# -----------------------------------------------------------------------------

# Resolve the top-level iRODS collection path for the matched run
RUNID_IRODS_DATA_PATH=$(iquest "%s" "SELECT COLL_NAME WHERE META_DATA_ATTR_NAME = '$IMETA_ATTRIBUTE' AND META_DATA_ATTR_VALUE = '$IMETA_VALUE'")
RUN_ID_UNDER_SEARCH=$(basename ${RUNID_IRODS_DATA_PATH})

{
    echo "=======================================SEARCH CRITERIA======================================================"
    echo -e "IMETA_ATTRIBUTE=${IMETA_ATTRIBUTE} \n
    IMETA_VALUE=${IMETA_VALUE} \n" 
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
} >> ${OUTPUT_DIR}/${RUN_ID_UNDER_SEARCH}.rundetails.txt
