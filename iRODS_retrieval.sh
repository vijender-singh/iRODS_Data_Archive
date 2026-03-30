#!/bin/bash
# =============================================================================
# Script:      iRODS_retrieval.sh
# Description: Top-level dispatcher for iRODS data retrieval utilities.
#              Parses the action name and flags, validates required arguments,
#              and delegates to the appropriate sub-script.
#
#              Supported actions:
#                extract       — Retrieve data objects by imeta attribute/value
#                                pair and data stage
#                rundetails    — Print all metadata details for a specific run
#                extract_file  — Retrieve a specific object/collection by its
#                                full iRODS path
#
# Usage:
#   irods_retrieve.sh <action> [options]
#
#   irods_retrieve.sh extract
#       -o <output_dir>
#       -a <imeta_attribute>        (Sample_Name | RUN_ID)
#       -v <imeta_value>
#       -s <raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN>
#       [-i <indexset>]             A|B|C|D or other indexset label
#       [-r TRUE|FALSE]             Restore from SMED (default FALSE)
#       [-z TRUE|FALSE]             Uncompress/untar after retrieval (default TRUE)
#       [-l TRUE|FALSE]             Verbose logging (default TRUE)
#
#   irods_retrieve.sh rundetails
#       -o <output_dir>
#       -a <imeta_attribute>
#       -v <imeta_value>
#
#   irods_retrieve.sh extract_file
#       -o <output_dir>
#       -p <iRODS_path>
#       [-z TRUE|FALSE]             Uncompress/untar after retrieval (default TRUE)
#
# Sub-scripts called:
#   ${APP_DIR}/scripts/extract.sh
#   ${APP_DIR}/scripts/rundetails.sh
#   ${APP_DIR}/scripts/extract_file.sh
#
# Supported imeta attributes:
#   Sample_Name   — returns data objects for a specific sample
#   RUN_ID        — use with -i (indexset) to scope to a specific indexset
#   FLOWCELL_ID, INDEXSET, RUN_DATE, SEQUENCER_ID, WORKFLOW
#                 — broader search returning multiple matching objects
# =============================================================================

set -x


# -----------------------------------------------------------------------------
# Section 1: Default Variable Initialisation
# All flags default to empty or safe values before argument parsing begins.
# -----------------------------------------------------------------------------
OUTPUT_DIR=""
SMED_RESTORE=FALSE
IMETA_ATTRIBUTE=""
IMETA_VALUE=""
DATA_STAGE=""        # Accepted values: raw_data | machine_fastqs | run_data | results | IndexsetRUN | RUN
INDEXSET=""
UNCOMPRESS="FALSE"
VERBOSE="TRUE"
FILEPATH=""


# -----------------------------------------------------------------------------
# Section 2: Usage Function
# Printed on -h / --help or when required arguments are missing.
# -----------------------------------------------------------------------------
usage() {
echo -e "\n -----------------------------------------------------------------------------------"
echo -e "This script will help to retrieve objects from iRODS platform.  The usages are shown below \n"
echo -e " Script supports following three utilities/functions"
echo -e "\t extract \t : Extract a data object based on <attribute : valu> pair and data stage (raw_data|machine_fastqs|run_data|resultsIndexsetRUN|RUN)"
echo -e "\t rundetails \t : Prints all the details of a specific run"
echo -e "\t extract_file \t : This will retrieve the object when object/collection name with path is specified \n"
echo -e "======================================================\n"
echo -e " Utility 1:"
echo -e " $0 extract -o <outdir> -a <attribute> -v <value> -s <raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN> [optional arguments]"
echo -e " \t Arguments"
echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here[REQUIRED]."
echo -e "\t -a \t <imeta attribute> \t\t Provide the attribute of imetadata.  Full list is given below [REQUIRED]."
echo -e "\t -v \t <imeta value> \t\t\t "'Provide the imeta-attribute "Value".[REQUIRED].'
echo -e "\t -s \t raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN   specify one of the data stages.[REQUIRED]"
echo -e "\t    \t\t\t raw_data \t: raw_data of the run where given sample was included"
echo -e "\t    \t\t\t machine_fastqs : machine_fastqs of indexset(if applicable) where given sample was included"
echo -e "\t    \t\t\t run_data \t: run_data of indexset(if applicable) where given sample was included"
echo -e "\t    \t\t\t results \t: results of indexset(if applicable) where given sample was included"
echo -e "\t    \t\t\t IndexsetRUN \t: All data of a indexset including raw_data, machine_fastqs, run_data, results where given sample was included"
echo -e "\t    \t\t\t RUN \t\t: All data of of the RUN including all indexsets and their raw_data, machine_fastqs, run_data, results where given sample was included"
echo -e "\t -i \t A|B|C|D or anyother indexset value."
echo -e "\t -r \t TRUE|FALSE \t\t\t Optional allow to files to be restored on SMED from where the object was originally archived [Not recommended] require sufficient previlages[Default FALSE]"
echo -e "\t -z \t TRUE|FALSE \t\t\t OPtional control Uncompress and untar the data [Default TRUE]"
echo -e "\t -l \t TRUE|FALSE\t\t\t Optional allows Detailed log of the archive process [Default TRUE]"
echo -e "======================================================\n"
echo -e " Utility 2:"
echo -e " $0 rundetails -o <outdir> -a <attribute> -v <value>"
echo -e " \t Arguments"
echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here[REQUIRED]."
echo -e "\t -a \t <imeta attribute> \t\t Provide the attribute of imetadata.  Full list is given below [REQUIRED]."
echo -e "\t -v \t <imeta value> \t\t\t "'Provide the imeta-attribute "Value".[REQUIRED].'
echo -e "======================================================\n"
echo -e " Utility 3:"
echo -e " $0 extract_file -o <outdir> -p <iRODS path to object>"
echo -e " \t Arguments"
echo -e "\t -o \t <path to Output directory> \t The retrieved data will be directed here[REQUIRED]."
echo -e "\t -p \t <path> \t\t\t Path to data/collection on iRODS. [REQUIRED]."
echo -e "\t -z \t  TRUE|FALSE \t\t\t Uncompress and untar the data [Default TRUE]"
echo -e "\n======================================================\n"
echo -e "Supported metadata attributes:"
echo -e "Sample_Name, RUN_ID"
echo -e "Use of Sample_Name will return the data object"
echo -e "Use of RUN_ID with -i flag (Indexset) will return"
echo -e "FLOWCELL_ID, INDEXSET, RUN_DATE, SEQUENCER_ID, WORKFLOW these are added metadata to dataobjects.  The search using them will return list of multiple dataobj matching them."
echo " "
exit 1
}


# -----------------------------------------------------------------------------
# Section 3: Environment Configuration
# Hard-coded paths for the output directory, application root, and iRODS
# credential file. APP_DIR is used to locate all sub-scripts.
# Note: OUTPUT_DIR here may be overridden by the -o flag in Section 5.
# -----------------------------------------------------------------------------
OUTPUT_DIR=/hpscol02/tenant1/ngsservice/vijender.singh/retrieve_irods_test/
APP_DIR=/home/phe.gov.uk/vijender.singh/irods_archive_and_retrieval
source /home/phe.gov.uk/vijender.singh/.irods_credential


# -----------------------------------------------------------------------------
# Section 4: Early Help and Action Argument Handling
# --help / -h are intercepted before getopts so they work without a
# preceding action name. The action (extract | rundetails | extract_file)
# is consumed as the first positional argument before flag parsing begins.
# -----------------------------------------------------------------------------

# Intercept --help and -h before any other processing
for arg in "$@"; do
    if [[ "$arg" == "--help" || "$arg" == "-h" ]]; then
        usage
    fi
done

# Require at least one argument (the action name)
if [[ $# -lt 1 ]]; then
    echo "Error: No function choosen Please choose one from extract/rundetails/extract_file." >&2
    usage
fi

ACTION="$1"
shift  # Remove the action from the positional arguments so getopts sees only flags

# Validate that the action is one of the three supported values
if [[ "$ACTION" != "extract" && "$ACTION" != "rundetails" && "$ACTION" != "extract_file" ]]; then
    echo "Error: Invalid function '$ACTION'. Must be 'extract', 'rundetails', or 'extract_file'." >&2
    usage
fi


# -----------------------------------------------------------------------------
# Section 5: Flag Parsing
# getopts processes all short flags after the action has been shifted off.
# -p requires an argument (FILEPATH) but is listed without a colon in the
# original — preserved as-is.
# -----------------------------------------------------------------------------
while getopts ":o:a:v:s:i:r:z:l:p" opt; do
    case $opt in
        o) OUTPUT_DIR="$OPTARG"      ;;   # Output directory for retrieved data
        a) IMETA_ATTRIBUTE="$OPTARG" ;;   # iRODS metadata attribute to search
        v) IMETA_VALUE="$OPTARG"     ;;   # Value for the metadata attribute
        s) DATA_STAGE="$OPTARG"      ;;   # Data stage: raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN
        i) INDEXSET="$OPTARG"        ;;   # Indexset label: A|B|C|D|...
        r) SMED_RESTORE="$OPTARG"    ;;   # Restore from SMED (TRUE|FALSE)
        z) UNCOMPRESS="$OPTARG"      ;;   # Uncompress/untar after retrieval (TRUE|FALSE)
        l) VERBOSE="$OPTARG"         ;;   # Verbose logging (TRUE|FALSE)
        p) FILEPATH="$OPTARG"        ;;   # Full iRODS path to object/collection
        h) usage                     ;;   # Print help and exit
        \?) echo "Invalid option: -$OPTARG" >&2; usage ;;
        :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
    esac
done


# -----------------------------------------------------------------------------
# Section 6: Required Argument Validation
# Each action has its own set of mandatory flags. Missing any required flag
# prints an error and shows usage.
# -----------------------------------------------------------------------------
case "$ACTION" in
    extract)
        if [[ -z "$OUTPUT_DIR" || -z "$IMETA_ATTRIBUTE" || -z "$IMETA_VALUE" || -z "$DATA_STAGE" ]]; then
            echo "Error: extract requires -o <outdir>, -a <attribute>, -v <value> and -s <raw_data|machine_fastqs|run_data|results|IndexsetRUN|RUN>" >&2
            usage
        fi
        ;;
    rundetails)
        if [[ -z "$OUTPUT_DIR" || -z "$IMETA_ATTRIBUTE" || -z "$IMETA_VALUE" ]]; then
            echo "Error: rundetails requires -o <outdir>, -a <attribute> and -v <value>" >&2
            usage
        fi
        ;;
    extract_file)
        if [[ -z "$OUTPUT_DIR" || -z "$FILEPATH" ]]; then
            echo "Error: extract_file requires -o <outdir> and -p <iRODS path to object>" >&2
            usage
        fi
        ;;
esac


# -----------------------------------------------------------------------------
# Section 7: Output Directory Validation
# Verify OUTPUT_DIR exists before dispatching to sub-scripts, so any error
# is reported before work begins rather than mid-transfer.
# -----------------------------------------------------------------------------
if [ ! -d "${OUTPUT_DIR}" ]; then
    echo "ERROR: Output directory not found: ${OUTPUT_DIR}" >&2
    exit 1
fi


# -----------------------------------------------------------------------------
# Section 8: Action Dispatch
# For each action: verify the target sub-script exists and is executable,
# then invoke it passing only the flags relevant to that action.
# Optional flags use the ${VAR:+-flag "$VAR"} idiom — the flag is only
# appended if the variable is non-empty, preserving sub-script defaults.
# -----------------------------------------------------------------------------
case "$ACTION" in

    extract)
        if [[ ! -x "${APP_DIR}/scripts/extract.sh" ]]; then
            echo "Error: ${APP_DIR}/scripts/extract.sh not found or not executable." >&2
            exit 1
        fi
        ${APP_DIR}/scripts/extract.sh \
            -o "$OUTPUT_DIR"       \
            -a "$IMETA_ATTRIBUTE"  \
            -v "$IMETA_VALUE"      \
            -s "$DATA_STAGE"       \
            ${INDEXSET:+-i      "$INDEXSET"}     \
            ${SMED_RESTORE:+-r  "$SMED_RESTORE"} \
            ${UNCOMPRESS:+-z    "$UNCOMPRESS"}   \
            ${VERBOSE:+-l       "$VERBOSE"}
        ;;

    rundetails)
        if [[ ! -x "${APP_DIR}/scripts/rundetails.sh" ]]; then
            echo "Error: ${APP_DIR}/scripts/rundetails.sh not found or not executable." >&2
            exit 1
        fi
        ${APP_DIR}/scripts/rundetails.sh \
            -o "$OUTPUT_DIR"      \
            -a "$IMETA_ATTRIBUTE" \
            -v "$IMETA_VALUE"
        ;;

    extract_file)
        if [[ ! -x "${APP_DIR}/scripts/extract_file.sh" ]]; then
            echo "Error: ${APP_DIR}/scripts/extract_file.sh not found or not executable." >&2
            exit 1
        fi
        ${APP_DIR}/scripts/extract_file.sh \
            -o "$OUTPUT_DIR"             \
            -p "$FILEPATH"               \
            ${UNCOMPRESS:+-z "$UNCOMPRESS"} \
            -w "${APP_DIR}"
        ;;

esac
