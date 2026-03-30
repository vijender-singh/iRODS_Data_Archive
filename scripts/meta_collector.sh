#!/bin/sh
# =============================================================================
# Script:      meta_collector.sh
# Description: Extracts run metadata from samplesheet CSV files located under
#              the raw_data and machine_fastqs directories for a given run ID.
#              Produces two outputs:
#                1. A collated master samplesheet CSV aggregating all source
#                   CSV files found for the run.
#                2. A metadata log file (META_LOG) that can be sourced into
#                   other shell scripts. It contains per-indexset and run-level
#                   variables:
#                     INDEXSETS                       - space-separated list
#                     Sample_Name_<indexset>          - sample names per indexset
#                     WORKFLOW_<indexset>             - workflows per indexset
#                     Sample_Name_ALL                 - deduplicated all-run list
#                     WORKFLOW_ALL                    - deduplicated all-run list
#
#              CSV files named "__caa*" are excluded from processing.
#              If no valid samplesheets are found, fallback empty values are
#              written to META_LOG and the script exits cleanly.
#
# Usage:
#   ./meta_collector.sh <RUNID> <PROCESSING_DIR> <META_LOG> \
#       <RAW_DATA_PATH> <MACHINE_FASTQ_PATH> [debug]
#
# Arguments:
#   $1  RUNID               - Sequencing run identifier
#   $2  PROCESSING_DIR      - Per-run processing directory for output files
#   $3  META_LOG            - Path to the metadata log file to write/append
#   $4  RAW_DATA_PATH       - Parent directory of raw_data run directories
#   $5  MACHINE_FASTQ_PATH  - Parent directory of machine_fastqs run directories
#   $6  INSTANCE_TYPE       - Set to any non-empty value (e.g. "debug") to
#                             retain temporary files after processing
#
# Output files:
#   ${PROCESSING_DIR}/${RUNID}_samplesheet.csv   - Collated master samplesheet
#   ${META_LOG}                                  - Sourced metadata variables
#   ${TEMP_DIR}/${RUNID}_merged.csv              - Intermediate (temp)
#   ${TEMP_DIR}/${RUNID}_normalized.csv          - Intermediate (temp)
#   ${TEMP_DIR}/${RUNID}_formated.csv            - Intermediate (temp)
# =============================================================================

set -ex

# -----------------------------------------------------------------------------
# Section 1: Argument Parsing and Validation
# Assign positional parameters to named variables. Exit with usage if the
# minimum required argument count is not met.
# -----------------------------------------------------------------------------
if [ $# -lt 1 ]; then
    echo "Usage: $0 <RUNID> <PROCESSING_DIR> <META_LOG> <RAW_DATA_PATH> <MACHINE_FASTQ_PATH> [debug]"
    exit 1
fi

RUNID="$1"
PROCESSING_DIR="$2"
META_LOG="$3"
DIR_PATH="$4"
MACHINE_FASTQ_PATH="${5}"
INSTANCE_TYPE="${6}"   # Non-empty value retains temporary files after processing

# -----------------------------------------------------------------------------
# Section 2: Debug Mode Setup
# Enable debug logging by setting DEBUG=1. Currently always active (DeBug is
# hardcoded to "debug"). Set INSTANCE_TYPE to a non-empty value to also retain
# temporary files.
# -----------------------------------------------------------------------------
DEBUG=0
# This is set in debug mode so that the .csv file processing can be logged
DEBUB_FLAG="debug"
[ "${DEBUB_FLAG}" = "debug" ] && DEBUG=1


# -----------------------------------------------------------------------------
# Section 3: Path Configuration
# Define all input glob patterns and output file paths used throughout the
TEMP_DIR="${RUN_PROCESS_DIR}/tmp"
#TEMP_DIR="/data/scratch/vijender.singh/tmp"
mkdir -p ${TEMP_DIR}

# Temporary working files
MERGED_TMP_CSV="/${TEMP_DIR}/${RUNID}_merged.csv"    # Raw extracted rows from all CSVs
NORMALIZED_CSV="/${TEMP_DIR}/${RUNID}_normalized.csv" # Rows with normalised indexset labels
FORMATED_CSV="/${TEMP_DIR}/${RUNID}_formated.csv"     # Single CSV with CR/null bytes stripped

# Output files
MASTER_SAMPLE_SHEET="${PROCESSING_DIR}/${RUNID}_samplesheet.csv"

# Glob patterns for locating samplesheet CSVs in both data stores
RAW_DATA_CSV="${DIR_PATH}/${RUNID}*/*.csv"
MACHINE_FASTQ_CSV="${MACHINE_FASTQ_PATH}/${RUNID}*/*.csv"


# -----------------------------------------------------------------------------
# Section 4: Logging Helper
# Prints debug messages to stdout when DEBUG=1.
# -----------------------------------------------------------------------------
log() {
    [ "$DEBUG" -eq 1 ] && echo "[DEBUG] $*"
}


# -----------------------------------------------------------------------------
# Section 5: Initialisation
# Clear all output and temporary files before processing begins.
# -----------------------------------------------------------------------------
log "RUNID: $RUNID"
log "Searching CSV files in: $RAW_DATA_CSV and $MACHINE_FASTQ_CSV"

> "$MERGED_TMP_CSV"
> "$NORMALIZED_CSV"
> "${MASTER_SAMPLE_SHEET}"


# -----------------------------------------------------------------------------
# Section 6: CSV File Discovery
# Find all samplesheet CSV files under the raw_data and machine_fastqs
# directories for this run, excluding files named "__caa*".
# If none are found, write fallback empty values to META_LOG and exit cleanly.
# -----------------------------------------------------------------------------
# CSVFILES=$(find $RAW_DATA_CSV $MACHINE_FASTQ_CSV 2>/dev/null)
CSVFILES=$(find $RAW_DATA_CSV $MACHINE_FASTQ_CSV ! -name "__caa*" 2>/dev/null)

if [ -z "$CSVFILES" ]; then
    > ${MASTER_SAMPLE_SHEET}
    #echo 'INDEXSET_META=""' > "${META_LOG}"
    echo 'INDEXSETS=""'       > "${META_LOG}"
    echo 'Sample_Name="NONE"' >> "${META_LOG}"
    echo 'WORKFLOW="NONE"'    >> "${META_LOG}"
    echo "No Samplesheets found for the RUN: ${RUNID}" > ${MASTER_SAMPLE_SHEET}
    log "No CSV files found. Wrote fallback to ${META_LOG}"
    exit 0
fi


# -----------------------------------------------------------------------------
# Section 7: CSV Parsing and Merging
# Iterate over each discovered CSV file. For each file:
#   1. Strip carriage returns and null bytes into FORMATED_CSV.
#   2. Locate the header row beginning with "Sample_ID,Sample_Name".
#   3. Append the raw file content to the master samplesheet for reference.
#   4. Extract Sample_Name, Workflow_Name, and Index_Set columns from all
#      data rows below the header and append to MERGED_TMP_CSV.
# Files without the expected header are silently skipped.
# -----------------------------------------------------------------------------
VALID_FOUND=0

echo "$CSVFILES" | while IFS= read -r CSVFILE; do
    log "Checking file: $CSVFILE"

    # Strip carriage returns and null/high bytes to normalise line endings
    tr -d '\r\000\200' < $CSVFILE > $FORMATED_CSV

    # Locate the header row (line number) — skip file if header is absent
    HEADER_LINE_NUM=$(grep -n "^Sample_ID,Sample_Name" "$FORMATED_CSV" | cut -d: -f1 | head -n 1)
    if [ -z "$HEADER_LINE_NUM" ]; then
        log "Skipped: Header not found"
        continue
    fi

    # Append raw CSV content to the master samplesheet with source annotation
    echo -e "=============================START===================================\n" >> ${MASTER_SAMPLE_SHEET}
    echo "SOURCE OF INFO BELOW : ${CSVFILE}"                                          >> ${MASTER_SAMPLE_SHEET}
    echo -e "\n"                                                                       >> ${MASTER_SAMPLE_SHEET}
    cat "$FORMATED_CSV"                                                                >> ${MASTER_SAMPLE_SHEET}
    echo -e "--------------------------END----------------------------------------\n" >> ${MASTER_SAMPLE_SHEET}

    log "Header found at line $HEADER_LINE_NUM"
    VALID_FOUND=1

    # Extract Sample_Name, Workflow_Name, and Index_Set columns from data rows
    # using the detected header line to identify column indices dynamically
    awk -v header_line="$HEADER_LINE_NUM" '
    NR == header_line {
        for (i=1; i<=NF; i++) {
            if ($i == "Sample_Name")   sn_idx=i;
            else if ($i == "Workflow_Name") wf_idx=i;
            else if ($i == "Index_Set")     ix_idx=i;
        }
        next;
    }
    NR > header_line {
        if (sn_idx && wf_idx && ix_idx) {
            print $sn_idx "," $wf_idx "," $ix_idx;
        }
    }
    ' FS=',' "$FORMATED_CSV" >> "$MERGED_TMP_CSV"
done


# -----------------------------------------------------------------------------
# Section 8: Empty Merge Guard
# If MERGED_TMP_CSV is empty after processing all files (no valid CSVs had
# the expected header), log a warning and exit cleanly without writing further
# metadata.
# -----------------------------------------------------------------------------
if [ ! -s "$MERGED_TMP_CSV" ]; then
    #echo 'INDEXSET_META="none_available"' >> "${META_LOG}"
    log "No valid CSVs with header found. Metadata written to ${META_LOG}"
    exit 0
fi


# -----------------------------------------------------------------------------
# Section 9: Index_Set Normalisation
# Normalise the Index_Set column values from each row into a consistent
# "indexset<LETTER>" format (e.g. "A", "Set_A", "indexA" → "indexsetA").
# The last alphabetical character in the Index_Set value is extracted,
# uppercased, and used as the canonical suffix. Output is sorted by indexset.
# -----------------------------------------------------------------------------
awk -F',' '
{
    # Extract the last alphabetical character from the Index_Set column
    match($3, /([A-Za-z])[^A-Za-z]*$/, arr);
    SUFFIX = toupper(arr[1]);
    NORMALIZED = "indexset" SUFFIX;
    print $1 "," $2 "," NORMALIZED;
}' "$MERGED_TMP_CSV" | sort -t',' -k3,3 > "$NORMALIZED_CSV"


# -----------------------------------------------------------------------------
# Section 10: Per-Indexset Metadata Extraction
# Extract the unique list of indexsets from the normalised CSV. For each
# indexset, collect deduplicated Sample_Name and Workflow_Name values and
# write them as shell-sourceable variables to META_LOG. Also accumulate
# run-level aggregates for Sample_Name_ALL and WORKFLOW_ALL.
# -----------------------------------------------------------------------------
INDEXSETS=$(cut -d',' -f3 "$NORMALIZED_CSV" | sort -u)
RUN_WORKFLOWS=""
RUN_SAMPLE_NAME=""

for IDX in $INDEXSETS; do
    log "Processing indexset: $IDX"

    # Collect unique sample names and workflows for this indexset
    SAMPLE_NAMES=$(awk -F',' -v id="$IDX" '$3 == id {print $1}' "$NORMALIZED_CSV" | sort -u | paste -sd' ')
    WORKFLOWS=$(awk -F',' -v id="$IDX" '$3 == id {print $2}' "$NORMALIZED_CSV" | sort -u | paste -sd' ')

    # Accumulate run-level aggregates (deduplication applied in Section 11)
    #RUN_SAMPLE_NAME+="${SAMPLE_NAMES}"
    #RUN_WORKFLOWS+="${WORKFLOWS}"
    RUN_SAMPLE_NAME+="${SAMPLE_NAMES} "
    RUN_WORKFLOWS+="${WORKFLOWS} "

    RUN_WORKFLOWS+="${WORKFLOWS} "

    # Write per-indexset variables — IDX_VAR preserves case as-is
    IDX_VAR=$(echo "$IDX") # | tr '[:lower:]' '[:upper:]')
    echo "Sample_Name_${IDX_VAR}=\"$SAMPLE_NAMES\"" >> "${META_LOG}"
    echo "WORKFLOW_${IDX_VAR}=\"$WORKFLOWS\""        >> "${META_LOG}"
done


# -----------------------------------------------------------------------------
# Section 11: Run-Level Aggregate Metadata
# Deduplicate the accumulated Sample_Name and Workflow values across all
# indexsets, then write the all-run aggregates to META_LOG.
# -----------------------------------------------------------------------------

# Deduplicate by splitting on spaces, sorting, deduplicating, and rejoining
SAMPLE_NAME_ALL=$(echo "$RUN_SAMPLE_NAME" | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/ $//')
WORKFLOW_ALL=$(echo "$RUN_WORKFLOWS"      | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/ $//')

#Sample_Name_ALL=$(echo "$Sample_Name_ALL" | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/ $//')
#WORKFLOW_ALL=$(echo "$WORKFLOW_ALL"        | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/ $//')

#echo "Sample_Name_ALL=\"${RUN_SAMPLE_NAME}\"" >> ${META_LOG}
#echo "WORKFLOW_ALL=\"${RUN_WORKFLOWS}\"" >> ${META_LOG}
echo "Sample_Name_ALL=\"${SAMPLE_NAME_ALL}\"" >> ${META_LOG}
echo "WORKFLOW_ALL=\"${WORKFLOW_ALL}\""        >> ${META_LOG}

log "Metadata written to: ${META_LOG}"

# Write the final deduplicated INDEXSETS list to META_LOG
echo "INDEXSETS=\"${INDEXSETS}\"" >> "${META_LOG}"


# -----------------------------------------------------------------------------
# Section 12: Temporary File Cleanup
# Remove intermediate working files unless INSTANCE_TYPE is set (non-empty),
# which indicates a debug or inspection run where files should be retained.
# -----------------------------------------------------------------------------
if [ -z "$INSTANCE_TYPE" ]; then
    rm -f "$MERGED_TMP_CSV" "$NORMALIZED_CSV" "$FORMATED_CSV"
else
    log "Temporary files retained:"
    log "  Merged:     $MERGED_TMP_CSV"
    log "  Normalized: $NORMALIZED_CSV"
    log "  Formatted:  $FORMATED_CSV"
fi

exit 0
