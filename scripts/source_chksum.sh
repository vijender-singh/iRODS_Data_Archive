#!/bin/bash
# =============================================================================
# Script:      source_chksum.sh
# Description: Computes SHA-256 checksums for all tarball files and the
#              collated samplesheet CSV associated with a sequencing run.
#              Checksums are written in a consistent format to a shared
#              checksum file that is later used by copy_2_iRODS.sh to verify
#              transfer integrity.
#
#              Checksum format (one entry per line):
#                src_chksum_<filename> <base64-encoded-sha256>
#
#              The SHA-256 digest is computed with sha256sum, the raw hex
#              bytes are converted to binary with xxd, and then base64-encoded
#              to match the iRODS iput -k checksum format.
#
# Usage:       Called by iRODS_archive.sh via sbatch — not run directly.
#              source_chksum.sh <RUN_ID> <TAR_DIRECTORY> <SRC_CHKSUM_FILE>
#
# Arguments:
#   $1  RUN_ID           - Sequencing run identifier
#   $2  TAR_DIRECTORY    - Directory containing tarballed files and the
#                          samplesheet CSV
#   $3  SRC_CHKSUM_FILE  - Output file to write checksum entries to
# =============================================================================


# -----------------------------------------------------------------------------
# Section 1: Argument Intake
# -----------------------------------------------------------------------------
RUN_ID="$1"
TAR_DIRECTORY="$2"
SRC_CHKSUM_FILE="$3"


# -----------------------------------------------------------------------------
# Section 2: Tarball Checksum Generation
# Discover all tarball files (*.tar.gz*) in the run's processing directory.
# If none are found, the tarballing stage either succeeded in a prior attempt
# (and files were already cleaned up) or failed — either way, skip gracefully.
# For each file found, compute its checksum and append to SRC_CHKSUM_FILE.
# The checksum file is truncated before writing to avoid stale entries from
# previous attempts.
# -----------------------------------------------------------------------------
TAR_FILES=$(find ${TAR_DIRECTORY} -maxdepth 1 -mindepth 1 -type f -name "*tar.gz*")

if [ -z "$TAR_FILES" ]; then
    echo "No tar files found. Skipping... Could be this step completed successfully in previous attempt or tar process failed."
    exit 0
else
    # Truncate the checksum file before writing to avoid duplicate entries
    > ${SRC_CHKSUM_FILE}

    for FILE_2_PROCESS in ${TAR_FILES}; do
        FILENAME=$(basename ${FILE_2_PROCESS})
        # Compute SHA-256, convert hex to binary, then base64-encode to match iRODS format
        SRC_CHKSUM=$(sha256sum "${FILE_2_PROCESS}" | awk '{print $1}' | xxd -r -p | base64)
        echo "src_chksum_${FILENAME} ${SRC_CHKSUM}" >> ${SRC_CHKSUM_FILE}
    done
fi


# -----------------------------------------------------------------------------
# Section 3: Samplesheet CSV Checksum Generation
# Compute and append the checksum for the collated samplesheet CSV.
# The CSV checksum is always appended regardless of whether SRC_CHKSUM_FILE
# already exists (both branches of the original if/else produce identical
# behaviour and are consolidated here).
# -----------------------------------------------------------------------------
CSV_FILE="${TAR_DIRECTORY}/${RUN_ID}_samplesheet.csv"
CSV_FILENAME="${RUN_ID}_samplesheet.csv"

CSV_SRC_CHKSUM=$(sha256sum "${CSV_FILE}" | awk '{print $1}' | xxd -r -p | base64)
echo "src_chksum_${CSV_FILENAME} ${CSV_SRC_CHKSUM}" >> ${SRC_CHKSUM_FILE}
