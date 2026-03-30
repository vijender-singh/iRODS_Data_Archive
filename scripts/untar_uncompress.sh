#!/bin/bash

# -----------------------------------------------------------------------------
# DESCRIPTION:
#   This script scans a specified output directory (OUTPUT_DIR) for split
#   compressed archive files (*.tar.gz.aa) and reassembles and extracts them
#   in-place.
#
#   Split archives are multi-part tar.gz files where a single large archive
#   has been divided into sequential chunks suffixed with .aa, .ab, .ac, etc.
#   For each dataset found, the script:
#     1. Locates the first chunk (*.tar.gz.aa) to identify each unique dataset
#     2. Collects all associated chunks (*.tar.gz.aa, *.tar.gz.ab, ...) in
#        the correct order
#     3. Concatenates the chunks and pipes them into tar for extraction
#     4. Extracts the contents into the same directory as the archive chunks
#
# USAGE:
#   OUTPUT_DIR=/path/to/dir bash script.sh
#   or set OUTPUT_DIR as an environment variable before running.
#
# INPUT:
#   OUTPUT_DIR - Path to the root directory to search for split archives.
#                Subdirectories are also searched recursively.
#
# OUTPUT:
#   Extracted contents of each split archive, written to the same directory
#   as the archive chunks.
#
# ASSUMPTIONS:
#   - Split archive chunks follow the naming convention:
#       <dataset_name>.tar.gz.aa, <dataset_name>.tar.gz.ab, ...
#   - All chunks for a given dataset reside in the same directory.
#   - Chunks are valid and complete (no missing or corrupt parts).
# =============================================================================


# -----------------------------------------------------------------------------
# Section 1: Argument Intake
# -----------------------------------------------------------------------------
OUTPUT_DIR="$1"

# Find all first-chunk split archives under OUTPUT_DIR, store safely in array
mapfile -d '' FILE_TYPES < <(find "${OUTPUT_DIR}" -type f -name "*.tar.gz.aa" -print0)

for EACH_TYPE in "${FILE_TYPES[@]}"; do

    # Get the directory containing this archive
    UNTAR_DIR_PATH=$(dirname "${EACH_TYPE}")

    # Glob all chunks for this dataset, sorted to guarantee correct order
    # e.g. raw_data-f1.tar.gz.aa, raw_data-f1.tar.gz.ab, raw_data-f1.tar.gz.ac
    mapfile -d '' FILES_FOR_SAME_DATASET < <(find "${UNTAR_DIR_PATH}" -maxdepth 1 \
        -name "${EACH_TYPE%.aa}*" -print0 | sort -z)

    # Concatenate chunks in order and extract via tar
    cat "${FILES_FOR_SAME_DATASET[@]}" | tar xvzf - -C "${UNTAR_DIR_PATH}"

done
# -----------------------------------------------------------------------------
echo "-----------The process is completed--------------"


