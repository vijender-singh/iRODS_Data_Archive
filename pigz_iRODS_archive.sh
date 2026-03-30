#!/bin/bash

RAW_DATA_DIR_PATH="/hpscol02/tenant1/ngsservice/raw_data/nextseq/VL00114/250207_VL00114_727_AAG2V2GM5"

PROCESS_DIR="/data/users/vijender.singh/irods_pigz"

mkdir -p ${PROCESS_DIR}

RUN_ID=$(basename ${RAW_DATA_DIR_PATH})

META_LOG=${PROCESS_DIR}/${RUN_ID}.metadata

LOG_FILE=${PROCESS_DIR}/${RUN_ID}.log

touch ${META_LOG} ${LOG_FILE}

JID1=$(sbatch --parsable -c 10 -p standard-wp --mem=100G -J iRODS_archive -o ${PROCESS_DIR}/${RUN_ID}-%j.out ./scripts/tarball_metadata_collection.sh ${RAW_DATA_DIR_PATH} ${PROCESS_DIR} ${META_LOG} ${LOG_FILE})

#sbatch --dependency=afterok:$JID1 -o ${PROCESS_DIR}/${RUN_ID}-${JID1}.out --mem=1G --wrap="echo STEP1-SUCCESSFUL"

while true; do
    JOB_STATE=$(sacct -n -o State -j $JID1 | head -1)

    if [[ "$JOB_STATE" == "COMPLETED" ]]; then
        break
    else
        sleep 120  # Sleep for 2 minutes
    fi
done

#ssh smedmaster01.smed.unix.phe.gov.uk

./scripts/copy_2_iRODS.sh ${PROCESS_DIR} ${META_LOG} ${LOG_FILE}




