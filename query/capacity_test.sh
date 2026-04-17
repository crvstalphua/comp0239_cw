#!/bin/bash

SUBMIT_SCRIPT="/home/almalinux/comp0239_cw/query/submit_query.py"
INPUT_DIR="/data/nyc-taxi/raw"
DURATION=$((24 * 60 * 60))
START_TIME=$(date +%s)
RUN=1

CONFIGS=(
    "2g:2:8:config_A_baseline"
    "4g:2:8:config_B_more_memory"
    "2g:4:16:config_C_more_cores"
    "4g:4:16:config_D_balanced"
)

# get all parquet files
PARQUET_FILES=($INPUT_DIR/yellow_tripdata_201{5,6,7,8,9}-*.parquet
                $INPUT_DIR/yellow_tripdata_202{0,1,2,3}-*.parquet)
TOTAL_FILES=${#PARQUET_FILES[@]}

echo "Capacity test started at $(date)"
echo "Total input files available: $TOTAL_FILES"

while true; do
    ELAPSED=$(( $(date +%s) - START_TIME ))

    if [ $ELAPSED -ge $DURATION ]; then
        echo "24 hours complete. Total runs: $((RUN - 1))"
        break
    fi

    # cycle through files
    FILE_INDEX=$(( (RUN - 1) % TOTAL_FILES ))
    INPUT="${PARQUET_FILES[$FILE_INDEX]}"

    # cycle through configs
    CONFIG_INDEX=$(( ((RUN - 1) / TOTAL_FILES) % ${#CONFIGS[@]} ))
    CONFIG="${CONFIGS[$CONFIG_INDEX]}"
    IFS=':' read -r MEM CORES PARALLELISM LABEL <<< "$CONFIG"

    REMAINING=$(( DURATION - ELAPSED ))
    echo "--- Run $RUN | Config: $LABEL | Elapsed: ${ELAPSED}s | Remaining: ${REMAINING}s ---"

    python3 "$SUBMIT_SCRIPT" \
        --input "$INPUT" \
        --executor-memory "$MEM" \
        --executor-cores "$CORES" \
        --parallelism "$PARALLELISM" \
        --config-label "$LABEL"

    RUN=$((RUN + 1))
done

echo "Capacity test complete."