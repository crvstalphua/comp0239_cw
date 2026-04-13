#!/bin/bash

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
DEST="/data/nyc-taxi/raw"

for year in $(seq 2009 2023); do
    for month in $(seq 1 12); do
        filename="yellow_tripdata_${year}-$(printf '%02d' $month).parquet"
        filepath="${DEST}/${filename}"

        # skip if already downloaded
        if [ -f "$filepath" ]; then
            echo "Already exists, skipping: ${filename}"
            continue
        fi

        # check if file actually exists on CDN
        http_code=$(curl -s -o /dev/null -w "%{http_code}" --head "${BASE_URL}/${filename}")

        if [ "$http_code" == "200" ]; then
            echo "Downloading: ${filename}"
            curl -f -o "$filepath" "${BASE_URL}/${filename}" || {
                echo "Download failed for ${filename}, removing partial file"
                rm -f "$filepath"
            }
        else
            echo "File not availble, skipping: ${filename}"
        fi 
    done 
done 