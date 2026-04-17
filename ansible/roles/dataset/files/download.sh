#!/bin/bash

BASE_URL="https://d37ci6vzurychx.cloudfront.net/trip-data"
DEST="/data/nyc-taxi/raw"
YEARS="2015 2016 2017 2018 2019 2020 2021 2022 2023"
MONTHS="01 02 03 04 05 06 07 08 09 10 11 12"

for year in $YEARS; do
    for month in $MONTHS; do
        filename="yellow_tripdata_${year}-${month}.parquet"
        filepath="${DEST}/${filename}"

        # skip if already downloaded
        if [ -f "$filepath" ]; then
            echo "Already exists, skipping: ${filename}"
            continue
        fi

        echo "Downloading: ${filename}"
        http_code=$(curl -s -o "$filepath" -w "%{http_code}" "${BASE_URL}/${filename}")

        if [ "$http_code" -eq "200" ]; then
            echo "Downloaded: ${filename}"
        else 
            echo "Not available (http $http_code), skipping ${filename}"
                rm -f "$filepath"
        fi
    done 
done 

echo "Download complete."
echo "Total files: $(ls $DEST/*.parquet 2>/dev/null | wc -l)"