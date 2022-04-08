#!/bin/bash

set -e pipefail # exit on error

# Download the NY Taxi green and yello taxi data for a 2019 and 2020 year for every month
url="https://s3.amazonaws.com/nyc-tlc/trip+data/"
for i in {1..12}; do
    for color in green yellow; do
        # Make color directory under data folder
        mkdir -p data/$color
        month=$(printf "%02d" $i)
        file_name="${color}_tripdata_2020-${month}.csv"
        wget -O "data/${color}/${file_name}" "${url}${file_name}"
    done
done
