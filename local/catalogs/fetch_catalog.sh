#!/bin/bash

set -e  # Exit immediately if a command exits with a non-zero status

local_date=$(date +'%F')
script_path="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
json_file="${script_path}/price_guide_1.json"

# Download the JSON file
if ! wget -q "https://downloads.s3.cardmarket.com/productCatalog/priceGuide/price_guide_1.json" -O "$json_file"; then
    #echo "Error: Failed to download the price guide."
    exit 1
fi

# Calculate the MD5 hash
md5_hash=$(md5sum "$json_file" | cut -d " " -f 1)

# Move the file to a new name with the date and hash
new_file="${script_path}/${local_date}_${md5_hash}_price_guide_1.json"
if ! mv "$json_file" "$new_file"; then
    #echo "Error: Failed to rename the JSON file."
    exit 1
fi

# Compress the new file
if ! pigz -f "$new_file"; then
    #echo "Error: Failed to compress the file."
    exit 1
fi

#echo "Successfully downloaded and processed price_guide_1.json"
