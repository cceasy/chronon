#!/bin/bash

# Script to compile Thrift definitions into Python code
echo "Compiling Thrift definitions..."

# Set the base directory
BASE_DIR=$(pwd)
THRIFT_DIR="${BASE_DIR}/api/thrift"
OUTPUT_DIR="${BASE_DIR}/api/python"

# Create the output directory structure if it doesn't exist
mkdir -p "${OUTPUT_DIR}/ai/chronon/api"

# Compile each Thrift file
for file in "${THRIFT_DIR}"/*.thrift; do
    filename=$(basename "$file")
    echo "Compiling $filename..."
    
    # Run thrift compiler with Python target
    thrift --gen py -out "${OUTPUT_DIR}" "${file}"
    
    # Check if compilation was successful
    if [ $? -eq 0 ]; then
        echo "Successfully compiled $filename"
    else
        echo "Failed to compile $filename"
        exit 1
    fi
done

echo "Thrift compilation complete!"
echo "Generated Python modules in ${OUTPUT_DIR}/gen-py/"
echo "You may need to move these files to the correct location in the Python package structure."
