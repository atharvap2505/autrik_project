#!/bin/bash

set -e

echo "=== ETL Pipeline Started ==="

echo "--- Extract Step ---"
bash extract.sh
if [ $? -ne 0 ]; then
    echo "Extract step failed!"
    exit 1
fi

echo "--- Transform Step ---"
python3 transform.py
if [ $? -ne 0 ]; then
    echo "Transform step failed!"
    exit 1
fi

echo "--- Load Step ---"
python3 load.py
if [ $? -ne 0 ]; then
    echo "Load step failed!"
    exit 1
fi

echo "=== ETL Pipeline Completed ==="