#!/bin/bash

local_run=$1
run_date=$2
unloading_url=$3
processing_s3_files_at_run=$4
is_overwriting=$5

if [[ -n $local_run ]]; then
    echo "passed local_run. Installing requirements..."
    python3 -m pip install virtualenv
    python3 -m virtualenv ./venv_craft
    ./venv_craft/bin/activate
    python3 -m pip install -r requirements.txt
fi

if [ -z $run_date ]; then
    echo "passed run_date is empty"
    run_date=$(date +%Y%m%d)
fi

echo "local_run: ${local_run}"
echo "run_date: ${run_date}"
echo "unloading_url: ${unloading_url}"
echo "processing_s3_files_at_run: ${processing_s3_files_at_run}"
echo "is_overwriting: ${is_overwriting}"

python3 run.py --run_date "${run_date}" --unloading_url "${unloading_url}" --processing_s3_files_at_run "${processing_s3_files_at_run}" --is_overwriting "${is_overwriting}"
