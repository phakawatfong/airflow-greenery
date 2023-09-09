#!/bin/sh

table_name_param=$1

check_file_num=$(ls -lrt /opt/airflow/data/* | wc -l)

echo "number of files : ${check_file_num}"


if [[ ${check_file_num} == 7 ]]
then
    echo "got all the files ---> proceed to next dag"
else
    echo "job failed"
    exit 1
fi